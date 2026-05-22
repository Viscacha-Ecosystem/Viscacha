"""
Stress test: AI agents under adversarial conditions.

Scenarios exercised:
  - Contention        : 5 agents competing for 3 angles (2 will be stranded)
  - Race conditions   : all agents start simultaneously, no staggering
  - Agent crash       : 1 agent claims an angle then dies; ANY living agent recovers it
  - Duplicate writes  : saboteur tries to inject extra research tuples
  - Stale claims      : crash leaves a claimed-but-never-researched angle
  - Double synthesis  : saboteur races the real synthesizer for the report token
  - Timing jitter     : every agent sleeps a random amount before each operation

Recovery is fully decentralised: every living agent checks for orphaned angles
during its wait loop and re-seeds them. No watchdog process needed.
"""

import json
import os
import random
import sys
import time
from pathlib import Path
from threading import Thread

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic
import pytest

from viscacha.tuplespace import TupleSpace, make_tuple
from viscacha.tuplespace.replay import replay

# ── config ────────────────────────────────────────────────────────────────────

ANGLES = ["classic_2d", "3d_era", "modern_era"]
ANGLE_DESCRIPTIONS = {
    "classic_2d":  "classic 2D Zelda (NES/SNES/GB era)",
    "3d_era":      "early 3D Zelda (N64/GCN era)",
    "modern_era":  "modern Zelda (Wii/Switch era)",
}
TOPIC = "hardest dungeon in The Legend of Zelda"
CLAIM_TTL = 6.0   # stale claim expires after this many seconds
MAX_WAIT  = 60.0  # total per-agent timeout


# ── helpers ───────────────────────────────────────────────────────────────────

def jitter(lo=0.0, hi=0.25):
    time.sleep(random.uniform(lo, hi))


def call_claude(client, prompt: str) -> str:
    jitter(0.05, 0.3)
    r = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return r.content[0].text.strip()


def recover_orphans(ts: TupleSpace, agent_id: str, log: list) -> None:
    """
    Distributed recovery: any agent can call this.
    If an angle has no research, no live claim, and no available token —
    it was orphaned by a crash. Re-seed it so any agent can claim it.
    Multiple agents may race to re-seed; that just produces extra
    angle_available tokens, which is harmless (second claim fails on research
    dedup, or the extra token expires unused).
    """
    researched = {r["payload"]["angle"] for r in ts.query({"type": "research"})}
    claimed    = {t["payload"]["angle"] for t in ts.query({"type": "angle_claimed"})}
    available  = {t["payload"]["angle"] for t in ts.query({"type": "angle_available"})}

    for angle in ANGLES:
        if angle not in researched and angle not in claimed and angle not in available:
            ts.out(make_tuple("angle_available", {"angle": angle}))
            log.append(f"[{agent_id}] recovered orphaned angle: {angle}")


# ── agents ────────────────────────────────────────────────────────────────────

def honest_agent(ts: TupleSpace, agent_id: str, client, log: list) -> None:
    jitter(0, 0.2)

    # attempt to claim an angle
    claimed = None
    for angle in random.sample(ANGLES, len(ANGLES)):
        token = ts.inp({"type": "angle_available", "payload.angle": angle}, timeout=0)
        if token:
            claimed = angle
            ts.out(make_tuple("angle_claimed", {"angle": angle, "agent": agent_id}, ttl=CLAIM_TTL))
            log.append(f"[{agent_id}] claimed {angle}")
            break

    if not claimed:
        log.append(f"[{agent_id}] stranded — polling for recovery work")

    if claimed:
        jitter(0, 0.15)
        findings = call_claude(client,
            f"Zelda expert: what is the single hardest dungeon in {ANGLE_DESCRIPTIONS[claimed]}? "
            f"One sentence, be specific."
        )
        # write research (intentionally twice to stress dedup in synthesizer)
        for _ in range(2):
            ts.out(make_tuple("research", {
                "angle": claimed,
                "findings": findings,
                "agent": agent_id,
            }))
        log.append(f"[{agent_id}] posted research for {claimed}")

    # wait loop — every agent participates in distributed recovery
    deadline = time.monotonic() + MAX_WAIT
    while time.monotonic() < deadline:
        done = {r["payload"]["angle"] for r in ts.query({"type": "research"})}
        if done >= set(ANGLES):
            break
        recover_orphans(ts, agent_id, log)  # <-- decentralised, no special process
        jitter(0.8, 1.5)

    # race for synthesis
    jitter(0, 0.1)
    token = ts.inp({"type": "synthesis_token"}, timeout=0)
    if not token:
        log.append(f"[{agent_id}] synthesis already taken")
        return

    log.append(f"[{agent_id}] synthesizing...")
    all_research = ts.query({"type": "research"})
    seen: dict[str, str] = {}
    for r in all_research:
        seen.setdefault(r["payload"]["angle"], r["payload"]["findings"])

    sections = "\n".join(f"- {a}: {f}" for a, f in seen.items())
    report = call_claude(client,
        f"Cross-validate and summarise: which is the single hardest Zelda dungeon overall?\n{sections}\n"
        f"One paragraph. Start with FINAL REPORT:"
    )
    ts.out(make_tuple("report", {"topic": TOPIC, "report": report, "by": agent_id}))
    log.append(f"[{agent_id}] report written")


def crasher_agent(ts: TupleSpace, agent_id: str, log: list) -> None:
    """Claims an angle then dies — stale claim expires, living agents recover it."""
    jitter(0, 0.05)
    for angle in ANGLES:
        token = ts.inp({"type": "angle_available", "payload.angle": angle}, timeout=0)
        if token:
            ts.out(make_tuple("angle_claimed", {"angle": angle, "agent": agent_id}, ttl=CLAIM_TTL))
            log.append(f"[{agent_id}] claimed {angle} then CRASHED (ttl={CLAIM_TTL}s)")
            return
    log.append(f"[{agent_id}] crasher found nothing to claim")


def saboteur_agent(ts: TupleSpace, agent_id: str, log: list) -> None:
    """Injects fake research and tries to steal the synthesis token early."""
    jitter(0.1, 0.3)
    ts.out(make_tuple("research", {
        "angle": "classic_2d",
        "findings": "SABOTEUR FAKE DATA",
        "agent": agent_id,
    }))
    log.append(f"[{agent_id}] injected duplicate research tuple")

    jitter(0, 0.05)
    stolen = ts.inp({"type": "synthesis_token"}, timeout=0)
    if stolen:
        log.append(f"[{agent_id}] STOLE synthesis token early — returning it")
        ts.out(make_tuple("synthesis_token", {}))
    else:
        log.append(f"[{agent_id}] synthesis token not yet available (expected)")


# ── test ──────────────────────────────────────────────────────────────────────

def test_ai_stress():
    ts = TupleSpace()
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    event_log = []

    for angle in ANGLES:
        ts.out(make_tuple("angle_available", {"angle": angle}))
    ts.out(make_tuple("synthesis_token", {}))

    threads = [
        Thread(target=crasher_agent,  args=(ts, "crasher",      event_log), name="crasher"),
        Thread(target=saboteur_agent, args=(ts, "saboteur",      event_log), name="saboteur"),
        Thread(target=honest_agent,   args=(ts, "researcher-0",  client, event_log), name="r0"),
        Thread(target=honest_agent,   args=(ts, "researcher-1",  client, event_log), name="r1"),
        Thread(target=honest_agent,   args=(ts, "researcher-2",  client, event_log), name="r2"),
        Thread(target=honest_agent,   args=(ts, "researcher-3",  client, event_log), name="r3"),
        Thread(target=honest_agent,   args=(ts, "researcher-4",  client, event_log), name="r4"),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=MAX_WAIT)

    print("\n-- Agent event log --")
    for line in event_log:
        print(" ", line)

    reports    = ts.query({"type": "report"})
    researched = {r["payload"]["angle"] for r in ts.query({"type": "research"})}

    assert len(reports) == 1,            f"Expected 1 report, got {len(reports)}"
    assert reports[0]["payload"]["by"] != "saboteur"
    assert researched == set(ANGLES),    f"Missing research for: {set(ANGLES) - researched}"
    assert ts.rd({"type": "synthesis_token"}, timeout=0) is None, "Token not consumed"

    replayed     = replay(ts.log_entries())
    replayed_ids = {t["id"] for t in replayed}
    report_ids   = {r["id"] for r in reports}
    assert report_ids <= replayed_ids, "Report missing from replayed state"

    log_path = Path(__file__).parent / "stress_test_log.json"
    log_path.write_text(json.dumps(ts.log_entries(), indent=2, default=str), encoding="utf-8")
    print(f"\nFull log -> {log_path}")
    print(f"\nReport:\n{reports[0]['payload']['report']}")
    print(f"\n{len(ts.log_entries())} events. All assertions passed.")


if __name__ == "__main__":
    test_ai_stress()
