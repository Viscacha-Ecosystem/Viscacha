"""
CAS storm: 20+ agents all racing to update the same key.

Invariants that must hold:
  - Exactly one winner per version (no two agents advance the same version)
  - No lost updates (every committed increment is accounted for)
  - No livelock (every agent finishes within bounded retries)

If all three pass, the CAS semantics are correct under real contention.
"""

import random
import time
from collections import Counter
from threading import Thread

import pytest

from viscacha.tuplespace import TupleSpace, make_tuple, make_failure_tuple, CASConflictError

N_AGENTS    = 25
MAX_RETRIES = 150


def cas_agent(
    ts: TupleSpace,
    agent_id: str,
    committed: list,
    retries: list,
    livelocked: list,
) -> None:
    backoff = 0.002  # start at 2ms
    current = None

    for attempt in range(MAX_RETRIES):
        # Always re-read: after a successful CAS the old ID is gone (new tuple, new ID)
        current = ts.rd({"type": "counter"}, timeout=2.0)
        if current is None:
            continue

        # jitter between read and CAS to maximise races
        time.sleep(random.uniform(0, 0.015))

        try:
            updated = ts.cas(
                current["id"],
                current["version"],
                make_tuple("counter", {"n": current["payload"]["n"] + 1}),
            )
            committed.append({
                "agent":   agent_id,
                "value":   updated["payload"]["n"],
                "version": updated["version"],
                "attempt": attempt,
            })
            retries.append(attempt)
            return

        except (CASConflictError, KeyError):
            # CASConflictError: wrong version on this ID
            # KeyError: another agent already replaced this tuple (new ID exists)
            # Both mean "retry with the current tuple"
            time.sleep(random.uniform(0, backoff))
            backoff = min(backoff * 2, 0.1)

    # exhausted retries — this is livelock
    livelocked.append(agent_id)
    ts.out(make_failure_tuple(
        original_id="counter",
        op="cas",
        reason="livelock",
        agent=agent_id,
        retry_count=MAX_RETRIES,
        max_retries=MAX_RETRIES,
    ))


def test_cas_storm():
    ts = TupleSpace()
    ts.out(make_tuple("counter", {"n": 0}))

    committed: list[dict] = []
    retries:   list[int]  = []
    livelocked: list[str] = []

    threads = [
        Thread(
            target=cas_agent,
            args=(ts, f"agent-{i}", committed, retries, livelocked),
            name=f"agent-{i}",
        )
        for i in range(N_AGENTS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # ── invariant 1: no livelock ──────────────────────────────────────────
    assert livelocked == [], f"Livelock detected in: {livelocked}"

    # ── invariant 2: no lost updates ─────────────────────────────────────
    assert len(committed) == N_AGENTS, (
        f"Expected {N_AGENTS} commits, got {len(committed)}"
    )
    final = ts.rd({"type": "counter"})
    assert final["payload"]["n"] == N_AGENTS, (
        f"Counter is {final['payload']['n']}, expected {N_AGENTS}"
    )

    # ── invariant 3: exactly one winner per version ───────────────────────
    versions = [c["version"] for c in committed]
    version_counts = Counter(versions)
    duplicates = {v: cnt for v, cnt in version_counts.items() if cnt > 1}
    assert not duplicates, f"Version written more than once: {duplicates}"
    assert sorted(versions) == list(range(1, N_AGENTS + 1)), (
        f"Versions not contiguous: {sorted(versions)}"
    )

    # ── log consistency ───────────────────────────────────────────────────
    cas_new_events = [e for e in ts.log_entries() if e["op"] == "cas_new"]
    assert len(cas_new_events) == N_AGENTS, (
        f"Expected {N_AGENTS} cas_new log entries, got {len(cas_new_events)}"
    )
    logged_versions = sorted(e["tuple"]["version"] for e in cas_new_events)
    assert logged_versions == list(range(1, N_AGENTS + 1))

    # ── print contention stats ────────────────────────────────────────────
    print(f"\n-- CAS storm results ({N_AGENTS} agents) --")
    print(f"  commits:    {len(committed)}")
    print(f"  livelocked: {len(livelocked)}")
    print(f"  retries — min={min(retries)}  max={max(retries)}  "
          f"avg={sum(retries)/len(retries):.1f}")
    by_agent = sorted(committed, key=lambda c: c["version"])
    for c in by_agent:
        bar = "#" * (c["attempt"] + 1)
        print(f"  v{c['version']:02d}  {c['agent']:10s}  attempt={c['attempt']}  {bar}")


if __name__ == "__main__":
    test_cas_storm()
