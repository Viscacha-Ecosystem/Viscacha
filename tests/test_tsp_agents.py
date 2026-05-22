"""
NP-complete test: Travelling Salesman Problem via ant-colony stigmergy.

5 Claude agents explore tours across 8 cities. Each agent:
  1. Reads the current pheromone map from the space (query)
  2. Asks Claude to construct a tour guided by high-pheromone edges
  3. If the tour is an improvement, atomically swaps the best_tour tuple
  4. Deposits pheromones on the edges it used (strength ∝ 1/tour_length)

Pheromone tuples carry a TTL — they evaporate automatically.
No orchestrator. No agent knows about any other agent.
"""

import json
import math
import os
import random
import re
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

# ── problem ───────────────────────────────────────────────────────────────────

CITIES: dict[str, tuple[float, float]] = {
    "A": (0, 0),
    "B": (1, 5),
    "C": (4, 3),
    "D": (6, 1),
    "E": (3, 7),
    "F": (7, 6),
    "G": (5, 9),
    "H": (2, 2),
}
CITY_NAMES  = list(CITIES.keys())
PHEROMONE_TTL = 40.0   # seconds before a pheromone evaporates
ITERATIONS    = 4      # tours each agent attempts
N_AGENTS      = 5


def tour_length(tour: list[str]) -> float:
    total = 0.0
    n = len(tour)
    for i in range(n):
        a, b = CITIES[tour[i]], CITIES[tour[(i + 1) % n]]
        total += math.dist(a, b)
    return total


def edge_key(a: str, b: str) -> str:
    return f"{min(a, b)}-{max(a, b)}"


def random_tour() -> list[str]:
    t = CITY_NAMES[:]
    random.shuffle(t)
    return t


# ── agent ─────────────────────────────────────────────────────────────────────

def ant_agent(ts: TupleSpace, agent_id: str, client, log: list) -> None:
    for iteration in range(ITERATIONS):
        time.sleep(random.uniform(0, 0.2))

        # read pheromone map
        phero_tuples = ts.query({"type": "pheromone"})
        phero_map = {t["payload"]["edge"]: t["payload"]["strength"] for t in phero_tuples}

        # ask Claude to construct a tour biased toward high-pheromone edges
        phero_summary = (
            ", ".join(f"{e}:{s:.1f}" for e, s in sorted(phero_map.items(), key=lambda x: -x[1])[:10])
            if phero_map else "none yet"
        )
        city_list = ", ".join(f"{k}={v}" for k, v in CITIES.items())

        prompt = (
            f"Find a short TSP tour visiting all 8 cities exactly once.\n"
            f"Cities: {city_list}\n"
            f"Prefer high-pheromone edges: {phero_summary}\n"
            f"Call submit_tour with your answer."
        )

        tour_tool = {
            "name": "submit_tour",
            "description": "Submit a TSP tour ordering all cities",
            "input_schema": {
                "type": "object",
                "properties": {
                    "tour": {
                        "type": "array",
                        "items": {"type": "string", "enum": CITY_NAMES},
                        "description": "All 8 city letters in visit order",
                    }
                },
                "required": ["tour"],
            },
        }

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                tools=[tour_tool],
                tool_choice={"type": "any"},
                messages=[{"role": "user", "content": prompt}],
            )
            tool_block = next(b for b in resp.content if b.type == "tool_use")
            tour = tool_block.input["tour"]
        except Exception as e:
            log.append(f"[{agent_id}] API error: {e}")
            continue

        if sorted(tour) != sorted(CITY_NAMES):
            log.append(f"[{agent_id}] iter {iteration}: invalid tour: {tour}")
            continue

        length = tour_length(tour)

        # atomically update best tour if improved
        current = ts.rd({"type": "best_tour"}, timeout=0)
        if current is None or length < current["payload"]["length"]:
            old = ts.inp({"type": "best_tour"}, timeout=0)
            ts.out(make_tuple("best_tour", {
                "tour": tour,
                "length": round(length, 4),
                "by": agent_id,
                "iteration": iteration,
            }))
            prev = f"{current['payload']['length']:.2f}" if current else "none"
            log.append(f"[{agent_id}] iter {iteration}: NEW BEST {length:.2f} (was {prev})  {tour}")
        else:
            log.append(f"[{agent_id}] iter {iteration}: {length:.2f} (best={current['payload']['length']:.2f})")

        # deposit pheromones — better tours leave stronger trails
        deposit = 100.0 / length
        for i in range(len(tour)):
            edge = edge_key(tour[i], tour[(i + 1) % len(tour)])
            existing = ts.inp({"type": "pheromone", "payload.edge": edge}, timeout=0)
            old_strength = existing["payload"]["strength"] if existing else 0.0
            ts.out(make_tuple("pheromone", {
                "edge": edge,
                "strength": round(old_strength + deposit, 4),
            }, ttl=PHEROMONE_TTL))


# ── test ──────────────────────────────────────────────────────────────────────

def test_tsp_agents():
    ts     = TupleSpace()
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    log: list[str] = []

    # seed with a random starting tour so agents have something to beat
    seed_tour   = random_tour()
    seed_length = tour_length(seed_tour)
    ts.out(make_tuple("best_tour", {
        "tour": seed_tour, "length": round(seed_length, 4),
        "by": "seed", "iteration": -1,
    }))
    print(f"\n[seed] initial tour length: {seed_length:.2f}  {seed_tour}")

    threads = [
        Thread(target=ant_agent, args=(ts, f"ant-{i}", client, log), name=f"ant-{i}")
        for i in range(N_AGENTS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)

    print("\n-- Agent log --")
    for line in log:
        print(" ", line)

    # ── assertions ────────────────────────────────────────────────────────────

    best = ts.rd({"type": "best_tour"})
    assert best is not None, "No best_tour in space"

    final_tour   = best["payload"]["tour"]
    final_length = best["payload"]["length"]

    # valid tour: visits all cities exactly once
    assert sorted(final_tour) == sorted(CITY_NAMES), f"Invalid tour: {final_tour}"

    # improved over seed
    assert final_length < seed_length, (
        f"No improvement: final={final_length:.2f} seed={seed_length:.2f}"
    )

    # pheromones deposited (stigmergy happened)
    pheros = ts.query({"type": "pheromone"})
    assert len(pheros) > 0, "No pheromones deposited"

    # replay reconstructs consistent state
    replayed     = replay(ts.log_entries())
    replayed_ids = {t["id"] for t in replayed}
    assert best["id"] in replayed_ids, "Best tour missing from replayed state"

    # dump log
    log_path = Path(__file__).parent / "tsp_log.json"
    log_path.write_text(json.dumps(ts.log_entries(), indent=2, default=str), encoding="utf-8")

    print(f"\n-- Final best tour --")
    print(f"  {' -> '.join(final_tour)} -> {final_tour[0]}")
    print(f"  Length: {final_length:.4f}  (seed was {seed_length:.2f}, improved by {seed_length - final_length:.2f})")
    print(f"  Found by: {best['payload']['by']} on iteration {best['payload']['iteration']}")
    print(f"  Pheromone trails active: {len(pheros)}")
    print(f"  Total space events: {len(ts.log_entries())}")
    print(f"  Full log -> {log_path}")


if __name__ == "__main__":
    test_tsp_agents()
