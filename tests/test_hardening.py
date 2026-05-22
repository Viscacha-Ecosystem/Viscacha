"""
Tests for the 4 hardening abstractions:
  1. Lease semantics (inp_lease / confirm / release / expiry)
  2. CAS with versioned tuples
  3. Hard / soft durability separation
  4. Failure tuples + idempotent out
"""

import time
import pytest
from threading import Thread

from viscacha.tuplespace import TupleSpace, make_tuple, make_failure_tuple, CASConflictError, LeaseExpiredError
from viscacha.tuplespace.replay import replay


# ── 1. Lease semantics ────────────────────────────────────────────────────────

def test_lease_hides_tuple_from_other_readers():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    _, lease_id = ts.inp_lease({"type": "task"})
    # tuple is soft-removed — other readers cannot see it
    assert ts.rd({"type": "task"}, timeout=0) is None


def test_lease_confirm_removes_permanently():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    _, lease_id = ts.inp_lease({"type": "task"})
    ts.confirm_lease(lease_id)
    assert ts.rd({"type": "task"}, timeout=0) is None
    # confirm again → raises
    with pytest.raises(LeaseExpiredError):
        ts.confirm_lease(lease_id)


def test_lease_release_returns_tuple():
    ts = TupleSpace()
    t = make_tuple("task")
    ts.out(t)
    _, lease_id = ts.inp_lease({"type": "task"})
    ts.release_lease(lease_id)
    got = ts.rd({"type": "task"}, timeout=0)
    assert got is not None and got["id"] == t["id"]


def test_lease_expiry_returns_tuple_to_space():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    _, lease_id = ts.inp_lease({"type": "task"}, lease_ttl=0.3)
    assert ts.rd({"type": "task"}, timeout=0) is None  # hidden while leased
    time.sleep(1.5)  # wait for sweeper to recover it
    assert ts.rd({"type": "task"}, timeout=0) is not None


def test_lease_no_duplicate_claim():
    """Two threads race for the same tuple; only one gets the lease."""
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    winners = []

    def try_lease():
        result = ts.inp_lease({"type": "task"}, timeout=0)
        if result:
            winners.append(result[1])

    threads = [Thread(target=try_lease) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len(winners) == 1


def test_lease_log_contains_acquire_and_confirm():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    _, lease_id = ts.inp_lease({"type": "task"})
    ts.confirm_lease(lease_id)
    ops = [e["op"] for e in ts.log_entries()]
    assert "lease_acquire" in ops
    assert "lease_confirm" in ops


# ── 2. CAS with versioned tuples ──────────────────────────────────────────────

def test_cas_succeeds_on_correct_version():
    ts = TupleSpace()
    t = make_tuple("state", {"val": 0})
    ts.out(t)
    updated = ts.cas(t["id"], expected_version=0,
                     new_tuple=make_tuple("state", {"val": 1}))
    assert updated["payload"]["val"] == 1
    assert updated["version"] == 1


def test_cas_fails_on_wrong_version():
    ts = TupleSpace()
    t = make_tuple("state", {"val": 0})
    ts.out(t)
    # CAS replaces the tuple — new ID, version 1
    updated = ts.cas(t["id"], 0, make_tuple("state", {"val": 1}))
    # trying version 0 again on the new tuple must fail
    with pytest.raises(CASConflictError):
        ts.cas(updated["id"], 0, make_tuple("state", {"val": 99}))


def test_cas_atomic_under_contention():
    """Many threads race to increment a counter via CAS; final value must be exact."""
    ts = TupleSpace()
    ts.out(make_tuple("counter", {"n": 0}))
    committed = []

    def increment():
        for _ in range(20):
            try:
                current = ts.rd({"type": "counter"}, timeout=1.0)
                if current is None:
                    continue
                new_val = current["payload"]["n"] + 1
                ts.cas(
                    current["id"],
                    current["version"],
                    make_tuple("counter", {"n": new_val}),
                )
                committed.append(new_val)
                break
            except (CASConflictError, KeyError):
                time.sleep(0.01)  # retry on conflict

    threads = [Thread(target=increment) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    final = ts.rd({"type": "counter"})
    assert final["payload"]["n"] == len(committed)


def test_cas_logged():
    ts = TupleSpace()
    t = make_tuple("state", {"v": 0})
    ts.out(t)
    ts.cas(t["id"], 0, make_tuple("state", {"v": 1}))
    ops = [e["op"] for e in ts.log_entries()]
    assert "cas_old" in ops
    assert "cas_new" in ops


# ── 3. Hard / soft durability ─────────────────────────────────────────────────

def test_hard_tuple_in_log():
    ts = TupleSpace()
    ts.out(make_tuple("result", {"x": 1}, durability="hard"))
    ts.out(make_tuple("pheromone", {"edge": "A-B"}, durability="soft"))
    hard_log = ts.log_entries(durability="hard")
    types_logged = {e["tuple"]["type"] for e in hard_log if e.get("tuple")}
    assert "result" in types_logged
    assert "pheromone" not in types_logged


def test_replay_hard_only_excludes_soft():
    ts = TupleSpace()
    h = make_tuple("result",   {"x": 1}, durability="hard")
    s = make_tuple("pheromone", {"e": "A-B"}, durability="soft")
    ts.out(h)
    ts.out(s)
    state = replay(ts.log_entries(), durability="hard")
    ids = {t["id"] for t in state}
    assert h["id"] in ids
    assert s["id"] not in ids


def test_replay_default_includes_soft():
    ts = TupleSpace()
    s = make_tuple("pheromone", durability="soft")
    ts.out(s)
    state = replay(ts.log_entries())
    assert any(t["id"] == s["id"] for t in state)


# ── 4. Failure tuples + idempotent out ────────────────────────────────────────

def test_make_failure_tuple_structure():
    f = make_failure_tuple("orig-id", "inp", "agent_crash", "ant-3")
    assert f["type"] == "failure"
    assert f["payload"]["retryable"] is True
    assert f["payload"]["retry_count"] == 0


def test_failure_tuple_not_retryable_at_max():
    f = make_failure_tuple("x", "out", "timeout", "ant-1", retry_count=3, max_retries=3)
    assert f["payload"]["retryable"] is False


def test_idempotent_out_deduplicates():
    ts = TupleSpace()
    t = make_tuple("task")
    first  = ts.out(t, idempotency_key="job-42")
    second = ts.out(t, idempotency_key="job-42")
    assert first  is True
    assert second is False
    assert len(ts.query({"type": "task"})) == 1


def test_idempotent_out_different_keys_both_write():
    ts = TupleSpace()
    ts.out(make_tuple("task"), idempotency_key="k1")
    ts.out(make_tuple("task"), idempotency_key="k2")
    assert len(ts.query({"type": "task"})) == 2


def test_failure_tuple_visible_in_space():
    ts = TupleSpace()
    f = make_failure_tuple("orig", "inp", "crash", "ant-0")
    ts.out(f)
    got = ts.rd({"type": "failure"}, timeout=0)
    assert got is not None
    assert got["payload"]["agent"] == "ant-0"
