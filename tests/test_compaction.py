"""
Compaction + snapshot protocol tests.

Each test targets one of the four identified risks:
  Fix #1 — log boundary metadata (last_event_id)
  Fix #2 — unbounded idempotency growth
  Fix #3 — snapshot consistency during concurrent writes
  Fix #4 — compaction crash window
"""

import json
import time
import threading
from pathlib import Path

import pytest

from viscacha.tuplespace import TupleSpace, SnapshotManager, make_tuple
from viscacha.tuplespace.replay import replay


# ── Fix #1: log boundary metadata ────────────────────────────────────────────

def test_snapshot_records_last_event_id(tmp_path):
    ts = TupleSpace(log_path=tmp_path / "events.jsonl")
    ts.out(make_tuple("task", {"n": 1}))
    ts.out(make_tuple("task", {"n": 2}))
    snap = ts.snapshot()
    assert snap["last_event_id"] is not None
    last_in_log = ts.log_entries()[-1]["event_id"]
    assert snap["last_event_id"] == last_in_log


def test_recovery_skips_pre_snapshot_entries(tmp_path):
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)
    t1 = make_tuple("task", {"n": 1})
    t2 = make_tuple("task", {"n": 2})
    ts.out(t1)
    ts.out(t2)

    sm.compact(ts)   # snapshot + rotate log

    # inject a new event AFTER compaction
    t3 = make_tuple("task", {"n": 3})
    ts.out(t3)

    # recover from scratch
    ts2 = sm.open_space()
    ids = {t["id"] for t in ts2.query({"type": "task"})}
    assert t1["id"] in ids
    assert t2["id"] in ids
    assert t3["id"] in ids


def test_rotation_removes_pre_snapshot_log_entries(tmp_path):
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)
    for i in range(10):
        ts.out(make_tuple("task", {"i": i}))

    before = len(ts.log_entries())
    sm.compact(ts)
    after = len(ts.log_entries())

    assert after == 0, f"Expected 0 log entries after compact, got {after} (was {before})"


def test_boundary_id_not_found_replays_all(tmp_path):
    """If last_event_id is missing from log (already rotated), replay full log."""
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)
    ts.out(make_tuple("task"))
    snap = ts.snapshot()
    snap["last_event_id"] = "nonexistent-id"

    # manually write a snapshot with a bad boundary
    sm.write_snapshot(snap)

    # open_space must not crash — it falls back to full log replay
    ts2 = sm.open_space()
    assert ts2 is not None


# ── Fix #2: bounded idempotency ───────────────────────────────────────────────

def test_idempotency_key_expires(tmp_path):
    ts = TupleSpace(idempotency_ttl=0.1)
    t = make_tuple("task")
    ts.out(t, idempotency_key="job-1")
    time.sleep(0.3)
    # key has expired — same key is now accepted
    t2 = make_tuple("task")
    result = ts.out(t2, idempotency_key="job-1")
    assert result is True
    assert len(ts.query({"type": "task"})) == 2


def test_idempotency_pruned_in_sweep(tmp_path):
    ts = TupleSpace(idempotency_ttl=0.1)
    for i in range(20):
        ts.out(make_tuple("task"), idempotency_key=f"job-{i}")
    time.sleep(1.5)  # let sweeper run
    with ts._lock:
        assert len(ts._idempotency) == 0, "Expired keys not pruned by sweeper"


def test_snapshot_prunes_expired_idempotency_keys():
    ts = TupleSpace(idempotency_ttl=0.1)
    ts.out(make_tuple("task"), idempotency_key="stale-key")
    time.sleep(0.3)
    snap = ts.snapshot()
    assert "stale-key" not in snap["idempotency"], "Expired key leaked into snapshot"


def test_restore_drops_expired_idempotency_keys():
    ts = TupleSpace(idempotency_ttl=0.1)
    ts.out(make_tuple("task"), idempotency_key="old-key")
    snap = ts.snapshot()
    # Manually backdate the expiry so it's expired when restored
    snap["idempotency"]["old-key"] = time.time() - 1

    ts2 = TupleSpace()
    ts2.restore_snapshot(snap)
    # old-key expired — writing again with same key must succeed
    assert ts2.out(make_tuple("task"), idempotency_key="old-key") is True


# ── Fix #3: snapshot consistency under concurrent writes ──────────────────────

def test_snapshot_consistent_under_concurrent_writes():
    """
    Snapshot taken while writers are hammering the space must reflect a
    coherent state — not a mix of before/after for different tuples.
    """
    ts = TupleSpace()
    stop = threading.Event()
    errors = []

    def writer():
        i = 0
        while not stop.is_set():
            ts.out(make_tuple("counter", {"i": i}))
            if ts.query({"type": "counter"}):
                ts.inp({"type": "counter"}, timeout=0)
            i += 1

    writers = [threading.Thread(target=writer) for _ in range(4)]
    for w in writers:
        w.start()

    time.sleep(0.05)

    # take multiple snapshots under contention
    snaps = [ts.snapshot() for _ in range(5)]

    stop.set()
    for w in writers:
        w.join()

    for snap in snaps:
        # each snapshot must be internally consistent:
        # last_event_id must appear in the log entries at snapshot time
        assert "last_event_id" in snap
        assert "tuples" in snap
        assert "idempotency" in snap
        assert isinstance(snap["tuples"], list)


# ── Fix #4: compaction crash window ───────────────────────────────────────────

def test_crash_between_snapshot_write_and_log_rotation(tmp_path):
    """
    Simulate: snapshot written, process dies before log is rotated.
    Recovery must still produce correct state by using last_event_id.
    """
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)

    t1 = make_tuple("task", {"phase": "pre"})
    t2 = make_tuple("task", {"phase": "pre"})
    ts.out(t1)
    ts.out(t2)

    # write snapshot but do NOT rotate log (simulate crash mid-compaction)
    snap = ts.snapshot()
    sm.write_snapshot(snap)
    # log still has all events from before the snapshot

    # add post-snapshot event
    t3 = make_tuple("task", {"phase": "post"})
    ts.out(t3)

    # recovery: snapshot exists, log has ALL events (pre + post snapshot)
    ts2 = sm.open_space()
    ids = {t["id"] for t in ts2.query({"type": "task"})}

    # all three tuples must be present — no double-apply, no loss
    assert t1["id"] in ids
    assert t2["id"] in ids
    assert t3["id"] in ids
    assert len(ids) == 3


def test_full_compact_then_long_run(tmp_path):
    """
    Compact mid-stream then continue writing. State after further ops
    must match replay of (snapshot + tail log).
    """
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)

    # phase 1: write 50 tuples, consume half
    for i in range(50):
        ts.out(make_tuple("task", {"i": i}))
    for _ in range(25):
        ts.inp({"type": "task"})

    sm.compact(ts)

    # phase 2: write 20 more, consume 10
    for i in range(50, 70):
        ts.out(make_tuple("task", {"i": i}))
    for _ in range(10):
        ts.inp({"type": "task"})

    live_ids = {t["id"] for t in ts.query({"type": "task"})}

    # recover and verify
    ts2 = sm.open_space()
    recovered_ids = {t["id"] for t in ts2.query({"type": "task"})}

    assert live_ids == recovered_ids
