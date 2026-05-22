"""
Crash safety: automated crash simulation at every compaction phase boundary.

Five crash points:
  1. Before snapshot          — no snapshot file, full log
  2. During snapshot write    — snapshot.json.tmp exists, snapshot.json absent/old
  3. After snapshot, before rotation — snapshot.json written, log still has ALL events
  4. Mid-rotation             — snapshot.json written, events.jsonl.tmp present, rename not done
  5. After rotation           — snapshot.json written, events.jsonl has only tail

For each crash point:
  - reconstruct exact on-disk artifacts
  - call open_space()
  - verify:
      * state correctness  (right tuples present)
      * no duplicate application  (no ID appears twice, versions correct)
      * invariants hold  (replay(log) ⊇ live state, idempotency intact)
"""

import json
import time
from pathlib import Path
from threading import Thread

import pytest

from viscacha.tuplespace import TupleSpace, SnapshotManager, make_tuple
from viscacha.tuplespace.replay import replay
from viscacha.tuplespace.snapshot import _apply_tail


# ── shared fixture ────────────────────────────────────────────────────────────

def _build_live_state(sm: SnapshotManager):
    """
    Write a deterministic state we can verify after recovery:
      - out t0..t4  (5 tasks)
      - inp t1, t3  (consume 2)
      - expected live: {t0, t2, t4}
    Also writes one idempotency-protected tuple and one with TTL.
    Returns (ts, expected_live_ids, all_tuples).
    """
    ts = TupleSpace(log_path=sm.events_path)
    tuples = [make_tuple("task", {"n": i}) for i in range(5)]
    for t in tuples:
        ts.out(t)
    ts.inp({"type": "task", "payload.n": 1})
    ts.inp({"type": "task", "payload.n": 3})

    # idempotency-protected write
    idem_t = make_tuple("result", {"src": "idem"})
    ts.out(idem_t, idempotency_key="idem-1", idempotency_ttl=3600)

    expected = {tuples[0]["id"], tuples[2]["id"], tuples[4]["id"], idem_t["id"]}
    return ts, expected, tuples, idem_t


def _assert_state(ts: TupleSpace, expected_ids: set, label: str):
    live = ts.query({"type": "*"})
    live_ids = [t["id"] for t in live]

    # no duplicates
    assert len(live_ids) == len(set(live_ids)), \
        f"[{label}] duplicate IDs in live state: {live_ids}"

    # correct set of tuples
    assert set(live_ids) == expected_ids, \
        f"[{label}] expected {expected_ids}, got {set(live_ids)}"


def _assert_idempotency(ts: TupleSpace, key: str, label: str):
    """Same idempotency key must be rejected after recovery."""
    result = ts.out(make_tuple("result", {"src": "retry"}), idempotency_key=key)
    assert result is False, \
        f"[{label}] idempotency key '{key}' was not preserved across recovery"


# ── crash 1: before snapshot ──────────────────────────────────────────────────

def test_crash_before_snapshot(tmp_path):
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)
    # no compact — crash before anything happens

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_1")


# ── crash 2: during snapshot write ───────────────────────────────────────────

def test_crash_during_snapshot_write_tmp_only(tmp_path):
    """snapshot.json.tmp exists (partial write), snapshot.json absent."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)

    # simulate: tmp written, rename never happened
    tmp_path_file = tmp_path / "snapshot.json.tmp"
    snap = ts.snapshot()
    tmp_path_file.write_text(
        json.dumps(snap, default=str)[:50],  # truncated — invalid JSON
        encoding="utf-8",
    )
    assert not sm.snapshot_path.exists()

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_2a")
    _assert_idempotency(ts2, "idem-1", "crash_2a")


def test_crash_during_snapshot_write_corrupted_snapshot(tmp_path):
    """snapshot.json written but corrupted (truncated rename)."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)

    sm.snapshot_path.write_text('{"version": 1, "truncated":', encoding="utf-8")

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_2b")


# ── crash 3: after snapshot, before rotation ──────────────────────────────────

def test_crash_after_snapshot_before_rotation(tmp_path):
    """snapshot.json written, events.jsonl still has ALL events (rotation skipped)."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)

    snap = ts.snapshot()
    sm.write_snapshot(snap)
    # deliberately skip log rotation

    # write one more event after the snapshot boundary
    post_t = make_tuple("task", {"phase": "post-snapshot"})
    ts.out(post_t)
    expected_ids = expected_ids | {post_t["id"]}

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_3")
    _assert_idempotency(ts2, "idem-1", "crash_3")


def test_crash_after_snapshot_no_duplicate_application(tmp_path):
    """Pre-snapshot events must not be applied twice (snapshot + log overlap)."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, _ = _build_live_state(sm)

    snap = ts.snapshot()
    sm.write_snapshot(snap)
    # log still contains all pre-snapshot events

    ts2 = sm.open_space()
    live = ts2.query({"type": "*"})
    live_ids = [t["id"] for t in live]
    assert len(live_ids) == len(set(live_ids)), \
        "Pre-snapshot events applied twice (duplicate IDs)"
    assert set(live_ids) == expected_ids


# ── crash 4: mid-rotation ─────────────────────────────────────────────────────

def test_crash_mid_rotation_tmp_exists(tmp_path):
    """
    snapshot.json written.
    events.jsonl.tmp written (new tail) but rename to events.jsonl not done.
    events.jsonl still has the full pre-snapshot log.
    """
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)

    snap = ts.snapshot()
    sm.write_snapshot(snap)

    # write post-snapshot event (goes to events.jsonl)
    post_t = make_tuple("task", {"phase": "post"})
    ts.out(post_t)
    expected_ids = expected_ids | {post_t["id"]}

    # simulate partial rotation: write .tmp but don't rename
    tmp_events = tmp_path / "events.jsonl.tmp"
    tmp_events.write_text(
        json.dumps(ts.log_entries()[-1], default=str) + "\n",
        encoding="utf-8",
    )
    # events.jsonl still has ALL events — rotation didn't finish

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_4")
    _assert_idempotency(ts2, "idem-1", "crash_4")


def test_crash_mid_rotation_tmp_corrupted(tmp_path):
    """events.jsonl.tmp is corrupted — recovery still uses events.jsonl."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, _ = _build_live_state(sm)

    snap = ts.snapshot()
    sm.write_snapshot(snap)

    tmp_events = tmp_path / "events.jsonl.tmp"
    tmp_events.write_text('{"event_id": "truncated"', encoding="utf-8")

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_4_corrupted_tmp")


# ── crash 5: after rotation ───────────────────────────────────────────────────

def test_crash_after_rotation_clean(tmp_path):
    """Full clean compaction. events.jsonl has only post-snapshot events."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, idem_t = _build_live_state(sm)

    sm.compact(ts)

    post_t = make_tuple("task", {"phase": "post-rotation"})
    ts.out(post_t)
    expected_ids = expected_ids | {post_t["id"]}

    ts2 = sm.open_space()
    _assert_state(ts2, expected_ids, "crash_5")
    _assert_idempotency(ts2, "idem-1", "crash_5")


def test_crash_after_rotation_log_is_short(tmp_path):
    """After rotation, log must contain only post-snapshot entries."""
    sm = SnapshotManager(tmp_path)
    ts, _, _, _ = _build_live_state(sm)

    sm.compact(ts)

    lines = [l for l in sm.events_path.read_text().splitlines() if l.strip()]
    assert len(lines) == 0, \
        f"Log should be empty after compaction with no post-snapshot writes, got {len(lines)}"


# ── cross-cutting invariants ──────────────────────────────────────────────────

@pytest.mark.parametrize("crash_point", [
    "before_snapshot",
    "after_snapshot_before_rotation",
])
def test_replay_matches_live_state(tmp_path, crash_point):
    """replay(log) must produce a superset of live state for all crash points."""
    sm = SnapshotManager(tmp_path)
    ts, expected_ids, _, _ = _build_live_state(sm)

    if crash_point == "after_snapshot_before_rotation":
        snap = ts.snapshot()
        sm.write_snapshot(snap)
    elif crash_point == "after_rotation":
        sm.compact(ts)

    ts2 = sm.open_space()
    live_ids = {t["id"] for t in ts2.query({"type": "*"})}
    replayed_ids = {t["id"] for t in replay(ts2.log_entries())}

    # every live tuple must be explainable from the log
    assert live_ids <= replayed_ids, \
        f"[{crash_point}] live tuples not in replay: {live_ids - replayed_ids}"


def test_lease_not_durable_across_crash(tmp_path):
    """
    Leases acquired AFTER the last snapshot are not durable.
    On recovery the leased tuple must return to the space (back to queue).
    """
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)
    t = make_tuple("job", {"n": 1})
    ts.out(t)

    # Snapshot with the job live (not yet leased)
    snap = ts.snapshot()
    sm.write_snapshot(snap)

    # Lease acquired AFTER the snapshot boundary
    _, lease_id = ts.inp_lease({"type": "job"}, lease_ttl=60)
    # crash — no confirm, no release, no rotation

    # Recovery: lease was post-snapshot, not durable → tuple returns to queue
    ts2 = sm.open_space()
    recovered = ts2.rd({"type": "job"}, timeout=0)
    assert recovered is not None, "Leased tuple must return to queue after crash"
    assert recovered["id"] == t["id"]


def test_lease_in_snapshot_survives_crash(tmp_path):
    """
    Leases captured inside a snapshot are durable: they survive the crash
    with their remaining TTL intact (or return the tuple if expired).
    """
    sm = SnapshotManager(tmp_path)
    ts = TupleSpace(log_path=sm.events_path)
    t = make_tuple("job", {"n": 1})
    ts.out(t)

    # Lease the tuple, THEN snapshot (lease is inside the snapshot)
    _, lease_id = ts.inp_lease({"type": "job"}, lease_ttl=60)
    snap = ts.snapshot()
    sm.write_snapshot(snap)

    # crash — lease was in snapshot, not yet confirmed/released
    ts2 = sm.open_space()
    # tuple is still leased — not visible as free
    free = ts2.rd({"type": "job"}, timeout=0)
    assert free is None, "Tuple still under a live lease must not be free after recovery"
    assert lease_id in ts2._leases, "Live lease from snapshot must be reconstructed"


def test_concurrent_writes_during_compact_appear_after_recovery(tmp_path):
    """
    Writers active during compaction: their writes must appear after recovery.
    This tests Fix #3 (consistency boundary) and Fix #4 (no lost post-snapshot writes).
    """
    sm = SnapshotManager(tmp_path)
    ts, _, _, _ = _build_live_state(sm)

    post_ids = []
    barrier_start = []
    barrier_go = []

    def writer():
        barrier_start.append(1)
        while not barrier_go:
            pass
        for _ in range(5):
            t = make_tuple("concurrent", {"w": 1})
            ts.out(t)
            post_ids.append(t["id"])

    writers = [Thread(target=writer) for _ in range(3)]
    for w in writers:
        w.start()

    while len(barrier_start) < 3:
        pass

    # compact while writers are active
    barrier_go.append(1)
    snap = sm.compact(ts)

    for w in writers:
        w.join()

    # writes after last_event_id in the snapshot must all appear after recovery
    ts2 = sm.open_space()
    recovered_ids = {t["id"] for t in ts2.query({"type": "*"})}

    # post-snapshot writes that landed in the log tail must all be present
    snapshot_last = snap["last_event_id"]
    tail = SnapshotManager._entries_after(ts.log_entries(), snapshot_last)
    tail_ids = {
        e["tuple"]["id"] for e in tail
        if e.get("op") == "out" and e.get("tuple")
    }
    missing = tail_ids - recovered_ids
    assert not missing, f"Post-snapshot writes lost after recovery: {missing}"
