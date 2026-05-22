"""
Phase 2 tests — Observability & Reliability

Module 2.1  Persistent Event Log
Module 2.2  Time-Travel Interface
Module 2.4  Inspector Interface  (2.3 Leases tested in test_hardening.py)
"""

import json
import time
import tempfile
from pathlib import Path

import pytest

from viscacha.tuplespace import TupleSpace, make_tuple, state_at, step_through, diff, Inspector
from viscacha.tuplespace.replay import replay


# ── 2.1  Persistent Event Log ─────────────────────────────────────────────────

def test_persistent_log_writes_to_file(tmp_path):
    log_file = tmp_path / "events.jsonl"
    ts = TupleSpace(log_path=log_file)
    ts.out(make_tuple("task", {"x": 1}))
    ts.out(make_tuple("task", {"x": 2}))

    assert log_file.exists()
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0])["op"] == "out"


def test_persistent_log_survives_restart(tmp_path):
    log_file = tmp_path / "events.jsonl"

    t1 = make_tuple("task", {"run": 1})
    t2 = make_tuple("result", {"run": 1})

    ts = TupleSpace(log_path=log_file)
    ts.out(t1)
    ts.out(t2)
    ts.inp({"type": "task"})
    # "crash" — discard the TupleSpace object

    # restart: new instance loads from file
    ts2 = TupleSpace(log_path=log_file)
    entries = ts2.log_entries()

    ops = [e["op"] for e in entries]
    assert "out" in ops
    assert "in"  in ops
    # can replay the log and get correct final state
    state = replay(entries)
    assert any(t["id"] == t2["id"] for t in state)
    assert all(t["id"] != t1["id"] for t in state)


def test_persistent_log_crash_safe_partial_line(tmp_path):
    """A truncated last line (simulated crash mid-write) must not break load."""
    log_file = tmp_path / "events.jsonl"
    ts = TupleSpace(log_path=log_file)
    ts.out(make_tuple("task"))

    # simulate crash: append a partial line
    with log_file.open("a") as f:
        f.write('{"event_id": "partial')   # no closing brace or newline

    ts2 = TupleSpace(log_path=log_file)   # must not raise
    assert len(ts2.log_entries()) == 1     # partial line skipped


def test_persistent_log_append_only(tmp_path):
    """Second TupleSpace on same file appends, not overwrites."""
    log_file = tmp_path / "events.jsonl"
    ts1 = TupleSpace(log_path=log_file)
    ts1.out(make_tuple("task"))

    ts2 = TupleSpace(log_path=log_file)
    ts2.out(make_tuple("result"))

    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    # ts1 wrote 1 line; ts2 loaded it silently then appended 1 new line = 2 total
    assert len(lines) == 2
    types = [json.loads(l)["tuple"]["type"] for l in lines]
    assert "task"   in types
    assert "result" in types


# ── 2.2  Time-Travel Interface ────────────────────────────────────────────────

def test_state_at_returns_correct_snapshot():
    ts = TupleSpace()
    t1 = make_tuple("task", {"n": 1})
    ts.out(t1)
    checkpoint = time.time()
    time.sleep(0.05)
    t2 = make_tuple("task", {"n": 2})
    ts.out(t2)

    snap = state_at(ts.log_entries(), checkpoint)
    ids = {t["id"] for t in snap}
    assert t1["id"] in ids
    assert t2["id"] not in ids


def test_state_at_after_all_events_matches_live():
    ts = TupleSpace()
    for i in range(5):
        ts.out(make_tuple("task", {"i": i}))
    ts.inp({"type": "task"})

    future = time.time() + 1
    snap = state_at(ts.log_entries(), future)
    live = ts.query({"type": "task"})
    assert {t["id"] for t in snap} == {t["id"] for t in live}


def test_step_through_yields_monotonic_state():
    ts = TupleSpace()
    ts.out(make_tuple("a"))
    ts.out(make_tuple("b"))
    ts.inp({"type": "a"})

    states = [list(s) for _, s in step_through(ts.log_entries())]
    sizes  = [len(s) for s in states]
    # sizes should be 1, 2, 1 (add a, add b, remove a)
    assert sizes == [1, 2, 1]


def test_step_through_event_and_state_aligned():
    ts = TupleSpace()
    t  = make_tuple("task")
    ts.out(t)

    for event, state in step_through(ts.log_entries()):
        if event["op"] == "out":
            assert any(s["id"] == t["id"] for s in state)


def test_diff_identifies_added_and_removed():
    ts = TupleSpace()
    t1 = make_tuple("task")
    ts.out(t1)
    snap1 = time.time()
    time.sleep(0.05)

    t2 = make_tuple("result")
    ts.out(t2)
    ts.inp({"type": "task"})
    snap2 = time.time()

    d = diff(ts.log_entries(), snap1, snap2)
    added_ids   = {t["id"] for t in d["added"]}
    removed_ids = {t["id"] for t in d["removed"]}
    assert t2["id"] in added_ids
    assert t1["id"] in removed_ids


def test_diff_empty_when_no_change():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    t = time.time()
    d = diff(ts.log_entries(), t, t)
    assert d["added"]   == []
    assert d["removed"] == []


# ── 2.4  Inspector Interface ──────────────────────────────────────────────────

def test_inspector_snapshot_contains_all_types():
    ts = TupleSpace()
    ts.out(make_tuple("task",   {"x": 1}))
    ts.out(make_tuple("result", {"x": 2}))
    insp = Inspector(ts)
    snap = insp.snapshot()
    assert "task"   in snap
    assert "result" in snap


def test_inspector_snapshot_type_filter():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    ts.out(make_tuple("result"))
    insp = Inspector(ts)
    snap = insp.snapshot(type_filter="task")
    assert "task"   in snap
    assert "result" not in snap


def test_inspector_tail_returns_recent_events():
    ts = TupleSpace()
    for i in range(10):
        ts.out(make_tuple("task", {"i": i}))
    insp  = Inspector(ts)
    tail  = insp.tail(3)
    lines = [l for l in tail.splitlines() if l.strip() and "===" not in l]
    assert len(lines) == 3


def test_inspector_filter_tuples():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    ts.out(make_tuple("task"))
    ts.out(make_tuple("result"))
    insp = Inspector(ts)
    assert len(insp.filter_tuples("task"))   == 2
    assert len(insp.filter_tuples("result")) == 1


def test_inspector_stats():
    ts = TupleSpace()
    ts.out(make_tuple("task"))
    ts.out(make_tuple("task"))
    ts.inp({"type": "task"})
    insp  = Inspector(ts)
    stats = insp.stats()
    assert stats["live_tuples"]   == 1
    assert stats["total_events"]  == 3
    assert stats["op_counts"]["out"] == 2
    assert stats["op_counts"]["in"]  == 1


def test_inspector_standalone_file(tmp_path):
    """inspector._tail_file should not raise on a valid jsonl log."""
    log_file = tmp_path / "events.jsonl"
    ts = TupleSpace(log_path=log_file)
    ts.out(make_tuple("task", {"x": 1}))
    ts.out(make_tuple("result", {"y": 2}))
    ts.inp({"type": "task"})

    from viscacha.tuplespace.inspector import _tail_file
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _tail_file(log_file, n=10)
    out = buf.getvalue()
    assert "result" in out
    assert "Final state" in out
