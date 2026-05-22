import time
import pytest
from threading import Thread
from viscacha.tuplespace import TupleSpace, make_tuple


def test_out_and_rd():
    ts = TupleSpace()
    t = make_tuple("task", {"x": 1})
    ts.out(t)
    got = ts.rd({"type": "task"})
    assert got["id"] == t["id"]
    # rd is non-destructive
    got2 = ts.rd({"type": "task"}, timeout=0)
    assert got2 is not None


def test_inp_removes_tuple():
    ts = TupleSpace()
    t = make_tuple("task")
    ts.out(t)
    taken = ts.inp({"type": "task"})
    assert taken["id"] == t["id"]
    assert ts.rd({"type": "task"}, timeout=0) is None


def test_inp_atomic_no_duplicate():
    ts = TupleSpace()
    ts.out(make_tuple("task", {"v": 1}))

    results = []

    def taker():
        got = ts.inp({"type": "task"}, timeout=1.0)
        if got:
            results.append(got)

    threads = [Thread(target=taker) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len(results) == 1


def test_rd_blocks_then_returns():
    ts = TupleSpace()
    result = []

    def reader():
        result.append(ts.rd({"type": "task"}, timeout=2.0))

    th = Thread(target=reader)
    th.start()
    time.sleep(0.05)
    ts.out(make_tuple("task"))
    th.join()
    assert result[0] is not None


def test_rd_timeout_returns_none():
    ts = TupleSpace()
    assert ts.rd({"type": "ghost"}, timeout=0.05) is None


def test_query_returns_all_matches():
    ts = TupleSpace()
    for i in range(5):
        ts.out(make_tuple("task", {"i": i}))
    ts.out(make_tuple("result", {"i": 0}))
    results = ts.query({"type": "task"})
    assert len(results) == 5


def test_query_limit():
    ts = TupleSpace()
    for i in range(5):
        ts.out(make_tuple("task", {"i": i}))
    assert len(ts.query({"type": "task"}, limit=3)) == 3


def test_ttl_expiry():
    ts = TupleSpace()
    ts.out(make_tuple("task", ttl=0.1))
    # wait for lazy expiry to kick in (rd skips expired) AND sweeper to log it (1s cycle)
    time.sleep(1.5)
    assert ts.rd({"type": "task"}, timeout=0) is None
    expired = [e for e in ts.log_entries() if e["op"] == "expire"]
    assert len(expired) == 1


def test_log_records_out_and_in():
    ts = TupleSpace()
    t = make_tuple("task")
    ts.out(t)
    ts.inp({"type": "task"})
    ops = [e["op"] for e in ts.log_entries()]
    assert "out" in ops
    assert "in" in ops


def test_replay_reconstructs_state():
    from viscacha.tuplespace.replay import replay
    ts = TupleSpace()
    t1 = make_tuple("task", {"v": 1})
    t2 = make_tuple("task", {"v": 2})
    ts.out(t1)
    ts.out(t2)
    ts.inp({"type": "task", "payload.v": 1})
    state = replay(ts.log_entries())
    ids = {t["id"] for t in state}
    assert t2["id"] in ids
    assert t1["id"] not in ids
