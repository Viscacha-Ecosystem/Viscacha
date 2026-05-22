import time
import pytest
from viscacha import Client, Worker


def _start(client, worker, poll_interval=0.1):
    worker.run(blocking=False, poll_interval=poll_interval)


# ── basic processing ──────────────────────────────────────────────────────────

def test_enqueue_and_process(tmp_path):
    client = Client(url="http://127.0.0.1:8000")
    worker = Worker(client)

    @worker.job("echo")
    def echo(msg):
        return {"msg": msg}

    _start(client, worker)

    handle = client.enqueue("echo", msg="hello")
    result = handle.wait(timeout=5)

    assert result.status == "done"
    assert result.result["msg"] == "hello"


def test_multiple_job_types(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("add")
    def add(a, b):
        return a + b

    @worker.job("upper")
    def upper(text):
        return text.upper()

    _start(client, worker)

    j1 = client.enqueue("add", a=2, b=3)
    j2 = client.enqueue("upper", text="hello")

    assert j1.wait(timeout=5).result == 5
    assert j2.wait(timeout=5).result == "HELLO"


def test_concurrent_jobs(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("square")
    def square(n):
        return n * n

    _start(client, worker)

    handles = [client.enqueue("square", n=i) for i in range(10)]
    results = [h.wait(timeout=10) for h in handles]

    assert all(r is not None for r in results)
    values = {r.args["n"]: r.result for r in results}
    for n in range(10):
        assert values[n] == n * n


# ── retries ───────────────────────────────────────────────────────────────────

def test_retry_on_exception(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)
    attempts = [0]

    @worker.job("flaky", max_retries=3, lease_ttl=1.0)
    def flaky():
        attempts[0] += 1
        if attempts[0] < 3:
            raise ValueError("not ready")
        return {"done": True}

    _start(client, worker)

    handle = client.enqueue("flaky")
    result = handle.wait(timeout=10)

    assert result.status == "done"
    assert attempts[0] == 3


def test_permanent_failure_after_max_retries(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("broken", max_retries=2, lease_ttl=1.0)
    def broken():
        raise RuntimeError("always fails")

    _start(client, worker)

    handle = client.enqueue("broken")
    result = handle.wait(timeout=10)

    assert result.status == "failed"
    assert "always fails" in result.error
    assert result.retries == 2


# ── observability ─────────────────────────────────────────────────────────────

def test_jobs_listing(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("noop")
    def noop():
        return None

    _start(client, worker)

    handles = [client.enqueue("noop") for _ in range(3)]
    for h in handles:
        h.wait(timeout=5)

    done = client.jobs(status="done")
    assert len(done) == 3
    assert all(j.status == "done" for j in done)


def test_get_job_by_id(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("ping")
    def ping():
        return "pong"

    _start(client, worker)

    handle = client.enqueue("ping")
    handle.wait(timeout=5)

    job = client.get(handle.id)
    assert job is not None
    assert job.id == handle.id
    assert job.result == "pong"


def test_job_status_before_completion():
    client = Client()  # in-memory, no worker running

    handle = client.enqueue("slow")
    job = handle.status()

    assert job is not None
    assert job.status == "pending"
    assert job.job_type == "slow"


def test_wait_raises_on_timeout():
    client = Client()  # no worker

    handle = client.enqueue("never_runs")
    with pytest.raises(TimeoutError):
        handle.wait(timeout=0.1, poll_interval=0.05)


# ── cancel ────────────────────────────────────────────────────────────────────

def test_cancel_pending_job():
    client = Client()  # no worker — job stays pending

    handle = client.enqueue("slow_job")
    assert handle.status().status == "pending"

    cancelled = handle.cancel()
    assert cancelled is True

    job = client.get(handle.id)
    assert job is not None
    assert job.status == "cancelled"


def test_cancel_returns_false_if_already_done(tmp_path):
    client = Client(log_path=tmp_path / "jobs.jsonl")
    worker = Worker(client)

    @worker.job("fast")
    def fast():
        return "ok"

    worker.run(blocking=False, poll_interval=0.1)

    handle = client.enqueue("fast")
    handle.wait(timeout=5)

    assert handle.cancel() is False  # already done, nothing to cancel


def test_cancel_not_in_jobs_listing_unless_filtered():
    client = Client()

    h = client.enqueue("thing")
    h.cancel()

    assert len(client.jobs(status="pending")) == 0
    assert len(client.jobs(status="cancelled")) == 1
    assert len(client.jobs()) == 1  # all statuses includes cancelled


# ── crash safety via lease TTL ────────────────────────────────────────────────

def test_job_returns_to_queue_if_lease_expires(tmp_path):
    """
    Simulate a worker crash: claim the job but never confirm.
    After lease_ttl the job must be available again.
    """
    client = Client(log_path=tmp_path / "jobs.jsonl")

    client.enqueue("task")

    # claim without completing — simulates a crash
    claimed = client._claim("task", lease_ttl=0.3)
    assert claimed is not None

    # job is invisible (leased)
    assert client.jobs(status="pending") == []

    # wait for lease to expire (sweeper runs every 1s, TTL is 0.3s)
    time.sleep(2.0)

    # job is back
    pending = client.jobs(status="pending")
    assert len(pending) == 1
