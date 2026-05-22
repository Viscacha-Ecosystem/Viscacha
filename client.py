import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from viscacha.tuplespace import TupleSpace, make_tuple


@dataclass
class JobResult:
    """Uniform result shape for any job status."""
    id: str
    status: str          # 'pending' | 'done' | 'failed' | 'cancelled'
    job_type: str
    args: dict = field(default_factory=dict)
    result: Any = None
    error: str | None = None
    retries: int = 0
    enqueued_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None

    @classmethod
    def _from_dict(cls, d: dict) -> "JobResult":
        return cls(
            id=d["id"],
            status=d["status"],
            job_type=d["job_type"],
            args=d.get("args", {}),
            result=d.get("result"),
            error=d.get("error"),
            retries=d.get("retries", 0),
            enqueued_at=d.get("enqueued_at"),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
        )


class JobHandle:
    """Returned by Client.enqueue(). Use .wait() to block for the result."""

    def __init__(self, job_id: str, client: "Client"):
        self.id = job_id
        self._client = client

    def wait(self, timeout: float = 60.0, poll_interval: float = 0.25) -> JobResult:
        """Block until done/failed/cancelled. Raises TimeoutError if timeout expires."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self._client._get_raw(self.id)
            if job and job["status"] in ("done", "failed", "cancelled"):
                return JobResult._from_dict(job)
            time.sleep(poll_interval)
        raise TimeoutError(f"job {self.id} did not complete within {timeout}s")

    def status(self) -> JobResult | None:
        """Return the current job status without blocking."""
        job = self._client._get_raw(self.id)
        return JobResult._from_dict(job) if job else None

    def cancel(self) -> bool:
        """Cancel this job if it is still pending. Returns True if cancelled."""
        return self._client.cancel(self.id)

    def __repr__(self) -> str:
        return f"JobHandle(id={self.id!r})"


class Client:
    """
    Enqueue jobs, observe results.

    In-process mode (default):
        client = Client()                        # in-memory, no persistence
        client = Client(log_path="jobs.jsonl")   # persistent

    Remote mode (workers anywhere):
        client = Client(url="http://localhost:8000")
    """

    def __init__(
        self,
        space: TupleSpace | None = None,
        url: str | None = None,
        log_path: str | Path | None = None,
        api_key: str | None = None,
    ):
        if url:
            try:
                import requests
            except ImportError:
                raise ImportError("pip install requests  to use remote mode")
            self._url = url.rstrip("/")
            self._session = requests.Session()
            if api_key:
                self._session.headers["Authorization"] = f"Bearer {api_key}"
            self._space = None
        else:
            self._url = None
            self._session = None
            self._space = space or TupleSpace(
                log_path=Path(log_path) if log_path else None
            )

    # ── public API ────────────────────────────────────────────────────────

    def enqueue(self, job_type: str, max_retries: int = 3, **kwargs) -> JobHandle:
        """Put a job on the queue. Returns a handle to check status or wait."""
        job_id = str(uuid.uuid4())
        if self._url:
            resp = self._session.post(
                f"{self._url}/jobs",
                json={"job_type": job_type, "max_retries": max_retries, "args": kwargs},
            )
            resp.raise_for_status()
            return JobHandle(resp.json()["job_id"], self)
        self._space.out(make_tuple("job.pending", _payload(job_id, job_type, kwargs, max_retries)))
        return JobHandle(job_id, self)

    def cancel(self, job_id: str) -> bool:
        """Cancel a pending job. Returns True if cancelled, False if not found or already done."""
        if self._url:
            resp = self._session.post(f"{self._url}/jobs/{job_id}/cancel")
            if resp.status_code in (404, 409):
                return False
            resp.raise_for_status()
            return True
        result = self._space.inp(
            {"type": "job.pending", "payload.id": job_id},
            timeout=0,
        )
        if result is None:
            return False
        job = result["payload"]
        self._space.out(make_tuple("job.cancelled", {
            **job,
            "status": "cancelled",
            "finished_at": time.time(),
        }))
        return True

    def jobs(self, status: str | None = None) -> list[JobResult]:
        """List jobs. status: 'pending' | 'done' | 'failed' | 'cancelled' | None (all)."""
        if self._url:
            params = {"status": status} if status else {}
            raw = self._session.get(f"{self._url}/jobs", params=params).json()["jobs"]
            return [JobResult._from_dict(j) for j in raw]
        statuses = [status] if status else ["pending", "done", "failed", "cancelled"]
        return [
            JobResult._from_dict(t["payload"])
            for s in statuses
            for t in self._space.query({"type": f"job.{s}"})
        ]

    def get(self, job_id: str) -> JobResult | None:
        """Get a single job by ID, or None if not found."""
        raw = self._get_raw(job_id)
        return JobResult._from_dict(raw) if raw else None

    # ── worker protocol (used by Worker internally) ───────────────────────

    def _get_raw(self, job_id: str) -> dict | None:
        if self._url:
            resp = self._session.get(f"{self._url}/jobs/{job_id}")
            return resp.json() if resp.status_code == 200 else None
        for status in ("pending", "done", "failed", "cancelled"):
            for t in self._space.query({"type": f"job.{status}"}):
                if t["payload"]["id"] == job_id:
                    return t["payload"]
        return None

    def _claim(self, job_type: str, lease_ttl: float, worker_id: str | None = None) -> tuple[dict, str] | None:
        if self._url:
            body: dict = {"job_type": job_type, "lease_ttl": lease_ttl}
            if worker_id:
                body["worker_id"] = worker_id
            resp = self._session.post(f"{self._url}/jobs/claim", json=body)
            if resp.status_code == 204:
                return None
            data = resp.json()
            return data["job"], data["lease_id"]
        result = self._space.inp_lease(
            {"type": "job.pending", "payload.job_type": job_type},
            lease_ttl=lease_ttl,
            timeout=0,
        )
        if result is None:
            return None
        t, lease_id = result
        return t["payload"], lease_id

    def _complete(self, job: dict, lease_id: str, result: Any) -> None:
        done = {**job, "result": result, "finished_at": time.time(), "status": "done"}
        if self._url:
            self._session.post(
                f"{self._url}/jobs/{job['id']}/complete",
                json={"lease_id": lease_id, "result": result},
            ).raise_for_status()
            return
        self._space.confirm_lease(lease_id)
        self._space.out(make_tuple("job.done", done))

    def _fail(self, job: dict, lease_id: str, error: str, max_retries: int) -> None:
        if self._url:
            self._session.post(
                f"{self._url}/jobs/{job['id']}/fail",
                json={"lease_id": lease_id, "error": error},
            ).raise_for_status()
            return
        retries = job["retries"] + 1
        self._space.confirm_lease(lease_id)
        if retries < max_retries:
            self._space.out(make_tuple("job.pending", {
                **job, "retries": retries, "status": "pending",
            }))
        else:
            self._space.out(make_tuple("job.failed", {
                **job, "error": error, "retries": retries,
                "finished_at": time.time(), "status": "failed",
            }))


    def heartbeat(self, job_id: str, lease_id: str, extend_secs: float = 30.0) -> None:
        """Extend a claimed job's lease. No-op in in-process mode."""
        if self._url:
            self._session.post(
                f"{self._url}/jobs/{job_id}/heartbeat",
                json={"lease_id": lease_id, "extend_secs": extend_secs},
            ).raise_for_status()

    def retry(self, job_id: str) -> None:
        """Re-queue a permanently-failed job."""
        if self._url:
            self._session.post(f"{self._url}/jobs/{job_id}/retry").raise_for_status()
            return
        result = self._space.inp(
            {"type": "job.failed", "payload.id": job_id},
            timeout=0,
        )
        if result is None:
            raise ValueError(f"Job {job_id} not found or not in failed state")
        job = result["payload"]
        self._space.out(make_tuple("job.pending", {
            **job,
            "status": "pending",
            "retries": 0,
            "error": None,
            "finished_at": None,
        }))


def _payload(job_id: str, job_type: str, args: dict, max_retries: int) -> dict:
    return {
        "id":          job_id,
        "job_type":    job_type,
        "args":        args,
        "status":      "pending",
        "retries":     0,
        "max_retries": max_retries,
        "enqueued_at": time.time(),
    }
