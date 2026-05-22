"""
HTTP server — thin FastAPI wrapper around Client (in-process / dev mode).

Wire-compatible with the viscacha-rs Rust server. For production use prefer
the Rust server; this wrapper is useful for local development, testing, and
environments where a Python-only stack is required.

Usage:
    from viscacha import Client
    from viscacha.server import create_app
    import uvicorn

    client = Client(log_path="jobs.jsonl")
    app = create_app(client)
    uvicorn.run(app, host="0.0.0.0", port=8000)

Endpoints:
    POST /jobs                  enqueue a job
    GET  /jobs                  list jobs (optional ?status=)
    GET  /jobs/{id}             get one job
    POST /jobs/claim            worker: claim next job of a type
    POST /jobs/{id}/complete    worker: mark done
    POST /jobs/{id}/fail        worker: mark failed
    POST /jobs/{id}/cancel      cancel a pending job
    POST /jobs/{id}/heartbeat   extend a lease (no-op in in-process mode)
    POST /jobs/{id}/retry       re-queue a permanently-failed job
"""

from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Response
    from pydantic import BaseModel
except ImportError:
    raise ImportError("pip install fastapi uvicorn  to use the HTTP server")

from .client import Client


def create_app(client: Client) -> FastAPI:
    app = FastAPI(title="Viscacha")

    # lease_id -> job dict, populated on claim, consumed on complete/fail
    _active: dict[str, dict] = {}

    # ── job management ────────────────────────────────────────────────────

    class EnqueueRequest(BaseModel):
        job_type: str
        max_retries: int = 3
        args: dict = {}

    @app.post("/jobs", status_code=201)
    def enqueue(req: EnqueueRequest):
        handle = client.enqueue(req.job_type, max_retries=req.max_retries, **req.args)
        return {"job_id": handle.id}

    @app.get("/jobs")
    def list_jobs(status: str | None = None):
        return {"jobs": client.jobs(status=status)}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = client.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str):
        cancelled = client.cancel(job_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail="Job not found or not pending")
        return {"status": "cancelled"}

    @app.post("/jobs/{job_id}/retry")
    def retry_job(job_id: str):
        try:
            client.retry(job_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok"}

    # ── worker protocol ───────────────────────────────────────────────────

    class ClaimRequest(BaseModel):
        job_type: str
        lease_ttl: float = 30.0
        worker_id: str | None = None

    class CompleteRequest(BaseModel):
        lease_id: str
        result: Any = None

    class FailRequest(BaseModel):
        lease_id: str
        error: str

    class HeartbeatRequest(BaseModel):
        lease_id: str
        extend_secs: float = 30.0

    @app.post("/jobs/claim")
    def claim(req: ClaimRequest):
        result = client._claim(req.job_type, lease_ttl=req.lease_ttl, worker_id=req.worker_id)
        if result is None:
            return Response(status_code=204)
        job, lease_id = result
        _active[lease_id] = job
        return {"job": job, "lease_id": lease_id}

    @app.post("/jobs/{job_id}/complete")
    def complete(job_id: str, req: CompleteRequest):
        job = _active.pop(req.lease_id, None)
        if job is None:
            raise HTTPException(status_code=400, detail="Unknown or expired lease")
        try:
            client._complete(job, req.lease_id, req.result)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok"}

    @app.post("/jobs/{job_id}/fail")
    def fail(job_id: str, req: FailRequest):
        job = _active.pop(req.lease_id, None)
        if job is None:
            raise HTTPException(status_code=400, detail="Unknown or expired lease")
        try:
            client._fail(job, req.lease_id, req.error, job.get("max_retries", 3))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok"}

    @app.post("/jobs/{job_id}/heartbeat")
    def heartbeat(job_id: str, req: HeartbeatRequest):
        # In-process: TupleSpace manages lease expiry internally; nothing to extend.
        return {"status": "ok"}

    return app
