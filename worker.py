import threading
import time
import uuid
from typing import Callable


class Worker:
    """
    Pull jobs off the queue and process them.

        worker = Worker(client)

        @worker.job("resize_image")
        def resize(url, width, height):
            ...
            return {"path": output_path}

        worker.run()           # blocking
        worker.run(blocking=False)   # background thread
    """

    def __init__(self, client: "Client", worker_id: str | None = None):
        self._client = client
        self._worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._handlers: dict[str, dict] = {}
        self._running = False

    def job(
        self,
        job_type: str,
        max_retries: int = 3,
        lease_ttl: float = 30.0,
    ) -> Callable:
        """Decorator. Register a function as the handler for job_type."""
        def decorator(fn: Callable) -> Callable:
            self._handlers[job_type] = {
                "fn":          fn,
                "max_retries": max_retries,
                "lease_ttl":   lease_ttl,
            }
            return fn
        return decorator

    def run(self, poll_interval: float = 0.5, blocking: bool = True, concurrency: int = 1):
        """
        Start the worker loop.
        blocking=True  → runs in the current thread (use for scripts / CLI).
        blocking=False → starts daemon thread(s), returns the first thread.
        concurrency    → number of parallel worker threads (default 1).
        """
        self._running = True
        threads = []
        for i in range(concurrency):
            t = threading.Thread(
                target=self._loop, args=(poll_interval,),
                daemon=True, name=f"worker-{i}",
            )
            t.start()
            threads.append(t)
        if blocking:
            for t in threads:
                t.join()
        else:
            return threads[0]

    def stop(self) -> None:
        self._running = False

    # ── internals ─────────────────────────────────────────────────────────

    def _loop(self, poll_interval: float) -> None:
        while self._running:
            did_work = False
            for job_type, handler in list(self._handlers.items()):
                claimed = self._client._claim(job_type, lease_ttl=handler["lease_ttl"], worker_id=self._worker_id)
                if claimed is None:
                    continue
                did_work = True
                job, lease_id = claimed
                self._process(job, lease_id, handler)
            if not did_work:
                time.sleep(poll_interval)

    def _process(self, job: dict, lease_id: str, handler: dict) -> None:
        job = {**job, "started_at": time.time()}
        stop_hb = threading.Event()
        hb = threading.Thread(
            target=self._heartbeat_loop,
            args=(job["id"], lease_id, handler["lease_ttl"], stop_hb),
            daemon=True,
        )
        hb.start()
        try:
            result = handler["fn"](**job["args"])
            self._client._complete(job, lease_id, result)
        except Exception as exc:
            self._client._fail(job, lease_id, str(exc), handler["max_retries"])
        finally:
            stop_hb.set()
            hb.join(timeout=2.0)

    def _heartbeat_loop(self, job_id: str, lease_id: str, lease_ttl: float, stop: threading.Event) -> None:
        interval = max(1.0, lease_ttl * 0.4)
        while not stop.wait(interval):
            try:
                self._client.heartbeat(job_id, lease_id, extend_secs=lease_ttl)
            except Exception:
                break
