"""
File layout inside a log directory:
    snapshot.json       — latest committed snapshot (atomic write)
    events.jsonl        — log entries since last snapshot (or full history)

"""

import json
import time
from pathlib import Path

from .core import TupleSpace
from .log import PersistentEventLog
from .replay import replay


class SnapshotManager:
    SNAPSHOT_FILE = "snapshot.json"
    EVENTS_FILE   = "events.jsonl"

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    @property
    def snapshot_path(self) -> Path:
        return self._dir / self.SNAPSHOT_FILE

    @property
    def events_path(self) -> Path:
        return self._dir / self.EVENTS_FILE


    def write_snapshot(self, snap: dict) -> None:

        tmp = self._dir / "snapshot.json.tmp"
        tmp.write_text(json.dumps(snap, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def load_snapshot(self) -> dict | None:
        """Return the snapshot dict, or None if absent """
        if not self.snapshot_path.exists():
            return None
        try:
            return json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None  


    def compact(self, ts: TupleSpace) -> dict:


        snap = ts.snapshot()
        self.write_snapshot(snap)

        if isinstance(ts._log, PersistentEventLog):
            ts._log.rotate_after(snap["last_event_id"])

        return snap

    def should_compact(self, ts: TupleSpace, max_events: int = 1000) -> bool:
        return len(ts.log_entries()) >= max_events

    # recovery factory

    def open_space(self, **kwargs) -> TupleSpace:
        """
        Reconstruct a TupleSpace from snapshot + log tail.

        Recovery is always:
          1. Create space connected to the log file (future writes go here)
          2. If snapshot exists: restore_snapshot + apply tail log entries
          3. If no snapshot:     replay full log into _tuples directly

        """
        ts = TupleSpace(log_path=self.events_path, **kwargs)
        snap = self.load_snapshot()

        if snap is None:
            # reconstruct state entirely from the log.
            entries = ts.log_entries()
            now = time.time()
            with ts._cv:
                ts._tuples = list(replay(entries))
                ts._idempotency = {
                    e["idempotency_key"]: e["idempotency_expires"]
                    for e in entries
                    if e.get("op") == "out"
                    and e.get("idempotency_key") is not None
                    and e.get("idempotency_expires", 0) > now
                }
            return ts

        ts.restore_snapshot(snap)


        last_id = snap.get("last_event_id")
        tail = self._entries_after(ts.log_entries(), last_id)
        if tail:
            _apply_tail(ts, tail)

        return ts

    @staticmethod
    def _entries_after(entries: list[dict], last_event_id: str | None) -> list[dict]:
        
        if last_event_id is None:
            return entries
        boundary = next(
            (i for i, e in enumerate(entries) if e["event_id"] == last_event_id),
            None,
        )
        if boundary is None:
            # last_event_id not found & all entries are new
            return entries
        return entries[boundary + 1:]


def _apply_tail(ts: TupleSpace, entries: list[dict]) -> None:
    """
    Re-apply a sequence of log entries to a TupleSpace without triggering
    duplicate logging. Used during recovery to replay the log tail
    """
    from .schema import validate_tuple

    _CONSTRUCTIVE = {"out", "cas_new", "lease_release", "lease_expire"}
    _DESTRUCTIVE  = {"in", "expire", "cas_old", "lease_confirm"}

    with ts._cv:
        for e in entries:
            op = e.get("op")
            t  = e.get("tuple")
            if t is None:
                continue
            if op in _CONSTRUCTIVE:
                if not any(x["id"] == t["id"] for x in ts._tuples):
                    ts._tuples.append(t)
            elif op in _DESTRUCTIVE:
                ts._tuples = [x for x in ts._tuples if x["id"] != t["id"]]
        ts._cv.notify_all()
