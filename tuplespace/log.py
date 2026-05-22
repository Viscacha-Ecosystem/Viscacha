import json
import time
import uuid
from pathlib import Path


class EventLog:
    def __init__(self):
        self._entries: list[dict] = []

    def append(self, op: str, tuple_: dict | None = None,
               pattern: dict | None = None, agent_id: str | None = None,
               idempotency_key: str | None = None,
               idempotency_expires: float | None = None) -> None:
        self._entries.append({
            "event_id":           str(uuid.uuid4()),
            "op":                 op,
            "timestamp":          time.time(),
            "tuple":              tuple_,
            "pattern":            pattern,
            "agent_id":           agent_id,
            "idempotency_key":    idempotency_key,
            "idempotency_expires": idempotency_expires,
        })

    def entries(self) -> list[dict]:
        return list(self._entries)

    def last_event_id(self) -> str | None:
        return self._entries[-1]["event_id"] if self._entries else None


class PersistentEventLog(EventLog):
    """
    Append-only file-backed event log.

    Each event is written as a single JSON line immediately on append,
    then flushed to disk 
    """

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._load_existing()
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)

    def _load_existing(self) -> None:
        if not self._path.exists():
            return
        with self._path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        self._entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  

    def append(self, op: str, tuple_: dict | None = None,
               pattern: dict | None = None, agent_id: str | None = None,
               idempotency_key: str | None = None,
               idempotency_expires: float | None = None) -> None:
        super().append(op, tuple_, pattern, agent_id, idempotency_key, idempotency_expires)
        entry = self._entries[-1]
        self._fh.write(json.dumps(entry, default=str) + "\n")
        self._fh.flush()

    def rotate_after(self, last_event_id: str | None) -> None:
        """
        Rewrite the log file keeping only entries that come AFTER last_event_id.
        If last_event_id is None, all entries are kept (no-op prtty much).
  
        """
        self._fh.close()

        if last_event_id is not None:
            boundary = next(
                (i for i, e in enumerate(self._entries)
                 if e["event_id"] == last_event_id),
                None,
            )
            tail = self._entries[boundary + 1:] if boundary is not None else self._entries
        else:
            tail = self._entries

        tmp = self._path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8", buffering=1) as f:
            for entry in tail:
                f.write(json.dumps(entry, default=str) + "\n")
            f.flush()
        tmp.replace(self._path)  # atomic on NIX; best-effort on Windows...........

        self._entries = list(tail)
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        self._fh.close()
