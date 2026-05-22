import copy
import random
import uuid
from pathlib import Path
from threading import Condition, Lock, Thread
import time

from .log import EventLog, PersistentEventLog
from .matcher import match
from .schema import validate_tuple


DEFAULT_IDEMPOTENCY_TTL = 86_400.0   # 24 hours


class LeaseExpiredError(Exception):
    pass


class CASConflictError(Exception):
    pass


class TupleSpace:
    def __init__(
        self,
        log_path: str | Path | None = None,
        idempotency_ttl: float = DEFAULT_IDEMPOTENCY_TTL,
    ):
        self._tuples: list[dict] = []
        self._leases: dict[str, dict] = {}        # lease_id -> {tuple, expires_at}
        self._idempotency: dict[str, float] = {}  # key -> expiry wall-clock time
        self._idempotency_ttl = idempotency_ttl
        self._lock = Lock()
        self._cv = Condition(self._lock)
        self._log = PersistentEventLog(log_path) if log_path else EventLog()
        self._sweeper = Thread(target=self._ttl_sweep, daemon=True)
        self._sweeper.start()

    # write 

    def out(
        self,
        t: dict,
        idempotency_key: str | None = None,
        idempotency_ttl: float | None = None,
    ) -> bool:
        validate_tuple(t)
        with self._cv:
            idem_expiry = None
            if idempotency_key is not None:
                expiry = self._idempotency.get(idempotency_key, 0)
                if expiry > time.time():
                    return False
                idem_expiry = time.time() + (idempotency_ttl or self._idempotency_ttl)
                self._idempotency[idempotency_key] = idem_expiry
            self._tuples.append(t)
            self._log.append("out", tuple_=t,
                             idempotency_key=idempotency_key,
                             idempotency_expires=idem_expiry)
            self._cv.notify_all()
            return True

    # read

    def rd(self, pattern: dict, timeout: float | None = None) -> dict | None:
        deadline = time.monotonic() + timeout if timeout is not None else None
        with self._cv:
            while True:
                hit = self._scan(pattern)
                if hit is not None:
                    self._log.append("rd", tuple_=hit, pattern=pattern)
                    return hit
                remaining = deadline - time.monotonic() if deadline is not None else None
                if remaining is not None and remaining <= 0:
                    return None
                self._cv.wait(timeout=remaining)

    # take 

    def inp(self, pattern: dict, timeout: float | None = None) -> dict | None:
        deadline = time.monotonic() + timeout if timeout is not None else None
        with self._cv:
            while True:
                idx = self._scan_index(pattern)
                if idx is not None:
                    t = self._tuples.pop(idx)
                    self._log.append("in", tuple_=t, pattern=pattern)
                    return t
                remaining = deadline - time.monotonic() if deadline is not None else None
                if remaining is not None and remaining <= 0:
                    return None
                self._cv.wait(timeout=remaining)

    # leased take 

    def inp_lease(
        self,
        pattern: dict,
        lease_ttl: float = 30.0,
        timeout: float | None = None,
    ) -> tuple[dict, str] | None:
        deadline = time.monotonic() + timeout if timeout is not None else None
        with self._cv:
            while True:
                idx = self._scan_index(pattern)
                if idx is not None:
                    t = self._tuples.pop(idx)
                    lease_id = str(uuid.uuid4())
                    self._leases[lease_id] = {
                        "tuple": t,
                        "expires_at": time.monotonic() + lease_ttl,
                        "expires_wall": time.time() + lease_ttl,
                    }
                    self._log.append("lease_acquire", tuple_=t, pattern=pattern,
                                     agent_id=lease_id)
                    return t, lease_id
                remaining = deadline - time.monotonic() if deadline is not None else None
                if remaining is not None and remaining <= 0:
                    return None
                self._cv.wait(timeout=remaining)

    def confirm_lease(self, lease_id: str) -> None:
        with self._cv:
            entry = self._leases.pop(lease_id, None)
            if entry is None:
                raise LeaseExpiredError(f"Lease {lease_id} expired or not found")
            self._log.append("lease_confirm", tuple_=entry["tuple"], agent_id=lease_id)

    def release_lease(self, lease_id: str) -> None:
        with self._cv:
            entry = self._leases.pop(lease_id, None)
            if entry is None:
                raise LeaseExpiredError(f"Lease {lease_id} expired or not found")
            self._tuples.append(entry["tuple"])
            self._log.append("lease_release", tuple_=entry["tuple"], agent_id=lease_id)
            self._cv.notify_all()

    # CAS

    def cas(self, id: str, expected_version: int, new_tuple: dict) -> dict:
        """
        Replace the tuple with the given id if and only if its version matches.
        Operates on identity, not pattern and mutation must be unambiguous.
        """
        validate_tuple(new_tuple)
        with self._cv:
            idx = next(
                (i for i, t in enumerate(self._tuples)
                 if t["id"] == id and not self._is_expired(t)),
                None,
            )
            if idx is None:
                raise KeyError(f"No tuple with id={id!r}")
            current = self._tuples[idx]
            if current.get("version", 0) != expected_version:
                raise CASConflictError(
                    f"Version mismatch: expected {expected_version}, "
                    f"got {current.get('version', 0)}"
                )
            self._tuples.pop(idx)
            self._log.append("cas_old", tuple_=current)
            updated = {**new_tuple, "version": expected_version + 1}
            self._tuples.append(updated)
            self._log.append("cas_new", tuple_=updated)
            self._cv.notify_all()
            return updated

    # batch read 

    def query(self, pattern: dict, limit: int | None = None) -> list[dict]:
        with self._lock:
            results = [t for t in self._tuples if match(pattern, t)]
            if limit is not None:
                results = results[:limit]
            return list(results)

    #  snapshot / restore

    def snapshot(self) -> dict:
        """
        Capture consistent state in a single critical section.
        The dict is safe to serialize outside the lock.
        """
        with self._lock:
            return {
                "version":          1,
                "timestamp":        time.time(),
                "last_event_id":    self._log.last_event_id(),
                "tuples":           copy.deepcopy(self._tuples),
                "leases":           {
                    lid: {
                        "tuple":           copy.deepcopy(e["tuple"]),
                        "expires_wall":    e["expires_wall"],
                    }
                    for lid, e in self._leases.items()
                },
                
                "idempotency": {
                    k: exp for k, exp in self._idempotency.items()
                    if exp > time.time()           # prune already-expired keys
                },
            }

    def restore_snapshot(self, snap: dict) -> None:
        """
        Load state from a snapshot dict.
       
        Expired leases are automatically returned to the tuple store.
        """
        with self._cv:
            self._tuples = list(snap.get("tuples", []))
            now_wall = time.time()
            now_mono = time.monotonic()
            self._leases = {}
            for lid, entry in snap.get("leases", {}).items():
                remaining = entry["expires_wall"] - now_wall
                if remaining > 0:
                    self._leases[lid] = {
                        "tuple":        entry["tuple"],
                        "expires_at":   now_mono + remaining,
                        "expires_wall": entry["expires_wall"],
                    }
                else:
                    # lease expired 
                    self._tuples.append(entry["tuple"])
            
            self._idempotency = {
                k: exp for k, exp in snap.get("idempotency", {}).items()
                if exp > now_wall
            }
            self._cv.notify_all()

    # log access 

    def log_entries(self, durability: str | None = None) -> list[dict]:
        entries = self._log.entries()
        if durability == "hard":
            return [
                e for e in entries
                if e.get("tuple") is None
                or e["tuple"].get("durability", "hard") == "hard"
            ]
        return entries

    # internal

    def _is_expired(self, t: dict) -> bool:
        return t.get("ttl") is not None and t["created_at"] + t["ttl"] < time.time()

    def _scan(self, pattern: dict) -> dict | None:
        indices = list(range(len(self._tuples)))
        random.shuffle(indices)
        for i in indices:
            t = self._tuples[i]
            if not self._is_expired(t) and match(pattern, t):
                return t
        return None

    def _scan_index(self, pattern: dict) -> int | None:
        indices = list(range(len(self._tuples)))
        random.shuffle(indices)
        for i in indices:
            t = self._tuples[i]
            if not self._is_expired(t) and match(pattern, t):
                return i
        return None

    def _ttl_sweep(self) -> None:
        while True:
            time.sleep(1)
            now_wall = time.time()
            now_mono = time.monotonic()
            with self._cv:
                # expire TTL tuples
                expired = [
                    t for t in self._tuples
                    if t.get("ttl") is not None and t["created_at"] + t["ttl"] < now_wall
                ]
                for t in expired:
                    self._tuples.remove(t)
                    self._log.append("expire", tuple_=t)

                # expire leases
                expired_leases = [
                    lid for lid, e in self._leases.items()
                    if e["expires_at"] < now_mono
                ]
                for lid in expired_leases:
                    entry = self._leases.pop(lid)
                    self._tuples.append(entry["tuple"])
                    self._log.append("lease_expire", tuple_=entry["tuple"],
                                     agent_id=lid)
                if expired_leases:
                    self._cv.notify_all()

                
                expired_keys = [k for k, exp in self._idempotency.items()
                                if exp <= now_wall]
                for k in expired_keys:
                    del self._idempotency[k]
                    
                    # : )
