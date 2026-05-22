import time
import uuid

REQUIRED_FIELDS = {"id", "type", "created_at"}
VALID_DURABILITY = {"hard", "soft"}


def validate_tuple(t: dict) -> None:
    missing = REQUIRED_FIELDS - t.keys()
    if missing:
        raise ValueError(f"Tuple missing required fields: {missing}")
    if not isinstance(t["id"], str) or not t["id"]:
        raise TypeError("'id' must be a non-empty str")
    if not isinstance(t["type"], str) or not t["type"]:
        raise TypeError("'type' must be a non-empty str")
    if not isinstance(t["created_at"], (int, float)):
        raise TypeError("'created_at' must be a float")
    if "ttl" in t and t["ttl"] is not None and not isinstance(t["ttl"], (int, float)):
        raise TypeError("'ttl' must be a float or None")
    if "payload" in t and not isinstance(t["payload"], dict):
        raise TypeError("'payload' must be a dict")
    if "durability" in t and t["durability"] not in VALID_DURABILITY:
        raise ValueError(f"'durability' must be one of {VALID_DURABILITY}")
    if "version" in t and not isinstance(t["version"], int):
        raise TypeError("'version' must be an int")


def make_tuple(
    type_: str,
    payload: dict | None = None,
    ttl: float | None = None,
    durability: str = "hard",
) -> dict:
    """
    durability="hard"  — authoritative truth; persisted / replayed.
    durability="soft"  — heuristic/stigmergy signal;  can be lost.
    """
    return {
        "id": str(uuid.uuid4()),
        "type": type_,
        "created_at": time.time(),
        "ttl": ttl,
        "durability": durability,
        "version": 0,
        "payload": payload if payload is not None else {},
    }


def make_failure_tuple(
    original_id: str,
    op: str,
    reason: str,
    agent: str,
    retry_count: int = 0,
    max_retries: int = 3,
) -> dict:
    """First-class failure signal. Agents write this explicitly on error."""
    return make_tuple("failure", {
        "original_id": original_id,
        "op": op,
        "reason": reason,
        "agent": agent,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "retryable": retry_count < max_retries,
    })
