def replay(events: list[dict], durability: str | None = None) -> list[dict]:
    """
    Reconstruct tuple space state from an event log.

    durability="hard"  — replay only hard-state events 
                         
    durability=None    — replay everything (default).
    """
    # lease_acquire removes the tuple from the visible space (it moves into a lease).
    # lease_expire / lease_release return it — both are constructive here.
    _CONSTRUCTIVE = {"out", "cas_new", "lease_release", "lease_expire"}
    _DESTRUCTIVE  = {"in", "expire", "cas_old", "lease_confirm", "lease_acquire"}

    tuples: dict[str, dict] = {}
    for event in events:
        op = event.get("op")
        t = event.get("tuple")
        if t is None:
            continue
        if durability == "hard" and t.get("durability", "hard") != "hard":
            continue
        if op in _CONSTRUCTIVE:
            tuples[t["id"]] = t
        elif op in _DESTRUCTIVE:
            tuples.pop(t["id"], None)
    return list(tuples.values())
