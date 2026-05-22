from .replay import replay

_DESTRUCTIVE = {"in", "expire", "cas_old", "lease_confirm", "lease_expire"}
_CONSTRUCTIVE = {"out", "cas_new", "lease_release"}


def state_at(events: list[dict], timestamp: float) -> list[dict]:

    window = [e for e in events if e["timestamp"] <= timestamp]
    return replay(window)


def step_through(events: list[dict]):
    """  Generator: replays events one by one.
    """
    state: dict[str, dict] = {}
    for event in events:
        op = event.get("op")
        t  = event.get("tuple")
        if t:
            if op in _CONSTRUCTIVE:
                state[t["id"]] = t
            elif op in _DESTRUCTIVE:
                state.pop(t["id"], None)
        yield event, list(state.values())


def diff(events: list[dict], t1: float, t2: float) -> dict:
 
    snap1 = {t["id"]: t for t in state_at(events, t1)}
    snap2 = {t["id"]: t for t in state_at(events, t2)}
    return {
        "added":   [t for tid, t in snap2.items() if tid not in snap1],
        "removed": [t for tid, t in snap1.items() if tid not in snap2],
    }
