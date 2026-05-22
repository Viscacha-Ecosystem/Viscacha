def _get_nested(t: dict, key: str):
    """Resolve dot-notation key against a tuple dict. Top-level keys checked first, then payload.x.y."""
    parts = key.split(".")
    node = t
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


_MISSING = object()


def match(pattern: dict, t: dict) -> bool:
    for key, expected in pattern.items():
        actual = _get_nested(t, key)
        if actual is _MISSING:
            return False
        if expected == "*":
            continue
        if actual != expected:
            return False
    return True
