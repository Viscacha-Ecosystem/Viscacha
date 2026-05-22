from viscacha.tuplespace.matcher import match
from viscacha.tuplespace.schema import make_tuple


def _t(type_="task", **payload):
    return make_tuple(type_, payload)


def test_exact_type_match():
    assert match({"type": "task"}, _t("task"))


def test_exact_type_no_match():
    assert not match({"type": "result"}, _t("task"))


def test_wildcard_type():
    assert match({"type": "*"}, _t("task"))
    assert match({"type": "*"}, _t("result"))


def test_nested_exact():
    t = make_tuple("task", {"job": "sentiment", "status": "pending"})
    assert match({"type": "task", "payload.job": "sentiment"}, t)
    assert not match({"payload.job": "other"}, t)


def test_nested_wildcard():
    t = make_tuple("task", {"status": "pending"})
    assert match({"payload.status": "*"}, t)


def test_missing_nested_key():
    t = make_tuple("task", {})
    assert not match({"payload.missing": "x"}, t)


def test_multi_field_pattern():
    t = make_tuple("task", {"job": "sentiment", "status": "pending"})
    assert match({"type": "task", "payload.job": "sentiment", "payload.status": "pending"}, t)
    assert not match({"type": "task", "payload.job": "sentiment", "payload.status": "done"}, t)
