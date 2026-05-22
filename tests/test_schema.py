import pytest
from viscacha.tuplespace.schema import validate_tuple, make_tuple


def test_valid_tuple_passes():
    t = make_tuple("task", {"x": 1})
    validate_tuple(t)


def test_missing_type_raises():
    with pytest.raises((ValueError, TypeError)):
        validate_tuple({"id": "abc", "created_at": 1.0})


def test_missing_id_raises():
    with pytest.raises((ValueError, TypeError)):
        validate_tuple({"type": "task", "created_at": 1.0})


def test_missing_created_at_raises():
    with pytest.raises((ValueError, TypeError)):
        validate_tuple({"id": "abc", "type": "task"})


def test_empty_type_raises():
    with pytest.raises(TypeError):
        validate_tuple({"id": "abc", "type": "", "created_at": 1.0})


def test_invalid_ttl_raises():
    with pytest.raises(TypeError):
        validate_tuple({"id": "abc", "type": "task", "created_at": 1.0, "ttl": "bad"})


def test_invalid_payload_raises():
    with pytest.raises(TypeError):
        validate_tuple({"id": "abc", "type": "task", "created_at": 1.0, "payload": "bad"})


def test_make_tuple_defaults():
    t = make_tuple("result")
    assert t["type"] == "result"
    assert t["payload"] == {}
    assert t["ttl"] is None
    assert isinstance(t["id"], str)
    assert isinstance(t["created_at"], float)
