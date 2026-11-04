"""Validate external execution references."""

from uuid import UUID

import pytest

from forge.shared.identity import parse_attempt_id, parse_run_id


def test_run_reference_is_normalized() -> None:
    value = "A573D6B9-6648-4BAC-AC91-AEC1C30C21BA"

    run_id = parse_run_id(value)

    assert isinstance(run_id, UUID)
    assert str(run_id) == value.lower()


def test_attempt_reference_is_normalized() -> None:
    value = "E81234E4-247A-4751-88C6-CC0989125077"

    attempt_id = parse_attempt_id(value)

    assert isinstance(attempt_id, UUID)
    assert str(attempt_id) == value.lower()


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not-an-id",
        "../another-run",
        "/tmp/another-run",
        "a573d6b9-6648-4bac-ac91-aec1c30c21ba/../other",
    ],
)
def test_invalid_external_references_are_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_run_id(value)

    with pytest.raises(ValueError):
        parse_attempt_id(value)
