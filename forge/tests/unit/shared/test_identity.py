"""Validate external execution references."""

from uuid import UUID

import pytest

from forge.shared.identity import parse_attempt_id, parse_run_id


def test_run_reference_is_normalized() -> None:
    value = "A573D6B9-6648-4BAC-AC91-AEC1C30C21BA"

    run_id = parse_run_id(value)

    assert isinstance(run_id.uuid, UUID) and run_id.operation is None
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


def test_all_public_operation_ids_round_trip() -> None:
    from forge.shared.identity import RunOperation, new_run_id

    identities = {new_run_id(operation) for operation in RunOperation for _ in range(20)}
    assert len(identities) == len(RunOperation) * 20
    for identity in identities:
        assert identity.operation is not None
        assert identity.uuid.version == 4
        assert str(identity).startswith(f"{identity.operation}-")
        assert parse_run_id(str(identity)) == identity
        with pytest.raises(ValueError):
            parse_run_id(f"arbitrary-{identity.uuid}")
