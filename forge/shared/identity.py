"""Application execution identities."""

from dataclasses import dataclass
from enum import StrEnum
from typing import NewType
from uuid import UUID, uuid4


class RunOperation(StrEnum):
    ACQUISITION = "acquisition"
    NORMALIZE = "normalize"
    MERGE = "merge"
    CONVERT = "convert"
    PIPELINE = "pipeline"


@dataclass(frozen=True, slots=True)
class RunId:
    uuid: UUID
    operation: RunOperation | None

    def __str__(self) -> str:
        return f"{self.operation}-{self.uuid}" if self.operation else str(self.uuid)


AttemptId = NewType("AttemptId", UUID)


def new_run_id(operation: RunOperation) -> RunId:
    return RunId(uuid4(), RunOperation(operation))


def new_attempt_id() -> AttemptId:
    return AttemptId(uuid4())


def parse_run_id(value: str) -> RunId:
    prefix, separator, remainder = value.partition("-")
    if separator and prefix in tuple(RunOperation):
        operation = RunOperation(prefix)
        identity = UUID(remainder)
        if remainder.lower() != str(identity):
            raise ValueError("Expected a hyphenated UUID.")
        return RunId(identity, operation)
    identity = UUID(value)
    if value.lower() != str(identity):
        raise ValueError("Expected an operation-prefixed run ID or legacy UUID.")
    return RunId(identity, None)


def parse_attempt_id(value: str) -> AttemptId:
    return AttemptId(UUID(value))
