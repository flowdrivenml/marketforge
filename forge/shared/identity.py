"""Application execution identities."""

from __future__ import annotations

from typing import NewType
from uuid import UUID, uuid4

RunId = NewType("RunId", UUID)
AttemptId = NewType("AttemptId", UUID)


def new_run_id() -> RunId:
    """Allocate an identity for a new execution."""
    return RunId(uuid4())


def new_attempt_id() -> AttemptId:
    """Allocate an identity for one execution attempt."""
    return AttemptId(uuid4())


def parse_run_id(value: str) -> RunId:
    """Validate and normalize an external run reference."""
    return RunId(UUID(value))


def parse_attempt_id(value: str) -> AttemptId:
    """Validate and normalize an external attempt reference."""
    return AttemptId(UUID(value))
