"""Bounded immutable market progress snapshots, independent of terminal rendering."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ScopeProgress:
    exchange: str
    market: str
    status: str
    collected: int


@dataclass(frozen=True)
class CollectionProgress:
    metadata: bool
    scopes: tuple[ScopeProgress, ...]

    @property
    def finished(self) -> int:
        return sum(s.status in {"complete", "failed", "cancelled"} for s in self.scopes)


ProgressCallback = Callable[[CollectionProgress], None]
