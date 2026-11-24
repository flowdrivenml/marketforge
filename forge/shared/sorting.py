"""Allowlisted collection ordering shared by queries and local inventories."""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Literal, LiteralString, Protocol, Self, cast

from forge.shared.errors import MarketForgeError

Direction = Literal["asc", "desc"]


@dataclass(frozen=True)
class SortField:
    label: str
    default: Direction = "asc"
    columns: tuple[LiteralString, ...] = ()
    group: LiteralString = ""


@dataclass(frozen=True)
class SortOrder:
    field: str
    direction: Direction
    spec: SortField
    tie: LiteralString

    def sql(self) -> LiteralString:
        suffix: LiteralString = (
            " DESC NULLS LAST" if self.direction == "desc" else " ASC NULLS LAST"
        )
        columns = (column + suffix for column in self.spec.columns)
        return cast(
            LiteralString, ", ".join(item for item in (self.spec.group, *columns, self.tie) if item)
        )

    @property
    def description(self) -> str:
        return f"Sorted by {self.spec.label} · {'ascending' if self.direction == 'asc' else 'descending'}"


@dataclass(frozen=True)
class SortSpec:
    fields: Mapping[str, SortField]
    default: str
    tie: LiteralString = ""
    aliases: Mapping[str, str] = field(default_factory=lambda: dict[str, str]())

    def resolve(self, field: str | None = None, order: str | None = None) -> SortOrder:
        requested = field or self.default
        key = self.aliases.get(requested, requested)
        if key not in self.fields:
            raise MarketForgeError(
                f"Unsupported sort field {requested!r}. Choose {', '.join(self.fields)}."
            )
        spec = self.fields[key]
        if order not in (None, "asc", "desc"):
            raise MarketForgeError("Sort direction must be asc or desc.")
        return SortOrder(key, order or spec.default, spec, self.tie)


class Comparable(Protocol):
    def __lt__(self, other: Self, /) -> bool: ...


def ordered[T, K: Comparable, Identity: Comparable](
    rows: Iterable[T],
    order: SortOrder,
    value: Callable[[T], K | None],
    identity: Callable[[T], Identity],
) -> list[T]:
    """Use an ascending identity tie-break and place unknown primary values last."""
    known: list[tuple[K, T]] = []
    missing: list[T] = []
    for row in sorted(rows, key=identity):
        key = value(row)
        if key is None:
            missing.append(row)
        else:
            known.append((key, row))
    known.sort(key=lambda item: item[0], reverse=order.direction == "desc")
    return [*(row for _, row in known), *missing]


NAMES = SortSpec({"name": SortField("name")}, "name")
