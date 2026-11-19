"""Physical file, publication compatibility view, and selected stream handles."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from . import parquet
from .metadata import bounds, content


class Rows:
    """Explicit row access shared by physical files and selected streams."""

    def _row_source(
        self,
    ) -> tuple[tuple[Path, ...], tuple[parquet.Footer, ...], tuple[parquet.Predicate, ...]]:
        raise NotImplementedError

    def schema(self) -> Any:
        files, footers, _ = self._row_source()
        return parquet.schema(files, footers=footers)

    def _time(self, start: Any, end: Any, time_field: str) -> tuple[parquet.Predicate, ...]:
        lo, hi = bounds(start, end)
        terms: list[parquet.Predicate] = list(self._row_source()[2])
        if lo is not None or hi is not None:
            schema = self.schema()
            if time_field not in schema.names:
                raise ValueError(f"Time field {time_field!r} is unavailable; consult .schema().")
            pa, _ = parquet.arrow_modules()
            dtype = schema.field(time_field).type
            if not (
                pa.types.is_int64(dtype) or (pa.types.is_timestamp(dtype) and dtype.unit == "ns")
            ):
                raise ValueError(
                    "Select a nanosecond timestamp column or signed int64 Unix ns column."
                )
        if lo is not None:
            terms.append((time_field, ">=", lo))
        if hi is not None:
            terms.append((time_field, "<", hi))
        return tuple(terms)

    def head(
        self,
        n: int = 10,
        *,
        columns: Sequence[str] | None = None,
        start: Any = None,
        end: Any = None,
        time_field: str = "event_timestamp_ns",
        input_rows: int | None = None,
    ) -> Any:
        files, footers, _ = self._row_source()
        return parquet.preview(
            files,
            n=n,
            columns=columns,
            footers=footers,
            predicates=self._time(start, end, time_field),
            input_rows=input_rows,
        )

    def batches(
        self,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 65_536,
        start: Any = None,
        end: Any = None,
        time_field: str = "event_timestamp_ns",
        input_rows: int | None = None,
    ) -> Iterator[Any]:
        files, _, _ = self._row_source()
        return parquet.batches(
            files,
            columns=columns,
            batch_size=batch_size,
            predicates=self._time(start, end, time_field),
            input_rows=input_rows,
        )

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        start: Any = None,
        end: Any = None,
        time_field: str = "event_timestamp_ns",
    ) -> Any:
        files, footers, _ = self._row_source()
        return parquet.scan(
            files,
            columns=columns,
            footers=footers,
            predicates=self._time(start, end, time_field),
        )


@dataclass(frozen=True, repr=False)
class Stream(Rows):
    files: tuple[Path, ...]
    fingerprint: str | None
    kind: str
    dataset_path: Path
    identity: dict[str, object] | None = None
    footers: tuple[parquet.Footer, ...] = ()
    predicates: tuple[parquet.Predicate, ...] = ()
    observed: tuple[int | None, int | None] = (None, None)
    evidence: str = "unknown"
    exact_rows: int | None = None
    context: dict[str, object] | None = None

    def _row_source(
        self,
    ) -> tuple[tuple[Path, ...], tuple[parquet.Footer, ...], tuple[parquet.Predicate, ...]]:
        return self.files, self.footers, self.predicates

    def membership(self) -> dict[str, object]:
        return {
            **(self.identity or {}),
            "data_type": self.kind,
            "fingerprint": self.fingerprint,
            "observed_start_ns": self.observed[0],
            "observed_end_ns": self.observed[1],
            "membership_evidence": self.evidence,
            "matching_rows": self.exact_rows,
        }

    def info(self) -> dict[str, Any]:
        physical = parquet.totals(self.footers) if self.footers else parquet.metadata(self.files)
        return {
            **(self.context or {}),
            "fingerprint": self.fingerprint,
            "kind": self.kind,
            "identity": self.identity,
            "path": str(self.dataset_path),
            **physical,
            "physical_rows": physical["rows"],
            "matching_rows": self.exact_rows,
            "observed_start_ns": self.observed[0],
            "observed_end_ns": self.observed[1],
            "membership_evidence": self.evidence,
        }

    def __repr__(self) -> str:
        identity = self.identity or {}
        return f"Stream({identity.get('stream_id', self.fingerprint)!r}, {self.kind}, {len(self.files)} files)"

    def _repr_html_(self) -> str:
        from .display import handle

        return handle(self)


@dataclass(frozen=True, repr=False)
class Dataset(Rows):
    path: Path
    streams_by_key: tuple[Stream, ...]
    status: str
    reason: str | None = None
    footers: tuple[parquet.Footer, ...] = ()
    publication_path: Path | None = None
    primary_file: str | None = None
    dataset_id: int | None = None
    registry_context: str | None = None
    publication_id: int | None = None
    engine_publication_id: int | str | None = None
    instrument_namespace: str | None = None
    operation: str | None = None
    data_type: str | None = None
    integrity: str | None = None
    integrity_scope: str | None = None
    metadata_scope: str = "unknown"
    observed: tuple[int | None, int | None] = (None, None)
    timestamp_evidence: str = "unknown"
    requested_interval: tuple[int, int] | None = None
    matching_streams: tuple[Stream, ...] | None = None

    @property
    def files(self) -> tuple[Path, ...]:
        return tuple(item.path for item in self.footers) or tuple(
            dict.fromkeys(path for stream in self.streams_by_key for path in stream.files)
        )

    @property
    def predicates(self) -> tuple[parquet.Predicate, ...]:
        return ()

    def _row_source(
        self,
    ) -> tuple[tuple[Path, ...], tuple[parquet.Footer, ...], tuple[parquet.Predicate, ...]]:
        return self.files, self.footers, self.predicates

    def info(self) -> dict[str, Any]:
        return {
            **parquet.totals(self.footers),
            "path": str(self.path),
            "status": self.status,
            "dataset_id": self.dataset_id,
            "registry_context": self.registry_context,
            "publication_id": self.publication_id,
            "engine_publication_id": self.engine_publication_id,
            "publication_path": str(self.publication_path) if self.publication_path else None,
            "primary_file": self.primary_file,
            "instrument_namespace": self.instrument_namespace,
            "operation": self.operation,
            "data_type": self.data_type,
            "integrity": self.integrity,
            "integrity_scope": self.integrity_scope,
            "metadata_scope": self.metadata_scope,
            "reason": self.reason,
            "observed_start_ns": self.observed[0],
            "observed_end_ns": self.observed[1],
            "timestamp_evidence": self.timestamp_evidence,
            "requested_interval": self.requested_interval,
        }

    def streams(self) -> Any:
        import polars as pl

        rows = [
            {
                **stream.membership(),
                "kind": stream.kind,
                "rows": stream.exact_rows,
                "files": len(stream.files),
            }
            for stream in self.streams_by_key
        ]
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def select(
        self,
        *,
        stream_id: str | None = None,
        exchange: str | None = None,
        symbol: str | None = None,
        kind: str | None = None,
        fingerprint: str | None = None,
        instrument_id: int | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
    ) -> Stream:
        filters = {
            "stream_id": stream_id,
            "exchange": exchange,
            "symbol": symbol,
            "instrument_id": instrument_id,
            "instrument_type": instrument_type,
            "market_category": market_category,
        }
        streams = (
            self.matching_streams if self.matching_streams is not None else self.streams_by_key
        )
        found = [
            stream
            for stream in streams
            if (kind is None or stream.kind == content(kind))
            and (fingerprint is None or stream.fingerprint == fingerprint)
            and all(
                value is None or (stream.identity or {}).get(key) == value
                for key, value in filters.items()
            )
        ]
        if len(found) != 1:
            raise ValueError(
                f"Expected exactly one matching stream, found {len(found)}; inspect .streams()"
            )
        stream = found[0]
        context = self.info()
        for key in ("rows", "bytes", "files", "row_groups", "path"):
            context.pop(key, None)
        return replace(stream, context=context)

    def trades(self, **filters: Any) -> Stream:
        return self.select(kind="trades", **filters)

    def __repr__(self) -> str:
        label = f"file #{self.dataset_id}" if self.dataset_id is not None else self.path.name
        return f"Dataset({label!r}, {self.metadata_scope}, {len(self.streams_by_key)} streams)"

    def _repr_html_(self) -> str:
        from .display import handle

        return handle(self)
