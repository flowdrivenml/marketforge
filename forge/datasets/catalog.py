"""Filesystem catalog with composable metadata-only file selection."""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, cast

from forge.shared.paths import ProjectPaths
from forge.shared.sorting import SortField, SortSpec, ordered

from .dataset import Dataset, Stream
from .discovery import Diagnostic, inventory, is_staging, open_dataset, standalone
from .metadata import bounds, content, interval_matches, utc

Text = str | Sequence[str]
IDs = int | Sequence[int]
Moment = int | str | datetime
Paths = str | Path | Sequence[str | Path]
CATALOG_SORT = SortSpec(
    {"time": SortField("observed start", "desc"), "path": SortField("path")}, "time"
)


def _values(value: Any) -> tuple[Any, ...] | None:
    if value is None:
        return None
    return (
        tuple(cast(Sequence[Any], value))
        if isinstance(value, Sequence) and not isinstance(value, str)
        else (value,)
    )


@dataclass(frozen=True, repr=False)
class DatasetCatalog:
    entries: tuple[Dataset, ...]
    scope: str = "committed"
    diagnostics: tuple[Diagnostic, ...] = ()
    filters: tuple[dict[str, object], ...] = ()

    def __iter__(self) -> Iterator[Dataset]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    def __getitem__(self, index: int | slice) -> Dataset | DatasetCatalog:
        if isinstance(index, slice):
            return replace(self, entries=self.entries[index])
        return self.entries[index]

    def one(self) -> Dataset:
        if len(self) != 1:
            raise ValueError(f"Expected exactly one matching file, found {len(self)}.")
        return self.entries[0]

    def first(self) -> Dataset:
        if not self.entries:
            raise ValueError("No datasets match this catalog.")
        return self.entries[0]

    def get(self, reference: int | str | Path) -> Dataset:
        matches = tuple(
            item
            for item in self
            if (
                item.dataset_id == reference
                if type(reference) is int
                else str(item.path) == str(reference)
                or item.output_ref == str(reference)
                or (item.dataset_id is not None and str(item.dataset_id) == str(reference))
            )
        )
        return replace(self, entries=matches).one()

    def records(self) -> list[dict[str, object]]:
        return [
            {
                **item.discovery_fields(),
                **item.info(),
                "memberships": [stream.membership() for stream in item.streams_by_key],
                "matching_memberships": None
                if item.matching_streams is None
                else [stream.membership() for stream in item.matching_streams],
                "first_observed_utc": utc(item.observed[0])
                if item.observed[0] is not None
                else None,
                "last_observed_utc": utc(item.observed[1])
                if item.observed[1] is not None
                else None,
            }
            for item in self
        ]

    def summary(self) -> Any:
        """Return one structured Polars record per file with exact integer IDs."""
        import polars as pl

        rows = self.records()
        if not rows:
            return pl.DataFrame(
                schema={
                    "reference": pl.String,
                    "reference_kind": pl.String,
                    "path": pl.String,
                    "dataset_id": pl.Int64,
                    "rows": pl.Int64,
                }
            )
        # Registry and engine IDs occupy distinct domains. Engine IDs may use
        # the full UInt64 range and also arrive as strings from registry JSON.
        for row in rows:
            if row["engine_publication_id"] is not None:
                row["engine_publication_id"] = str(row["engine_publication_id"])
        return pl.from_dicts(rows, infer_schema_length=None)

    @cached_property
    def _overview(self) -> dict[str, Any]:
        known = [item.observed for item in self if item.observed[0] is not None]
        streams = {
            (
                item.publication_path,
                stream.identity.get("stream_id"),
                stream.fingerprint,
                stream.kind,
                stream.predicates,
            )
            for item in self
            for stream in item.streams_by_key
            if stream.identity
        }
        return {
            "scope": self.scope,
            "files": len(self.entries),
            "publications": len({item.publication_path for item in self if item.publication_path}),
            "known_streams": len(streams),
            "physical_rows": sum(
                footer.metadata.num_rows for item in self for footer in item.footers
            ),
            "bytes": sum(footer.size for item in self for footer in item.footers),
            "observed_start_ns": min((lo for lo, _ in known if lo is not None), default=None),
            "observed_end_ns": max((hi for _, hi in known if hi is not None), default=None),
            "diagnostics": len(self.diagnostics),
        }

    def __post_init__(self) -> None:
        _ = self._overview

    def overview(self) -> dict[str, Any]:
        return dict(self._overview)

    def find(
        self,
        *,
        exchange: Text | None = None,
        symbol: Text | None = None,
        symbol_match: Literal["exact", "contains"] = "exact",
        instrument_id: IDs | None = None,
        instrument_namespace: Text | None = None,
        instrument_type: Text | None = None,
        market_category: Text | None = None,
        data_type: Text | None = None,
        stream_id: Text | None = None,
        fingerprint: Text | None = None,
        operation: Text | None = None,
        publication_id: IDs | None = None,
        publication_path: Paths | None = None,
        engine_publication_id: int | Text | None = None,
        dataset_id: IDs | None = None,
        path: Paths | None = None,
        min_rows: int | None = None,
        max_rows: int | None = None,
        min_bytes: int | None = None,
        max_bytes: int | None = None,
        start: Moment | None = None,
        end: Moment | None = None,
        interval: Literal["overlap", "envelope_contained"] = "overlap",
        status: Text | None = None,
        integrity: Text | None = None,
        output_ref: Text | None = None,
        target_quote: Text | None = None,
        sort: Literal["time", "path"] | None = None,
        order: Literal["asc", "desc"] | None = None,
    ) -> DatasetCatalog:
        """Filter file metadata. All requested market fields match one membership."""
        if symbol_match not in {"exact", "contains"} or interval not in {
            "overlap",
            "envelope_contained",
        }:
            raise ValueError("Unsupported symbol or interval matching mode.")
        if sort is None:
            sort = (
                "path"
                if any(
                    value is not None
                    for value in (publication_id, publication_path, engine_publication_id)
                )
                else "time"
            )
        if sort not in {"time", "path"} or order not in {None, "asc", "desc"}:
            raise ValueError("Use sort='time' or 'path' and order='asc' or 'desc'.")
        time = bounds(start, end)
        limits = {
            "min_rows": min_rows,
            "max_rows": max_rows,
            "min_bytes": min_bytes,
            "max_bytes": max_bytes,
        }
        if any(value is not None and value < 0 for value in limits.values()):
            raise ValueError("Physical row and byte bounds must be nonnegative.")
        if any(
            low is not None and high is not None and low > high
            for low, high in ((min_rows, max_rows), (min_bytes, max_bytes))
        ):
            raise ValueError("Physical minimum must not exceed its maximum.")
        file_fields = {
            "operation": operation,
            "publication_id": publication_id,
            "publication_path": publication_path,
            "engine_publication_id": engine_publication_id,
            "dataset_id": dataset_id,
            "path": path,
            "status": status,
            "integrity": integrity,
            "output_ref": output_ref,
            "target_quote": target_quote,
        }
        member_fields = {
            "exchange": exchange,
            "symbol": symbol,
            "instrument_id": instrument_id,
            "instrument_namespace": instrument_namespace,
            "instrument_type": instrument_type,
            "market_category": market_category,
            "stream_id": stream_id,
            "fingerprint": fingerprint,
        }
        file_filters = {
            key: _values(value) for key, value in file_fields.items() if value is not None
        }
        member_filters = {
            key: _values(value) for key, value in member_fields.items() if value is not None
        }
        kinds = (
            tuple(content(kind) for kind in (_values(data_type) or ()))
            if data_type is not None
            else None
        )
        for key in ("path", "publication_path"):
            if key in file_filters:
                file_filters[key] = tuple(
                    os.path.abspath(Path(value).expanduser()) for value in file_filters[key] or ()
                )
        if "engine_publication_id" in file_filters:
            file_filters["engine_publication_id"] = tuple(
                map(str, file_filters["engine_publication_id"] or ())
            )

        result: list[Dataset] = []
        for item in self:
            info = item.info()
            if item.engine_publication_id is not None:
                info["engine_publication_id"] = str(item.engine_publication_id)
            if any(
                info[key] not in choices
                for key, choices in file_filters.items()
                if choices is not None
            ):
                continue
            if any(
                value is not None
                and (info[key[4:]] < value if key.startswith("min") else info[key[4:]] > value)
                for key, value in limits.items()
            ):
                continue
            if not interval_matches(*item.observed, *time, interval):
                continue
            if not member_filters and (kinds is None or item.data_type in kinds):
                result.append(item)
                continue
            candidates = (
                item.matching_streams if item.matching_streams is not None else item.streams_by_key
            )
            matches: list[Stream] = []
            for stream in candidates:
                if (
                    kinds is not None
                    and stream.kind not in kinds
                    and not (item.data_type == "combined" and "combined" in kinds)
                ):
                    continue
                identity = {**(stream.identity or {}), "fingerprint": stream.fingerprint}
                if all(
                    any(needle in identity[key] for needle in choices)
                    if key == "symbol"
                    and symbol_match == "contains"
                    and isinstance(identity.get(key), str)
                    else identity.get(key) in choices
                    for key, choices in member_filters.items()
                    if choices is not None
                ):
                    matches.append(stream)
            if matches:
                result.append(replace(item, matching_streams=tuple(matches)))
        applied = {
            key: value
            for key, value in {
                **file_filters,
                **member_filters,
                **limits,
                "data_type": kinds,
                "start_ns": time[0],
                "end_ns": time[1],
                "interval": interval if any(x is not None for x in time) else None,
                "symbol_match": symbol_match if symbol is not None else None,
            }.items()
            if value is not None
        }
        ordering = CATALOG_SORT.resolve(sort, order)
        if sort == "time":
            result = ordered(
                result, ordering, lambda item: item.observed[0], lambda item: str(item.path)
            )
        else:
            result = ordered(
                result, ordering, lambda item: str(item.path), lambda item: str(item.path)
            )
        return replace(self, entries=tuple(result), filters=(*self.filters, applied))

    def __repr__(self) -> str:
        return f"DatasetCatalog(scope={self.scope!r}, files={len(self)}, diagnostics={len(self.diagnostics)})"

    def _repr_html_(self) -> str:
        from .display import catalog

        return catalog(self)


class MarketData:
    """Read-only entry point for filesystem discovery and supplied registry summaries."""

    def __init__(self, *, root: str | Path | None = None, data_root: str | Path | None = None):
        self.paths = ProjectPaths.resolve(
            root=Path(root) if root else None, data_root=Path(data_root) if data_root else None
        )

    def open(
        self,
        path: str | Path,
        *,
        allow_staging: bool = False,
        metadata_only: bool = False,
        strict: bool = False,
    ) -> Dataset:
        source = Path(path).expanduser()
        return open_dataset(
            self.paths.root / source,
            allow_staging=allow_staging,
            metadata_only=metadata_only,
            strict=strict,
        )

    async def find_registered(
        self,
        *,
        testing: bool = False,
        **filters: Any,
    ) -> dict[str, Any]:
        """Browse a registry publication page without reading Parquet footers."""
        from forge.datasets.query import DatasetRequest, browse
        from forge.shared.database.connection import database_environment

        with database_environment(testing=testing):
            return await browse(self.paths, DatasetRequest("find", view="publications", **filters))

    async def open_registered(
        self,
        *,
        publication_id: int | None = None,
        file_id: int | None = None,
        output_ref: str | None = None,
        testing: bool = False,
    ) -> Dataset:
        from forge.datasets.query import DatasetRequest, open_registered
        from forge.shared.database.connection import (
            database_connection,
            database_dsn,
            database_environment,
        )

        with database_environment(testing=testing):
            async with (
                database_connection(database_dsn(self.paths.root)) as connection,
                connection.transaction(),
            ):
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                return await open_registered(
                    connection,
                    DatasetRequest(
                        "show", id=file_id, publication_id=publication_id, output_ref=output_ref
                    ),
                )

    def discover(
        self,
        *,
        scope: str = "committed",
        include_staging: bool = False,
        registry_records: Iterable[Mapping[str, Any]] = (),
        registry_context: str | None = None,
        metadata_only: bool = False,
    ) -> DatasetCatalog:
        """Refresh filesystem metadata. Supplied file summaries are optional and read-only."""
        if scope not in {"committed", "tests", "all"}:
            raise ValueError("scope must be 'committed', 'tests', or 'all'")
        registry: dict[Path, Mapping[str, Any]] = {}
        for row in registry_records:
            if not registry_context:
                raise ValueError(
                    "Identify the registry context when supplying registered file summaries."
                )
            path = Path(row["storage_path"])
            if not path.is_absolute() or path in registry:
                raise ValueError(
                    "Registry records require distinct absolute registered file paths."
                )
            registry[path] = dict(row) | {"registry_context": registry_context}
        locations = [self.paths.datasets_dir] if scope != "tests" else []
        if scope != "committed":
            locations.extend(
                (
                    self.paths.data_root / "tests/historical/normalize",
                    self.paths.data_root / "test/normalized",
                )
            )
        diagnostics: list[Diagnostic] = []
        entries: dict[Path, Dataset] = {}

        def on_error(error: OSError) -> None:
            diagnostics.append(
                Diagnostic(Path(error.filename or "."), "unreadable_directory", str(error))
            )

        for location in locations:
            if not location.exists():
                continue
            for name, dirs, files in os.walk(location, onerror=on_error):
                root = Path(name)
                if is_staging(root) and not include_staging:
                    dirs[:] = []
                    continue
                dirs.sort()
                if "manifest.json" in files:
                    datasets, issues = inventory(
                        root, registry=registry, metadata_only=metadata_only
                    )
                    entries.update({item.path: item for item in datasets})
                    diagnostics.extend(issues)
                    dirs[:] = []
                    continue
                for file in sorted(files):
                    path = root / file
                    if path.suffix != ".parquet" or path.name in {
                        "events.parquet",
                        "depth-events.parquet",
                    }:
                        continue
                    try:
                        entries[path] = standalone(path)
                    except (OSError, ValueError) as error:
                        diagnostics.append(Diagnostic(path, "unreadable_file", str(error)))
                    else:
                        diagnostics.append(
                            Diagnostic(
                                path, "missing_identity", "No manifest-backed file identity."
                            )
                        )
        return DatasetCatalog(
            tuple(entries[path] for path in sorted(entries)), scope, tuple(diagnostics)
        )

    def datasets(self, *, include_tests: bool = False, include_staging: bool = False) -> Any:
        return self.discover(
            scope="all" if include_tests else "committed", include_staging=include_staging
        ).summary()
