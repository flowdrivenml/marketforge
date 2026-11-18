"""Filesystem-based dataset catalog and public access API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import polars as pl

from forge.shared.paths import ProjectPaths

from .dataset import Dataset
from .discovery import open_dataset


@dataclass(frozen=True)
class DatasetCatalog:
    entries: tuple[Dataset, ...]

    def summary(self) -> pl.DataFrame:
        import polars as pl

        rows: list[dict[str, object]] = []

        for dataset in self.entries:
            streams = dataset.streams_by_key

            rows.append(
                {
                    "path": str(dataset.path),
                    "status": dataset.status,
                    "streams": len(streams),
                    "rows": sum(cast(int, stream.info()["rows"]) for stream in streams),
                }
            )

        return pl.DataFrame(rows) if rows else pl.DataFrame()


class MarketData:
    """Read-only entry point for MarketForge datasets."""

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        data_root: str | Path | None = None,
    ):
        self.paths = ProjectPaths.resolve(
            root=Path(root) if root else None,
            data_root=Path(data_root) if data_root else None,
        )

    def open(
        self,
        path: str | Path,
        *,
        allow_staging: bool = False,
    ) -> Dataset:
        source = Path(path).expanduser()

        if not source.is_absolute():
            source = self.paths.root / source

        return open_dataset(
            source,
            allow_staging=allow_staging,
        )

    def discover(
        self,
        *,
        scope: str = "committed",
        include_staging: bool = False,
    ) -> DatasetCatalog:
        if scope not in {"committed", "tests", "all"}:
            raise ValueError("scope must be 'committed', 'tests', or 'all'")

        locations: list[Path] = []

        if scope in {"committed", "all"}:
            locations.append(self.paths.datasets_dir)

        if scope in {"tests", "all"}:
            locations.extend(
                (
                    self.paths.data_root / "tests" / "historical" / "normalize",
                    self.paths.data_root / "test" / "normalized",
                )
            )

        entries: list[Dataset] = []
        seen: set[Path] = set()

        for root in locations:
            if not root.is_dir():
                continue

            candidate_roots = sorted(path for path in root.rglob("streams") if path.is_dir())

            for streams in candidate_roots:
                parent = streams.parent

                if parent in seen:
                    continue

                seen.add(parent)

                if "staging" in parent.parts and not include_staging:
                    continue

                try:
                    dataset = self.open(
                        parent,
                        allow_staging=include_staging,
                    )
                    entries.append(dataset)

                except (ValueError, OSError):
                    continue

        return DatasetCatalog(tuple(entries))

    def datasets(
        self,
        *,
        include_tests: bool = False,
        include_staging: bool = False,
    ) -> pl.DataFrame:
        scope = "all" if include_tests else "committed"

        return self.discover(
            scope=scope,
            include_staging=include_staging,
        ).summary()
