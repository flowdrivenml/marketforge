"""Logical dataset and instrument stream handles."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from . import parquet


@dataclass(frozen=True)
class Stream:
    files: tuple[Path, ...]
    fingerprint: str
    kind: str
    dataset_path: Path
    identity: dict[str, object] | None = None

    def info(self) -> dict[str, object]:
        return {
            "fingerprint": self.fingerprint,
            "kind": self.kind,
            "identity": self.identity,
            "path": str(self.dataset_path),
            **parquet.metadata(self.files),
        }

    def schema(self):
        return parquet.schema(self.files)

    def head(
        self,
        n: int = 10,
        *,
        columns: Sequence[str] | None = None,
    ):
        return parquet.preview(
            self.files,
            n=n,
            columns=columns,
        )

    def batches(
        self,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 65_536,
    ) -> Iterator:
        return parquet.batches(
            self.files,
            columns=columns,
            batch_size=batch_size,
        )

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
    ):
        return parquet.scan(
            self.files,
            columns=columns,
        )


@dataclass(frozen=True)
class Dataset:
    path: Path
    streams_by_key: tuple[Stream, ...]
    status: str
    reason: str | None = None

    def streams(self):
        import polars as pl

        rows = []

        for stream in self.streams_by_key:
            meta = stream.info()
            identity = stream.identity or {}

            rows.append(
                {
                    "fingerprint": stream.fingerprint,
                    "kind": stream.kind,
                    "exchange": identity.get("exchange"),
                    "symbol": identity.get("symbol"),
                    "stream_id": identity.get("stream_id"),
                    "rows": meta["rows"],
                    "files": meta["files"],
                    "bytes": meta["bytes"],
                }
            )

        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def select(
        self,
        *,
        stream_id: str | None = None,
        exchange: str | None = None,
        symbol: str | None = None,
        kind: str | None = None,
        fingerprint: str | None = None,
    ) -> Stream:
        found = [
            stream
            for stream in self.streams_by_key
            if (
                (kind is None or stream.kind == kind)
                and (fingerprint is None or stream.fingerprint == fingerprint)
                and (
                    stream_id is None
                    or (stream.identity or {}).get("stream_id") == stream_id
                )
                and (
                    exchange is None
                    or (stream.identity or {}).get("exchange") == exchange
                )
                and (symbol is None or (stream.identity or {}).get("symbol") == symbol)
            )
        ]

        if len(found) != 1:
            raise ValueError(
                f"Expected exactly one matching stream, "
                f"found {len(found)}; inspect .streams()"
            )

        return found[0]

    def trades(self, **filters) -> Stream:
        return self.select(kind="trades", **filters)
