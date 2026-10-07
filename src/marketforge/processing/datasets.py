from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from psycopg import Connection


@dataclass(frozen=True)
class Dataset:
    """Canonical MarketForge dataset metadata."""

    id: int

    exchange_id: int | None
    instrument_id: int | None

    data_type: str

    start_timestamp_ns: int
    end_timestamp_ns: int

    storage_format: str
    storage_path: Path | None

    status: str


@dataclass(frozen=True)
class ResolvedDataset:
    """Canonical dataset joined with instrument identity."""

    id: int

    exchange_id: int | None
    exchange: str | None

    instrument_id: int | None
    symbol: str | None

    instrument_type: str | None
    market_category: str | None

    base_asset: str | None
    quote_asset: str | None
    settlement_asset: str | None

    data_type: str

    start_timestamp_ns: int
    end_timestamp_ns: int

    storage_format: str
    storage_path: Path | None

    status: str


class DatasetRepository:
    """Persistence and resolution for canonical datasets."""

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn

    def create(
        self,
        *,
        exchange_id: int | None,
        instrument_id: int | None,
        data_type: str,
        start_timestamp_ns: int,
        end_timestamp_ns: int,
        storage_format: str = "parquet",
        storage_path: Path | None = None,
        status: str = "pending",
    ) -> Dataset:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.datasets (
                    exchange_id,
                    instrument_id,
                    data_type,
                    start_timestamp_ns,
                    end_timestamp_ns,
                    storage_format,
                    storage_path,
                    status
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                RETURNING
                    id,
                    exchange_id,
                    instrument_id,
                    data_type,
                    start_timestamp_ns,
                    end_timestamp_ns,
                    storage_format,
                    storage_path,
                    status
                """,
                (
                    exchange_id,
                    instrument_id,
                    data_type,
                    start_timestamp_ns,
                    end_timestamp_ns,
                    storage_format,
                    (str(storage_path) if storage_path is not None else None),
                    status,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Failed to create dataset")

        return self._dataset_from_row(row)

    def get(
        self,
        dataset_id: int,
    ) -> Dataset | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    exchange_id,
                    instrument_id,
                    data_type,
                    start_timestamp_ns,
                    end_timestamp_ns,
                    storage_format,
                    storage_path,
                    status

                FROM catalog.datasets

                WHERE id = %s
                """,
                (dataset_id,),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return self._dataset_from_row(row)

    def get_many(
        self,
        dataset_ids: Iterable[int],
    ) -> list[Dataset]:
        ids = list(dict.fromkeys(dataset_ids))

        if not ids:
            return []

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    exchange_id,
                    instrument_id,
                    data_type,
                    start_timestamp_ns,
                    end_timestamp_ns,
                    storage_format,
                    storage_path,
                    status

                FROM catalog.datasets

                WHERE id = ANY(%s)

                ORDER BY id
                """,
                (ids,),
            )

            rows = cursor.fetchall()

        return [self._dataset_from_row(row) for row in rows]

    def get_resolved_many(
        self,
        dataset_ids: Iterable[int],
    ) -> list[ResolvedDataset]:
        ids = list(dict.fromkeys(dataset_ids))

        if not ids:
            return []

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    d.id,

                    d.exchange_id,
                    e.code AS exchange,

                    d.instrument_id,
                    i.symbol,
                    i.instrument_type,
                    i.market_category,

                    i.base_asset,
                    i.quote_asset,
                    i.settlement_asset,

                    d.data_type,

                    d.start_timestamp_ns,
                    d.end_timestamp_ns,

                    d.storage_format,
                    d.storage_path,

                    d.status

                FROM catalog.datasets AS d

                LEFT JOIN catalog.exchanges AS e
                    ON e.id = d.exchange_id

                LEFT JOIN catalog.instruments AS i
                    ON i.id = d.instrument_id

                WHERE d.id = ANY(%s)

                ORDER BY d.id
                """,
                (ids,),
            )

            rows = cursor.fetchall()

        return [self._resolved_dataset_from_row(row) for row in rows]

    def set_status(
        self,
        dataset_id: int,
        status: str,
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE catalog.datasets
                SET status = %s
                WHERE id = %s
                """,
                (
                    status,
                    dataset_id,
                ),
            )

            if cursor.rowcount != 1:
                raise LookupError("Dataset not found: " f"{dataset_id}")

    def set_storage_path(
        self,
        dataset_id: int,
        storage_path: Path,
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE catalog.datasets
                SET storage_path = %s
                WHERE id = %s
                """,
                (
                    str(storage_path),
                    dataset_id,
                ),
            )

            if cursor.rowcount != 1:
                raise LookupError("Dataset not found: " f"{dataset_id}")

    def add_input(
        self,
        *,
        dataset_id: int,
        input_dataset_id: int,
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.dataset_inputs (
                    dataset_id,
                    input_dataset_id
                )
                VALUES (
                    %s,
                    %s
                )
                ON CONFLICT (
                    dataset_id,
                    input_dataset_id
                )
                DO NOTHING
                """,
                (
                    dataset_id,
                    input_dataset_id,
                ),
            )

    def add_inputs(
        self,
        *,
        dataset_id: int,
        input_dataset_ids: Iterable[int],
    ) -> None:
        ids = list(dict.fromkeys(input_dataset_ids))

        if not ids:
            return

        with self.conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO catalog.dataset_inputs (
                    dataset_id,
                    input_dataset_id
                )
                VALUES (
                    %s,
                    %s
                )
                ON CONFLICT (
                    dataset_id,
                    input_dataset_id
                )
                DO NOTHING
                """,
                [
                    (
                        dataset_id,
                        input_dataset_id,
                    )
                    for input_dataset_id in ids
                ],
            )

    def get_inputs(
        self,
        dataset_id: int,
    ) -> list[int]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    input_dataset_id

                FROM catalog.dataset_inputs

                WHERE dataset_id = %s

                ORDER BY input_dataset_id
                """,
                (dataset_id,),
            )

            rows = cursor.fetchall()

        return [row["input_dataset_id"] for row in rows]

    @staticmethod
    def _dataset_from_row(
        row: dict,
    ) -> Dataset:
        return Dataset(
            id=row["id"],
            exchange_id=row["exchange_id"],
            instrument_id=row["instrument_id"],
            data_type=row["data_type"],
            start_timestamp_ns=row["start_timestamp_ns"],
            end_timestamp_ns=row["end_timestamp_ns"],
            storage_format=row["storage_format"],
            storage_path=(
                Path(row["storage_path"]) if row["storage_path"] is not None else None
            ),
            status=row["status"],
        )

    def get_input_datasets(
        self,
        dataset_id: int,
    ) -> list[ResolvedDataset]:
        """
        Return the immediate input datasets of one dataset.
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    d.id,

                    d.exchange_id,
                    e.code AS exchange,

                    d.instrument_id,
                    i.symbol,
                    i.instrument_type,
                    i.market_category,

                    i.base_asset,
                    i.quote_asset,
                    i.settlement_asset,

                    d.data_type,

                    d.start_timestamp_ns,
                    d.end_timestamp_ns,

                    d.storage_format,
                    d.storage_path,

                    d.status

                FROM catalog.dataset_inputs AS di

                JOIN catalog.datasets AS d
                    ON d.id = di.input_dataset_id

                LEFT JOIN catalog.exchanges AS e
                    ON e.id = d.exchange_id

                LEFT JOIN catalog.instruments AS i
                    ON i.id = d.instrument_id

                WHERE di.dataset_id = %s

                ORDER BY d.id
                """,
                (dataset_id,),
            )

            rows = cursor.fetchall()

        return [self._resolved_dataset_from_row(row) for row in rows]

    def resolve_leaf_datasets(
        self,
        dataset_ids: Iterable[int],
    ) -> list[ResolvedDataset]:
        """
        Recursively resolve selected datasets to their underlying
        single-instrument leaf datasets.

        A dataset with instrument_id is a leaf.

        A dataset without instrument_id must have dataset_inputs,
        which are recursively traversed.

        Duplicate leaves are returned only once.
        """

        root_ids = list(dict.fromkeys(dataset_ids))

        if not root_ids:
            return []

        resolved: dict[int, ResolvedDataset] = {}

        visiting: set[int] = set()

        for dataset_id in root_ids:
            self._resolve_leaf_dataset(
                dataset_id=dataset_id,
                resolved=resolved,
                visiting=visiting,
            )

        return [resolved[dataset_id] for dataset_id in sorted(resolved)]

    def _resolve_leaf_dataset(
        self,
        *,
        dataset_id: int,
        resolved: dict[int, ResolvedDataset],
        visiting: set[int],
    ) -> None:
        """
        Recursive implementation for resolve_leaf_datasets().
        """

        if dataset_id in visiting:
            raise RuntimeError(
                "Dataset provenance cycle detected " f"at dataset {dataset_id}"
            )

        dataset_rows = self.get_resolved_many([dataset_id])

        if not dataset_rows:
            raise LookupError(f"Dataset not found: {dataset_id}")

        dataset = dataset_rows[0]

        if dataset.instrument_id is not None:
            resolved.setdefault(
                dataset.id,
                dataset,
            )

            return

        visiting.add(dataset_id)

        try:
            inputs = self.get_input_datasets(dataset_id)

            if not inputs:
                raise RuntimeError(
                    f"Dataset {dataset_id} has no " "instrument and no input datasets"
                )

            for input_dataset in inputs:
                self._resolve_leaf_dataset(
                    dataset_id=input_dataset.id,
                    resolved=resolved,
                    visiting=visiting,
                )

        finally:
            visiting.remove(dataset_id)

    @staticmethod
    def _resolved_dataset_from_row(
        row: dict,
    ) -> ResolvedDataset:
        return ResolvedDataset(
            id=row["id"],
            exchange_id=row["exchange_id"],
            exchange=row["exchange"],
            instrument_id=row["instrument_id"],
            symbol=row["symbol"],
            instrument_type=row["instrument_type"],
            market_category=row["market_category"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            settlement_asset=row["settlement_asset"],
            data_type=row["data_type"],
            start_timestamp_ns=row["start_timestamp_ns"],
            end_timestamp_ns=row["end_timestamp_ns"],
            storage_format=row["storage_format"],
            storage_path=(
                Path(row["storage_path"]) if row["storage_path"] is not None else None
            ),
            status=row["status"],
        )

    def list_resolved(
        self,
        *,
        exchange: str | None = None,
        symbol: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
    ) -> list[ResolvedDataset]:
        conditions: list[str] = []
        params: list[object] = []

        if exchange is not None:
            conditions.append("e.code = %s")
            params.append(exchange)

        if symbol is not None:
            conditions.append("i.symbol ILIKE %s")
            params.append(f"%{symbol}%")

        if data_type is not None:
            conditions.append("d.data_type = %s")
            params.append(data_type)

        if status is not None:
            conditions.append("d.status = %s")
            params.append(status)

        where_clause = ""

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT
                d.id,

                d.exchange_id,
                e.code AS exchange,

                d.instrument_id,
                i.symbol,
                i.instrument_type,
                i.market_category,

                i.base_asset,
                i.quote_asset,
                i.settlement_asset,

                d.data_type,

                d.start_timestamp_ns,
                d.end_timestamp_ns,

                d.storage_format,
                d.storage_path,

                d.status

            FROM catalog.datasets AS d

            LEFT JOIN catalog.exchanges AS e
                ON e.id = d.exchange_id

            LEFT JOIN catalog.instruments AS i
                ON i.id = d.instrument_id

            {where_clause}

            ORDER BY d.id
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                params,
            )

            rows = cursor.fetchall()

        return [self._resolved_dataset_from_row(row) for row in rows]
