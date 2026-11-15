"""Arrow-native Parquet access with exact decimal preservation."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path


def _arrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "Install the research dependencies: " "uv sync --project forge --group lab"
        ) from exc

    return pa, pq


def metadata(
    files: Sequence[Path],
) -> dict[str, int]:
    """Read lightweight Parquet footer metadata."""

    _, pq = _arrow()

    summaries = [pq.ParquetFile(path).metadata for path in files]

    return {
        "files": len(files),
        "rows": sum(item.num_rows for item in summaries),
        "row_groups": sum(item.num_row_groups for item in summaries),
        "bytes": sum(path.stat().st_size for path in files),
    }


def schema(files: Sequence[Path]):
    """Return the Arrow schema, validating consistency across files."""

    _, pq = _arrow()

    if not files:
        raise ValueError("No Parquet files supplied.")

    first = pq.ParquetFile(files[0]).schema_arrow

    for path in files[1:]:
        other = pq.ParquetFile(path).schema_arrow

        if not first.equals(other, check_metadata=False):
            raise ValueError(f"Incompatible Parquet schemas: {path}")

    return first


def batches(
    files: Sequence[Path],
    *,
    columns: Sequence[str] | None = None,
    batch_size: int = 65_536,
) -> Iterator:
    """Yield Arrow record batches without materializing the dataset."""

    if batch_size < 1:
        raise ValueError("batch_size must be positive.")

    _, pq = _arrow()

    for path in files:
        reader = pq.ParquetFile(path)

        yield from reader.iter_batches(
            columns=list(columns) if columns is not None else None,
            batch_size=batch_size,
        )


def preview(
    files: Sequence[Path],
    *,
    n: int = 10,
    columns: Sequence[str] | None = None,
):
    """
    Return a small Polars DataFrame.

    Decimal256 values are converted to exact strings for display.
    The original Parquet files remain unchanged.
    """

    if n < 0:
        raise ValueError("n must be nonnegative.")

    pa, _ = _arrow()

    import polars as pl

    selected_schema = schema(files)

    if columns is not None:
        selected_schema = pa.schema([selected_schema.field(name) for name in columns])

    if n == 0:
        return pl.DataFrame({field.name: [] for field in selected_schema})

    chunks = []
    remaining = n

    for batch in batches(
        files,
        columns=columns,
        batch_size=min(65_536, n),
    ):
        if remaining <= 0:
            break

        chunks.append(batch.slice(0, remaining))

        remaining -= min(
            remaining,
            batch.num_rows,
        )

        if remaining == 0:
            break

    if not chunks:
        return pl.DataFrame({field.name: [] for field in selected_schema})

    table = pa.Table.from_batches(chunks)

    for index, field in enumerate(table.schema):
        if pa.types.is_decimal256(field.type):
            table = table.set_column(
                index,
                field.name,
                table.column(index).cast(pa.string()),
            )

    return pl.from_arrow(table)


def scan(
    files: Sequence[Path],
    *,
    columns: Sequence[str] | None = None,
):
    """
    Return a lazy Polars query.

    Decimal256 columns are unsupported by Polars and must
    be excluded from the projection.
    """

    import polars as pl

    pa, _ = _arrow()

    available_schema = schema(files)

    if columns is None:
        selected = list(available_schema)
    else:
        selected = [available_schema.field(name) for name in columns]

    unsupported = [
        field.name for field in selected if pa.types.is_decimal256(field.type)
    ]

    if unsupported:
        raise ValueError(
            "Polars cannot scan Decimal256 columns: "
            + ", ".join(unsupported)
            + ". Use .batches() for exact values "
            + "or .head() for display."
        )

    return pl.scan_parquet([str(path) for path in files]).select(
        [field.name for field in selected]
    )
