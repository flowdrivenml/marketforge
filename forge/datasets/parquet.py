"""Read cached footers and explicitly request exact Arrow rows."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

Predicate = tuple[str, str, int | str]


def arrow_modules() -> tuple[Any, Any]:
    try:
        pa = import_module("pyarrow")
        pq = import_module("pyarrow.parquet")
    except ImportError as exc:
        raise ImportError(
            "Install research dependencies with uv sync --project forge --group lab"
        ) from exc
    return pa, pq


@dataclass(frozen=True)
class Footer:
    path: Path
    schema: Any
    metadata: Any
    size: int

    def groups(self) -> Iterator[Any]:
        for index in range(self.metadata.num_row_groups):
            yield self.metadata.row_group(index)

    def chunks(self, name: str) -> Iterator[Any]:
        for group in self.groups():
            yield next(
                (
                    column
                    for index in range(group.num_columns)
                    if (column := group.column(index)).path_in_schema == name
                ),
                None,
            )

    def extent(self, name: str) -> tuple[int | None, int | None]:
        stat = [column.statistics if column is not None else None for column in self.chunks(name)]
        if not stat or any(item is None or not item.has_min_max for item in stat):
            return None, None
        low, high = (
            min(item.min_raw for item in stat if item is not None),
            max(item.max_raw for item in stat if item is not None),
        )
        return (low, high) if type(low) is int and type(high) is int else (None, None)


def footer(path: Path) -> Footer:
    _, pq = arrow_modules()
    with pq.ParquetFile(path) as file:
        return Footer(path, file.schema_arrow, file.metadata, path.stat().st_size)


def metadata(files: Sequence[Path]) -> dict[str, int]:
    return totals(tuple(footer(path) for path in files))


def totals(footers: Sequence[Footer]) -> dict[str, int]:
    return {
        "files": len(footers),
        "rows": sum(item.metadata.num_rows for item in footers),
        "row_groups": sum(item.metadata.num_row_groups for item in footers),
        "bytes": sum(item.size for item in footers),
    }


def schema(files: Sequence[Path], *, footers: Sequence[Footer] = ()) -> Any:
    inventory = footers or tuple(footer(path) for path in files)
    if not inventory:
        raise ValueError("No Parquet files supplied.")
    first = inventory[0].schema
    for item in inventory[1:]:
        if not first.equals(item.schema, check_metadata=False):
            raise ValueError(f"Incompatible Parquet schemas: {item.path}")
    return first


def _expression(predicates: Sequence[Predicate], table_schema: Any) -> Any:
    ds: Any = import_module("pyarrow.dataset")

    expression = None
    for name, op, value in predicates:
        if name not in table_schema.names:
            raise ValueError(f"Predicate column {name!r} is unavailable.")
        pa, _ = arrow_modules()
        field_type = table_schema.field(name).type
        if pa.types.is_timestamp(field_type):
            if field_type.unit != "ns":
                raise ValueError(
                    "Time predicates require a nanosecond timestamp or integer ns field."
                )
            value = pa.scalar(value, type=pa.int64()).cast(field_type)
        field = ds.field(name)
        term = field == value if op == "=" else field >= value if op == ">=" else field < value
        expression = term if expression is None else expression & term
    return expression


def eligible_groups(
    item: Footer, predicates: Sequence[Predicate], indices: Sequence[int] | None = None
) -> tuple[int, ...]:
    """Exclude groups only when exact scalar footer statistics prove no match."""
    result: list[int] = []
    for index in range(item.metadata.num_row_groups) if indices is None else indices:
        group = item.metadata.row_group(index)
        pending = {name for name, _, _ in predicates}
        stats: dict[str, Any] = {}
        for position in range(group.num_columns):
            chunk = group.column(position)
            name = chunk.path_in_schema
            if name in pending:
                stats[name] = chunk.statistics
                pending.remove(name)
                if not pending:
                    break
        for name, op, value in predicates:
            stat = stats.get(name)
            if stat is None or not stat.has_min_max:
                continue
            lo, hi = stat.min_raw, stat.max_raw
            if type(value) is not int or type(lo) is not int or type(hi) is not int:
                continue
            if (
                (op == "=" and not lo <= value <= hi)
                or (op == ">=" and hi < value)
                or (op == "<" and lo >= value)
            ):
                break
        else:
            result.append(index)
    return tuple(result)


def batches(
    files: Sequence[Path],
    *,
    columns: Sequence[str] | None = None,
    batch_size: int = 65_536,
    predicates: Sequence[Predicate] = (),
    input_rows: int | None = None,
    row_groups: Mapping[Path, Sequence[int]] | None = None,
    bounded: bool = False,
) -> Iterator[Any]:
    """Yield exact Arrow batches. A budget clips a physical prefix before filtering."""
    if batch_size < 1 or (input_rows is not None and input_rows < 0):
        raise ValueError("Require a positive batch size and nonnegative input-row budget.")
    pa, pq = arrow_modules()
    remaining = input_rows
    for path in files:
        if remaining == 0:
            return
        if predicates and remaining is None and row_groups is None and not bounded:
            ds: Any = import_module("pyarrow.dataset")

            file = ds.dataset(str(path), format="parquet")
            yield from file.to_batches(
                columns=list(columns) if columns is not None else None,
                filter=_expression(predicates, file.schema),
                batch_size=batch_size,
                batch_readahead=0,
                fragment_readahead=0,
                use_threads=False,
            )
            continue
        with pq.ParquetFile(path, pre_buffer=False) as file:
            indices = row_groups.get(path) if row_groups is not None else None
            if predicates and remaining is None:
                indices = eligible_groups(
                    Footer(path, file.schema_arrow, file.metadata, 0), predicates, indices
                )
            projected = (
                list(dict.fromkeys([*columns, *(p[0] for p in predicates)]))
                if columns is not None
                else None
            )
            expression = _expression(predicates, file.schema_arrow)
            for batch in file.iter_batches(
                columns=projected,
                row_groups=indices,
                batch_size=min(batch_size, remaining) if remaining is not None else batch_size,
            ):
                if remaining is not None:
                    batch = batch.slice(0, remaining)
                    remaining -= batch.num_rows
                if expression is not None:
                    filtered = pa.Table.from_batches([batch]).filter(expression)
                    if columns is not None:
                        filtered = filtered.select(columns)
                    yield from filtered.to_batches()
                else:
                    yield batch
                if remaining == 0:
                    break


def _display_type(dtype: Any, pa: Any) -> Any:
    if pa.types.is_decimal256(dtype):
        return pa.string()
    if pa.types.is_struct(dtype):
        return pa.struct([field.with_type(_display_type(field.type, pa)) for field in dtype])
    if (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
    ):
        child = dtype.value_field.with_type(_display_type(dtype.value_type, pa))
        if pa.types.is_fixed_size_list(dtype):
            return pa.list_(child, dtype.list_size)
        return pa.large_list(child) if pa.types.is_large_list(dtype) else pa.list_(child)
    return dtype


def _display_array(array: Any, dtype: Any, target: Any, pa: Any) -> Any:
    if dtype == target:
        return array
    if pa.types.is_decimal256(dtype):
        return array.cast(target)
    if pa.types.is_struct(dtype):
        return pa.StructArray.from_arrays(
            [
                _display_array(array.field(index), field.type, target[index].type, pa)
                for index, field in enumerate(dtype)
            ],
            fields=list(target),
            mask=array.is_null(),
        )
    values = _display_array(array.values, dtype.value_type, target.value_type, pa)
    if pa.types.is_fixed_size_list(dtype):
        return pa.FixedSizeListArray.from_arrays(values, type=target, mask=array.is_null())
    cls = pa.LargeListArray if pa.types.is_large_list(dtype) else pa.ListArray
    return cls.from_arrays(array.offsets, values, type=target, mask=array.is_null())


def preview(
    files: Sequence[Path],
    *,
    n: int = 10,
    columns: Sequence[str] | None = None,
    predicates: Sequence[Predicate] = (),
    input_rows: int | None = None,
    footers: Sequence[Footer] = (),
    row_groups: Mapping[Path, Sequence[int]] | None = None,
) -> Any:
    """Return a bounded physical-order Polars preview with exact Decimal256 strings."""
    if n < 0:
        raise ValueError("n must be nonnegative.")
    pl: Any = import_module("polars")

    pa, _ = arrow_modules()
    available = schema(files, footers=footers)
    selected = (
        pa.schema([available.field(name) for name in columns]) if columns is not None else available
    )
    chunks: list[Any] = []
    remaining = n
    if remaining:
        for batch in batches(
            files,
            columns=columns,
            batch_size=min(max(n, 256) if predicates else n, 65_536),
            bounded=True,
            row_groups=row_groups,
            predicates=predicates,
            input_rows=input_rows,
        ):
            chunks.append(batch.slice(0, remaining))
            remaining -= min(remaining, batch.num_rows)
            if remaining == 0:
                break
    table = pa.Table.from_batches(chunks, schema=selected)
    for index, field in enumerate(table.schema):
        display = _display_type(field.type, pa)
        if display != field.type:
            array = _display_array(table.column(index).combine_chunks(), field.type, display, pa)
            table = table.set_column(index, field.with_type(display), array)
    return pl.from_arrow(table)


def native_columns(arrow_schema: Any) -> list[str]:
    pa, _ = arrow_modules()
    return [field.name for field in arrow_schema if _display_type(field.type, pa) == field.type]


def scan(
    files: Sequence[Path],
    *,
    columns: Sequence[str] | None = None,
    predicates: Sequence[Predicate] = (),
    footers: Sequence[Footer] = (),
) -> Any:
    """Construct a lazy Polars query without reading records."""
    pl: Any = import_module("polars")

    pa, _ = arrow_modules()
    available = schema(files, footers=footers)
    selected = list(available) if columns is None else [available.field(name) for name in columns]
    unsupported = [field.name for field in selected if _display_type(field.type, pa) != field.type]
    if unsupported:
        raise ValueError(
            "Polars cannot scan Decimal256 projections: "
            + ", ".join(unsupported)
            + ". Select other columns or use .batches() for exact Arrow values."
        )
    query = pl.scan_parquet([str(path) for path in files], hive_partitioning=False)
    for name, op, value in predicates:
        dtype = available.field(name).type
        field = pl.col(name)
        if pa.types.is_timestamp(dtype):
            if dtype.unit != "ns":
                raise ValueError("Time predicates require nanosecond timestamps.")
            value = pl.lit(value, dtype=pl.Int64).cast(pl.Datetime("ns", dtype.tz))
        query = query.filter(
            field == value if op == "=" else field >= value if op == ">=" else field < value
        )
    return query.select([field.name for field in selected])
