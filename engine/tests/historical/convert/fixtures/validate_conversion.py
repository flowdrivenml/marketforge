from __future__ import annotations

from collections import Counter
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path("/home/pasha/projects/bybit-history-downloader")
INPUT = ROOT / "data/.work/convert-validation/input"
OUTPUT = ROOT / "data/.work/convert-validation/multihop"

SCALE = Decimal("1e-28")
MAX_STALENESS_NS = 5_000_000_000

IDENTITY = 0
CONVERTED = 1
UNAVAILABLE = 2

DIRECT = 0
INVERSE = 1


def event_key(row):
    return (
        row["event_timestamp_ns"],
        row["stream_rank"],
        row["task_id"],
        row["event_ordinal"],
    )


def reference_key(ref):
    return (
        ref["event_timestamp_ns"],
        ref["stream_rank"],
        ref["task_id"],
        ref["event_ordinal"],
    )


def quantize(value):
    return value.quantize(SCALE, rounding=ROUND_HALF_EVEN)


def validate_row(row):
    status = row["conversion_status"]

    price = row["price"]
    quantity = row["quantity_quote"]

    price_target = row["price_target"]
    quantity_target = row["quantity_quote_target"]

    rate = row["conversion_rate"]
    path = row["conversion_path"]
    references = row["conversion_references"]

    if status == IDENTITY:
        assert rate == Decimal(1)
        assert price_target == price
        assert quantity_target == quantity
        assert path == [row["target_quote"]]
        assert references == []
        assert row["conversion_failure"] is None
        return

    if status == UNAVAILABLE:
        assert rate is None
        assert price_target is None
        assert quantity_target is None
        assert path is None
        assert references == []
        assert row["conversion_failure"] is not None
        return

    assert status == CONVERTED
    assert references
    assert path
    assert len(path) == len(references) + 1
    assert path[-1] == row["target_quote"]
    assert row["conversion_failure"] is None

    with localcontext() as ctx:
        ctx.prec = 160
        ctx.rounding = ROUND_HALF_EVEN

        # Keep the rate as an exact rational until final rounding.
        numerator = Decimal(1)
        denominator = Decimal(1)

        for ref in references:
            assert reference_key(ref) < event_key(row)

            age = row["event_timestamp_ns"] - ref["event_timestamp_ns"]

            assert 0 <= age <= MAX_STALENESS_NS

            reference_price = ref["reference_price"]

            assert reference_price > 0

            if ref["direction"] == DIRECT:
                numerator *= reference_price

            elif ref["direction"] == INVERSE:
                denominator *= reference_price

            else:
                raise AssertionError(
                    f"Unknown conversion direction: {ref['direction']}"
                )

        exact_rate = numerator / denominator

        expected_rate = quantize(exact_rate)
        expected_price = quantize(price * exact_rate)

        expected_quantity = (
            quantize(quantity * exact_rate)
            if quantity is not None
            else None
        )

        assert rate == expected_rate, (
            f"Rate mismatch: {rate} != {expected_rate}"
        )

        assert price_target == expected_price, (
            f"Price mismatch: {price_target} != {expected_price}"
        )

        assert quantity_target == expected_quantity, (
            f"Quantity mismatch: {quantity_target} != {expected_quantity}"
        )


def load_rows(directory):
    rows = []

    for path in sorted(directory.rglob("*.parquet")):
        parquet = pq.ParquetFile(path)

        for batch in parquet.iter_batches(batch_size=4096):
            rows.extend(batch.to_pylist())

    return rows


def validate_source_preservation(source_rows, converted_rows):
    assert len(source_rows) == len(converted_rows)

    original_fields = set(source_rows[0])

    for index, (source, converted) in enumerate(
        zip(source_rows, converted_rows)
    ):
        for field in original_fields:
            assert source[field] == converted[field], (
                f"Original field changed at row {index}: {field}"
            )


def main():
    source_rows = load_rows(INPUT)
    converted_rows = load_rows(OUTPUT)

    if not converted_rows:
        raise RuntimeError("No converted Parquet rows found")

    validate_source_preservation(source_rows, converted_rows)

    counts = Counter()

    for row in converted_rows:
        validate_row(row)
        counts[row["conversion_status"]] += 1

    print("=" * 80)
    print("MARKETFORGE — INDEPENDENT CONVERSION VALIDATION")
    print("=" * 80)

    print("Source rows :", len(source_rows))
    print("Output rows :", len(converted_rows))

    print("Identity    :", counts[IDENTITY])
    print("Converted   :", counts[CONVERTED])
    print("Unavailable :", counts[UNAVAILABLE])

    assert counts[IDENTITY] > 0
    assert counts[CONVERTED] > 0
    assert counts[UNAVAILABLE] > 0

    print("\nSTATUS: PASS")


if __name__ == "__main__":
    main()
