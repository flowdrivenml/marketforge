"""Check the first indexed Bybit book observation against its saved boundary."""
import json
import sys
from decimal import Decimal
from pathlib import Path

from pyarrow.parquet import ParquetFile


def main(result_path: Path) -> None:
    result = json.loads(result_path.read_text())
    for job in result["jobs"]:
        path = Path(job["manifest_path"])
        manifest = json.loads(path.read_text())
        if manifest["content_type"] == "depth":
            break
    else:
        raise SystemExit("The result has no depth publication.")

    (stream,) = manifest["streams"]
    directory = path.parent / stream["directory"]
    event_index = ParquetFile(directory / "events.parquet")
    (event,) = next(event_index.iter_batches(batch_size=1)).to_pylist()
    boundary = json.loads((directory / "boundary.json").read_text())
    initial = boundary["sources"][0]["boundary"]["initial"]
    assert event["canonical_row_offset"] == 0
    assert event["event_timestamp_ns"] == initial["timestamp_ns"]
    assert event["sequence_end"] == initial["sequence"]

    # The first observation in this example fits in its first primary part.
    first = ParquetFile(path.parent / manifest["files"][0]["path"])
    count = event["canonical_row_count"]
    rows = next(first.iter_batches(batch_size=count)).to_pylist()
    actual = {(row["side"], row["price"]): row["quantity_base"] for row in rows}
    expected = {
        (side, Decimal(level["price"])): Decimal(level["quantity_base"])
        for side in ("bid", "ask")
        for level in initial["state"][side + "s"]
    }
    assert len(rows) == len(actual) == count
    assert actual == expected
    print(f"Verified initial observation {event['event_ordinal']} with {count} levels.")
    print(f"Indexed observations: {event_index.metadata.num_rows:,}")
    print(f"Boundary coverage: {initial['coverage']}")
    print(f"External historical coverage: "
          f"{manifest['processing_quality']['external_historical_coverage_established']}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
