"""Publication-scoped physical inspection and selective exact Arrow reads."""

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from forge.datasets import Dataset, DatasetCatalog, parquet
from forge.datasets.discovery import open_dataset


@pytest.fixture
def physical(tmp_path: Path) -> tuple[Dataset, Path]:
    pa, pq = parquet.arrow_modules()
    root = tmp_path / "converted"
    files: list[dict[str, object]] = []
    for index, partition in enumerate((0, 0, 1)):
        location = root / f"partition-{partition:06}" / f"part-{index:06}.parquet"
        location.parent.mkdir(parents=True, exist_ok=True)
        # Causal order deliberately regresses in event time.
        time = [10 + index * 10, 12 + index * 10, 11 + index * 10, 15 + index * 10]
        decimal = pa.decimal256(76, 28)
        trade = pa.struct([pa.field("price", decimal)])
        table = pa.table(
            {
                "event_timestamp_ns": pa.array(time, type=pa.int64()),
                "instrument_id": pa.array([7, 7, 8, 8], type=pa.int64()),
                "stream_id": ["x.trades", "x.depth", "y.trades", "y.depth"],
                "event_kind": pa.array([0, 1, 0, 1], type=pa.uint8()),
                "timeline_sequence": pa.array(
                    list(range(index * 4, index * 4 + 4)), type=pa.uint64()
                ),
                "trade": pa.array(
                    [
                        {"price": Decimal("1.0000000000000000000000000001")},
                        None,
                        {"price": None},
                        None,
                    ],
                    type=trade,
                ),
                "depth": pa.array(
                    [None, {"price": Decimal("2")}, None, {"price": Decimal("3")}], type=trade
                ),
                "reconstruction_only": [False, True, False, False],
            }
        )
        pq.write_table(table, location, row_group_size=2)
        files.append(
            {
                "path": location.relative_to(root).as_posix(),
                "size_bytes": location.stat().st_size,
                "rows": len(time),
                "row_groups": 2,
            }
        )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "protocol_version": 1,
                "conversion_schema_version": 1,
                "job_id": 99,
                "integrity": {"status": "pass"},
                "input": {"merge_job_id": 98},
                "conversion": {"target_quote": "USDT"},
                "output": {"content_type": "combined", "files": files, "files_written": 3},
            }
        )
    )
    return open_dataset(root, strict=True), root


def test_physical_locators_and_footer_inventory(physical: tuple[Dataset, Path]) -> None:
    dataset, root = physical
    assert dataset.operation == "convert" and dataset.output_ref is None
    assert dataset.lineage()["input"]["merge_job_id"] == 98
    first, second = dataset.partitions()
    assert [first.reference, second.reference] == ["partition:0", "partition:1"]
    assert first.info()["files"] == 2 and first.info()["rows"] == 8
    assert [row["rows"] for row in first.row_groups()] == [2, 2, 2, 2]
    assert first.info()["observed_start_ns"] == 10
    assert dataset.query(partition=["partition:1", "partition:0"]).head(
        2, columns=["timeline_sequence"]
    ).to_series().to_list() == [0, 1]
    assert first.select_row_groups([1, 0, 1]).head(
        10, columns=["timeline_sequence"]
    ).to_series().to_list() == [0, 1, 2, 3]
    assert first.row_group(0, file=2).head(
        5, columns=["timeline_sequence"]
    ).to_series().to_list() == [4, 5]
    with pytest.raises(ValueError, match="Unknown row group"):
        first.row_group(2)
    with pytest.raises(ValueError, match="Unknown partition"):
        dataset.partition("partition:99")
    second.footers[0].path.unlink()
    with pytest.raises(ValueError, match="part-000002"):
        open_dataset(root, strict=True)


def test_exact_selection_and_lazy_plan(physical: tuple[Dataset, Path]) -> None:
    dataset, _ = physical
    query = dataset.query(data_type="trades")
    assert query.head(5)["timeline_sequence"].to_list() == [0, 2, 4, 6, 8]
    batches = list(query.batches())
    assert sum(batch.num_rows for batch in batches) == 6
    assert "depth" not in batches[0].schema.names
    assert [value for batch in batches for value in batch["trade"].to_pylist()][:2] == [
        {"price": Decimal("1.0000000000000000000000000001")},
        {"price": None},
    ]
    assert dataset.query(data_type="depth").head(2)["reconstruction_only"].to_list() == [
        True,
        False,
    ]
    query = dataset.query(data_type="trades", instrument_id=8, stream_id="y.trades")
    frame = query.scan(columns=["event_timestamp_ns"], start=11, end=21)
    assert frame.collect().to_series().to_list() == [11]
    assert "SELECTION" in frame.explain()
    assert query.head(10, start=11, end=11 + 1)["event_timestamp_ns"].to_list() == [11]
    assert query.head(10, start=12, end=19).is_empty()
    report = query.explain(start=11, end=12)
    assert [
        file["eligible_row_groups"] for file in cast(list[dict[str, Any]], report["files"])
    ] == [(1,), (), ()]


def test_file_summary_preserves_registry_domains(physical: tuple[Dataset, Path]) -> None:
    dataset, _ = physical
    first = replace(dataset, output_ref="a" * 64, observed=(10, 35))
    second = replace(dataset, output_ref="b" * 64, observed=(5, 30))
    catalog = DatasetCatalog((second, first))
    summary = catalog.find(operation="convert", target_quote="USDT").summary()
    assert summary.columns[:5] == [
        "reference",
        "reference_kind",
        "exchange",
        "market",
        "instrument",
    ]
    assert summary["reference"].to_list() == ["a" * 64, "b" * 64]
    assert summary["reference_kind"].to_list() == ["indexed output", "indexed output"]
    assert summary["publication_id"].to_list() == [None, None]


def test_normalized_file_locator_uses_manifest_order(physical: tuple[Dataset, Path]) -> None:
    dataset, _ = physical
    sample = replace(dataset, operation="normalize", file_partitions=(None, None, None))
    assert [part.reference for part in sample.partitions()] == ["file:1", "file:2", "file:3"]


def test_strict_open_reports_footer_conflict(physical: tuple[Dataset, Path]) -> None:
    _, root = physical
    path = root / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["output"]["files"][0]["row_groups"] = 4
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="row-group count"):
        open_dataset(root, strict=True)
