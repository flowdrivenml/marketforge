"""Inspect causal physical publications using the Rust manifest and Arrow layouts."""

import json
from pathlib import Path
from typing import Any

import pytest
from docs.content.examples.notebooks.dataset_fixtures import ID_BASE, PRICE, START, build

from forge.datasets import MarketData, parquet
from forge.datasets.registration import merged_file_memberships


@pytest.fixture
def causal(tmp_path: Path) -> tuple[MarketData, Path, dict[str, Any]]:
    build(tmp_path)
    pa, pq = parquet.arrow_modules()
    parent = tmp_path / "datasets/merged"
    legacy = parent / "501"
    root = parent / "502"
    manifest = json.loads((legacy / "manifest.json").read_bytes())
    table = pq.read_table(legacy / manifest["files"][0]["path"])
    schema = table.schema.append(pa.field("timeline_sequence", pa.uint64(), False)).append(
        pa.field("reconstruction_only", pa.bool_(), False)
    )
    trade, depth, later_trade = table.to_pylist()
    depth_stream, trade_stream = manifest["streams"][1], manifest["streams"][0]
    # All three ranks are in one row group. Its endpoints cannot establish rank 2
    # or rule out rank 3, which occurs only in the second partition.
    parts = [
        [
            dict(depth, stream_rank=2, event_timestamp_ns=START - 1),
            trade,
            dict(later_trade, stream_rank=5),
            dict(depth, stream_rank=2),
        ],
        [dict(later_trade, stream_rank=3, event_timestamp_ns=START + 3)],
    ]
    files: list[dict[str, Any]] = []
    for partition, rows in enumerate(parts):
        for index, row in enumerate(rows, sum(file["rows"] for file in files)):
            row.update(timeline_sequence=index, reconstruction_only=index == 0)
        name = f"timeline/partition-{partition:06}/part-000000.parquet"
        file = root / name
        file.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), file)
        timestamps = [row["event_timestamp_ns"] for row in rows]
        files.append(
            dict(
                path=name,
                size_bytes=file.stat().st_size,
                rows=len(rows),
                partition_id=partition,
                start_timestamp_ns=min(timestamps),
                end_timestamp_ns=max(timestamps),
            )
        )
    manifest.update(
        job_id=502,
        physical_schema_version=2,
        timeline_ordering="causal",
        reconstruction_events=1,
        streams=[
            trade_stream,
            dict(depth_stream, stream_rank=2),
            dict(trade_stream, stream_rank=3),
            dict(trade_stream, stream_rank=5),
        ],
        time_range={"start_ns": START, "end_ns": START + 4},
        events_written=5,
        trade_events=3,
        depth_events=2,
        canonical_rows=5,
        files_written=2,
        row_groups=2,
        start_timestamp_ns=START - 1,
        end_timestamp_ns=START + 3,
        files=files,
    )
    (root / "manifest.json").write_text(json.dumps(manifest))
    return MarketData(data_root=tmp_path), root, manifest


def test_causal_catalog_and_rows(causal: tuple[MarketData, Path, dict[str, Any]]) -> None:
    market, root, manifest = causal
    catalog = market.discover(scope="committed")
    assert catalog.find(engine_publication_id=501).one().streams().height == 2
    found = catalog.find(operation="merge", engine_publication_id=502)
    assert [item.info()["partition_id"] for item in found] == [0, 1]
    first = found.entries[0]
    assert first.reason is None
    assert first.streams()["stream_rank"].to_list() == [0, 2, 5]
    assert first.info()["observed_start_ns"] == START - 1
    assert first.schema().field("trade").type.field("price").type.precision == 76

    stream_id = manifest["streams"][1]["stream_id"]
    matches = found.find(
        exchange="okx",
        symbol="BTC-USDT-SWAP",
        instrument_type="perpetual",
        market_category="linear",
        data_type="order_book_l2",
        stream_id=stream_id,
    )
    depth = matches.one().select(instrument_id=ID_BASE + 1)
    assert not found.find(exchange="okx", data_type="trade")
    assert depth.info()["reconstruction_rows"] == 1
    assert depth.head()["reconstruction_only"].to_list() == [True, False]
    assert (
        depth.head().to_dicts()[0]["depth"]["changes"][0]["price"]
        == "95000.1250000000000000000000000000"
    )
    assert depth.scan(columns=["event_timestamp_ns"], start=START).collect().item() == START + 1
    assert [
        row["event_timestamp_ns"] for batch in depth.batches() for row in batch.to_pylist()
    ] == [
        START - 1,
        START + 1,
    ]

    second = found.entries[1]
    trade = second.trades()
    assert trade.head().to_dicts()[0]["trade"]["price"] == str(PRICE)
    assert next(trade.batches()).to_pylist()[0]["trade"]["price"] == PRICE
    assert first.head()["timeline_sequence"].to_list() == [0, 1, 2, 3]
    assert first.scan(
        columns=["event_timestamp_ns"], end=START + 1
    ).collect().to_series().to_list() == [
        START - 1,
        START,
    ]
    assert market.open(root).select(stream_id=stream_id).head().height == 2


def test_registry_enrichment_and_provenance(
    causal: tuple[MarketData, Path, dict[str, Any]],
) -> None:
    market, root, manifest = causal
    name = manifest["files"][0]["path"]
    members = merged_file_memberships(root, manifest, [])[name]
    record = dict(
        id=84,
        publication_id=42,
        engine_dataset_id="502",
        primary_file=name,
        storage_path=str(root / name),
        summary={"membership_scope": "file", "memberships": members},
    )
    catalog = market.discover(registry_records=[record], registry_context="fixture")
    file = catalog.find(publication_id=42, data_type="depth").one()
    assert file.select().info()["reconstruction_rows"] == 1
    assert file.select().head()["event_timestamp_ns"].to_list() == [START - 1, START + 1]

    members[0]["source_publication_id"] = "102"
    catalog = market.discover(registry_records=[record], registry_context="fixture")
    assert not catalog.find(publication_id=42, data_type="depth")
    assert any(
        d.code == "conflicting_metadata" and d.path == root / name for d in catalog.diagnostics
    )


@pytest.mark.parametrize(
    ("schema", "ordering", "timeline"),
    [(99, "causal", 7), (2, "chronological", None), (1, "causal", None)],
)
def test_unknown_version_diagnostic(
    causal: tuple[MarketData, Path, dict[str, Any]],
    schema: int,
    ordering: str,
    timeline: int | None,
) -> None:
    market, root, manifest = causal
    manifest.update(physical_schema_version=schema, timeline_ordering=ordering)
    if timeline is not None:
        manifest["timeline_schema_version"] = timeline
    (root / "manifest.json").write_text(json.dumps(manifest))
    catalog = market.discover()
    assert not catalog.find(engine_publication_id=502)
    diagnostic = next(d for d in catalog.diagnostics if d.code == "unsupported_version")
    for field in (
        "protocol_version=1",
        f"physical_schema_version={schema}",
        f"timeline_ordering={ordering!r}",
    ):
        assert field in diagnostic.message
    if timeline is not None:
        assert f"timeline_schema_version={timeline}" in diagnostic.message


@pytest.mark.parametrize(
    ("column", "value"),
    [("stream_rank", 4), ("dataset_id", 102), ("event_kind", 1), ("reconstruction_only", True)],
)
def test_discovery_rejects_conflicting_physical_provenance(
    causal: tuple[MarketData, Path, dict[str, Any]], column: str, value: int | bool
) -> None:
    market, root, manifest = causal
    entry = manifest["files"][0]
    path = root / entry["path"]
    pa, pq = parquet.arrow_modules()
    original = pq.read_table(path)
    rows = original.to_pylist()
    # The trade at rank 0 has exactly this stream identity and source.
    rows[1][column] = value
    pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), path)
    entry["size_bytes"] = path.stat().st_size
    (root / "manifest.json").write_text(json.dumps(manifest))
    catalog = market.discover()
    assert not catalog.find(path=path, data_type="trades")
    assert any(d.code == "conflicting_metadata" and d.path == path for d in catalog.diagnostics)


def test_metadata_only_summary_does_not_read_event_rows(
    causal: tuple[MarketData, Path, dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    market, root, _ = causal

    def no_events(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("Metadata-only discovery attempted to read event rows.")

    monkeypatch.setattr(parquet, "batches", no_events)
    catalog = market.discover(metadata_only=True).find(engine_publication_id=502)
    assert catalog.summary()["rows"].sum() == 5
    publication = market.open(root, metadata_only=True, strict=True)
    assert [part.info()["rows"] for part in publication.partitions()] == [4, 1]
