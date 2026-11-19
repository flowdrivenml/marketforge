"""Metadata and row-selection tests with tiny local Parquet publications."""

import json
import shutil
from collections.abc import Iterator
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest
from docs.content.examples.notebooks.dataset_fixtures import ID_BASE, PRICE, START, build

from forge.datasets import Dataset, DatasetCatalog, MarketData, parquet
from forge.datasets import catalog as catalog_module
from forge.datasets.metadata import timestamp_ns, utc
from forge.lab.inspection import compare, inspect


@pytest.fixture
def market(tmp_path: Path) -> MarketData:
    build(tmp_path)
    return MarketData(data_root=tmp_path)


def test_file_selection_and_same_membership(market: MarketData) -> None:
    catalog = market.discover()
    assert len(catalog) == 4
    assert list(catalog) == sorted(catalog.entries, key=lambda item: item.path)
    assert isinstance(catalog[:1], DatasetCatalog)
    assert len(catalog.find(exchange="okx", symbol="ETHUSDT")) == 0
    assert len(catalog.find(symbol="ethusdt")) == 0
    assert len(catalog.find(symbol="ETH", symbol_match="contains")) == 2
    assert len(catalog.find(symbol="ETH")) == 0
    assert len(catalog.find(exchange=["okx", "binance"], data_type=["l2", "trade"])) == 3
    assert len(catalog.find(instrument_type="spot", market_category="linear")) == 0
    with pytest.raises(ValueError, match="found 2"):
        catalog.find(symbol="ETHUSDT").one()
    merged = catalog.find(operation="merge", start=START, end=START + 1)
    assert merged.overview()["physical_rows"] == 3
    assert merged.one().data_type == "combined"
    okx = merged.find(exchange="okx")
    assert okx.find(symbol="ETHUSDT").entries == ()
    record = okx.records()[0]
    assert len(cast(list[object], record["memberships"])) == 2
    assert len(cast(list[object], record["matching_memberships"])) == 1
    assert len(catalog.find(instrument_id=ID_BASE, data_type="trade")) == 2
    assert catalog.summary().height == 4
    assert (
        catalog.find(path=merged.one().path, engine_publication_id="501", min_rows=3).one()
        == merged.one()
    )
    assert catalog.find(publication_path=merged.one().path.parent).one() == merged.one()


def test_exact_intervals_and_physical_statistics(market: MarketData) -> None:
    catalog = market.discover()
    instant = catalog.find(operation="normalize", data_type="depth").one()
    assert instant.observed == (START + 1, START + 1)
    assert not catalog.find(path=instant.path, end=START + 1)
    assert catalog.find(path=instant.path, start=START + 1, end=START + 2).one() == instant
    assert not catalog.find(path=instant.path, end=START + 1, interval="envelope_contained")
    assert (
        catalog.find(path=instant.path, end=START + 2, interval="envelope_contained").one()
        == instant
    )
    assert timestamp_ns("2026-10-27T01:00:00.000000001+01:00") == START
    assert utc(-1) == "1969-12-31 23:59:59.999999999Z"
    with pytest.raises(ValueError, match="timezone-aware"):
        timestamp_ns("2026-10-27T00:00:00.000000001")
    report = inspect(catalog.find(operation="merge").one())
    assert report.timestamps()[0]["min_ns"] == START
    assert report.row_groups()[0]["rows"] + report.row_groups()[1]["rows"] == 3
    leaf = next(
        row
        for row in report.columns()
        if row["column"].startswith("depth.changes") and row["column"].endswith("price")
    )
    assert leaf["min"] == Decimal("95000.125")
    assert not cast(list[dict[str, Any]], compare(instant, report.target)["comparisons"])[0][
        "schema_compatible"
    ]


def test_mixed_rows_exact_projection_and_bounded_profile(market: MarketData) -> None:
    files = market.discover().find(operation="merge")
    with pytest.raises(ValueError, match="found 2"):
        files.one().select()
    stream = files.find(exchange="binance").one().select()
    assert stream.info()["matching_rows"] is None
    assert stream.info()["physical_rows"] == 3
    assert stream.head(3, columns=["event_kind", "trade"]).to_dicts()[0]["trade"]["price"] == str(
        PRICE
    )
    assert stream.head(8, columns=["event_timestamp_ns"], start=START + 1).to_dicts() == [
        {"event_timestamp_ns": START + 2}
    ]
    assert (
        stream.head(columns=["symbol"], start=START + 100, time_field="system_timestamp_ns").height
        == 2
    )
    arrow = next(stream.batches(columns=["trade"]))
    assert arrow.column(0)[0].as_py()["price"] == PRICE
    assert stream.scan(columns=["event_timestamp_ns"]).collect()[
        "event_timestamp_ns"
    ].to_list() == [START, START + 2]
    with pytest.raises(ValueError, match="Decimal256"):
        stream.scan(columns=["trade"])
    assert inspect(stream).profile(["event_timestamp_ns"], input_rows=2)["rows_examined"] == 1
    standalone = market.discover().find(path=market.paths.datasets_dir / "unknown.parquet").one()
    assert not standalone.streams_by_key
    assert standalone.head(1).item() == 42
    normalized = market.discover().find(operation="normalize", data_type="trades").one()
    assert (
        cast(dict[str, Any], inspect(normalized).profile(["price"], input_rows=1)["columns"])[
            "price"
        ]["min"]
        == PRICE
    )


def test_diagnostics_scope_and_registry_evidence(market: MarketData) -> None:
    root = market.paths.datasets_dir
    for name, body in (
        ("bad", "{"),
        ("future", '{"protocol_version": 99, "dataset_id": 1, "content_type": "trades"}'),
    ):
        (root / name).mkdir()
        (root / name / "manifest.json").write_text(body)
    (root / "unreadable.parquet").write_text("no parquet footer")
    staged = root / ".777.staging.1"
    shutil.copytree(root / "normalized/101", staged)
    catalog = market.discover()
    assert {d.code for d in catalog.diagnostics} == {
        "malformed_manifest",
        "unsupported_version",
        "unreadable_file",
        "missing_identity",
    }
    assert len(catalog.find(status="staging")) == 0
    assert len(market.discover(include_staging=True).find(status="staging")) == 1
    merged = catalog.find(operation="merge").one()
    row: dict[str, Any] = {
        "id": ID_BASE,
        "publication_id": 31,
        "engine_dataset_id": "501",
        "primary_file": merged.primary_file,
        "status": "complete",
        "storage_path": str(merged.path),
        "instrument_namespace": "demo",
        "summary": {
            "membership_scope": "file",
            "integrity": "physical_validation",
            "integrity_scope": "publication",
            "memberships": [
                dict(
                    s.identity or {},
                    kind=s.kind,
                    start_timestamp_ns=START,
                    end_timestamp_ns=START + 2,
                )
                for s in merged.streams_by_key
            ],
        },
    }
    matched = (
        market.discover(registry_records=[row], registry_context="local testing")
        .find(dataset_id=ID_BASE)
        .one()
    )
    assert matched.registry_context == "local testing"
    assert matched.publication_id == 31
    assert matched.integrity_scope == "publication"
    assert matched.select(exchange="okx").predicates == (
        ("stream_rank", "=", 1),
        ("event_kind", "=", 1),
    )
    row["engine_dataset_id"] = "wrong"
    conflicted = market.discover(registry_records=[row], registry_context="local testing")
    assert not conflicted.find(dataset_id=ID_BASE)
    assert conflicted.find(operation="merge").one().metadata_scope == "conflicting"


def test_format_and_repeat_render_do_not_read(
    market: MarketData, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = catalog_module.inventory
    reads = Mock(side_effect=original)
    monkeypatch.setattr(catalog_module, "inventory", reads)
    catalog = market.discover()
    assert reads.call_count == 3
    target = catalog.find(operation="merge").one()
    identity = {**(target.streams_by_key[0].identity or {}), "symbol": "<script>evil</script>"}
    poisoned = replace(
        target, streams_by_key=(replace(target.streams_by_key[0], identity=identity),)
    )
    report = inspect(poisoned)
    many = DatasetCatalog((poisoned,) * 15)

    def fail(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("Unexpected read during display or lazy query construction")

    monkeypatch.setattr(parquet, "footer", fail)
    monkeypatch.setattr(parquet, "batches", fail)
    monkeypatch.setattr(Path, "stat", fail)
    for _ in range(2):
        assert "Showing 12 of 15" in many._repr_html_()  # pyright: ignore[reportPrivateUsage]
        html = report._repr_html_()  # pyright: ignore[reportPrivateUsage]
        assert "&lt;script&gt;evil&lt;/script&gt;" in html
        assert "<script>evil" not in html
        assert "decimal256(76, 28)" in html
        assert "Row groups" in html
        assert "Inspection" in repr(report)
    assert type(target.scan(columns=["event_kind"])).__name__ == "LazyFrame"


def test_open_group_and_unresolved_ranks(market: MarketData) -> None:
    file = market.discover().find(operation="merge").one()
    assert isinstance(file, Dataset)
    group = market.open(file.path.parent)
    assert group.metadata_scope == "publication grouping"
    assert group.dataset_id is None
    assert group.select(exchange="binance").head(columns=["event_kind"]).height == 2
    manifest_path = file.path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    # Rank 1 could be present in the footer range 0..2, but no row group proves it.
    pa, pq = parquet.arrow_modules()
    source = pq.read_table(file.path)
    ranks = source.schema.get_field_index("stream_rank")
    source = source.set_column(
        ranks, source.schema.field(ranks), pa.array([0, 2, 0], type=pa.uint32())
    )
    pq.write_table(source, file.path)
    manifest["files"][0]["size_bytes"] = file.path.stat().st_size
    manifest["streams"].append({**manifest["streams"][1], "stream_rank": 2, "kind": "depth"})
    manifest_path.write_text(json.dumps(manifest))
    discovered = market.discover()
    assert discovered.find(operation="merge").one().metadata_scope == "partial membership"
    assert any(
        issue.path == file.path and issue.code == "missing_identity"
        for issue in discovered.diagnostics
    )


def test_scope_and_missing_stats(market: MarketData) -> None:
    pa, pq = parquet.arrow_modules()
    test_dir = market.paths.data_root / "test/normalized"
    test_dir.mkdir(parents=True)
    path = test_dir / "no-statistics.parquet"
    pq.write_table(
        pa.table({"event_timestamp_ns": [START], "missing": [None]}), path, write_statistics=False
    )
    found = market.discover(scope="tests")
    assert len(found) == 1
    assert len(market.discover(scope="all")) == 5
    assert not found.find(start=START)
    report = inspect(found.one())
    assert report.timestamps()[0]["min_ns"] is None
    assert report.columns()[1]["null_count"] is None
    assert report.target.head(1, columns=["event_timestamp_ns"]).item() == START


def test_instrument_namespace_and_source_precision(market: MarketData) -> None:
    files = market.discover()
    merged = files.find(
        operation="merge",
        exchange="binance",
        instrument_namespace="marketforge:testing:historical:v1",
    ).one()
    view = merged.select()
    report = inspect(view)
    assert report.stream is view
    assert (
        report.target.info()["identity"]["instrument_namespace"]
        == "marketforge:testing:historical:v1"
    )
    assert view.head(columns=["event_timestamp_ns"], end=START + 1).item() == START


def test_outward_truncated_footer_bounds_use_manifest_stream_id(
    market: MarketData, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    original = parquet.Footer.chunks
    target = market.discover().find(operation="normalize", data_type="trades").one()
    exact = target.streams_by_key[0].identity
    assert exact is not None and isinstance(stream_id := exact["stream_id"], str)
    # Rust's Parquet writer can retain outward-truncated byte-array bounds.
    prefix = stream_id[:12]
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)

    def chunks(item: parquet.Footer, name: str) -> Iterator[Any]:
        if item.path == target.path and name == "stream_id":
            stat = SimpleNamespace(has_min_max=True, min=prefix, max=upper)
            yield SimpleNamespace(physical_type="BYTE_ARRAY", statistics=stat)
        else:
            yield from original(item, name)

    monkeypatch.setattr(parquet.Footer, "chunks", chunks)
    current = market.discover().find(path=target.path).one()
    assert current.select().identity == exact


def test_conflicting_merge_venue_keeps_physical_file(market: MarketData) -> None:
    merged = market.discover().find(operation="merge").one()
    manifest = merged.path.parent / "manifest.json"
    data = json.loads(manifest.read_bytes())
    data["streams"][0]["exchange"] = "another_venue"
    manifest.write_text(json.dumps(data))
    result = market.discover()
    item = result.find(operation="merge").one()
    assert item.info()["rows"] == 3
    assert not result.find(operation="merge", exchange="another_venue")
    assert item.select(symbol="ETHUSDT").head(columns=["event_kind"]).height == 2
    assert item.metadata_scope == "conflicting"
    assert any(
        d.path == merged.path and d.code == "conflicting_metadata" for d in result.diagnostics
    )
