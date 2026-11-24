"""Forge conversion preparation contracts and provenance without a database mutation."""

import asyncio
import json
import os
import shlex
import shutil
import subprocess
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from forge.cli.commands import conversion as cli_conversion
from forge.cli.main import main
from forge.datasets.parquet import arrow_modules
from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.models import archive_scope_document, archive_scope_key
from forge.historical.conversion import discovery, execution, recipes, registry, service
from forge.historical.conversion.contract import (
    Identity,
    Job,
    Parquet,
    Policy,
    Resources,
    build_job,
)
from forge.historical.conversion.evidence import stream_metadata, verify_merge_inventory
from forge.historical.conversion.planning import references
from forge.historical.engine.managed import checksum, plan, select
from forge.historical.normalization.config import NormalizationRecipe
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

START = 1_793_059_200_000_000_000
END = 1_793_318_400_000_000_000
ENGINE_ID = "2079541492669036549"
SOURCE_CONFIG = """
version = 1
[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-30T00:00:00Z
[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = {kind = "instrument", identifier = "BTCUSDT"}
"""
DIRECT = f"dataset_ids = [21, 22]\nstart_ns = {START}\nend_ns = {END}\n"


@pytest.fixture
def paths(tmp_path: Path) -> ProjectPaths:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths.resolve(root=tmp_path)
    for folder in (
        paths.acquisition_config_dir,
        paths.normalization_recipe_dir,
        paths.workflow_dir("merge"),
    ):
        folder.mkdir(parents=True)
    (paths.acquisition_config_dir / "example.toml").write_text(SOURCE_CONFIG)
    (paths.normalization_recipe_dir / "example.toml").write_text(SOURCE_CONFIG)
    (paths.workflow_dir("merge") / "example.toml").write_text(DIRECT)
    return paths


def invoke(paths: ProjectPaths, *args: str) -> int:
    return main(["--project-root", str(paths.root), "--json", "convert", *args])


@pytest.mark.parametrize("kind", ["merge", "normalize", "acquisition"])
def test_new_and_validate(
    paths: ProjectPaths,
    capsys: pytest.CaptureFixture[str],
    kind: recipes.SourceKind,
) -> None:
    source = recipes.source_path(paths, kind, "example")
    reference = str(source) if kind == "normalize" else "example"
    argv = ("new", "converted", f"--from-{kind}", reference, "--target-quote", "USDT")
    assert invoke(paths, *argv) == 0
    created = json.loads(capsys.readouterr().out)
    path = Path(created["path"])
    original = path.read_bytes()
    assert created["source"]["sha256"] == checksum(source)
    assert invoke(paths, "validate", "converted") == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "valid"
    assert report["recipe"]["interval"] == {"start_ns": START, "end_ns": END}
    assert invoke(paths, *argv) == 2
    assert "already exists" in capsys.readouterr().out
    assert path.read_bytes() == original
    source.write_bytes(source.read_bytes() + b"# edit\n")
    assert invoke(paths, "validate", str(path)) == 2
    assert "fingerprint changed" in capsys.readouterr().out


def test_recipe_cli_rejects_conflicting_arguments_and_legacy(
    paths: ProjectPaths,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        invoke(
            paths,
            "new",
            "x",
            "--from-merge",
            "example",
            "--from-normalize",
            "example",
            "--target-quote",
            "USDT",
        )
    assert error.value.code == 2
    assert invoke(paths, "run", "x", "--testing", "--tui") == 2
    assert "incompatible with global --json" in capsys.readouterr().out
    source = paths.normalization_recipe_dir / "legacy.toml"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("job_id = 12\n")
    assert invoke(paths, "new", "x", "--from-normalize", "legacy", "--target-quote", "USDT") == 2
    assert "Legacy archive-ID" in capsys.readouterr().out


def test_exact_single_job_contract(paths: ProjectPaths, tmp_path: Path) -> None:
    source = publication(paths, combined=False)
    write_trade_publication(source)
    binding = service.prepared_binding(source)
    policy = Policy(target_quote="USDT", max_staleness_ns=60_000_000_000, max_hops=2)
    job = build_job(
        int(ENGINE_ID),
        tmp_path / "merged",
        tmp_path / "converted",
        policy,
        Resources(),
        [Identity(exchange="bybit", symbol="BTCUSDT", instrument_kind="spot")],
        tmp_path / "trade-reference",
        source_binding=binding,
        external_binding=binding,
        catalog_sha256=checksum(Path(source["root"]) / "manifest.json"),
        parquet=Parquet(row_group_target_mb=1, file_target_mb=4, compression_level=3),
    )
    engine = json.loads(job.model_dump_json())
    assert set(engine) == {
        "schema_version",
        "source",
        "external_reference",
        "catalog_sha256",
        "parquet",
        "job_id",
        "operation",
        "input_merge",
        "conversion",
        "resources",
        "output",
    }
    assert engine["job_id"] == int(ENGINE_ID) and engine["operation"] == "convert"
    assert engine["output"] == {"dataset_path": str(tmp_path / "converted")}
    assert set(engine["resources"]) == {"workers", "memory_limit_mb", "max_open_files"}
    config = engine["conversion"]
    assert set(config) == {
        "target_quote",
        "price_source",
        "max_staleness_ns",
        "max_hops",
        "instrument_kinds",
        "references",
        "reference_merge",
        "path_policy",
    }
    assert config["references"] == [
        {"exchange": "bybit", "symbol": "BTCUSDT", "instrument_kind": "spot", "contract_kind": None}
    ]
    assert config["price_source"] == "last_trade"
    assert Job.model_validate(engine) == job
    with pytest.raises(ValidationError):
        Job.model_validate({**engine, "schema_version": 3})
    with pytest.raises(ValidationError, match="Extra inputs"):
        Job.model_validate({**engine, "unexpected": True})
    with pytest.raises(ValidationError):
        Policy(target_quote="USDT", max_staleness_ns=0, max_hops=5)
    with pytest.raises(ValidationError):
        recipes.Selection(file_dataset_ids=[360], publication_ids=[60])


def stream(symbol: str, base: str, quote: str, rank: int, kind: str = "trade") -> dict[str, Any]:
    return {
        "stream_id": f"bybit.{symbol}.{kind}",
        "stream_rank": rank,
        "dataset_id": "4968760985013963110",
        "instrument_namespace": "test:conversion",
        "instrument_id": {"BTCUSDT": 1, "BTCUSDC": 65, "ETHUSDT": 2, "ETHBTC": 4}[symbol],
        "kind": kind,
        "identity": Identity(exchange="bybit", symbol=symbol, instrument_kind="spot").model_dump(),
        "base_currency": base,
        "quote_currency": quote,
        "instrument_type": "spot",
        "market_observed": True,
        "observed": True,
    }


def publication(paths: ProjectPaths, *, combined: bool, key: int = 60) -> dict[str, Any]:
    return {
        "registry_publication_id": key,
        "engine_publication_id": ENGINE_ID,
        "root": str(paths.datasets_dir / "merged" / str(key)),
        "mode": "combined" if combined else "trades",
        "physical_schema_version": 2 if combined else 1,
        "timeline_ordering": "causal" if combined else "chronological",
        "events": 1,
        "streams": (
            [
                stream("BTCUSDT", "BTC", "USDT", 0),
                stream("BTCUSDT", "BTC", "USDT", 1, "depth"),
                stream("BTCUSDC", "BTC", "USDC", 2),
                stream("ETHBTC", "ETH", "BTC", 3, "depth"),
                stream("ETHUSDT", "ETH", "USDT", 4),
            ]
            if combined
            else [stream("BTCUSDT", "BTC", "USDT", 0)]
        ),
    }


def write_trade_publication(publication: dict[str, Any]) -> None:
    """One complete physical trade with known Arrow types and committed pins."""
    pa, pq = arrow_modules()
    root = Path(publication["root"])
    relative = "timeline/partition-000000/part-000000.parquet"
    file = root / relative
    file.parent.mkdir(parents=True, exist_ok=True)
    row = publication["streams"][0]
    decimal = pa.decimal256(76, 28)
    fields = [
        ("event_timestamp_ns", pa.int64(), False, START),
        ("system_timestamp_ns", pa.int64(), True, None),
        ("exchange", pa.string(), False, "bybit"),
        ("instrument_id", pa.int64(), False, row["instrument_id"]),
        ("symbol", pa.string(), False, "BTCUSDT"),
        ("stream_id", pa.string(), False, row["stream_id"]),
        ("trade_id", pa.string(), True, "trade-1"),
        ("sequence", pa.uint64(), True, None),
        ("side", pa.string(), False, "buy"),
        ("price", decimal, False, Decimal("100")),
        ("quantity_base", decimal, True, Decimal("2")),
        ("quantity_quote", decimal, True, Decimal("200")),
        ("quantity_contracts", decimal, True, None),
        ("is_rpi", pa.bool_(), True, None),
        *(
            (name, decimal, True, None)
            for name in ("trade_iv", "mark_iv", "index_price", "mark_price")
        ),
        ("stream_rank", pa.uint32(), False, row["stream_rank"]),
        ("dataset_id", pa.uint64(), False, int(row["dataset_id"])),
        ("task_id", pa.uint64(), False, 1),
        ("event_ordinal", pa.uint64(), False, 0),
        ("canonical_row_offset", pa.uint64(), False, 0),
        ("canonical_row_count", pa.uint64(), False, 1),
    ]
    schema = pa.schema([pa.field(name, kind, nullable) for name, kind, nullable, _ in fields])
    pq.write_table(
        pa.Table.from_pylist([{name: value for name, _, _, value in fields}], schema), file
    )

    normalized = root.parent / f"normalized-{root.name}"
    normalized.mkdir(exist_ok=True)
    (normalized / "manifest.json").write_text(json.dumps({"dataset_id": int(row["dataset_id"])}))
    (normalized / "processing-job.json").write_text(json.dumps({"job_id": 1}))
    normalized_pin = checksum(normalized / "manifest.json")
    publication["normalized_inputs"] = [
        {
            "root": str(normalized),
            "engine_publication_id": row["dataset_id"],
            "manifest_sha256": normalized_pin,
            "processing_job_sha256": checksum(normalized / "processing-job.json"),
        }
    ]
    (root / "merge-input-checksums.json").write_text(json.dumps([normalized_pin]))
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "protocol_version": 1,
                "physical_schema_version": 1,
                "timeline_ordering": "chronological",
                "job_id": int(publication["engine_publication_id"]),
                "mode": "trades",
                "input_datasets": [
                    {
                        "dataset_id": int(row["dataset_id"]),
                        "dataset_path": str(normalized),
                        "manifest_version": 5,
                    }
                ],
                "time_range": {"start_ns": START, "end_ns": END},
                "streams": [
                    {
                        **{
                            name: row[name]
                            for name in (
                                "stream_id",
                                "stream_rank",
                                "kind",
                                "instrument_id",
                                "base_currency",
                                "quote_currency",
                            )
                        },
                        "exchange": "bybit",
                        "symbol": "BTCUSDT",
                        "dataset_id": int(row["dataset_id"]),
                    }
                ],
                "events_written": 1,
                "trade_events": 1,
                "depth_events": 0,
                "canonical_rows": 1,
                "files_written": 1,
                "row_groups": 1,
                "start_timestamp_ns": START,
                "end_timestamp_ns": START,
                "files": [
                    {
                        "path": relative,
                        "size_bytes": file.stat().st_size,
                        "rows": 1,
                        "partition_id": 0,
                        "start_timestamp_ns": START,
                        "end_timestamp_ns": START,
                    }
                ],
                "boundary_links": [],
                "skipped_initializations": [],
                "checkpoints": [],
                "initial_reconstruction": [],
            }
        )
    )
    publication["fingerprints"] = {
        name: checksum(root / name)
        for name in ("manifest.json", "merge-input-checksums.json", relative)
    }
    (root / "publication-checksums.json").write_text(json.dumps(publication["fingerprints"]))


def test_explicit_trade_reference_selection(paths: ProjectPaths) -> None:
    source = publication(paths, combined=True)
    chosen = references(source, None, ["bybit.BTCUSDT.trade", "bybit.BTCUSDC.trade"])
    assert [item["stream_id"] for item in chosen] == ["bybit.BTCUSDT.trade", "bybit.BTCUSDC.trade"]
    assert all(item["provider"] == "internal" for item in chosen)
    with pytest.raises(MarketForgeError, match="No observed trade reference"):
        references(source, None, ["bybit.BTCUSDT.depth"])


def test_external_trade_reference_selection(paths: ProjectPaths) -> None:
    source = publication(paths, combined=False)
    source.update(mode="depth", streams=[stream("ETHBTC", "ETH", "BTC", 0, "depth")])
    external = publication(paths, combined=False, key=61)
    picked = references(source, external, ["bybit.BTCUSDT.trade"])
    assert picked[0]["provider"] == "external"
    assert picked[0]["merge_registry_publication_id"] == 61


@pytest.mark.asyncio
async def test_file_selectors_deduplicate_parent_publication(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, ...]] = []

    async def lookup(_: object, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        calls.append(parameters)
        if "SELECT id,publication_id" in query:
            return [{"id": 360, "publication_id": 60}, {"id": 361, "publication_id": 60}]
        return [
            {
                "id": 60,
                "engine_dataset_id": ENGINE_ID,
                "instrument_namespace": "derived",
                "storage_root": str(paths.datasets_dir),
                "manifest_path": str(paths.datasets_dir / "manifest.json"),
                "evidence": {"manifest": {"streams": [{"instrument_id": 65}]}},
            }
        ]

    monkeypatch.setattr(registry, "rows", lookup)
    connection = cast(registry.Connection, object())
    result = await registry.publications(connection, file_ids=[360, 361])
    assert calls[1][:2] == ([60], [60])
    assert [
        (pub.id, pub.engine_id, pub.manifest["streams"][0]["instrument_id"]) for pub in result
    ] == [
        (60, ENGINE_ID, 65),
    ]
    with pytest.raises(MarketForgeError, match="complete registered merge"):
        await registry.publications(connection, file_ids=[360, 362])


def test_normalization_and_acquisition_require_retained_lineage(paths: ProjectPaths) -> None:
    normalize = recipes.load_source(paths, "normalize", "example")
    assert isinstance(normalize.config, NormalizationRecipe)
    config = recipes.load_source(paths, "acquisition", "example").config
    assert isinstance(config, AcquisitionConfig)
    path = str(paths.raw_dir / "BTCUSDT.csv")
    member = {
        "exchange": "bybit",
        "instrument_type": "spot",
        "market_category": "spot",
        "symbol": "BTCUSDT",
    }
    task = {"input_path": path, "raw_schema": {"marketforge_submission_sha256": "submitted"}}
    parent = {
        "id": 21,
        "instrument_namespace": "namespace",
        "engine_dataset_id": ENGINE_ID,
        "storage_root": "/norm",
        "summary": {"memberships": [member]},
        "evidence": {
            "content_type": "trades",
            "execution": {
                "execution_id": "normalize-execute-fixture",
                "submitted_sha256": "submitted",
            },
            "raw_links_by_path": {path: [15, "raw-sha"]},
            "selected_raw_inputs": [[15, "raw-sha"]],
            "raw_sha256": {path: "raw-sha"},
        },
    }
    normalize_context = discovery.Lineage(
        normalize,
        {1: normalize.config.requests[0]},
        {("namespace", ENGINE_ID, "/norm", "submitted"): "normalize-execute-fixture"},
    )
    assert discovery.match_normalization(parent, normalize_context, [task], {}) == {1}
    with pytest.raises(MarketForgeError, match="no matching retained"):
        discovery.match_normalization(parent, discovery.Lineage(normalize, {}, {}), [task], {})
    request = config.requests[0]
    raw = {
        "local_archive_id": 15,
        "input_sha256": "raw-sha",
        "venue_code": "bybit",
        "data_type": "trades",
        "scope_key": archive_scope_key(request),
        "scope_json": archive_scope_document(request),
        "start_at": datetime(2026, 10, 27, tzinfo=UTC),
        "end_at": datetime(2026, 10, 28, tzinfo=UTC),
    }
    assert discovery.match_acquisition(parent, config, [task], {path: raw}) == {1}
    with pytest.raises(MarketForgeError, match="stale raw provenance"):
        discovery.match_acquisition(
            parent, config, [task], {path: raw | {"input_sha256": "changed"}}
        )
    with pytest.raises(MarketForgeError, match="outside the acquisition scope"):
        discovery.match_acquisition(
            parent, config, [task], {path: raw | {"scope_key": "another scope"}}
        )


def test_historical_instrument_matches_exact_task() -> None:
    identity = {"instrument_id": 65, "exchange": "bybit", "symbol": "BTCUSDC"}
    spec = {"instrument_kind": "spot", "contract_kind": None, "tick_size": "0.01"}
    task = {**identity, "stream_id": "bybit.BTCUSDC.trade", "instrument": spec}
    parent = {
        "publication_id": 11,
        "instrument_namespace": "namespace",
        "evidence": {
            "resolved_job": {"tasks": [task | {"symbol": "ETHBTC"}, task]},
            "manifest": {
                "instrument_namespace": "namespace",
                "resolved_instruments": [
                    {
                        "instrument": {
                            **identity,
                            "base_asset": "BTC",
                            "quote_asset": "USDC",
                            "instrument_type": "spot",
                            "market_category": "spot",
                            "instrument": {"tick_size": "0.01"},
                        },
                        "metadata_origin": "exact_catalog",
                    }
                ],
            },
        },
    }
    declared = {**identity, "dataset_id": int(ENGINE_ID), "stream_id": task["stream_id"]}
    metadata = stream_metadata(declared, parent)
    assert metadata["quote_currency"] == "USDC"
    assert metadata["instrument_namespace"] == "namespace"
    with pytest.raises(MarketForgeError, match="Conflicting historical"):
        stream_metadata(declared | {"quote_currency": "USDT"}, parent)


def test_whole_scope_and_competing_publications(paths: ProjectPaths) -> None:
    source = recipes.load_source(paths, "merge", "example")
    recipe = recipes.Recipe(
        version=1,
        source=source.declaration,
        interval=source.interval,
        conversion=Policy(target_quote="USDT", max_staleness_ns=1),
    )
    root = paths.datasets_dir / "merged"
    manifest = {
        "time_range": {"start_ns": START, "end_ns": END},
        "start_timestamp_ns": START,
        "end_timestamp_ns": END - 1,
        "streams": [
            {"exchange": "bybit", "symbol": "BTCUSDT", "instrument_id": 1, "kind": "trade"}
        ],
    }
    pub = registry.Publication(
        60, ENGINE_ID, "n", root, root / "manifest.json", {"manifest": manifest}
    )
    discovery.check_scope(recipe, pub)
    narrower = recipe.model_copy(
        update={"interval": recipes.Interval(start_ns=START, end_ns=END - 1)}
    )
    with pytest.raises(MarketForgeError, match="whole_publication"):
        discovery.check_scope(narrower, pub)
    discovery.check_scope(
        narrower.model_copy(update={"selection": recipes.Selection(whole_publication=True)}), pub
    )
    assert discovery.competing(pub, pub)
    after = manifest | {"time_range": {"start_ns": END, "end_ns": END + 1}}
    separate = registry.Publication(
        61, "21", "n", root, root / "manifest.json", {"manifest": after}
    )
    assert not discovery.competing(pub, separate)


def test_complete_merge_manifest_and_fingerprints(paths: ProjectPaths) -> None:
    root = paths.datasets_dir / "merged" / ENGINE_ID
    root.mkdir(parents=True)
    pa, pq = arrow_modules()
    files: list[dict[str, Any]] = []
    for index in range(2):
        relative = f"timeline/partition-{index:06}/part-000000.parquet"
        path = root / relative
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table({"event_timestamp_ns": pa.array([START + index], type=pa.int64())}), path
        )
        files.append({"path": relative, "size_bytes": path.stat().st_size})
    document = {
        "job_id": int(ENGINE_ID),
        "protocol_version": 1,
        "physical_schema_version": 2,
        "mode": "combined",
        "timeline_ordering": "causal",
        "files": files,
    }
    job = {"job_id": int(ENGINE_ID), "operation": "merge"}
    artifacts: tuple[tuple[str, object], ...] = (
        ("manifest.json", document),
        ("merge-job.json", job),
        ("merge-input-checksums.json", []),
        ("merge-partitions.json", []),
    )
    for name, content in artifacts:
        (root / name).write_text(json.dumps(content))
    hashes = {
        str(path.relative_to(root)): checksum(path) for path in root.rglob("*") if path.is_file()
    }
    (root / "publication-checksums.json").write_text(json.dumps(hashes))
    entry = registry.Publication(
        60,
        ENGINE_ID,
        "n",
        root,
        root / "manifest.json",
        {
            "manifest": document,
            "path": str(root),
            "instrument_namespace": "n",
            "content_type": "combined",
            "engine_dataset_id": ENGINE_ID,
            "manifest_path": str(root / "manifest.json"),
            "resolved_job": job,
            "inventory_sha256": hashes,
            "manifest_sha256": hashes["manifest.json"],
            "processing_job_sha256": hashes["merge-job.json"],
        },
    )
    assert verify_merge_inventory(entry) == hashes
    (root / files[1]["path"]).write_bytes(b"changed")
    with pytest.raises(MarketForgeError, match="inventory"):
        verify_merge_inventory(entry)


def test_managed_convert_target_is_explicit(paths: ProjectPaths) -> None:
    assert "convert" not in {target.name for target in select(None)}
    target = select("convert")[0]
    assert target.smoke == ("--help",) and target.smoke_expected == "--job"
    assert "marketforge-convert" in plan(paths, target)["command"]


def prepare_fixture(paths: ProjectPaths) -> Path:
    result = recipes.create(paths, "converted", "merge", "example", "USDT")
    path = Path(str(result["path"]))
    runtime = paths.runtime_config("convert")
    runtime.parent.mkdir(parents=True)
    runtime.write_text("""
schema_version = 1
[resources]
workers = 2
memory_limit_mb = 1024
max_open_files = 64
[parquet]
row_group_target_mb = 64
file_target_mb = 256
compression_level = 3
""")
    return path


def test_dry_run_and_generated_compatibility(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def no_registry(*_args: object) -> nullcontext[None]:
        return nullcontext()

    monkeypatch.setattr(cli_conversion, "cli_execution", no_registry)
    prepare_fixture(paths)
    source = publication(paths, combined=False)
    write_trade_publication(source)

    async def select_source(*_args: object) -> dict[str, Any]:
        return {"sources": [source], "external_reference": None, "candidates": [], "blockers": []}

    monkeypatch.setattr(service, "discover", select_source)
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="registry-fixture"))
    monkeypatch.setattr(execution.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(execution, "require_live_prepared_inputs", AsyncMock())
    monkeypatch.setattr(execution.WorkIndex, "acquire", AsyncMock(return_value={}))
    monkeypatch.setattr(execution.WorkIndex, "complete", AsyncMock())
    monkeypatch.setattr(
        execution, "selected_registry_id", AsyncMock(return_value="registry-fixture")
    )
    assert invoke(paths, "prepare", "converted", "--testing", "--dry-run") == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["status"] == "generated" and dry["compatibility"] == "not_validated"
    assert dry["job_path"] is None and not paths.runs_dir.exists()
    assert invoke(paths, "prepare", "converted", "--testing") == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "generated"
    assert any(
        "Managed engine convert: missing" in item for item in report["compatibility_blockers"]
    )
    assert not (paths.datasets_dir / "converted").exists()
    assert invoke(paths, "run", report["job_path"], "--testing") == 1
    blocked = json.loads(capsys.readouterr().out)
    assert blocked["execution"] == "not_started"
    assert blocked["report_path"] == report["report_path"]


@pytest.mark.asyncio
async def test_collection_submits_original_single_job_bytes(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recipe = prepare_fixture(paths)
    candidates = [
        publication(paths, combined=False, key=60),
        publication(paths, combined=False, key=61),
    ]

    for source in candidates:
        write_trade_publication(source)

    async def select_sources(*_args: object) -> dict[str, Any]:
        return {"sources": candidates, "external_reference": None, "candidates": [], "blockers": []}

    binary = paths.root / "converter"
    binary.write_text('#!/bin/sh\n[ "$3" = --validate ]\n')
    binary.chmod(0o755)
    monkeypatch.setattr(service, "discover", select_sources)
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="registry-fixture"))
    monkeypatch.setattr(execution.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(execution, "require_live_prepared_inputs", AsyncMock())
    monkeypatch.setattr(execution.WorkIndex, "acquire", AsyncMock(return_value={}))
    monkeypatch.setattr(execution.WorkIndex, "complete", AsyncMock())
    monkeypatch.setattr(
        execution, "selected_registry_id", AsyncMock(return_value="registry-fixture")
    )

    def resolve_fake(_paths: ProjectPaths, _target: str) -> Path:
        return binary

    monkeypatch.setattr(service, "resolve", resolve_fake)
    monkeypatch.setattr(execution, "resolve", resolve_fake)
    catalog = paths.root / "data/catalog/instruments.json"
    catalog.parent.mkdir(parents=True)
    catalog.write_text(service.catalog_snapshot(candidates).model_dump_json())
    ready: dict[str, dict[str, Any]] = {}

    async def acquire(_index: object, jobs: dict[str, Any]) -> dict[str, Any]:
        return {key: ("original", {}, ready[key]) for key in jobs if key in ready}

    async def complete(
        _index: object, key: str, receipt: dict[str, Any], **_kwargs: object
    ) -> None:
        ready[key] = receipt

    monkeypatch.setattr(execution.WorkIndex, "acquire", acquire)
    monkeypatch.setattr(execution.WorkIndex, "complete", complete)
    report = await service.prepare(paths, recipe)
    assert report["compatibility_blockers"] == []
    assert report["status"] == "generated" and report["compatibility"] == "validated"
    submitted = [await asyncio.to_thread(Path(job["path"]).read_bytes) for job in report["jobs"]]
    recipe.unlink()
    observed: list[bytes] = []

    def supervise(argv: list[str], *_args: object, tui: bool) -> tuple[int, bool]:
        assert not tui
        assert argv[1:] == ["--job", argv[2], "--run"]
        document = Path(argv[argv.index("--job") + 1]).read_bytes()
        Job.model_validate_json(document)
        observed.append(document)
        return 0, False

    def output(job: Job, pinned: dict[str, Any]) -> dict[str, Any]:
        assert job.input_merge == pinned["root"]
        return {
            "manifest": str(Path(job.output.dataset_path) / "manifest.json"),
            "fingerprints": {"manifest.json": "fixture"},
            "events": 1,
        }

    monkeypatch.setattr(execution, "run_process", supervise)
    monkeypatch.setattr(execution, "verify_output", output)
    result = await execution.execute(paths, Path(report["job_path"]))
    assert result["status"] == "complete" and observed == submitted
    repeated = await execution.execute(paths, Path(report["job_path"]))
    assert repeated["disposition"] == "reused" and observed == submitted
    assert [job["publication"] for job in repeated["jobs"]] == [
        job["publication"] for job in result["jobs"]
    ]

    second_key = result["jobs"][1]["work_key"]

    async def blocked(_index: object, work: dict[str, Any]) -> dict[str, Any]:
        if second_key in work:
            raise MarketForgeError("Equivalent work is already in progress.")
        return await acquire(_index, work)

    monkeypatch.setattr(execution.WorkIndex, "acquire", blocked)
    partial = await execution.execute(paths, Path(report["job_path"]))
    assert partial["status"] == "blocked" and observed == submitted
    assert [job["status"] for job in partial["jobs"]] == ["complete", "blocked"]
    assert partial["jobs"][0]["disposition"] == "reused"

    job_path = Path(report["jobs"][0]["path"])
    await asyncio.to_thread(job_path.write_bytes, submitted[0] + b" ")
    with pytest.raises(MarketForgeError, match="Retained artifact changed"):
        await execution.execute(paths, job_path)


def test_blocked_preparation_cli(paths: ProjectPaths, capsys: pytest.CaptureFixture[str]) -> None:
    path = prepare_fixture(paths)
    path.write_text(path.read_text().replace('"max_hops" = 3', '"max_hops" = 9'))
    assert invoke(paths, "prepare", "converted", "--testing", "--dry-run") == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "blocked" and not paths.runs_dir.exists()


@pytest.mark.asyncio
async def test_direct_merge_requires_exact_registered_and_retained_inputs(
    paths: ProjectPaths,
) -> None:
    root = paths.datasets_dir / "merged" / ENGINE_ID
    source = recipes.load_source(paths, "merge", "example")
    execution_id = "merge-execute-fixture"
    directory = paths.runs_dir / execution_id
    directory.mkdir(parents=True)
    submitted = directory / "submitted-merge.json"
    manifest = {
        "streams": [{"stream_id": "bybit.BTCUSDT.trade", "stream_rank": 3}],
        "time_range": {"start_ns": START, "end_ns": END},
    }
    job = {"operation": "merge", "job_id": int(ENGINE_ID), "time_range": manifest["time_range"]}
    submitted.write_text(json.dumps(job))
    digest = checksum(submitted)
    (directory / "execution-intent.json").write_text(
        json.dumps(
            {
                "operation": "merge",
                "environment": "production",
                "execution_id": execution_id,
                "job_id": int(ENGINE_ID),
                "path": str(root),
                "recipe": "example",
                "submission_path": str(submitted),
                "submitted_sha256": digest,
            }
        )
    )
    proof = {
        "manifest": manifest,
        "selected_dataset_inputs": [21, 22],
        "resolved_job": job,
        "execution": {"execution_id": execution_id, "submitted_sha256": digest},
    }
    connection = cast(registry.Connection, object())
    pub = registry.Publication(60, ENGINE_ID, "merge", root, root / "manifest.json", proof)
    context = discovery.lineage(paths, source)
    _, retained = await discovery.prove_source(connection, paths, pub, context, [])
    assert retained["merge_submission_sha256"] == digest
    proof["selected_dataset_inputs"] = [22, 21]
    with pytest.raises(MarketForgeError, match="normalized file selection"):
        await discovery.prove_source(connection, paths, pub, context, [])
    await asyncio.to_thread(submitted.write_bytes, b"{}")
    with pytest.raises(MarketForgeError, match="retained|Retained"):
        await discovery.prove_source(connection, paths, pub, context, [])


@pytest.mark.asyncio
async def test_parent_namespaces_must_match(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def lookup(_: object, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert "d.operation='normalize'" in query and parameters == ([21, 22],)
        return [
            {"id": 21, "instrument_namespace": "first"},
            {"id": 22, "instrument_namespace": "second"},
        ]

    monkeypatch.setattr(registry, "rows", lookup)
    root = paths.datasets_dir / "merged"
    pub = registry.Publication(
        60,
        ENGINE_ID,
        "merge",
        root,
        root / "manifest.json",
        {"selected_dataset_inputs": [21, 22]},
    )
    with pytest.raises(MarketForgeError, match="cross instrument namespaces"):
        await registry.parents(cast(registry.Connection, object()), pub)


@pytest.mark.parametrize("kind", ["merge", "normalize", "acquisition"])
def test_forge_prepared_job_runs_in_real_engine(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kind: recipes.SourceKind,
) -> None:
    configured = os.environ.get("MARKETFORGE_TEST_CONVERT_BINARY")
    if not configured:
        pytest.skip("set MARKETFORGE_TEST_CONVERT_BINARY to the freshly built converter")
    binary = Path(configured).resolve(strict=True)
    prepare_fixture(paths)
    recipe = Path(str(recipes.create(paths, f"from-{kind}", kind, "example", "USDT")["path"]))
    source = publication(paths, combined=False)
    write_trade_publication(source)

    async def select_source(
        _paths: ProjectPaths, candidate: recipes.Recipe, _upstream: object
    ) -> dict[str, Any]:
        assert candidate.source.kind == kind
        return {"sources": [source], "external_reference": None, "blockers": []}

    monkeypatch.setattr(service, "discover", select_source)
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="registry-fixture"))
    monkeypatch.setattr(execution.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(execution, "require_live_prepared_inputs", AsyncMock())
    ready: dict[str, dict[str, Any]] = {}

    async def acquire(_index: object, jobs: dict[str, Any]) -> dict[str, Any]:
        return {key: ("original", {}, ready[key]) for key in jobs if key in ready}

    async def complete(
        _index: object, key: str, receipt: dict[str, Any], **_kwargs: object
    ) -> None:
        ready[key] = receipt

    monkeypatch.setattr(execution.WorkIndex, "acquire", acquire)
    monkeypatch.setattr(execution.WorkIndex, "complete", complete)
    monkeypatch.setattr(
        execution, "selected_registry_id", AsyncMock(return_value="registry-fixture")
    )

    def resolve_binary(_paths: ProjectPaths, _target: str) -> Path:
        return binary

    monkeypatch.setattr(service, "resolve", resolve_binary)
    monkeypatch.setattr(execution, "resolve", resolve_binary)
    first = asyncio.run(service.prepare(paths, recipe))
    second = asyncio.run(service.prepare(paths, recipe))
    for report in (first, second):
        assert report["compatibility"] == "validated", report.get("compatibility_blockers")
        assert report["jobs"][0]["planned_routes"][0]["routes"] == [["USDT"]]
    first_path = Path(first["jobs"][0]["path"])
    assert service.validate_reference(paths, Path(first["job_path"]))["validated_job_count"] == 1
    assert service.validate_reference(paths, first_path)["validation_layers"]["engine"] == "passed"
    first_bytes = first_path.read_bytes()
    first_path.write_bytes(first_bytes + b" ")
    with pytest.raises(MarketForgeError, match="Retained artifact changed"):
        service.validate_reference(paths, first_path)
    first_path.write_bytes(first_bytes)
    first_job = Job.model_validate_json(first_path.read_bytes())
    second_job = Job.model_validate_json(Path(second["jobs"][0]["path"]).read_bytes())
    assert first_job.model_dump(exclude={"job_id", "output"}) == second_job.model_dump(
        exclude={"job_id", "output"}
    )
    assert first_job.source.merge_job_id == int(ENGINE_ID)
    assert first_job.source.streams[0].dataset_id == 4968760985013963110

    for artifact in ("catalog_snapshot", "runtime", "recipe_snapshot", "upstream_snapshot"):
        Path(first["artifacts"][artifact]["path"]).unlink()
    for parent in source["normalized_inputs"]:
        shutil.rmtree(parent["root"])
    workspace = tmp_path / "unrelated"
    workspace.mkdir()
    output = Path(first_job.output.dataset_path)
    validated = subprocess.run(
        [str(binary), "--job", str(first_path), "--validate"],
        cwd=workspace,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert validated.returncode == 0, validated.stderr
    assert not output.exists() and not output.with_suffix(".staging").exists()

    if kind == "merge" and (pty_command := shutil.which("script")):
        # Exercise the live Rust monitor against a disposable job under a real PTY.
        monitor_job = first_job.model_dump()
        monitor_job["output"]["dataset_path"] = str(output.with_name("monitor-output"))
        monitor_path = workspace / "monitor-job.json"
        monitor_path.write_text(json.dumps(monitor_job))
        command = shlex.join(
            [
                str(binary),
                "--job",
                str(monitor_path),
                "--run",
                "--tui",
                "--managed-output",
            ]
        )
        monitored = subprocess.run(
            [
                pty_command,
                "--quiet",
                "--return",
                "--command",
                f"stty rows 38 cols 110; {command}",
                "/dev/null",
            ],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            env={**os.environ, "TERM": "xterm"},
            check=False,
            timeout=20,
        )
        assert monitored.returncode == 0, monitored.stdout[-2000:]
        assert b"\x1b[?1049h" in monitored.stdout and b"\x1b[?1049l" in monitored.stdout
        after_monitor = monitored.stdout.rsplit(b"\x1b[?1049l", 1)[-1]
        assert b"Source publication" not in after_monitor and b"Manifest " not in after_monitor
        assert (output.with_name("monitor-output") / "manifest.json").is_file()

    result = asyncio.run(execution.execute(paths, first_path))
    assert result["status"] == "complete", result
    assert output.is_dir()
    assert result["jobs"][0]["argv"][1::2] == ["--job", "--run"]
    assert result["jobs"][0]["publication"]["events"] == 1
    from io import StringIO

    from forge.shared.config.models import PresentationSettings
    from forge.shared.presentation.conversion import ConversionView
    from forge.shared.terminal.console import create_console
    from forge.shared.terminal.conversion import render

    for width in (64, 120):
        sink = StringIO()
        console = create_console(PresentationSettings(color="never"), stream=sink)
        console.width = width
        render(
            console,
            ConversionView.from_result(result, "run", "example", 0.1),
            paths.root,
        )
        rendered = sink.getvalue()
        assert rendered.count("MarketForge · Convert · Run") == 1
        assert "Verified" in rendered and "Catalogue registration" in rendered
        assert "Identity valuations" in rendered and "Indexed output reference" in rendered
        assert "\x1b" not in rendered

    original = execution.run_process
    launch = AsyncMock(side_effect=AssertionError("The materializing process must be skipped."))
    monkeypatch.setattr(execution, "run_process", launch)
    repeated = asyncio.run(execution.execute(paths, Path(second["jobs"][0]["path"])))
    assert repeated["disposition"] == "reused"
    assert repeated["jobs"][0]["publication"] == result["jobs"][0]["publication"]
    assert not Path(second_job.output.dataset_path).exists()
    launch.assert_not_called()
    monkeypatch.setattr(execution, "run_process", original)
    ready.clear()
    monkeypatch.setattr(execution.WorkIndex, "release", AsyncMock())

    manifest = Path(source["root"]) / "manifest.json"
    manifest.write_text(manifest.read_text() + " ")
    failure = asyncio.run(execution.execute(paths, Path(second["jobs"][0]["path"])))
    assert failure["status"] == "blocked" and failure["execution"] == "not_started"
    assert "pin mismatch" in failure["jobs"][0]["error"]
    assert not Path(second_job.output.dataset_path).exists()


def test_local_recipe_lifecycle(
    paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def prohibited(*_args: object) -> None:
        raise AssertionError("Local inspection should not retain evidence or resolve the Engine.")

    monkeypatch.setattr(recipes, "retain_recipe", prohibited)
    monkeypatch.setattr(service, "resolve", prohibited)
    assert invoke(paths, "list") == 0
    assert json.loads(capsys.readouterr().out)["recipes"] == []
    active = prepare_fixture(paths)
    stale = Path(str(recipes.create(paths, "stale", "normalize", "example", "USDT")["path"]))
    upstream = recipes.source_path(paths, "normalize", "example")
    upstream.write_bytes(upstream.read_bytes() + b"# changed\n")
    broken = stale.with_name("bad.toml")
    broken.write_text("[invalid")
    assert invoke(paths, "list") == 0
    rows = json.loads(capsys.readouterr().out)["recipes"]
    assert [(item["name"], item["status"]) for item in rows] == [
        ("bad", "invalid"),
        ("converted", "valid"),
        ("stale", "stale"),
    ]
    assert invoke(paths, "show", str(stale)) == 0
    assert "fingerprint changed" in json.loads(capsys.readouterr().out)["diagnostic"]
    assert invoke(paths, "edit", str(active)) == 2
    assert "requires a terminal" in json.loads(capsys.readouterr().out)["error"]["message"]

    def editor(draft: Path, **_: object) -> Path:
        draft.write_bytes(draft.read_bytes().replace(b"USDT", b"USDC"))
        return draft

    monkeypatch.setattr("forge.shared.config.recipe_editing.edit_file", editor)
    from forge.startup.context import build_context

    cli_conversion.edit(build_context(project_root=paths.root), active)
    assert recipes.read_recipe(active)[0].conversion.target_quote == "USDC"
    assert invoke(paths, "delete", "converted.toml") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready_to_delete"
    assert active.is_file()
    assert invoke(paths, "delete", "converted", "--apply") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "deleted"
    assert not active.exists() and stale.is_file() and broken.is_file() and upstream.is_file()
    assert not paths.runs_dir.exists()


def test_conversion_runtime_cli_validates_with_rust(
    paths: ProjectPaths, capsys: pytest.CaptureFixture[str]
) -> None:
    configured = os.environ.get("MARKETFORGE_TEST_CONVERT_BINARY")
    if not configured:
        pytest.skip("set MARKETFORGE_TEST_CONVERT_BINARY to the freshly built converter")
    prepare_fixture(paths)
    binary = Path(configured).resolve(strict=True)
    installed = paths.root / ".bin"
    installed.mkdir()
    executable = installed / "marketforge-convert"
    shutil.copy2(binary, executable)
    (installed / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "targets": {"convert": {"sha256": checksum(executable)}},
            }
        )
    )
    assert invoke(paths, "config", "schema") == 0
    fields = {field["path"] for field in json.loads(capsys.readouterr().out)["fields"]}
    assert fields == {
        "schema_version",
        "resources.workers",
        "resources.memory_limit_mb",
        "resources.max_open_files",
        "parquet.file_target_mb",
        "parquet.row_group_target_mb",
        "parquet.compression_level",
    }
    runtime = paths.runtime_config("convert")
    before = b"# retain this comment\n" + runtime.read_bytes()
    runtime.write_bytes(before)
    assert invoke(paths, "config", "set", "resources.memory_limit_mb", "4GiB") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready_to_apply"
    assert runtime.read_bytes() == before
    assert invoke(paths, "config", "set", "parquet.file_target_mb", "1MiB", "--apply") == 1
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    assert runtime.read_bytes() == before
    assert invoke(paths, "config", "unset", "resources.max_open_files", "--apply") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "applied"
    assert runtime.read_bytes().startswith(b"# retain this comment\n")
    assert invoke(paths, "config", "validate") == 0
    assert json.loads(capsys.readouterr().out)["effective"]["resources"]["max_open_files"] == 64


@pytest.mark.parametrize("change", ["invalid", "conflict", "cancel"])
def test_recipe_editor_keeps_original_when_draft_cannot_apply(
    paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from forge.startup.context import build_context

    recipe = prepare_fixture(paths)
    original = recipe.read_bytes()
    concurrent = original + b"# concurrent edit\n"

    def editor(draft: Path, **_: object) -> Path:
        if change == "cancel":
            raise MarketForgeError("Editor exited unsuccessfully")
        if change == "conflict":
            recipe.write_bytes(concurrent)
            draft.write_bytes(original.replace(b"USDT", b"USDC"))
        else:
            draft.write_bytes(b"[invalid")
        return draft

    monkeypatch.setattr("forge.shared.config.recipe_editing.edit_file", editor)
    with pytest.raises(MarketForgeError, match="Retained draft:") as error:
        cli_conversion.edit(build_context(project_root=paths.root), recipe)
    draft = Path(str(error.value).rsplit("Retained draft: ", 1)[1])
    assert draft.is_file()
    shutil.rmtree(draft.parent)
    assert recipe.read_bytes() == (concurrent if change == "conflict" else original)


def test_new_argument_diagnostics(paths: ProjectPaths, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        invoke(paths, "new", "--from-merge", "example", "--target-quote", "USDT")
    error = capsys.readouterr().err
    assert "needs a name" in error and "getting_started_usdt" in error
    with pytest.raises(SystemExit):
        invoke(paths, "new", "new_recipe", "--from-acuisition", "example", "--target-quote", "USDT")
    assert "Did you mean --from-acquisition" in capsys.readouterr().err
    for route in ("merge", "normalize", "acquisition"):
        with pytest.raises(SystemExit):
            invoke(
                paths,
                "new",
                "new_recipe",
                "--from-publication",
                "60",
                f"--from-{route}",
                "example",
                "--target-quote",
                "USDT",
            )
    assert (
        invoke(paths, "new", "new_recipe", "--from-merge", "example", "--target-quote", "a_recipe")
        == 2
    )
    assert "--target-quote" in json.loads(capsys.readouterr().out)["error"]["message"]


def publication_declaration(source: dict[str, Any]) -> recipes.PublicationSource:
    root = Path(source["root"])
    return recipes.PublicationSource(
        registry_id="fb19f358-fcab-4bf7-98ed-5835ff24b455",
        environment="testing",
        registry_publication_id=source["registry_publication_id"],
        engine_publication_id=source["engine_publication_id"],
        manifest_sha256=source["fingerprints"]["manifest.json"],
        processing_job_sha256="1" * 64,
        publication_checksums_sha256=checksum(root / "publication-checksums.json"),
        interval=recipes.Interval(start_ns=START, end_ns=END),
    )


def test_publication_recipe_local_lifecycle(
    paths: ProjectPaths,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = publication(paths, combined=False)
    write_trade_publication(source)
    binding = publication_declaration(source)
    resolve_binding = AsyncMock(return_value=binding)
    monkeypatch.setattr(cli_conversion, "inspect_publication_source", resolve_binding)
    assert (
        invoke(
            paths,
            "new",
            "direct",
            "--testing",
            "--from-publication",
            "60",
            "--target-quote",
            "USDT",
        )
        == 0
    )
    created = json.loads(capsys.readouterr().out)
    assert created["source"] == binding.model_dump()
    assert created["execution_readiness"] == "not_assessed"
    assert created["reference_configuration"] == "review_required"
    resolve_binding.assert_awaited_once_with(paths, 60)
    for argv in (("list",), ("show", "direct"), ("validate", "direct")):
        assert invoke(paths, *argv) == 0
        document = json.loads(capsys.readouterr().out)
        if argv[0] == "list":
            document = document["recipes"][0]
        assert document["recipe"]["source"] == binding.model_dump()
    resolve_binding.assert_awaited_once()
    assert not paths.runs_dir.exists()
    assert invoke(paths, "prepare", "direct") == 2
    assert "Select --testing" in json.loads(capsys.readouterr().out)["error"]["message"]
    assert not paths.runs_dir.exists()
    recipe, _ = recipes.read_recipe(Path(created["path"]))
    for field in ("file_dataset_ids", "publication_ids", "expected_engine_publication_ids"):
        configured = recipe.model_dump()
        configured["selection"][field] = ["60"] if field.startswith("expected") else [60]
        with pytest.raises(ValidationError, match="remove other source selectors"):
            recipes.Recipe.model_validate(configured)
    configured = recipe.model_dump()
    configured["interval"]["end_ns"] -= 1
    with pytest.raises(ValidationError, match="cropping is unsupported"):
        recipes.Recipe.model_validate(configured)
    resolve_binding.side_effect = MarketForgeError("ineligible publication")
    assert (
        invoke(
            paths,
            "new",
            "blocked",
            "--testing",
            "--from-publication",
            "60",
            "--target-quote",
            "USDT",
        )
        == 2
    )
    assert not (recipes.library(paths) / "blocked.toml").exists()


def test_publication_preparation_keeps_standalone_job_and_work_key(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.historical.engine.work import convert_key
    from forge.shared.database.connection import database_environment

    configured = os.environ.get("MARKETFORGE_TEST_CONVERT_BINARY")
    if not configured:
        pytest.skip("set MARKETFORGE_TEST_CONVERT_BINARY to the managed converter")
    binary = Path(configured).resolve(strict=True)
    recipe_route = prepare_fixture(paths)
    source = publication(paths, combined=False)
    write_trade_publication(source)
    binding = publication_declaration(source)
    direct = Path(
        str(recipes.create_bound(paths, "direct", binding, binding.interval, "USDT")["path"])
    )
    monkeypatch.setattr(
        service, "selected_registry_id", AsyncMock(return_value=binding.registry_id)
    )
    monkeypatch.setattr(execution.WorkIndex, "peek", AsyncMock(return_value={}))

    def resolve_binary(_paths: ProjectPaths, _target: str) -> Path:
        return binary

    monkeypatch.setattr(service, "resolve", resolve_binary)

    async def discover_source(
        _paths: ProjectPaths,
        recipe: recipes.Recipe,
        declaration: recipes.LoadedSource | recipes.PublicationSource,
    ) -> dict[str, Any]:
        if recipe.source.kind == "publication":
            assert declaration == binding
        return {"sources": [source], "external_reference": None, "blockers": []}

    monkeypatch.setattr(service, "discover", discover_source)
    with database_environment(testing=True):
        via_recipe = asyncio.run(service.prepare(paths, recipe_route))
        recipes.source_path(paths, "merge", "example").unlink()
        via_publication = asyncio.run(service.prepare(paths, direct))
    key = checksum(binary)
    work_keys: list[str] = []
    for report in (via_recipe, via_publication):
        assert report["compatibility"] == "validated", report.get("compatibility_blockers")
        entry = report["jobs"][0]
        job = Job.model_validate_json(Path(entry["path"]).read_bytes())
        assert entry["references"] == []  # BTCUSDT -> USDT needs no reference stream.
        assert entry["engine_validation"] == "passed"
        work_keys.append(convert_key(binding.registry_id, key, job.model_dump(mode="json")))
    assert work_keys[0] == work_keys[1]
    _, collection, _ = service.retained_collection(Path(via_publication["job_path"]))
    assert collection["sources"][0]["publication_source"] == binding.model_dump()
    assert "upstream_snapshot" not in collection["pins"]
    assert not Path(via_publication["job_path"]).with_name("upstream.toml").exists()
    Path(via_publication["artifacts"]["catalog_snapshot"]["path"]).unlink()
    assert (
        service.validate_reference(paths, Path(via_publication["job_path"]))["validation_layers"][
            "engine"
        ]
        == "passed"
    )
    canonical = {
        "publication": {"manifest": "/fixture/canonical/manifest.json"},
        "registration": "not_available",
        "status": "complete",
        "execution": "complete",
        "compatibility": "validated",
        "result_path": "/fixture/results/execution-result.json",
    }
    monkeypatch.setattr(
        execution.WorkIndex, "peek", AsyncMock(return_value={work_keys[0]: ("old", {}, canonical)})
    )
    verify = AsyncMock(return_value=canonical["publication"])
    monkeypatch.setattr(execution, "verify_reused_conversion", verify)
    with database_environment(testing=True):
        reused = asyncio.run(service.prepare(paths, direct))
    assert reused["disposition"] == "reused"
    assert reused["execution"] == "not_started"
    assert reused["jobs"][0]["publication"] == canonical["publication"]
    verify.assert_awaited_once()
    assert not (paths.datasets_dir / "converted").exists()

    connection = MagicMock()
    connection.execute = AsyncMock()

    @asynccontextmanager
    async def connect(_: str) -> AsyncGenerator[registry.Connection]:
        yield cast(registry.Connection, connection)

    root = Path(source["root"])
    record = registry.Publication(
        binding.registry_publication_id,
        binding.engine_publication_id,
        "namespace",
        root,
        root / "manifest.json",
        {},
    )
    recheck = AsyncMock(return_value=record)
    monkeypatch.setattr(execution, "database_dsn", fixture_dsn)
    monkeypatch.setattr(execution, "database_connection", connect)
    monkeypatch.setattr(execution, "resolve", resolve_binary)
    monkeypatch.setattr(execution, "resolve_binding", recheck)
    monkeypatch.setattr(
        execution, "selected_registry_id", AsyncMock(return_value=binding.registry_id)
    )
    monkeypatch.setattr(execution, "require_live_prepared_inputs", AsyncMock())
    monkeypatch.setattr(execution.WorkIndex, "recover_committed", AsyncMock())
    monkeypatch.setattr(
        execution.WorkIndex,
        "acquire",
        AsyncMock(return_value={work_keys[0]: ("old", {}, canonical)}),
    )
    with database_environment(testing=True):
        executed = asyncio.run(execution.execute(paths, Path(via_publication["job_path"])))
    recheck.assert_awaited_once_with(connection, binding, check_files=True)
    assert executed["disposition"] == "reused"
    assert executed["jobs"][0]["engine_execution_skipped"]
    assert not (paths.datasets_dir / "converted").exists()


def fixture_dsn(_: Path) -> str:
    return "fixture"


@pytest.mark.asyncio
async def test_publication_discovery_requires_normalized_parent_verification(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = publication(paths, combined=False)
    write_trade_publication(source)
    binding = publication_declaration(source)
    recipe_path = Path(
        str(recipes.create_bound(paths, "direct", binding, binding.interval, "USDT")["path"])
    )
    recipe, declaration, _ = recipes.load_recipe(paths, recipe_path, retain=False)
    root = Path(source["root"])
    record = registry.Publication(
        binding.registry_publication_id,
        binding.engine_publication_id,
        "namespace",
        root,
        root / "manifest.json",
        {"manifest": json.loads((root / "manifest.json").read_bytes())},
    )
    connection = MagicMock()
    connection.execute = AsyncMock()

    @asynccontextmanager
    async def connect(_: str) -> AsyncGenerator[registry.Connection]:
        yield cast(registry.Connection, connection)

    monkeypatch.setattr(discovery, "database_connection", connect)
    monkeypatch.setattr(discovery, "database_dsn", fixture_dsn)
    resolve = AsyncMock(return_value=record)
    monkeypatch.setattr(discovery, "resolve_binding", resolve)
    monkeypatch.setattr(
        discovery, "prove_source", AsyncMock(side_effect=AssertionError("No upstream TOML"))
    )
    parent_rows = [{"id": 21, "instrument_namespace": "namespace"}]
    select_parents = AsyncMock(return_value=parent_rows)
    monkeypatch.setattr(discovery, "parents", select_parents)
    verify = AsyncMock(return_value=source)
    monkeypatch.setattr(discovery, "verify_publication", verify)
    result = await discovery.discover(paths, recipe, declaration)
    resolve.assert_awaited_once_with(connection, binding)
    select_parents.assert_awaited_once_with(connection, record)
    verify.assert_awaited_once_with(connection, paths, record, parent_rows)
    assert result["sources"][0]["lineage"] == {"selection": "bound_registry_publication"}
    verify.side_effect = MarketForgeError("Normalized parent fingerprint changed")
    with pytest.raises(MarketForgeError, match="Normalized parent fingerprint changed"):
        await discovery.discover(paths, recipe, declaration)
