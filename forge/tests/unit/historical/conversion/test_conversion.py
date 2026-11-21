"""Forge conversion preparation contracts and provenance without a database mutation."""

import asyncio
import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

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
        paths.config_dir / "historical/merge",
    ):
        folder.mkdir(parents=True)
    (paths.acquisition_config_dir / "example.toml").write_text(SOURCE_CONFIG)
    (paths.normalization_recipe_dir / "example.toml").write_text(SOURCE_CONFIG)
    (paths.config_dir / "historical/merge/example.toml").write_text(DIRECT)
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
    assert "already exists" in capsys.readouterr().err
    assert path.read_bytes() == original
    source.write_bytes(source.read_bytes() + b"# edit\n")
    assert invoke(paths, "validate", str(path)) == 2
    assert "fingerprint changed" in capsys.readouterr().err


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
    assert "incompatible with global --json" in capsys.readouterr().err
    source = paths.normalization_config_dir / "legacy.toml"
    source.parent.mkdir(parents=True)
    source.write_text("job_id = 12\n")
    assert invoke(paths, "new", "x", "--from-normalize", "legacy", "--target-quote", "USDT") == 2
    assert "Legacy archive-ID" in capsys.readouterr().err


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
    runtime = paths.config_dir / "historical/general.toml"
    runtime.write_text("[resources]\nworkers = 2\nmemory_budget_bytes = 1073741824\n")
    return path


def test_dry_run_and_generated_compatibility(
    paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    prepare_fixture(paths)
    source = publication(paths, combined=False)
    write_trade_publication(source)

    async def select_source(*_args: object) -> dict[str, Any]:
        return {"sources": [source], "external_reference": None, "candidates": [], "blockers": []}

    monkeypatch.setattr(service, "discover", select_source)
    assert invoke(paths, "prepare", "converted", "--testing", "--dry-run") == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["status"] == "generated" and dry["compatibility"] == "not_validated"
    assert dry["job_path"] is None and not paths.runs_dir.exists()
    assert invoke(paths, "prepare", "converted", "--testing") == 0
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

    def resolve_fake(_paths: ProjectPaths, _target: str) -> Path:
        return binary

    monkeypatch.setattr(service, "resolve", resolve_fake)
    monkeypatch.setattr(execution, "resolve", resolve_fake)
    catalog = paths.root / "data/catalog/instruments.json"
    catalog.parent.mkdir(parents=True)
    catalog.write_text(service.catalog_snapshot(candidates).model_dump_json())
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
        return {"fixture": True}

    monkeypatch.setattr(execution, "run_process", supervise)
    monkeypatch.setattr(execution, "verify_output", output)
    result = await execution.execute(paths, Path(report["job_path"]))
    assert result["status"] == "complete" and observed == submitted
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
                "environment": "testing",
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

    def resolve_binary(_paths: ProjectPaths, _target: str) -> Path:
        return binary

    monkeypatch.setattr(service, "resolve", resolve_binary)
    monkeypatch.setattr(execution, "resolve", resolve_binary)
    first = asyncio.run(service.prepare(paths, recipe))
    second = asyncio.run(service.prepare(paths, recipe))
    for report in (first, second):
        assert report["compatibility"] == "validated", report.get("compatibility_blockers")
    first_path = Path(first["jobs"][0]["path"])
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

    result = asyncio.run(execution.execute(paths, first_path))
    assert result["status"] == "complete", result
    assert output.is_dir()
    assert result["jobs"][0]["argv"][1::2] == ["--job", "--run"]
    assert result["jobs"][0]["publication"]["events"] == 1

    manifest = Path(source["root"]) / "manifest.json"
    manifest.write_text(manifest.read_text() + " ")
    failure = asyncio.run(execution.execute(paths, Path(second["jobs"][0]["path"])))
    assert failure["status"] == "blocked" and failure["execution"] == "not_started"
    assert "pin mismatch" in failure["jobs"][0]["error"]
    assert not Path(second_job.output.dataset_path).exists()
