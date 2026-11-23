"""Managed merge selection and preparation tests with no processing subprocess."""

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from forge.historical.merge import service
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def test_recipe_and_coverage() -> None:
    recipe = service.Recipe(dataset_ids=[8, 3], start_ns=10, end_ns=50)
    assert recipe.dataset_ids == [8, 3]
    with pytest.raises(ValidationError, match="once"):
        service.Recipe(dataset_ids=[8, 8], start_ns=10, end_ns=50)
    with pytest.raises(ValidationError, match="start_ns < end_ns"):
        service.Recipe(dataset_ids=[8], start_ns=50, end_ns=10)
    result = service.coverage(
        [
            {
                "database_id": 8,
                "memberships": [{"stream_id": "first"}],
                "start_ns": 11,
                "end_ns": 40,
            },
            {
                "database_id": 3,
                "memberships": [{"stream_id": "first"}],
                "start_ns": 41,
                "end_ns": 45,
            },
        ],
        10,
        50,
    )
    assert result["observed_extent_gaps"] == {"first": [(10, 11), (46, 50)]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,expected",
    [
        ("valid", None),
        ("collision", "Ambiguous engine publication ID"),
        ("namespace", "single instrument namespace"),
        ("missing", "lacks a registered file path"),
        ("hardlink", "same physical file"),
        ("multi", None),
        ("segmented", None),
        ("depth", None),
    ],
)
async def test_selection_uses_registry_order_and_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected: str | None,
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    evidence: dict[str, Any] = {}
    records = []
    for db_id in (4, 7):
        engine_id = 20 if mode == "collision" else db_id + 40
        namespace = str(db_id) if mode == "namespace" else "namespace"
        root = tmp_path / "custom" / str(db_id)
        details = {
            "path": str(root),
            "manifest_path": str(root / "manifest.json"),
            "manifest_sha256": f"manifest-{db_id}",
            "engine_dataset_id": str(engine_id),
            "instrument_namespace": namespace,
            "processing_job_sha256": "job",
            "processing_runtime_sha256": "runtime",
        }
        name = "part.parquet"
        root.mkdir(parents=True)
        if mode == "hardlink" and db_id == 7:
            (root / name).hardlink_to(tmp_path / "custom" / "4" / name)
        else:
            (root / name).touch()
        records.append(
            (
                db_id,
                str(root / name),
                name,
                "sha",
                {"memberships": [{"exchange": "bybit" if db_id == 4 else "okx"}]},
                10,
                50,
                str(root),
                str(engine_id),
                namespace,
                details["manifest_sha256"],
                None if mode == "missing" else details["manifest_path"],
            )
        )
        streams = [{"stream_id": "first"}]
        if mode == "multi":
            streams.append({"stream_id": "second"})
        evidence[str(root)] = {
            **details,
            "inventory_sha256": {name: "sha"},
            "resolved_job": {"streams": streams},
            "manifest": {
                "streams": [
                    {"stream_id": "first", "directory": "."},
                    {"stream_id": "second", "directory": "unused"},
                ]
                if mode == "multi"
                else [],
                "dataset_id": engine_id,
                "protocol_version": 5 if mode == "segmented" else 3,
                "content_type": "depth" if mode == "depth" else "trades",
                "files": [
                    {"path": file, "start_timestamp_ns": 10, "end_timestamp_ns": 50}
                    for file in ([name, "sibling.parquet"] if mode == "depth" else [name])
                ],
                "start_timestamp_ns": 10,
                "end_timestamp_ns": 50,
                "integrity_status": "clean",
            },
        }

    @asynccontextmanager
    async def connect(_: str) -> Any:
        yield type(
            "Connection",
            (),
            {
                "execute": AsyncMock(
                    return_value=type(
                        "Cursor",
                        (),
                        {
                            "fetchall": AsyncMock(return_value=records),
                        },
                    )()
                ),
            },
        )()

    monkeypatch.setattr(service, "database_dsn", lambda root: "fixture")
    monkeypatch.setattr(service, "database_connection", connect)
    monkeypatch.setattr(service, "verify", lambda paths, source, **kwargs: evidence[str(source)])
    if expected:
        with pytest.raises(MarketForgeError, match=expected):
            await service.select_inputs(paths, ids=[7, 4])
    else:
        selected = await service.select_inputs(paths, ids=[7, 4])
        assert [item["engine_id"] for item in selected] == (
            [20, 20] if mode == "collision" else [47, 44]
        )
        assert [item["path"] for item in selected] == [row[7] for row in reversed(records)]
        assert await service.select_inputs(paths, ids=[7, 4]) == selected
        service.assign_ranks(selected, {"first": 7}, {})
        inputs = [service.file_input(item) for item in selected]
        assert [item["path"] for item in inputs] == [row[1] for row in reversed(records)]
        assert await service.select_inputs(paths, job={"inputs": inputs}) == selected
        with pytest.raises(MarketForgeError, match="without duplicates"):
            await service.select_inputs(paths, job={"inputs": inputs + inputs[:1]})
        inputs[0]["publication"]["manifest_sha256"] = "tampered"
        with pytest.raises(MarketForgeError, match="registered paths or fingerprints"):
            await service.select_inputs(paths, job={"inputs": inputs})


@pytest.mark.asyncio
async def test_two_file_ids_share_one_verified_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = tmp_path / "publication"
    directory = "streams/s/trades"
    names = [f"{directory}/part-{i}.parquet" for i in range(3)]
    (root / directory).mkdir(parents=True)
    for name in names:
        (root / name).touch()
    records = [
        (
            i,
            str(root / name),
            name,
            f"sha-{i}",
            {"memberships": []},
            10,
            20,
            str(root),
            "77",
            "ns",
            "manifest-sha",
            str(root / "manifest.json"),
        )
        for i, name in enumerate(names[:2], 1)
    ]
    cursor = type("Cursor", (), {"fetchall": AsyncMock(return_value=records)})()

    @asynccontextmanager
    async def connect(_: str) -> Any:
        yield type("Connection", (), {"execute": AsyncMock(return_value=cursor)})()

    verification_calls = []

    def verify(_paths: ProjectPaths, source: Path, *, selected_files: set[str]) -> dict[str, Any]:
        assert selected_files == set(names[:2])
        verification_calls.append(source)
        return {
            "manifest_sha256": "manifest-sha",
            "manifest_path": str(root / "manifest.json"),
            "instrument_namespace": "ns",
            "inventory_sha256": {name: f"sha-{i}" for i, name in enumerate(names, 1)},
            "resolved_job": {"streams": [{"stream_id": "one"}, {"stream_id": "other"}]},
            "manifest": {
                "dataset_id": 77,
                "protocol_version": 5,
                "content_type": "trades",
                "integrity_status": "clean",
                "processing_quality": {"timestamp_ordering": "event_timestamp_ns"},
                "streams": [
                    {"directory": directory, "stream_id": "one", "sources": [{"task_id": 5}]}
                ],
                "files": [
                    {"path": name, "start_timestamp_ns": 10, "end_timestamp_ns": 20}
                    for name in names
                ],
            },
        }

    monkeypatch.setattr(service, "database_connection", connect)
    monkeypatch.setattr(service, "database_dsn", lambda root: "fixture")
    monkeypatch.setattr(service, "verify", verify)
    selected = await service.select_inputs(paths, ids=[2, 1])
    assert verification_calls == [root]
    assert [item["primary_file"] for item in selected] == [names[1], names[0]]
    assert len(service.publication_selection(selected)) == 1


@pytest.mark.parametrize("tui", [False, True])
def test_execution_uses_pinned_runtime_and_rejects_changed_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tui: bool
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    runtime = service.runtime_path(paths)
    runtime.parent.mkdir(parents=True)
    original_runtime = b"[resources]\nworkers = 2\n"
    runtime.write_bytes(original_runtime)
    binary = tmp_path / "marketforge-merge"
    binary.touch()
    selected = {
        "database_id": 3,
        "engine_id": 20,
        "stream_id": "stream",
        "content_type": "trades",
        "namespace": "ns",
        "path": "/input",
        "registered_path": "/input/part.parquet",
        "manifest_path": "/input/manifest.json",
        "file_sha256": "a" * 64,
        "manifest_sha256": "b" * 64,
        "start_ns": 10,
        "end_ns": 19,
        "memberships": [{"stream_id": "stream"}],
    }

    async def select(_paths: ProjectPaths, *, ids=None, job=None):
        item = dict(selected)
        if job:
            item["stream_rank"] = job["inputs"][0]["stream_rank"]
        return [item]

    validated = []

    def validate(_paths, _binary, _source, config):
        validated.append(config)

    expected_tui = tui

    def process(argv, _root, directory, *, tui):
        assert tui == expected_tui
        if tui:
            assert Path(argv[argv.index("--config-origin") + 1]) == runtime
        else:
            assert "--config-origin" not in argv
        config = Path(argv[argv.index("--config") + 1])
        assert config == validated[-1] == directory / runtime.name
        assert config.read_bytes() == original_runtime
        return 1, False

    monkeypatch.setattr(service, "resolve", lambda *_: binary)
    monkeypatch.setattr(service, "select_inputs", select)
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="registry-fixture"))
    monkeypatch.setattr(service.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "acquire", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "release", AsyncMock())
    monkeypatch.setattr(service, "validate_job", validate)
    monkeypatch.setattr(service, "effective_runtime", lambda *_: {"resources": {"workers": 2}})
    monkeypatch.setattr(service, "run_process", process)
    monkeypatch.setattr(service.os, "isatty", lambda _: True)
    registration = AsyncMock()
    monkeypatch.setattr(service, "register_verified", registration)
    recipe = service.Recipe(dataset_ids=[3], start_ns=10, end_ns=20, stream_ranks={"stream": 7})
    prepared = service.prepare_job(paths, recipe, "manual", 1, paths.runs_dir / "prepare", [3], {})
    source = Path(prepared["job_path"])
    receipt = service.execute(paths, source, tui=tui)
    assert receipt["configuration_origin"] == str(runtime)
    assert receipt["effective_runtime"]["resources"]["workers"] == 2
    assert receipt["configuration_sha256"] == service.checksum(runtime)
    assert receipt["registration"] == "not_started"
    runtime.write_text("[resources]\nworkers = 4\n")
    resumed = service.execute(paths, source, tui=tui)
    assert resumed["configuration_sha256"] == receipt["configuration_sha256"]
    registration.assert_not_called()

    job = service.read_json(source)
    job["inputs"][0]["stream_rank"] = 3
    source.write_text(json.dumps(job))
    with pytest.raises(MarketForgeError, match="Prepared merge job.*changed"):
        service.execute(paths, source)
    job["inputs"][0]["stream_rank"] = 7
    source.write_text(json.dumps(job))
    source.with_name(runtime.name).write_text("[resources]\nworkers = 1\n")
    with pytest.raises(MarketForgeError, match="Prepared merge job.*changed"):
        service.execute(paths, source)
