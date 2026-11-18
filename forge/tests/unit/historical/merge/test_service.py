"""Managed merge selection and preparation tests with no processing subprocess."""

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
            {"database_id": 8, "stream_id": "first", "start_ns": 11, "end_ns": 40},
            {"database_id": 3, "stream_id": "first", "start_ns": 41, "end_ns": 45},
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
        ("collision", "Ambiguous engine"),
        ("namespace", "single instrument namespace"),
        ("multi", "multi-stream"),
        ("segmented", "requires v3"),
        ("depth", "requires explicit selection"),
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
            "manifest_sha256": f"manifest-{db_id}",
            "engine_dataset_id": str(engine_id),
            "instrument_namespace": namespace,
            "processing_job_sha256": "job",
            "processing_runtime_sha256": "runtime",
        }
        name = "part.parquet"
        records.append(
            (
                db_id,
                str(root / name),
                name,
                "sha",
                {"memberships": []},
                10,
                50,
                str(root),
                str(engine_id),
                namespace,
                details["manifest_sha256"],
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
                "streams": streams,
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

    monkeypatch.setattr(service, "testing_dsn", lambda root: "fixture")
    monkeypatch.setattr(service, "testing_connection", connect)
    monkeypatch.setattr(service, "verify", lambda paths, source: evidence[str(source)])
    if expected:
        with pytest.raises(MarketForgeError, match=expected):
            await service.select_inputs(paths, ids=[7, 4])
    else:
        selected = await service.select_inputs(paths, ids=[7, 4])
        assert [item["engine_id"] for item in selected] == [47, 44]
        assert [item["path"] for item in selected] == [row[7] for row in reversed(records)]


@pytest.mark.asyncio
async def test_two_file_ids_share_one_verified_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = tmp_path / "publication"
    directory = "streams/s/trades"
    names = [f"{directory}/part-{i}.parquet" for i in range(3)]
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
        )
        for i, name in enumerate(names[:2], 1)
    ]
    cursor = type("Cursor", (), {"fetchall": AsyncMock(return_value=records)})()

    @asynccontextmanager
    async def connect(_: str) -> Any:
        yield type("Connection", (), {"execute": AsyncMock(return_value=cursor)})()

    verification_calls = []

    def verify(_paths: ProjectPaths, source: Path) -> dict[str, Any]:
        verification_calls.append(source)
        return {
            "manifest_sha256": "manifest-sha",
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

    monkeypatch.setattr(service, "testing_connection", connect)
    monkeypatch.setattr(service, "testing_dsn", lambda root: "fixture")
    monkeypatch.setattr(service, "verify", verify)
    selected = await service.select_inputs(paths, ids=[2, 1])
    assert verification_calls == [root]
    assert [item["primary_file"] for item in selected] == [names[1], names[0]]
    assert len(service.publication_selection(selected)) == 1
