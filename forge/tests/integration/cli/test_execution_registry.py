"""Registry integration against the explicitly verified testing database."""

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio

from forge.orchestration import registry
from forge.orchestration.registry_maintenance import index_legacy, reconcile
from forge.orchestration.runs import inspect_run
from forge.shared.database.connection import DatabaseError, database_environment
from forge.shared.database.credentials import TEST_DATABASE_KEY
from forge.shared.database.testing import testing_connection as registry_connection
from forge.shared.database.testing import testing_dsn as registry_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation, new_run_id
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]
ROOT = Path(__file__).resolve().parents[4]


@pytest_asyncio.fixture
async def managed_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[ProjectPaths]:
    dsn = registry_dsn(ROOT)
    monkeypatch.setenv(TEST_DATABASE_KEY, dsn)
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    prefix = str(paths.runs_dir) + "/"
    with database_environment(testing=True):
        yield paths
    async with registry_connection(dsn) as connection, connection.transaction():
        await connection.execute(
            """DELETE FROM historical.execution_resources WHERE attempt_id IN (
                SELECT a.attempt_id FROM historical.execution_attempts a JOIN historical.execution_runs r USING(run_id)
                WHERE left(r.artifact_root,length(%s))=%s)""",
            (prefix, prefix),
        )
        await connection.execute(
            """DELETE FROM historical.execution_attempts WHERE run_id IN (
                SELECT run_id FROM historical.execution_runs WHERE left(artifact_root,length(%s))=%s)""",
            (prefix, prefix),
        )
        await connection.execute(
            "DELETE FROM historical.execution_runs WHERE left(artifact_root,length(%s))=%s",
            (prefix, prefix),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", tuple(RunOperation))
async def test_each_operation_is_indexed_with_bounded_attempt_evidence(
    managed_paths: ProjectPaths,
    operation: RunOperation,
) -> None:
    with database_environment(testing=True):
        execution = await registry.begin(managed_paths, operation, "prepare", recipe="fixture")
        assert execution.id.startswith(f"{operation}-")
        assert execution.root.parent == managed_paths.runs_dir
        await execution.finish({"status": "prepared", "batch_path": "fixture-batch.json"})
        result = await inspect_run(managed_paths, execution.id, action="show", limit=10)
        assert result["status"] == "prepared" and result["duration_seconds"] >= 0
        assert result["database_purpose"] == "testing"
        assert len(result["attempts"]) == 1
        assert result["attempts"][0]["number"] == 1
        assert result["resources"][0]["kind"] == "prepared_batch"
        page = await registry.list_index(
            managed_paths, limit=1, recipe="fixture", operation=operation
        )
        assert page["runs"]
        with pytest.raises(MarketForgeError, match="does not match"):
            await registry.begin(
                managed_paths,
                operation,
                "run",
                identity=new_run_id(
                    RunOperation.PIPELINE
                    if operation != RunOperation.PIPELINE
                    else RunOperation.MERGE
                ),
            )


@pytest.mark.asyncio
async def test_pipeline_attempts_child_and_finalization_retry(
    managed_paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with database_environment(testing=True):
        identifier = new_run_id(RunOperation.PIPELINE)
        parent = await registry.begin(
            managed_paths, RunOperation.PIPELINE, "run", identity=identifier
        )
        with pytest.raises(DatabaseError):
            await registry.begin(managed_paths, RunOperation.PIPELINE, "run", identity=identifier)
        with registry.active(parent):
            child = await registry.begin(managed_paths, RunOperation.ACQUISITION, "map")
            await child.finish({"status": "complete", "requests": [{"scan_id": 32}]})
        await parent.finish({"state": "interrupted"})
        retained = parent.outcome.read_bytes()
        with pytest.raises(DatabaseError):
            await registry.begin(
                managed_paths,
                RunOperation.PIPELINE,
                "resume",
                identity=identifier,
                resume=True,
                evidence={"environment": "production", "registry_id": str(uuid4())},
            )
        next_attempt = await registry.begin(
            managed_paths,
            RunOperation.PIPELINE,
            "resume",
            identity=identifier,
            resume=True,
            evidence={"environment": "testing", "registry_id": parent.registry},
        )
        with monkeypatch.context() as patch:
            patch.setattr(registry, "finalize", AsyncMock(side_effect=DatabaseError("offline")))
            result = await next_attempt.finish({"state": "stopped"})
        assert "execution_registry_warning" in result
        page = await reconcile(managed_paths, limit=1000, offset=0, apply=False)
        assert any(
            row["status"] == "retained_outcome" and row["run_id"] == parent.id
            for row in page["outcomes"]
        )
        assert (await registry.show_index(managed_paths, parent.id) or {})["status"] == "running"
        await reconcile(managed_paths, limit=1000, offset=0, apply=True)
        result = await registry.show_index(managed_paths, parent.id)
        assert result is not None and result["status"] == "stopped"
        assert [attempt["status"] for attempt in result["attempts"]] == ["interrupted", "stopped"]
        assert [item["run_id"] for item in result["children"]] == [child.id]
        assert parent.outcome.read_bytes() == retained


@pytest.mark.asyncio
async def test_legacy_index_uses_retained_identity_and_is_idempotent(
    managed_paths: ProjectPaths,
) -> None:
    import json

    with database_environment(testing=True):
        async with registry.connection_for(managed_paths) as (_, identity):
            pass
        run = str(uuid4())
        path = managed_paths.runs_dir / run / "report.json"
        path.parent.mkdir(parents=True)
        evidence: dict[str, Any] = {
            "kind": "historical_pipeline",
            "run_id": run,
            "environment": "testing",
            "registry_id": identity,
            "state": "complete",
            "started_at": "2026-11-20T00:00:00Z",
            "finished_at": "2026-11-20T00:00:04Z",
        }
        path.write_text(json.dumps(evidence))
        original = path.read_bytes()
        await index_legacy(managed_paths, limit=10, offset=0, apply=False)
        assert await registry.show_index(managed_paths, run) is None
        for _ in range(2):
            outcome = await index_legacy(managed_paths, limit=10, offset=0, apply=True)
            assert outcome["outcomes"][0]["status"] == "indexed"
        assert path.read_bytes() == original
        assert (await inspect_run(managed_paths, run, action="report", limit=10)) == evidence


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["normalize", "merge", "convert"])
async def test_prepare_and_execute_cli_share_one_logical_run(
    managed_paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
) -> None:
    import asyncio
    import json
    from unittest.mock import Mock

    from forge.cli.commands import conversion, merge, normalization
    from forge.cli.main import main

    family = {"normalize": "normalization", "merge": "merge", "convert": "conversion"}[operation]
    directory = managed_paths.workflow_dir(family)
    directory.mkdir(parents=True)
    source = directory / "combined.toml"
    source.write_text("dataset_ids=[1]\nstart_ns=1\nend_ns=2\n")
    if operation == "normalize":
        from forge.historical.archives.templates import ACQUISITION_TEMPLATE

        source.write_text(ACQUISITION_TEMPLATE)
    elif operation == "convert":
        from forge.historical.conversion.recipes import create

        setting = managed_paths.workflow_dir("merge")
        setting.mkdir()
        (setting / "upstream.toml").write_text(source.read_text())
        source.unlink()
        create(managed_paths, "combined", "merge", "upstream", "USDT")
    job = managed_paths.runs_dir / "prepared.json"
    execution = managed_paths.runs_dir / "execution-result.json"

    prepare_result = {"status": "prepared", "batch_path": str(job), "job_path": str(job)}
    result = {
        "status": "complete",
        "execution_id": "fixture-execution",
        "report_path": str(execution),
    }
    if operation == "normalize":
        monkeypatch.setattr(normalization, "prepare_batch", AsyncMock(return_value=prepare_result))
        monkeypatch.setattr(normalization, "execute", Mock(return_value=result))
    elif operation == "merge":
        monkeypatch.setattr(merge.service, "prepare", Mock(return_value=prepare_result))
        monkeypatch.setattr(merge.service, "execute", Mock(return_value=result))
    else:
        monkeypatch.setattr(
            conversion.service,
            "prepare",
            AsyncMock(return_value={**prepare_result, "status": "generated"}),
        )
        monkeypatch.setattr(conversion, "execute", AsyncMock(return_value=result))

    managed_paths.runs_dir.mkdir(parents=True)
    execution.write_text(json.dumps(result))
    args = ["--project-root", str(managed_paths.root), "--json"]
    assert await asyncio.to_thread(main, [*args, operation, "run", "combined", "--testing"]) == 0
    public: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert public["run_id"].startswith(f"{operation}-")
    assert (
        await asyncio.to_thread(main, [*args, "runs", "show", public["run_id"], "--testing"]) == 0
    )
    indexed: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert len(indexed["attempts"]) == 1
    assert indexed["status"] == "completed"
    assert {(item["kind"], item["reference"]) for item in indexed["resources"]} >= {
        ("engine_execution", "fixture-execution"),
        ("prepared_job", str(job)),
    }
    assert (
        await asyncio.to_thread(main, [*args, "runs", "report", public["run_id"], "--testing"]) == 0
    )
    assert json.loads(capsys.readouterr().out) == result
    assert (
        await asyncio.to_thread(main, [*args, "runs", "logs", public["run_id"], "--testing"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "events_missing"


@pytest.mark.asyncio
async def test_pipeline_service_resumes_with_same_registry_identity(
    managed_paths: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.historical.pipelines import service
    from forge.startup.context import build_context

    source = ROOT / "forge/tests/unit/historical/fixtures/bybit_pipeline.toml"
    with database_environment(testing=True):
        context = build_context(project_root=managed_paths.root, testing=True)
        pipeline = service.new(context, source.stem, source.read_bytes(), source=source)
        pipeline.report.update(engines={}, settings_files={})
        monkeypatch.setattr(service.PipelineRun, "acquire", AsyncMock(return_value=None))
        monkeypatch.setattr(service.PipelineRun, "normalize", AsyncMock(return_value=None))
        first = await pipeline.run(stop=True)
        resumed = service.resume(context, first["run_id"])
        second = await resumed.run(stop=True)
        assert first["run_id"] == second["run_id"]
        record = await registry.show_index(managed_paths, first["run_id"])
        assert record is not None and record["status"] == "stopped"
        assert [attempt["action"] for attempt in record["attempts"]] == ["run", "resume"]
        assert all(
            map(Path.is_file, [Path(attempt["report_path"]) for attempt in record["attempts"]])
        )


@pytest.mark.asyncio
async def test_run_history_order_is_global_and_stable(managed_paths: ProjectPaths) -> None:
    from forge.shared.presentation.runs import inventory

    name = uuid4().hex
    created: list[str] = []
    for i in range(4):
        execution = await registry.begin(managed_paths, RunOperation.MERGE, "run", recipe=name)
        if i < 3:
            await execution.finish({"status": "complete"})
        created.append(execution.id)
    async with registry.connection_for(managed_paths) as (connection, _):
        await connection.execute(
            "UPDATE historical.execution_runs SET started_at=%s WHERE run_id=ANY(%s)",
            ("2025-01-01T00:00:00Z", created[:2]),
        )
        await connection.execute(
            "UPDATE historical.execution_runs SET started_at=%s WHERE run_id=%s",
            ("2025-01-01T01:00:00.000001+01:00", created[2]),
        )
    tied = sorted(created[:2])
    first = await registry.list_index(managed_paths, recipe=name, limit=1, order="asc")
    second = await registry.list_index(
        managed_paths, recipe=name, limit=1, sort="time", order="asc", offset=1
    )
    assert first["runs"][0]["run_id"] == tied[0]
    assert second["runs"][0]["run_id"] == tied[1]
    newest = await registry.list_index(managed_paths, recipe=name, limit=2)
    assert [row["run_id"] for row in newest["runs"]] == [created[3], created[2]]
    for direction in ("asc", "desc"):
        ranked = await registry.list_index(
            managed_paths, recipe=name, limit=4, sort="end", order=direction
        )
        assert ranked["runs"][-1]["finished_at"] is None
    page = inventory(first, (("recipe", name),), "testing", 1, sort="time", order="asc")
    assert f"--recipe {name} --sort time --order asc" in page.next_command


@pytest.mark.asyncio
@pytest.mark.parametrize("recover", [False, True])
async def test_four_stage_completion_and_exact_reconciliation(
    managed_paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch, recover: bool
) -> None:
    import json

    with database_environment(testing=True):
        execution = await registry.begin(managed_paths, RunOperation.PIPELINE, "run")
        unrelated = await registry.begin(managed_paths, RunOperation.PIPELINE, "run")
        reference = "a" * 64
        report = execution.root / "report.json"
        result = {
            "run_id": execution.id,
            "state": "complete",
            "registry_id": execution.registry,
            "environment": "testing",
            "report_path": str(report),
            "stages": {
                "acquire": {"state": "complete"},
                "normalize": {"state": "complete", "prepared": {"job": "/fixture/batch.json"}},
                "merge": {"state": "complete"},
                "convert": {
                    "state": "complete",
                    "result": {
                        "jobs": [
                            {
                                "status": "complete",
                                "indexing": "complete",
                                "work_key": reference,
                                "publication": {"manifest": "/fixture/output/manifest.json"},
                                "input": {
                                    "registry_publication_id": 41,
                                    "engine_publication_id": "501",
                                },
                            }
                        ]
                    },
                },
            },
        }
        report.write_text(json.dumps(result))
        if recover:
            with monkeypatch.context() as patch:
                patch.setattr(registry, "finalize", AsyncMock(side_effect=DatabaseError("offline")))
                failed = await execution.finish(result)
                await unrelated.finish({"state": "complete"})
            assert failed["execution_registry_warning"]
            kept = execution.outcome.read_bytes()
            assert (await registry.show_index(managed_paths, execution.id) or {})[
                "status"
            ] == "running"
            outcome = json.loads(kept)
            async with registry.connection_for(managed_paths) as (connection, _):
                for invalid in (
                    ["unsupported", "report", "/fixture/report"],
                    ["convert", "unknown_kind", reference],
                    ["merge", "conversion_output_ref", reference],
                ):
                    with pytest.raises(MarketForgeError, match="resource association"):
                        await registry.finalize(
                            connection, {**outcome, "resources": [invalid]}, execution.outcome
                        )
                with pytest.raises(DatabaseError):
                    await registry.finalize(
                        connection, {**outcome, "registry_id": str(uuid4())}, execution.outcome
                    )
                with pytest.raises(MarketForgeError, match="indexed execution attempt"):
                    await registry.finalize(
                        connection, {**outcome, "attempt_id": str(uuid4())}, execution.outcome
                    )
            preview = await reconcile(
                managed_paths, limit=1, offset=0, run_id=execution.id, apply=False
            )
            assert [item["status"] for item in preview["outcomes"]] == ["retained_outcome"]
            repaired = await reconcile(
                managed_paths, limit=1, offset=0, run_id=execution.id, apply=True
            )
            assert [item["status"] for item in repaired["outcomes"]] == ["reconciled"]
            assert (await registry.show_index(managed_paths, unrelated.id) or {})[
                "status"
            ] == "running"
            assert execution.outcome.read_bytes() == kept
        else:
            assert (await execution.finish(result))["execution_registry_status"] == "finalized"
        before_retry = execution.outcome.read_bytes()
        assert (await execution.finish(result))["execution_registry_status"] == "finalized"
        assert execution.outcome.read_bytes() == before_retry
        repeated = await reconcile(
            managed_paths, limit=1, offset=0, run_id=execution.id, apply=True
        )
        assert [item["status"] for item in repeated["outcomes"]] == ["already_reconciled"]
        record = await registry.show_index(managed_paths, execution.id)
        assert record is not None and record["status"] == "completed"
        assert [str(item["attempt_id"]) for item in record["attempts"]] == [
            str(execution.attempt_id)
        ]
        resources = [
            (item["stage"], item["kind"], item["reference"]) for item in record["resources"]
        ]
        assert ("convert", "conversion_output_ref", reference) in resources
        assert ("convert", "registry_publication", "41") in resources
        assert ("convert", "source_publication", "501") in resources
        assert ("normalize", "prepared_batch", "/fixture/batch.json") in resources
        assert len(resources) == len(set(resources))
