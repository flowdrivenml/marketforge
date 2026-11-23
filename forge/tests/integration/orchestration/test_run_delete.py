"""Run deletion against isolated registry fixtures and temporary artifact trees."""

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from forge.orchestration import registry, run_delete
from forge.orchestration.inspection import run_page
from forge.orchestration.registry_maintenance import index_legacy, reconcile
from forge.orchestration.runs import inspect_run
from forge.shared.database.connection import database_environment
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def paths(tmp_path: Path) -> ProjectPaths:
    return ProjectPaths(tmp_path, tmp_path / "data")


async def run(
    paths: ProjectPaths,
    *,
    state: str = "failed",
    operation: RunOperation = RunOperation.NORMALIZE,
    report: Path | None = None,
) -> registry.Execution:
    execution = await registry.begin(paths, operation, "run", recipe=uuid4().hex)
    (execution.root / "engine-stderr.log").write_text("retained log")
    if state != "running":
        await execution.finish({"status": state, "report_path": str(report) if report else None})
    return execution


async def delete(
    paths: ProjectPaths, *runs: str, children: bool = False, history: bool = False
) -> tuple[dict[str, Any], dict[str, Any]]:
    plan = await run_delete.preview(paths, list(runs), children=children, history_only=history)
    return plan, await run_delete.apply(paths, Path(plan["plan_path"]))


@pytest.mark.asyncio
async def test_preview_and_full_cleanup_are_distinct(paths: ProjectPaths) -> None:
    target = await run(paths)
    raw = paths.raw_dir / "archive.zip"
    output = paths.datasets_dir / "normalized" / "publication" / "part.parquet"
    for file in (raw, output):
        file.parent.mkdir(parents=True)
        file.write_bytes(b"outside the selected run")
    plan = await run_delete.preview(paths, [target.id])
    assert plan["status"] == "preview" and plan["estimated"]["removable_files"] == 3
    assert target.outcome.is_file()
    indexed = await registry.show_index(paths, target.id)
    assert indexed is not None and indexed["status"] == "failed"
    result = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert result["status"] == "completed"
    assert result["runs_hidden"] == 1 and result["attempt_records_removed"] == 1
    assert result["files_removed"] == 3 and result["removed_file_bytes"] > 0
    assert not target.root.exists()
    assert raw.is_file() and output.is_file()
    assert await registry.show_index(paths, target.id) is None
    for action in ("show", "logs", "report"):
        view = await inspect_run(paths, target.id, action=action, limit=10)
        assert view["history_removal"]["state"] == "removed"
    assert (await run_delete.apply(paths, Path(plan["plan_path"])))["status"] == "already_deleted"


@pytest.mark.asyncio
async def test_history_only_keeps_reuse_and_relationships(paths: ProjectPaths) -> None:
    parent = await registry.begin(paths, RunOperation.PIPELINE, "run")
    with registry.active(parent):
        target = await run(paths, state="complete")
    await parent.finish({"status": "complete"})
    async with registry.connection_for(paths) as (connection, identity):
        await connection.execute(
            """INSERT INTO historical.resolved_work(registry_id,operation,work_key,owner,evidence,result)
               VALUES (%s,'normalize',%s,%s,%s,%s)""",
            (
                identity,
                uuid4().hex * 2,
                target.id,
                Jsonb({"report_path": str(target.outcome)}),
                Jsonb({"status": "complete"}),
            ),
        )
    full = await run_delete.preview(paths, [target.id])
    assert full["status"] == "blocked" and any(
        "reuse" in reason for reason in full["targets"][0]["blockers"]
    )
    _, result = await delete(paths, target.id, history=True)
    assert result["status"] == "completed" and result["storage_reclaimed_bytes"] == 0
    assert result["files_removed"] == 0 and target.outcome.is_file()
    indexed = await registry.show_index(paths, target.id)
    assert indexed is not None
    assert indexed["status"] == "completed" and indexed["parent_run_id"] == parent.id
    page = await registry.list_index(paths, limit=10, recipe=(indexed["recipe"]))
    assert not page["runs"]
    assert not any(item["run_id"] == target.id for item in run_page(paths.runs_dir, 20, 0)[0])
    visible = await inspect_run(paths, target.id, action="show", limit=10)
    assert visible["history_removal"]["mode"] == "history_only"
    assert any(
        item["run_id"] == target.id and item["status"] == "history_removed"
        for item in (await index_legacy(paths, limit=20, offset=0, apply=False))["outcomes"]
    )
    assert not any(
        item["run_id"] == target.id
        for item in (await reconcile(paths, limit=20, offset=0, apply=False))["outcomes"]
    )
    async with registry.connection_for(paths) as (connection, _):
        row = await (
            await connection.execute(
                "SELECT result FROM historical.resolved_work WHERE owner=%s", (target.id,)
            )
        ).fetchone()
    assert row == ({"status": "complete"},)


@pytest.mark.asyncio
async def test_exact_batch_active_and_changed_descendants(paths: ProjectPaths) -> None:
    first = await run(paths)
    active = await run(paths, state="running")
    plan, result = await delete(paths, first.id, active.id)
    assert plan["status"] == result["status"] == "blocked"
    assert first.root.exists() and active.root.exists()
    with WorkspaceOwnership(paths):
        blocked = await run_delete.preview(paths, [first.id], history_only=True)
        assert blocked["status"] == "blocked"
    unknown = f"normalize-{uuid4()}"
    _, result = await delete(paths, first.id, unknown)
    assert result["status"] == "blocked" and first.root.is_dir()
    parent = await registry.begin(paths, RunOperation.PIPELINE, "run")
    with registry.active(parent):
        child = await run(paths)
    await parent.finish({"status": "complete"})
    no_children = await run_delete.preview(paths, [parent.id])
    assert "Surviving children" in no_children["targets"][0]["blockers"][0]
    saved = await run_delete.preview(paths, [parent.id, child.id], children=True)
    assert len(saved["targets"]) == 2 and saved["estimated"]["additional_descendants"] == 0
    with registry.active(parent):
        extra = await run(paths)
    denied = await run_delete.apply(paths, Path(saved["plan_path"]))
    assert denied["status"] == "blocked" and parent.root.is_dir() and extra.root.is_dir()
    _, done = await delete(paths, parent.id, children=True)
    assert done["status"] == "completed" and done["run_records_removed"] == 3


@pytest.mark.asyncio
async def test_artifact_ownership_alias_and_unsafe_paths(paths: ProjectPaths) -> None:
    target = await registry.begin(paths, RunOperation.MERGE, "run")
    detached = paths.runs_dir / "merge-execute-100"
    detached.mkdir()
    report = detached / "execution-result.json"
    report.write_text(
        json.dumps(
            {
                "logical_run_id": target.id,
                "registry_id": target.registry,
                "environment": target.purpose,
            }
        )
    )
    external = paths.runs_dir / "merge-prepare-101"
    external.mkdir()
    external_report = external / "prepare-report.json"
    external_report.write_text("{}")
    await target.finish(
        {"status": "failed", "report_path": str(report), "batch_path": str(external_report)}
    )
    hazard = target.root / "malicious.log"
    hazard.symlink_to(external_report)
    denied = await run_delete.preview(paths, [target.id])
    assert denied["status"] == "blocked"
    hazard.unlink()
    saved, result = await delete(paths, target.id)
    assert result["status"] == "completed"
    assert str(detached) in saved["targets"][0]["roots"] and not detached.exists()
    assert external_report.is_file()
    legacy = await inspect_run(paths, detached.name, action="report", limit=10)
    assert legacy["history_removal"]["state"] == "removed"


@pytest.mark.asyncio
async def test_stale_new_claim_new_dependency_and_interrupted_retry(
    paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = await run(paths)
    plan = await run_delete.preview(paths, [target.id])
    new = paths.runs_dir / "merge-prepare-701" / "prepare-report.json"
    new.parent.mkdir()
    new.write_text(json.dumps({"submitted_source": str(target.outcome)}))
    denied = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert denied["status"] == "blocked" and target.outcome.is_file()
    assert any("Surviving preparation" in item["reason"] for item in denied["blockers"])
    new.unlink()
    key = uuid4().hex * 2
    async with registry.connection_for(paths) as (connection, identity):
        await connection.execute(
            """INSERT INTO historical.resolved_work(registry_id,operation,work_key,owner,evidence)
               VALUES (%s,'normalize',%s,%s,'{}')""",
            (identity, key, target.id),
        )
    denied = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert denied["status"] == "blocked" and any(
        "claim" in item["reason"] for item in denied["blockers"]
    )
    async with registry.connection_for(paths) as (connection, _):
        await connection.execute("DELETE FROM historical.resolved_work WHERE work_key=%s", (key,))
    original = run_delete.unlink_reviewed

    def interrupt(root: Path, name: str, evidence: run_delete.FileSnapshot) -> None:
        original(root, name, evidence)
        raise OSError("process stopped before updating receipt")

    with monkeypatch.context() as patch:
        patch.setattr(run_delete, "unlink_reviewed", interrupt)
        partial = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert partial["status"] == "partial" and partial["runs_hidden"] == 1
    assert (await inspect_run(paths, target.id, action="show", limit=10))["history_removal"][
        "state"
    ] == "retiring"
    final = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert final["status"] == "completed" and final["files_removed"] == 2
    assert len(final["missing_on_retry"]) == 1
    assert not target.root.exists()


@pytest.mark.asyncio
async def test_plan_environment_must_match(paths: ProjectPaths) -> None:
    target = await run(paths)
    plan = await run_delete.preview(paths, [target.id])
    with database_environment(testing=False), pytest.raises(MarketForgeError, match="environment"):
        await run_delete.apply(paths, Path(plan["plan_path"]))
    assert target.root.is_dir()


@pytest.mark.asyncio
async def test_operational_evidence_blocks_full_batch(paths: ProjectPaths) -> None:
    disposable = await run(paths)
    recovery = await run(paths)
    (recovery.root / "execution-intent.json").write_text("{}")
    (recovery.root / "execution-result.json").write_text(
        json.dumps({"verification": "verified", "registration": "failed"})
    )
    discovery = await run(paths)
    (discovery.root / "reuse-result.json").write_text(
        json.dumps(
            {
                "operation": "normalize",
                "disposition": "reused",
                "verification": "verified",
                "registration": "registered",
                "recipe_source": "fixture.toml",
            }
        )
    )
    checkpoint = await run(paths, operation=RunOperation.PIPELINE)
    (checkpoint.root / "report.json").write_text("{}")
    selected = [disposable.id, recovery.id, discovery.id, checkpoint.id]
    plan = await run_delete.preview(paths, selected)
    blocked = await run_delete.apply(paths, Path(plan["plan_path"]))
    reasons = {item["run_id"]: item["reason"] for item in blocked["blockers"]}
    assert blocked["status"] == "blocked"
    assert "Registration recovery" in reasons[recovery.id]
    assert "Normalization-source discovery" in reasons[discovery.id]
    assert "Pipeline resume" in reasons[checkpoint.id]
    assert disposable.root.is_dir()
    _, hidden = await delete(paths, *selected, history=True)
    assert hidden["runs_hidden"] == 4
    assert hidden["files_removed"] == 0
    assert (recovery.root / "execution-intent.json").is_file()


@pytest.mark.asyncio
async def test_changed_file_invalidates_saved_plan(paths: ProjectPaths) -> None:
    target = await run(paths)
    plan = await run_delete.preview(paths, [target.id])
    log = target.root / "engine-stderr.log"
    log.write_text("replaced since preview")
    denied = await run_delete.apply(paths, Path(plan["plan_path"]))
    assert denied["status"] == "blocked"
    assert log.read_text() == "replaced since preview"
    assert target.outcome.is_file()


@pytest.mark.asyncio
async def test_cli_reports_same_effects_in_human_and_json(paths: ProjectPaths) -> None:
    from io import StringIO

    from forge.cli.main import main

    (paths.root / "forge").mkdir()
    (paths.root / "forge" / "pyproject.toml").touch()
    (paths.root / "engine").mkdir()
    target = await run(paths)

    import asyncio
    from contextlib import redirect_stdout

    prefix = ["--project-root", str(paths.root)]

    def invoke(*args: str) -> tuple[int, str]:
        buffer = StringIO()
        with redirect_stdout(buffer):
            code = main([*prefix, *args])
        return code, buffer.getvalue()

    code, output = await asyncio.to_thread(
        invoke, "--json", "runs", "delete", target.id, "--testing", "--history-only"
    )
    plan = json.loads(output)
    assert code == 0 and plan["estimated"]["removable_files"] == 0
    options = ("runs", "delete", "--testing", "--apply", "--plan", plan["plan_path"])
    code, output = await asyncio.to_thread(invoke, *options)
    assert code == 0 and "History removed" in output and "Evidence retained" in output
    code, output = await asyncio.to_thread(invoke, "--json", *options)
    result = json.loads(output)
    assert code == 0 and result["status"] == "already_deleted"
    assert result["runs_hidden"] == 1 and result["files_removed"] == 0
    assert target.outcome.is_file()


@pytest.mark.asyncio
async def test_stale_attempt_requires_retained_reconciliation(
    paths: ProjectPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = await run(paths, state="running")

    async def database_unavailable(*args: object) -> None:
        raise MarketForgeError("Fixture interrupted index finalization")

    with monkeypatch.context() as patch:
        patch.setattr(registry, "finalize", database_unavailable)
        await target.finish({"status": "failed"})
    assert target.outcome.is_file()
    denied = await run_delete.preview(paths, [target.id], history_only=True)
    assert denied["status"] == "blocked"
    reviewed = await reconcile(paths, limit=1000, offset=0, apply=False)
    assert any(
        row["run_id"] == target.id and row["status"] == "retained_outcome"
        for row in reviewed["outcomes"]
    )
    reconciled = await reconcile(paths, limit=1000, offset=0, apply=True)
    assert any(
        row["run_id"] == target.id and row["status"] == "reconciled"
        for row in reconciled["outcomes"]
    )
    _, result = await delete(paths, target.id)
    assert result["status"] == "completed"
