"""Diagnostic projection tests based on retained producer contracts."""

import hashlib
import json
from pathlib import Path

import pytest

from forge.orchestration import run_diagnostics
from forge.orchestration.run_diagnostics import DiagnosticReader
from forge.orchestration.runs import RunInspection
from forge.shared.paths import ProjectPaths
from forge.shared.presentation.run_diagnostics import (
    Diagnostic,
    EvidenceSource,
    diagnostics,
    explain,
)

ID = "pipeline-11111111-1111-4111-8111-111111111111"
CHILD = "acquisition-22222222-2222-4222-8222-222222222222"


def indexed(root: Path, name: str, operation: str, **extras: object) -> dict[str, object]:
    return {
        "run_id": name,
        "operation": operation,
        "status": "failed",
        "database_id": "fixture-registry",
        "database_purpose": "testing",
        "artifact_root": str(root / name),
        **extras,
    }


def save(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


@pytest.mark.parametrize(
    ("operation", "value", "messages"),
    [
        (
            "acquisition",
            {
                "requests": [
                    {
                        "request": {"scope": {"identifier": "SOLUSDT"}, "exchange": "bybit"},
                        "status": "failed",
                        "error": "HTTP 403",
                    },
                    {"status": "failed", "error": "Unsupported request"},
                ]
            },
            [("error", "HTTP 403"), ("error", "Unsupported request")],
        ),
        (
            "normalize",
            {
                "requests": [
                    {"blockers": [{"local_id": 17, "reason": "Missing historical specification"}]},
                    {
                        "selected_archives": [
                            {"metadata_warnings": ["Optional metadata unavailable"]}
                        ]
                    },
                ],
                "engine_result": {
                    "jobs": [
                        {
                            "job_id": 2**63 + 12,
                            "status": "failed",
                            "failure": {
                                "failed_task_id": 928,
                                "error_code": "source_open",
                                "error_message": "Open failed\nCaused by: archive",
                            },
                        }
                    ]
                },
                "registration_error": "Registry failure",
            },
            [
                ("error", "Missing historical specification"),
                ("warning", "Optional metadata unavailable"),
                ("error", "Open failed\nCaused by: archive"),
                ("error", "Registry failure"),
            ],
        ),
        (
            "merge",
            {
                "blockers": [{"publication_id": 1, "reason": "Ordering conflict"}],
                "engine_result": {"status": "failed", "error": "Engine error"},
                "verification_error": "Manifest mismatch",
            },
            [
                ("error", "Ordering conflict"),
                ("error", "Engine error"),
                ("error", "Manifest mismatch"),
            ],
        ),
        (
            "convert",
            {
                "compatibility_blockers": ["No compatible reference"],
                "jobs": [{"indexing": "failed", "error": "Output index failed"}],
                "statistics": {"unavailable_events": 500, "reason": "No route"},
            },
            [("error", "No compatible reference"), ("error", "Output index failed")],
        ),
        (
            "pipeline",
            {
                "error": "Pipeline interrupted",
                "state": "interrupted",
                "stages": {
                    "normalize": {"state": "failed", "summary": "Profile missing"},
                    "merge": {"state": "pending", "summary": "Waiting"},
                },
            },
            [("info", "Pipeline interrupted"), ("error", "Profile missing")],
        ),
    ],
)
def test_projects_actual_diagnostic_fields(
    tmp_path: Path, operation: str, value: object, messages: list[tuple[str, str]]
) -> None:
    entries = list(diagnostics(value, EvidenceSource(tmp_path / "report.json", ID, operation)))
    assert [(item.severity, item.message) for item in entries] == messages
    if operation == "acquisition":
        assert "retained evidence records an error" in explain(entries[0]).happened
    if operation == "convert":
        assert "compatibility validation" in explain(entries[0]).happened
    if operation == "merge":
        assert "verification" in explain(entries[-1]).happened
    if operation == "normalize":
        assert "could not open" in explain(entries[2]).happened
        assert dict(entries[2].metadata)["job_id"] == 2**63 + 12
        assert dict(entries[2].metadata)["failed_task_id"] == 928
    if operation == "pipeline":
        assert entries[1].source.stage == "normalize"
        assert entries[1].pointer == "/stages/normalize/summary"


async def all_entries(
    reader: DiagnosticReader,
) -> tuple[list[Diagnostic], run_diagnostics.DiagnosticPage]:
    result: list[Diagnostic] = []
    for _ in range(50):
        page = await reader.load()
        result.extend(page.entries)
        if not page.more:
            return result, page
    pytest.fail("Pagination did not finish")


@pytest.mark.asyncio
async def test_paged_logs_preserve_order_occurrences_and_source_context(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.runs_dir
    directory = root / CHILD
    report = save(
        directory / "report.json",
        {
            "requests": [
                {"request": {"exchange": "bybit", "scope": {"identifier": "SOLUSDT"}}},
            ]
        },
    )
    log = directory / "events.jsonl"
    message = "Traceback (most recent call last):\n" + "line of retained evidence\n" * 150
    events = [
        {"level": "info", "message": "error counter updated"},
        *(
            {
                "status": "failed",
                "request_index": 0,
                "remote_archive_id": 2**56 + 1,
                "timestamp": f"2026-11-23T20:00:00.{i:09d}Z",
                "error": message,
            }
            for i in range(75)
        ),
        {"severity": "warning", "message": "Incomplete coverage"},
    ]
    log.write_text("".join(json.dumps(item) + "\n" for item in events) + '{"partial":')
    record = indexed(root, CHILD, "acquisition", report_path=str(report))
    reader = DiagnosticReader(paths, RunInspection(CHILD, record, record))
    before = log.read_bytes()
    first = await reader.load()
    assert first.more and len(first.entries) == 30
    entries, final = await all_entries(reader)
    entries = [*first.entries, *entries]
    assert [item.line for item in entries] == list(range(2, 78))
    assert all(item.message == message for item in entries[:-1])
    assert entries[0].timestamp.endswith(".000000000Z")
    assert entries[0].pointer == "/error"
    assert dict(entries[0].metadata)["scope"] == {"identifier": "SOLUSDT"}
    assert dict(entries[0].metadata)["remote_archive_id"] == 2**56 + 1
    assert entries[-1].severity == "warning"
    assert "incomplete" in " ".join(final.notices)
    assert log.read_bytes() == before


@pytest.mark.asyncio
async def test_pipeline_children_attempts_and_unsafe_or_missing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.runs_dir
    child_report = save(root / CHILD / "report.json", {"error": "Child failure"})
    other = "merge-33333333-3333-4333-8333-333333333333"
    child_attempt = save(
        root / CHILD / "attempts/attempt-two/outcome.json", {"error": "Retry failure"}
    )
    save(root / other / "report.json", {"error": "Another registry"})
    unsafe = tmp_path / "outside.json"
    unsafe.write_text('{"error":"Must not expose"}')
    malicious = root / ID / "malicious.json"
    report = save(
        root / ID / "report.json",
        {
            "error": "Parent failure",
            "stages": {
                "acquire": {
                    "state": "failed",
                    "child_run_id": CHILD,
                    "child_report": str(child_report),
                },
                "normalize": {"state": "pending"},
                "merge": {"state": "skipped"},
            },
        },
    )
    malicious.symlink_to(unsafe)
    record = indexed(
        root,
        ID,
        "pipeline",
        report_path=str(report),
        children=[
            {"run_id": CHILD, "operation": "acquisition"},
            {"run_id": other, "operation": "merge"},
        ],
        resources=[
            {"kind": "report", "reference": str(malicious)},
            {"kind": "report", "reference": str(root / ID / "missing.log")},
        ],
    )

    async def inspect(_paths: ProjectPaths, name: str, **_: object) -> RunInspection:
        if name == CHILD:
            result = indexed(
                root,
                CHILD,
                "acquisition",
                parent_run_id=ID,
                report_path=str(child_report),
                attempts=[{"attempt_id": "attempt-two", "outcome_path": str(child_attempt)}],
            )
        else:
            result = indexed(root, name, "merge", parent_run_id="some-other-run")
        return RunInspection(name, result, result)

    monkeypatch.setattr(run_diagnostics, "inspect_run_evidence", inspect)
    reader = DiagnosticReader(paths, RunInspection(ID, record, record))
    entries, page = await all_entries(reader)
    assert [(item.source.run_id, item.message) for item in entries] == [
        (ID, "Parent failure"),
        (CHILD, "Child failure"),
        (CHILD, "Retry failure"),
    ]
    assert entries[-1].source.attempt == "attempt-two"
    assert [stage.state for stage in page.stages] == ["failed", "pending", "skipped"]
    assert "missing.log" in " ".join(page.notices)
    assert "malicious.json" in " ".join(page.notices)
    assert "selected registry" in " ".join(page.notices)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["complete", "failed"])
async def test_empty_report_keeps_outcome_and_zero_diagnostics(tmp_path: Path, state: str) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    report = save(paths.runs_dir / ID / "report.json", {"state": state})
    record = indexed(paths.runs_dir, ID, "pipeline", status=state, report_path=str(report))
    reader = DiagnosticReader(paths, RunInspection(ID, record, record))
    entries, page = await all_entries(reader)
    assert reader.status == state
    assert not entries and not page.notices


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["missing", "invalid", "oversized", "other-registry"])
async def test_report_gaps_do_not_create_diagnostics(tmp_path: Path, problem: str) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    report = paths.runs_dir / ID / "report.json"
    report.parent.mkdir(parents=True)
    if problem == "oversized":
        with report.open("wb") as stream:
            stream.truncate(run_diagnostics.REPORT_LIMIT + 1)
    elif problem == "invalid":
        report.write_bytes(b'{"state":')
    elif problem == "other-registry":
        save(report, {"registry_id": "mismatched", "error": "Must remain unshown"})
    record = indexed(paths.runs_dir, ID, "pipeline", report_path=str(report))
    entries, page = await all_entries(DiagnosticReader(paths, RunInspection(ID, record, record)))
    assert not entries
    assert any("execution report is missing or unavailable" in notice for notice in page.notices)


@pytest.mark.asyncio
async def test_indexed_nested_engine_validation_and_large_event(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    name = "normalize-11111111-1111-4111-8111-111111111111"
    root = paths.runs_dir
    report = save(
        root
        / "normalize-prepare-11111111111111111111111111111111"
        / "validation-abc/prepare-report.json",
        {
            "status": "invalid",
            "failed_stage": "Engine batch validation",
            "validation_error": "error: failed to resolve profile\nCaused by: missing metadata",
        },
    )
    (report.parent / "validation-stderr.log").write_text("first Rust line\ncontinuation\n")
    events = root / name / "events.jsonl"
    events.parent.mkdir()
    events.write_bytes(
        b"x" * (run_diagnostics.EVENT_LIMIT + 1)
        + b'\n{"level":"error","message":"next intact event"}\n'
    )
    record = indexed(root, name, "normalize", report_path=str(report))
    entries, page = await all_entries(DiagnosticReader(paths, RunInspection(name, record, record)))
    assert [(item.severity, item.message) for item in entries] == [
        ("error", "error: failed to resolve profile\nCaused by: missing metadata"),
        ("info", "first Rust line\ncontinuation\n"),
        ("error", "next intact event"),
    ]
    assert entries[0].source.stage == "Engine batch validation"
    assert entries[-1].line == 2
    assert "over 1 MiB" in " ".join(page.notices)


@pytest.mark.asyncio
async def test_distinct_attempt_outcomes_and_shared_report_keep_correct_identity(
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    folder = paths.runs_dir / ID
    report = save(folder / "report.json", {"error": "Current checkpoint"})
    attempts = [
        {
            "attempt_id": identity,
            "report_path": str(report),
            "outcome_path": str(
                save(folder / "attempts" / identity / "outcome.json", {"error": msg})
            ),
        }
        for identity, msg in (("attempt-one", "First attempt"), ("attempt-two", "Second attempt"))
    ]
    record = indexed(paths.runs_dir, ID, "pipeline", report_path=str(report), attempts=attempts)
    entries, _ = await all_entries(DiagnosticReader(paths, RunInspection(ID, record, record)))
    assert [(entry.source.attempt, entry.message) for entry in entries] == [
        ("", "Current checkpoint"),
        ("attempt-one", "First attempt"),
        ("attempt-two", "Second attempt"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition",
    [
        "verified",
        "structured",
        "missing",
        "invalid",
        "registry",
        "changed",
        "unsafe",
        "oversized",
        "symlink",
    ],
)
async def test_pipeline_conversion_explanation(tmp_path: Path, condition: str) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.runs_dir
    parent = root / ID
    recipe = parent / "historical/conversion-recipe.toml"
    recipe.parent.mkdir(parents=True)
    recipe.write_text("version = 1\n")
    preparation = root / ("convert-prepare-" + "a" * 32) / "prepare-report.json"
    if condition == "unsafe":
        preparation = tmp_path / "outside/prepare-report.json"
    job = preparation.parent / "job-1.json"
    reason = (
        f"Engine validation failed for {job}: Conversion failed: "
        "invalid configuration: trade conversion streams have no available path"
    )
    if condition != "missing":
        save(
            preparation,
            {
                "registry_id": "wrong" if condition == "registry" else "fixture-registry",
                "environment": "testing",
                "report_path": str(preparation),
                "recipe": {
                    "path": str(recipe),
                    "sha256": hashlib.sha256(recipe.read_bytes()).hexdigest(),
                },
                "compatibility": "not_validated",
                "compatibility_blockers": [reason],
                "upstream": {"registry_publication_id": 1375},
                "settings": {"target_quote": "USDT"},
                "job_path": str(preparation.parent / "convert.json"),
                "jobs": [{"path": str(job), "job_id": 1, "engine_validation": "failed"}],
            },
        )
    if condition == "invalid":
        preparation.write_text("[invalid JSON")
    if condition == "oversized":
        preparation.write_bytes(b"x" * (run_diagnostics.REPORT_LIMIT + 1))
    if condition == "symlink":
        moved = preparation.with_name("original.json")
        preparation.rename(moved)
        preparation.symlink_to(moved)
    stage: dict[str, object] = {"state": "blocked", "phase": "validate"}
    if condition == "changed":
        stage["failed_preparation"] = {"report": str(preparation), "report_sha256": "0" * 64}
    summary = f"Conversion preparation did not validate; inspect {preparation}."
    if condition == "structured":
        stage["failed_preparation"] = {
            "report": str(preparation),
            "report_sha256": hashlib.sha256(preparation.read_bytes()).hexdigest(),
        }
        summary = "Conversion preparation did not validate."

    document = {
        "kind": "historical_pipeline",
        "run_id": ID,
        "state": "blocked",
        "active_stage": "convert",
        "stages": {"merge": {"state": "complete"}, "convert": stage},
        "error": summary,
    }
    report = save(parent / "report.json", document)
    reader = DiagnosticReader(
        paths, RunInspection(ID, document, indexed(root, ID, "pipeline", report_path=str(report)))
    )
    page = await reader.load()
    entry = next(item for item in page.entries if item.message == summary)
    assert entry.source.stage == "convert"
    explanation = entry.explanation
    assert explanation is not None
    assert explanation.impact == "The Pipeline stopped before Conversion execution."
    assert [(item.name, item.state) for item in page.stages] == [
        ("merge", "complete"),
        ("convert", "blocked"),
    ]
    if condition in {"verified", "structured"}:
        assert explanation.root_cause and explanation.cause == (reason,)
        assert dict(explanation.affected)["target_quote"] == "USDT"
        assert explanation.evidence and explanation.evidence.path == preparation
        assert sum(item.message == reason for item in page.entries) == 1
    else:
        assert explanation.limitation and not explanation.cause and not explanation.evidence
        assert all(item.source.path != preparation for item in page.entries)
    reader.close()


@pytest.mark.asyncio
async def test_stage_report_chain_keeps_cycles_and_unknown_causes_bounded(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.runs_dir
    parent = root / ID
    report = parent / "report.json"
    stage_report = parent / "historical/prepare-report.json"
    detail = parent / "historical/engine-result.json"
    save(detail, {"error": "Engine result details", "report_path": str(stage_report)})
    save(
        stage_report,
        {"blockers": ["Preparation was blocked"], "engine_result_path": str(detail)},
    )
    document = {
        "kind": "historical_pipeline",
        "run_id": ID,
        "active_stage": "merge",
        "stages": {"merge": {"state": "blocked", "child_report": str(stage_report)}},
        "error": "Pipeline stopped",
    }
    save(report, document)
    reader = DiagnosticReader(
        paths, RunInspection(ID, document, indexed(root, ID, "pipeline", report_path=str(report)))
    )
    page = await reader.load()
    assert not page.more
    assert [entry.message for entry in page.entries] == [
        "Pipeline stopped",
        "Preparation was blocked",
        "Engine result details",
    ]
    explanation = page.entries[0].explanation
    assert explanation and explanation.cause == ("Preparation was blocked",)
    assert not explanation.root_cause
    assert explanation.evidence and explanation.evidence.path == stage_report
    reader.close()


@pytest.mark.asyncio
async def test_report_reference_depth_is_bounded(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    directory = paths.runs_dir / ID
    depth = run_diagnostics.REFERENCE_DEPTH
    report = directory / "report.json"
    current = report
    for number in range(depth + 2):
        next_path = directory / f"result-{number}.json"
        save(current, {"error": str(number), "result_path": str(next_path)})
        current = next_path
    reader = DiagnosticReader(
        paths,
        RunInspection(ID, {}, indexed(paths.runs_dir, ID, "pipeline", report_path=str(report))),
    )
    entries, page = await all_entries(reader)
    assert [entry.message for entry in entries] == [str(n) for n in range(depth + 1)]
    assert any("Reference depth limit reached" in notice for notice in page.notices)
    reader.close()
