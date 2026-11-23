"""Exercise retained run projections and CLI machine-output compatibility."""

import argparse
import json
from collections.abc import Callable
from hashlib import sha256
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from rich.console import Console

from forge.cli.commands import maintenance
from forge.orchestration.inspection import read_events, read_managed_logs
from forge.orchestration.registry_maintenance import MaintenanceEvidence
from forge.orchestration.runs import (
    RunInspection,
    conversion_metadata,
    conversion_receipts,
    safe_report,
)
from forge.shared.presentation import runs as views
from forge.shared.terminal import runs as terminal
from forge.shared.terminal.theme import build_theme
from forge.startup.context import ApplicationContext

RUN = "normalize-c469e5f4-4337-4d58-a17a-3188cad65bf8"
ATTEMPT = "73a47aaf-1f42-45de-99d7-9c6deed6f31a"
START = "2026-11-22T20:40:20.483233+01:00"
END = "2026-11-22T19:42:58.313180Z"


def rendered(action: Callable[[Console], None], width: int = 80) -> str:
    output = StringIO()
    action(Console(file=output, width=width, color_system=None, theme=build_theme()))
    return output.getvalue()


@pytest.mark.parametrize("width", [80, 120, 160])
def test_inventory_filters_and_exact_run_ids(width: int) -> None:
    rows = [
        {
            "run_id": RUN,
            "operation": "normalize",
            "action": "run",
            "status": "completed",
            "recipe": "α [bold] test",
            "started_at": START,
            "finished_at": END,
            "summary": {"counts": {"successful": 2, "primary_files": 7}},
        },
        {
            "run_id": "merge-68d2c10a-b6cf-486e-93ed-1185f36830a1",
            "operation": "merge",
            "action": "prepare",
            "status": "running",
            "liveness": "unknown",
            "started_at": START,
        },
    ]
    filters = (
        ("operation", "normalize"),
        ("action", "run"),
        ("status", "failed"),
        ("recipe", "α [bold] test"),
        ("start", START),
        ("end", END),
    )
    model = views.inventory({"runs": rows, "more": True, "offset": 25}, filters, "testing", 25)
    assert model.rows[0].elapsed == "00:02:37.829947"
    assert model.rows[1].elapsed == "—"
    output = rendered(lambda console: terminal.render_inventory(console, model, "testing"), width)
    assert RUN in output and str(rows[1]["run_id"]) in output
    assert "liveness unknown" in output
    assert "2 jobs successful" in output and "TESTING" in output
    assert (
        "--offset 50" in output
        and "--operation normalize" in output
        and f"--start {START}" in output
    )
    assert "[bold]" in output and "\x1b" not in output
    assert "No runs match these filters" in rendered(
        lambda console: terminal.render_inventory(
            console, views.inventory({}, filters, "testing", 5), "testing"
        )
    )


def test_detail_attempts_children_resources_and_path_bounds(tmp_path: Path) -> None:
    path = str(tmp_path / "data/runs/report.json")
    child = "merge-68d2c10a-b6cf-486e-93ed-1185f36830a1"
    record = {
        "run_id": RUN,
        "operation": "normalize",
        "action": "run",
        "status": "partial",
        "started_at": START,
        "finished_at": END,
        "parent_run_id": "pipeline-parent",
        "summary": {"execution": "complete", "verification": "verified", "registration": "failed"},
        "attempts": [
            {"number": 1, "attempt_id": ATTEMPT, "status": "failed", "started_at": START},
            {"number": 2, "attempt_id": "another", "status": "completed", "report_path": path},
        ],
        "attempts_truncated": True,
        "resources_truncated": True,
        "report_path": path,
        "children": [
            {"run_id": child, "operation": "merge", "action": "run", "status": "completed"}
        ],
        "resources": [
            {"number": 2, "stage": "normalize", "kind": "engine_publication", "reference": "101"},
            {"number": 2, "stage": "merge", "kind": "registry_publication", "reference": "42"},
            {"number": 2, "stage": "", "kind": "report", "reference": path},
            {"number": 2, "stage": "", "kind": "child_run", "reference": child},
        ]
        + [
            {
                "number": 2,
                "stage": "normalize",
                "kind": "primary_file_dataset",
                "reference": str(300 + idx),
            }
            for idx in range(35)
        ],
    }
    view = views.detail(RunInspection(RUN, record, record), tmp_path, "testing")
    assert view.attempts.records[0][1:3] == (ATTEMPT, "Failed")
    assert view.attempts.records[1][2] == "Completed"
    assert view.lifecycle == (
        ("Engine execution", "complete"),
        ("Verification", "verified"),
        ("Registration", "failed"),
    )
    assert view.children.records[0][-1] == child
    assert view.resources[0].total == 36 and len(view.resources[0].records) == 20
    output = rendered(lambda console: terminal.render_detail(console, view, "testing"), 160)
    assert "Parent run ID" in output and "pipeline-parent" in output
    assert "Engine publication ID" in output and "Registry publication ID" in output
    assert "first 1,000 attempts" in output and "first 4,096 resources" in output
    assert output.count("data/runs/report.json") == 1 and output.count(child) == 1
    assert str(tmp_path) not in output


def test_bounded_readers_and_untrusted_logs(tmp_path: Path) -> None:
    root = tmp_path / "events.jsonl"
    root.write_bytes(
        json.dumps(
            {
                "timestamp": START,
                "level": "warning",
                "stage": "merge",
                "message": "literal [bold] \x1b[2J\x1b]8;;https://bad.test\x1b\\ message token=private",
            }
        ).encode()
        + b"\nnot json\n"
        + b"x" * (1024 * 1024 + 10)
    )
    evidence = read_events(root, RUN, 2)
    view = views.logs(RunInspection(RUN, evidence, None))
    assert view.truncated and len(view.events) == 2
    assert view.events[0][:3] == ("2026-11-22 19:40:20.483233", "WARNING", "merge")
    assert view.events[1][:3] == ("—", "—", "—")
    text = rendered(lambda console: terminal.render_logs(console, view, "testing"))
    assert "Incomplete event line" in text and "1 MiB read limit" in text
    assert (
        "[bold]" in text and "\x1b" not in text and "private" not in text and "bad.test" not in text
    )
    identity = "normalize-execute-2026"
    directory = tmp_path / identity
    directory.mkdir()
    for name in (
        "engine-stdout.log",
        "engine-stderr.log",
        "validation-stdout.log",
        "validation-stderr.log",
    ):
        (directory / name).write_bytes(
            b"x" * 13000 + b"\nfirst\nsecond\xff" if "stderr" in name else b""
        )
    (directory / "engine-tui.log").write_text("Engine replay diagnostic")
    truncated: set[str] = set()
    supplemental: dict[str, str] = {}
    logs = read_managed_logs(tmp_path, identity, truncated=truncated, supplemental=supplemental)
    assert "engine-tui.log" not in logs and len(logs) == 4
    assert set(truncated) == {"engine-stderr.log", "validation-stderr.log"}
    model = views.logs(RunInspection(identity, logs, None, truncated, supplemental))
    output = rendered(lambda console: terminal.render_logs(console, model, "testing"))
    assert output.count("Showing the last 12 KiB") == 2
    assert output.count("retained stream is empty") == 2
    assert output.index("first") < output.index("second�")
    assert "Engine TUI diagnostics" in output


def test_operation_specific_report_evidence(tmp_path: Path) -> None:
    def render(operation: str, action: str, evidence: dict[str, Any]) -> str:
        index = {"operation": operation, "action": action}
        model = views.report(RunInspection(RUN, evidence, index), tmp_path, "testing")
        return rendered(lambda console: terminal.render_report(console, model, "testing"), 160)

    acquisition = {
        "status": "partial",
        "request_counts": {"archives": 14},
        "file_counts": {"failed": 2},
    }
    assert "Archives discovered" in render("acquisition", "map", acquisition)
    assert "Failed downloads" not in render("acquisition", "map", acquisition)
    assert "Failed downloads" in render("acquisition", "download", acquisition)
    normalization = render(
        "normalize",
        "run",
        {
            "status": "complete",
            "execution": "complete",
            "verification": "verified",
            "registration": "failed",
            "registration_error": "Registration timed out",
            "counts": {"requested": 2, "successful": 2, "reused": 1, "primary_files": 0},
            "jobs": [
                {
                    "job_id": 78,
                    "dataset_id": 97,
                    "verification": "verified",
                    "status": "complete",
                    "disposition": "reused",
                    "publication_id": 8,
                }
            ],
        },
    )
    assert "PARTIAL" in normalization and "Registration timed out" in normalization
    assert "Reused jobs" in normalization and "Engine publication ID" in normalization
    blocked = render(
        "merge",
        "run",
        {
            "status": "blocked",
            "compatibility": "blocked",
            "blockers": [
                {"reason": "Multiple verified submissions match.", "publication_ids": [3, 7]}
            ],
            "matched_count": 14,
            "selected_count": 0,
        },
    )
    assert (
        "Preparation" in blocked
        and "not attempted" in blocked
        and "Multiple verified submissions match." in blocked
    )
    assert "Candidate registry publications" in blocked and "3, 7" in blocked
    completed_merge = render(
        "merge",
        "run",
        {
            "status": "complete",
            "execution": "complete",
            "verification": "verified",
            "registration": "registered",
            "execution_id": "merge-execute-100",
            "dataset": {
                "engine_dataset_id": 100,
                "timeline_ordering": "causal",
                "events_written": 120,
                "files_written": 1,
            },
            "catalogue_dataset_ids": {"part.parquet": 42},
            "publications": {"merge": {"publication_id": 19}},
        },
    )
    assert (
        "Engine publication ID" in completed_merge and "Registry publication IDs" in completed_merge
    )
    assert "Market events" in completed_merge and "causal" in completed_merge
    conversion = render(
        "convert",
        "run",
        {
            "status": "complete",
            "settings": {"target_quote": "USDT"},
            "jobs": [
                {
                    "job_id": 22,
                    "status": "complete",
                    "registration": "not_available",
                    "work_key": "a" * 64,
                    "input": {"registry_publication_id": 21},
                    "publication": {"fingerprints": {"manifest.json": "sha"}},
                }
            ],
        },
    )
    assert "USDT" in conversion and "Verified" in conversion and "Not available" in conversion
    assert "Source merge registry publication IDs" in conversion and "a" * 64 in conversion
    pipeline = render(
        "pipeline",
        "run",
        {
            "kind": "historical_pipeline",
            "state": "partial",
            "stages": {
                "merge": {"state": "blocked"},
                "acquire": {"state": "complete"},
                "normalize": {"state": "complete", "child_run_id": RUN},
            },
        },
    )
    assert pipeline.index("Acquisition") < pipeline.index("Normalization") < pipeline.index("Merge")
    assert "Child run ID" in pipeline and "PARTIAL" in pipeline
    assert "Validation" in render("normalize", "validate", {"status": "valid"})
    assert "Registration recovery" in render(
        "merge", "register", {"status": "registered", "primary_file_ids": [42]}
    )
    bad = tmp_path / "report.json"
    bad.write_bytes(b"{\xff\n")
    assert "incomplete or invalid" in render("normalize", "run", safe_report(bad, tmp_path))
    assert "report unavailable" in render(
        "normalize", "run", safe_report(tmp_path / "missing.json", tmp_path)
    )
    fallback = render(
        "normalize", "legacy", {"unknown_payload": {"secret_value": "do-not-display"}}
    )
    assert (
        "unknown_payload" in fallback and "do-not-display" not in fallback and "--json" in fallback
    )


@pytest.mark.parametrize("action", ["list", "show", "logs", "report", "index-legacy", "reconcile"])
def test_machine_output_passes_through(
    action: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture = (
        {"runs": [{"run_id": RUN}]}
        if action == "list"
        else {"arbitrary": {"integer": 2**60}, "status": "valid"}
    )
    monkeypatch.setattr(maintenance, "list_index", AsyncMock(return_value=fixture))
    index = AsyncMock(return_value=fixture)
    reconcile = AsyncMock(return_value=fixture)
    monkeypatch.setattr(maintenance, "index_legacy", index)
    monkeypatch.setattr(maintenance, "reconcile", reconcile)
    monkeypatch.setattr(
        maintenance,
        "inspect_run_evidence",
        AsyncMock(return_value=RunInspection(RUN, fixture, None)),
    )
    parser = argparse.ArgumentParser()
    parser.set_defaults(command="runs", as_json=True)
    maintenance.configure_runs(parser)
    args = [action, RUN] if action in {"show", "logs", "report"} else [action]
    if action == "report":
        args += ["--section", "outputs"]
    context = cast(ApplicationContext, SimpleNamespace(paths=None))
    assert maintenance.run(parser.parse_args(args), context) == 0
    raw = capsys.readouterr().out
    assert json.loads(raw) == fixture and "\x1b" not in raw
    for mock in (index, reconcile):
        if mock.called:
            assert mock.call_args.kwargs["apply"] is False


def test_maintenance_decisions_use_returned_units() -> None:
    payload = {
        "dry_run": True,
        "offset": 5,
        "outcomes": [
            {"run_id": RUN, "attempt_id": ATTEMPT, "status": "retained_outcome"},
            {"run_id": RUN, "attempt_id": "another", "status": "missing_or_unsafe_report"},
            {"run_id": RUN, "status": "unindexed_local_evidence_requires_review"},
        ],
    }
    view = views.maintenance(payload, "reconcile")
    assert ("Attempt results on this page", "2") in view.summary
    assert ("Local evidence requiring review", "1") in view.summary
    assert view.results.records[0][1:3] == (ATTEMPT, "Recoverable")
    assert view.results.records[1][2] == "Blocked"
    with_attempt = views.maintenance(
        payload, "reconcile", MaintenanceEvidence(attempts={ATTEMPT: (3, "running")})
    )
    assert with_attempt.results.records[0][2:5] == ("3", "Running", "Recoverable")
    indexed = {"dry_run": False, "outcomes": [{"run_id": RUN, "status": "indexed"}]}
    assert ("Indexed (new or existing)", "1") in views.maintenance(indexed, "index-legacy").summary
    assert ("Already indexed", "1") in views.maintenance(
        {**indexed, "dry_run": True}, "index-legacy"
    ).summary


def test_conversion_receipts_are_scoped_to_latest_attempt_and_verified_registry(
    tmp_path: Path,
) -> None:
    current = tmp_path / "convert-execute-101" / "execution-result.json"
    old = tmp_path / "convert-execute-99" / "execution-result.json"
    mismatch = tmp_path / "convert-execute-102" / "execution-result.json"
    for path in (current, old, mismatch):
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "job_id": int(path.parent.name.split("-")[-1]),
                    "execution": "complete",
                    "registry_id": "different" if path == mismatch else "test-registry",
                }
            )
        )
    record = {
        "database_id": "test-registry",
        "attempts": [{"number": 1}, {"number": 2}],
        "resources": [
            {"kind": "report", "number": number, "reference": str(path)}
            for number, path in ((1, old), (2, current), (2, mismatch))
        ],
    }
    jobs, notes = conversion_receipts(tmp_path, record)
    assert [job["job_id"] for job in jobs] == [101]
    assert jobs[0]["result_path"] == str(current)
    assert any("different registry" in note for note in notes)
    inspection = RunInspection(
        RUN,
        {"settings": {"target_quote": "USDC"}, "compatibility": "validated"},
        {"operation": "convert", "action": "run"},
        conversion_jobs=jobs,
        inspection_notes=notes,
    )
    view = views.report(inspection, tmp_path, "testing")
    assert view.tables[0].records[0][0] == "101"
    assert len(view.tables[0].columns) == len(view.tables[0].records[0])
    assert ("Target quote", "USDC") in view.inputs
    assert any("different registry" in note for note in view.diagnostics)


def test_convert_counters_are_read_from_pinned_manifest_only(tmp_path: Path) -> None:
    path = tmp_path / "101" / "manifest.json"
    path.parent.mkdir()
    payload = json.dumps(
        {
            "job_id": 101,
            "conversion": {"target_quote": "USDT"},
            "statistics": {"converted": 14, "identity": 5, "skipped": 0},
        }
    )
    path.write_text(payload)
    evidence = {
        "job_id": 101,
        "status": "complete",
        "publication": {
            "manifest": str(path),
            "fingerprints": {"manifest.json": sha256(payload.encode()).hexdigest()},
        },
    }
    pinned = conversion_metadata(evidence, tmp_path)
    view = views.report(
        RunInspection(RUN, pinned, {"operation": "convert", "action": "run"}), tmp_path, "testing"
    )
    assert ("Target quote", "USDT") in view.inputs
    assert view.tables[1].records[0][:3] == ("101", "5", "14")
    path.write_text("{}")
    assert conversion_metadata(evidence, tmp_path) == evidence
