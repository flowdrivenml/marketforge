"""Normalization outcomes rendered from verified receipts."""

import io
from collections.abc import Mapping
from pathlib import Path

import pytest
from rich.cells import cell_len

from forge.shared.config.models import PresentationSettings
from forge.shared.presentation.normalization import NormalizationOutcome, registry_ids
from forge.shared.terminal.console import create_console
from forge.shared.terminal.normalization import render_normalization_outcome


def receipt(root: Path) -> dict[str, object]:
    directory = root / "data/runs/normalize-execute-fixture"
    return {
        "status": "complete",
        "environment": "testing",
        "recipe": "example",
        "logical_run_id": "normalize-registry-fixture",
        "execution_id": "normalize-execute-fixture",
        "run_id": "normalize-engine-fixture",
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:02:32+00:00",
        "normalization": "complete",
        "verification": "verified",
        "registration": "registered",
        "report_path": str(directory / "execution-result.json"),
        "execution_receipt": str(directory / "execution-intent.json"),
        "engine_diagnostics_path": str(directory / "engine-tui.log"),
        "counts": {
            "requested": 2,
            "verified_publications": 2,
            "verified_primary_files": 3,
            "primary_files": 3,
            "new": 2,
            "reused": 1,
        },
        "catalogue_dataset_ids": {str(root / f"part-{i}.parquet"): i for i in (3, 4, 8)},
        "jobs": [
            job("trades", 9007199254740993, 1074, 1),
            job("depth", 9007199254740995, 50150, 2),
        ],
    }


def job(kind: str, identifier: int, rows: int, files: int) -> dict[str, object]:
    return {
        "dataset_id": identifier,
        "content_type": kind,
        "status": "complete",
        "verification": "verified",
        "events_written": rows,
        "files_written": files,
        "bytes_written": 100_450_000,
        "integrity_status": "clean" if kind == "trades" else "degraded",
        "processing_counters": {"records_read": 800, "records_rejected": 0},
        "start_timestamp_ns": 1727683200000000001,
        "end_timestamp_ns": 1727683200000000002,
    }


def render(result: Mapping[str, object], root: Path, width: int, *, register: bool = False) -> str:
    stream = io.StringIO()
    console = create_console(PresentationSettings(color="never"), stream=stream)
    console.width = width
    render_normalization_outcome(
        console,
        NormalizationOutcome.from_result(result, registering=register),
        root=root,
        registering=register,
    )
    return stream.getvalue()


@pytest.mark.parametrize("width", [80, 120, 160])
def test_compact_execution_result(tmp_path: Path, width: int) -> None:
    text = render(receipt(tmp_path), tmp_path, width)
    assert all(cell_len(line) <= width for line in text.splitlines())
    for value in (
        "TESTING",
        "COMPLETED",
        "2 / 2 jobs successful",
        "✓ VERIFIED",
        "✓ REGISTERED",
        "02:32",
        "1,600",
        "51,224",
        "Trades",
        "Depth",
        "9007199254740993",
        "9007199254740995",
        "Clean",
        "Degraded",
        "95.8 MiB",
        "3, 4, 8",
        "Primary files registered",
        "New files",
        "Reused files",
        "Engine run",
        "normalize-registry-fixture",
        "data/runs/normalize-execute-fixture/execution-result.json",
        "mf datasets list --testing --operation normalize",
    ):
        assert value in text
    for value in (
        str(tmp_path),
        "parquet",
        "execution-intent",
        "engine-tui.log",
        "\x1b[",
        "1727683200",
    ):
        assert value not in text
    assert text.count("MarketForge · Normalization") == 1
    assert "\n\n\n" not in text


def test_registry_file_ids_require_contiguity() -> None:
    assert registry_ids({"a": 8, "b": 10, "c": 9}) == "8–10"
    assert registry_ids({"a": 8, "b": 10}) == "8, 10"
    assert registry_ids({str(i): 2 * i for i in range(48)}) == "48 registered files (see report)"


@pytest.mark.parametrize("status", ["failed", "interrupted", "partial"])
def test_failure_and_partial_diagnostics(tmp_path: Path, status: str) -> None:
    result = receipt(tmp_path)
    result.update(
        status=status,
        normalization="interrupted"
        if status == "interrupted"
        else "complete"
        if status == "failed"
        else "failed",
        registration="failed"
        if status == "failed"
        else "registered"
        if status == "partial"
        else "not_started",
        registration_error="Registry unavailable" if status == "failed" else None,
    )
    if status == "partial":
        result["jobs"] = [
            job("trades", 7, 1074, 1),
            {
                "status": "failed",
                "job_id": 8,
                "registration": "omitted",
                "failure": {"message": "Invalid stream"},
            },
        ]
    else:
        result["recovery"] = "mf normalize register execution-intent.json --testing"
    text = render(result, tmp_path, 120)
    assert status.upper() in text
    assert "engine-tui.log" in text
    if status == "partial":
        assert "Job 8 · Invalid stream" in text
        assert "Failed jobs omitted" in text
        assert "Successful jobs" in text
    else:
        assert "mf normalize register execution-intent.json --testing" in text
        if status == "failed":
            assert "Registry unavailable" in text


def test_registration_recovery_projects_manifest_without_processing_totals(tmp_path: Path) -> None:
    result = {
        "environment": "testing",
        "registration": "registered",
        "publication": "committed",
        "execution": {"execution_id": "normalize-execute-fixture"},
        "evidence": [{"manifest": job("depth", 9007199254740995, 50150, 2)}],
        "counts": {"primary_files": 2, "new": 1, "reused": 1},
        "registration_receipt": str(tmp_path / "registration-result.json"),
    }
    text = render(result, tmp_path, 80, register=True)
    assert "9007199254740995" in text
    assert "✓ VERIFIED" in text
    assert "registration-result.json" in text
    assert "Engine duration" not in text
    assert "Records read" not in text


def test_missing_counters_are_omitted_and_nanoseconds_remain_exact(tmp_path: Path) -> None:
    result = receipt(tmp_path)
    item = job("trades", 7, 2, 1)
    del item["processing_counters"]
    result["jobs"] = [item]
    text = render(result, tmp_path, 80)
    assert "Records read" not in text
    assert "Rejected records" not in text
    assert item["start_timestamp_ns"] == 1727683200000000001
