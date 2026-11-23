"""Diagnostic grouping and presentation must not change preparation readiness."""

from copy import deepcopy
from io import StringIO
from pathlib import Path

from forge.historical.normalization.diagnostics import summarize
from forge.markets.models import array, obj
from forge.shared.presentation.normalization import PreparationView
from forge.shared.terminal.console import create_console
from forge.shared.terminal.normalization import render_batch


def request(index: int, identifiers: list[int], reason: str) -> dict[str, object]:
    return {
        "request_index": index,
        "request": {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "data_type": "order_book_l2",
            "scope": {"kind": "instrument", "identifier": "BTCUSDT"},
        },
        "matched_archives": [{"local_id": identity} for identity in identifiers],
        "blockers": [{"local_id": identity, "reason": reason} for identity in identifiers],
        "coverage": {"gap_count": 0, "diagnostics": {"unknown feed identity": identifiers}},
    }


def test_summary_preserves_evidence_and_deduplicates_repeated_requests() -> None:
    reason = "No immutable historical specification covers this raw interval; complete original evidence."
    requests = [request(1, [1, 2], reason), request(2, [2, 3], reason)]
    original = deepcopy(requests)
    summary = summarize(requests)
    assert requests == original
    assert summary["matched_archive_count"] == summary["blocking_archive_count"] == 3
    assert summary["warning_archive_count"] == 3
    groups = [obj(group) for group in array(summary["groups"])]
    blockers = [group for group in groups if group["severity"] == "blocker"]
    assert len(blockers) == 1
    assert blockers[0]["local_ids"] == [1, 2, 3]
    assert blockers[0]["request_indices"] == [1, 2]
    assert blockers[0]["reasons"] == [reason]
    assert blockers[0]["category"] == "missing_historical_specifications"
    assert blockers[0]["exchange"] == "bybit" and blockers[0]["instrument"] == "BTCUSDT"


def test_coverage_warnings_do_not_become_blocking_errors() -> None:
    entry = request(1, [1, 2], "unused")
    entry["blockers"] = []
    summary = summarize([entry])
    assert summary["blocking_archive_count"] == 0 and summary["warning_archive_count"] == 2
    assert obj(array(summary["groups"])[0])["severity"] == "warning"
    entry["coverage"] = {"gap_count": 1, "diagnostics": {}}
    summary = summarize([entry])
    assert summary["coverage_gap_count"] == 1
    assert summary["coverage_gap_request_indices"] == [1]
    assert summary["blocking_archive_count"] == 0


def test_categories_distinguish_family_authority_and_unclassified_failures() -> None:
    entries = [
        request(1, [1], "Historical family catalogue prerequisite missing: details"),
        request(2, [2], "Managed input checksum mismatch: source"),
    ]
    summary = summarize(entries)
    assert {
        obj(group)["category"]
        for group in array(summary["groups"])
        if obj(group)["severity"] == "blocker"
    } == {"missing_historical_family_authority", "input_resolution_error"}


def test_partial_v4_presentation_reports_blockers_not_obsolete_protocol_failure() -> None:
    output = StringIO()
    result: dict[str, object] = {
        "status": "partial",
        "dry_run": True,
        "prepared_job_count": 0,
        "prepared_task_count": 0,
        "prepared_stream_count": 0,
        "batch_schema_version": 4,
        "batch_path": "not-written.json",
        "report_path": "not-written-report.json",
        "engine_ready": False,
        "engine_compatibility": {
            "direct_execution_supported": True,
            "current_rust_protocol_version": 4,
        },
        "diagnostic_summary": summarize(
            [request(1, [1, 2], "No immutable historical specification covers this raw interval.")]
        ),
        "ranking_resolution": {"snapshot": "snapshot-id", "policy": "turnover"},
    }
    render_batch(
        create_console(stream=output),
        PreparationView.from_result(
            result, environment="testing", recipe="fixture", dry_run=bool(result.get("dry_run"))
        ),
        root=Path.cwd(),
    )
    text = output.getvalue()
    assert "Batch schema" in text and "4" in text
    assert "Missing historical specifications" in text
    assert "Blockers" in text and "Coverage and Metadata Warnings" in text
    assert "snapshot-id" in text
    assert "DRY RUN" in text and "Not Written" in text
    assert "v3" not in text and "unsupported" not in text
    assert "passed" not in text


def test_validated_preparation_is_distinct_from_execution() -> None:
    output = StringIO()
    result: dict[str, object] = {
        "status": "prepared",
        "prepared_job_count": 1,
        "prepared_task_count": 1,
        "prepared_stream_count": 1,
        "batch_schema_version": 4,
        "engine_ready": True,
        "batch_path": "batch.json",
        "report_path": "report.json",
        "engine_compatibility": {"direct_execution_supported": True},
    }
    render_batch(
        create_console(stream=output),
        PreparationView.from_result(
            result, environment="testing", recipe="fixture", dry_run=bool(result.get("dry_run"))
        ),
        root=Path.cwd(),
    )
    assert "Prepared batch contract validated" in output.getvalue()
    assert "Preparation finished before execution." in output.getvalue()


def test_grouping_keeps_exchange_instrument_market_and_content_distinct() -> None:
    entries = [
        request(index, [index], "No immutable historical specification covers this raw interval.")
        for index in range(1, 5)
    ]
    obj(entries[1]["request"])["exchange"] = "bitget"
    obj(entries[2]["request"])["market_category"] = "linear"
    obj(entries[2]["request"])["instrument_type"] = "perpetual"
    obj(entries[3]["request"])["data_type"] = "trades"
    summary = summarize(entries)
    blockers = [
        obj(group) for group in array(summary["groups"]) if obj(group)["severity"] == "blocker"
    ]
    assert len(blockers) == 4
    assert all(group["archive_count"] == 1 for group in blockers)


def test_manual_preparation_uses_confirmed_selected_copy_ids() -> None:
    summary = summarize(
        [
            {
                "request_index": 1,
                "selected_archives": [{"local_id": 42}],
                "blockers": [],
                "legacy_selection": {"archive_id": 42},
            }
        ]
    )
    assert summary["matched_archive_count"] == 1
    assert summary["blocking_archive_count"] == 0
