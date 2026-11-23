"""Bounded final acquisition display tests without Engine or network access."""

import io
from typing import cast

import pytest
from rich.cells import cell_len

from forge.historical.archives.operation import OperationReport
from forge.shared.config.models import PresentationSettings
from forge.shared.presentation.acquisition import AcquisitionResult
from forge.shared.terminal.acquisition_result import render_result
from forge.shared.terminal.console import create_console


@pytest.mark.parametrize("width", [80, 120, 160])
@pytest.mark.parametrize("mode", ["map", "download", "cancelled"])
def test_final_result_uses_exact_identity_and_retained_outcomes(width: int, mode: str) -> None:
    run = "acquisition-a573d6b9-6648-4bac-ac91-aec1c30c21ba"
    records: list[dict[str, object]] = [
        {
            "request": {
                "exchange": "okx",
                "market_category": "linear",
                "instrument_type": "perpetual",
                "data_type": "trades",
                "scope": {"identifier": "BTC-USDT", "kind": "family"},
            },
            "status": "complete",
            "discovery": "archives",
            "cache": "reused",
            "archives_found": 1,
        }
        for _ in range(22)
    ]
    records.extend(
        [
            {
                "request": {
                    "exchange": "bybit",
                    "data_type": "order_book_l2",
                    "scope": {"identifier": "ETH", "kind": "base_coin"},
                },
                "status": "failed",
                "discovery": "unsupported",
                "error": "Request blocked at https://example.org/?signature=secret",
            },
            {
                "request": {
                    "exchange": "binance",
                    "data_type": "trades",
                    "scope": {"identifier": "ETHUSDT", "kind": "instrument"},
                },
                "status": "complete",
                "discovery": "empty",
                "cache": "refreshed",
                "archives_found": 0,
            },
        ]
    )
    # Exercise compact multi-scope output and keep issue rows above the truncation boundary.
    document = cast(
        OperationReport,
        {
            "schema_version": 1,
            "run_id": run,
            "operation": "acquisition",
            "environment": "testing",
            "status": "cancelled" if mode == "cancelled" else "failed",
            "requests": records,
            "request_counts": {"successful": 23, "cached": 22, "archives": 22},
            "file_counts": {
                "completed": int(mode == "download"),
                "reused": 0,
                "failed": 1,
                "cancelled": 0,
            },
            "selection": {"archives": [{"local_archive_id": 1}, {"local_archive_id": 1}]},
            "report": f"data/runs/{run}/report.json",
        },
    )
    model = AcquisitionResult.from_report(document, download=mode == "download", recipe="fixture")
    assert model.requests[-1].cache == "Refreshed" and model.requests[-1].discovery == "empty"
    output = io.StringIO()
    console = create_console(PresentationSettings(color="never"), stream=output)
    console.width = width
    render_result(console, model)
    text = output.getvalue()
    assert all(cell_len(line) <= width for line in text.splitlines())
    assert run in text and document["report"] in text
    assert "Showing 20 of 24 requests" in text and "ETH (base coin)" in text
    assert "BTC-USDT (family)" in text and "Cached" in text
    assert "Market-data coverage has not been established" in text
    assert "signature" not in text and "example.org" not in text and "\x1b" not in text
    if mode == "download":
        assert "Download Summary" in text
        assert "Bytes in completed downloads" in text and "Unknown" in text
        assert model.registered_archives == 1
    else:
        assert "Discovery Summary" in text and "Completed downloads" not in text
    assert ("CANCELLED" if mode == "cancelled" else "PARTIAL") in text


def test_discovery_evidence_survives_transfer_failure() -> None:
    report = cast(
        OperationReport,
        {
            "run_id": "acquisition-fixture",
            "status": "failed",
            "requests": [
                {
                    "status": "failed",
                    "discovery": "failed",
                    "candidate_diagnostics": {"registered": 100},
                },
                {
                    "status": "failed",
                    "discovery": "failed",
                    "scan_id": 1,
                    "archives_found": 2,
                    "cache": "reused",
                },
                {"status": "failed", "discovery": "failed"},
            ],
            "file_counts": {"failed": 1},
            "selection": {},
            "report": "report.json",
        },
    )
    result = AcquisitionResult.from_report(report, download=True, recipe="fixture")
    assert [request.discovery for request in result.requests] == ["partial", "archives", "failed"]
    assert all(request.status == "failed" for request in result.requests)
