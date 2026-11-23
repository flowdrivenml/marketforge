"""Acquisition commands using managed configuration names."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from forge.cli.main import main

_VALID_DOCUMENT = """
version = 1

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-03T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
"""


def _run(workspace: Path, *arguments: str) -> int:
    return main(["--project-root", str(workspace), *arguments])


@pytest.mark.parametrize("name", ["request", "request.toml"])
def test_validates_by_name_from_outside_workspace(
    workspace: Path,
    request_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    name: str,
) -> None:
    assert request_file.is_file()
    monkeypatch.chdir(tmp_path)

    result = _run(
        workspace,
        "--json",
        "acquisition",
        "validate",
        name,
    )
    captured = capsys.readouterr()

    assert result == 0, captured.err
    assert captured.err == ""

    payload = json.loads(captured.out)
    assert payload["valid"] is True
    assert payload["request_count"] == 1
    assert "\x1b[" not in captured.out


@pytest.mark.parametrize(
    ("original", "replacement", "expected_field"),
    [
        (
            "end = 2026-09-03T00:00:00Z",
            "end = 2026-08-01T00:00:00Z",
            "interval",
        ),
        (
            "start = 2026-09-01T00:00:00Z",
            "start = 2026-09-01T00:00:00",
            "interval.start",
        ),
        (
            'identifier = "BTCUSDT"',
            'identifier = "   "',
            "requests.0.scope.identifier",
        ),
        (
            'exchange = "bybit"',
            'exchange = "bybit"\nunknown_option = true',
            "requests.0.unknown_option",
        ),
    ],
)
def test_invalid_request_returns_field_diagnostics(
    workspace: Path,
    request_file: Path,
    capsys: pytest.CaptureFixture[str],
    original: str,
    replacement: str,
    expected_field: str,
) -> None:
    request_file.write_text(
        _VALID_DOCUMENT.replace(original, replacement),
        encoding="utf-8",
    )

    result = _run(workspace, "--json", "acquisition", "validate", "request")
    captured = capsys.readouterr()

    assert result == 2
    assert captured.out == ""
    assert expected_field in json.loads(captured.err)["error"]["message"]


def test_new_creates_a_configuration_that_validates(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    destination = (
        workspace / "data" / ".config" / "workflows/data_engineering/acquisition" / "new.toml"
    )

    result = _run(workspace, "--json", "acquisition", "new", "new")
    captured = capsys.readouterr()

    assert result == 0, captured.err
    assert json.loads(captured.out)["configuration_file"] == str(destination)
    assert destination.is_file()

    result = _run(workspace, "--json", "acquisition", "validate", "new")
    captured = capsys.readouterr()

    assert result == 0, captured.err
    assert json.loads(captured.out)["valid"] is True


def test_new_preserves_an_existing_configuration(
    workspace: Path,
    request_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original = request_file.read_bytes()

    result = _run(workspace, "--json", "acquisition", "new", "request")
    captured = capsys.readouterr()

    assert result == 2
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["message"]
    assert request_file.read_bytes() == original


def test_edit_revalidates_saved_contents(
    workspace: Path,
    request_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_edit(path: Path, *, editor: str | None = None) -> Path:
        assert path == request_file
        path.write_text(
            _VALID_DOCUMENT.replace(
                'data_type = "trades"',
                'data_type = "unsupported"',
            ),
            encoding="utf-8",
        )
        return path

    monkeypatch.setattr(
        "forge.cli.commands.acquisition.edit_file",
        fake_edit,
    )

    result = _run(workspace, "acquisition", "edit", "request")
    captured = capsys.readouterr()

    assert result == 2
    assert captured.out == ""
    assert "requests.0.data_type" in captured.err


def test_lists_names_without_requiring_valid_contents(
    workspace: Path,
    request_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    request_file.with_name("unfinished.toml").write_text(
        "[unfinished",
        encoding="utf-8",
    )

    result = _run(workspace, "--json", "acquisition", "list")
    captured = capsys.readouterr()

    assert result == 0, captured.err
    assert json.loads(captured.out)["configurations"] == [
        "request",
        "unfinished",
    ]


def test_human_inventory_empty_then_with_invalid_recipe(
    workspace: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_database(*_: object) -> None:
        pytest.fail("Local inventory must not resolve database credentials")

    monkeypatch.setattr("forge.cli.commands.acquisition.database_dsn", unexpected_database)
    assert _run(workspace, "acquisition", "list") == 0
    assert "No acquisition configurations found." in capsys.readouterr().out
    directory = workspace / "data/.config/workflows/data_engineering/acquisition"
    directory.mkdir(parents=True)
    (directory / "request.toml").write_text(_VALID_DOCUMENT)
    (directory / "broken.toml").write_text("[incomplete")
    assert _run(workspace, "acquisition", "list") == 0
    output = capsys.readouterr()
    assert "Requests Period (UTC)" in output.out
    assert "1–3 Sep 2026" in output.out
    assert any("broken" in line and "Invalid" in line for line in output.out.splitlines())
    assert "2 configurations" in output.out
    assert not output.err and not output.out.startswith("\n") and not output.out.endswith("\n\n")


def test_human_validation_scope_and_toml_location(
    workspace: Path, request_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run(workspace, "acquisition", "validate", "request") == 0
    output = capsys.readouterr()
    assert "✓ Configuration structure valid" in output.out
    assert "Source support and archive availability were not checked." in output.out
    assert str(workspace) not in output.out

    request_file.write_text("invalid = ?\n")
    assert _run(workspace, "acquisition", "validate", "request") == 2
    output = capsys.readouterr()
    assert not output.out
    assert "✗ Configuration structure invalid" in output.err
    assert "line 1, column 11" in output.err and "request.toml" in output.err


def test_historical_sources_report_adapter_capabilities_without_database(
    workspace: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_database(*_: object) -> None:
        pytest.fail("Historical capabilities must not resolve database credentials")

    monkeypatch.setattr("forge.cli.commands.acquisition.database_dsn", unexpected_database)
    assert _run(workspace, "acquisition", "sources") == 0
    output = capsys.readouterr()
    assert "Historical Sources" in output.out
    assert "/reference/acquisition-sources/" in output.out
    assert not output.err

    assert _run(workspace, "--json", "acquisition", "sources") == 0
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["schema_version"] == 1
    assert not output.err and "\x1b" not in output.out
    rows = result["capabilities"]
    assert {row["exchange"] for row in rows} == {"binance", "bybit", "bitget", "okx", "gateio"}
    assert {
        row["scope"]
        for row in rows
        if row["exchange"] == "bybit" and row["instrument_type"] == "option"
    } == {"base_coin"}
    assert {
        row["scope"]
        for row in rows
        if row["exchange"] == "okx" and row["instrument_type"] == "future"
    } == {"family"}
    assert all(
        row["data_type"] == "trades" and row["instrument_type"] != "option"
        for row in rows
        if row["exchange"] == "binance"
    )
    assert not any(
        row["exchange"] == "bitget"
        and row["instrument_type"] == "future"
        and row["market_category"] == "linear"
        for row in rows
    )
