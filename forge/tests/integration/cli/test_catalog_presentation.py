"""Catalog dispatch and rendering without live catalogue access."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from rich.cells import cell_len

from forge.cli.commands import catalog
from forge.cli.main import main
from forge.shared.config.models import PresentationSettings
from forge.shared.presentation.catalog import project
from forge.shared.terminal.catalog import render
from forge.shared.terminal.console import create_console

SYMBOL = "BTC.USD/近[test]"
REFERENCE = f"venue.future.inverse.{SYMBOL}"
INSTRUMENT = {
    "id": 101,
    "exchange": "venue",
    "instrument_type": "future",
    "market_category": "inverse",
    "symbol": SYMBOL,
    "status": "active",
    "base_asset": "BTC",
    "quote_asset": "USD",
    "settlement_asset": None,
    "historical_instrument_id": 9001,
    "namespace": "namespace:fixture",
}
EXCHANGE = {"id": 34, "code": "venue", "name": "Échange [paper]"}
CURRENT = {
    "instrument_id": 101,
    "quantity_type": "contracts",
    "tick_size": Decimal("0.000000000000000001"),
    "qty_step": Decimal("0.125"),
    "contract_value": Decimal("90100000000.0000000001234"),
    "contract_value_asset": "USD",
    "min_qty": Decimal(0),
    "fetched_at": datetime(2026, 3, 1, 12, 30, tzinfo=UTC),
}
EVIDENCE = {
    "id": 400,
    "fingerprint": "f" * 64,
    "evidence_kind": "reference_contract_inferred",
    "definition": {
        "record_kind": "inference",
        "historically_verified": False,
        "valid_from": "2026-02-01T00:00:00Z",
        "valid_to": "2026-02-02T00:00:00Z",
        "assumed_applicable_from": "2026-02-01T00:00:00Z",
        "assumed_applicable_to": "2026-02-02T00:00:00Z",
        "source_observed_at": "2026-03-01T14:30:00+02:00",
        "provenance": "Fixture [metadata] · " * 9,
        "reference_instrument": "venue/future/inverse/BTC.USD/reference",
        "inherited_fields": ["tick_size"],
        "min_notional": "0",
        "max_notional": "10000000000000001.5",
        "max_notional_asset": "USD",
    },
}
DECLARATIONS = {
    "exchange": 'code = "venue"\nname = "Échange [paper]"\n',
    "instrument": f'''
exchange = "venue"
instrument_type = "future"
market_category = "inverse"
symbol = "{SYMBOL}"
base_asset = "BTC"
quote_asset = "USD"
''',
    "specs": f'''
instrument = "{REFERENCE}"
quantity_type = "contracts"
tick_size = "0.000000000000000001"
contract_value = "90100000000.0000000001234"
contract_value_asset = "USD"
min_qty = 0
as_of = 2026-03-01T14:30:00+02:00
provenance = "Retained [feed]"
''',
}
DETAIL = {
    "exchange": EXCHANGE,
    "instrument": INSTRUMENT,
    "specs": {"instrument": INSTRUMENT, "current": CURRENT, "evidence": [EVIDENCE]},
}
LIST = {
    "exchange": {"exchanges": [EXCHANGE]},
    "instrument": {"instruments": [INSTRUMENT]},
    "specs": {
        "current_specifications": [{**INSTRUMENT, **CURRENT}],
        "specifications": [{**EVIDENCE, "identity_key": f"venue/future/inverse/{SYMBOL}"}],
    },
}
ADD = {"exchange": EXCHANGE, "instrument": INSTRUMENT, "specs": EVIDENCE}


@pytest.mark.parametrize("entity", ("exchange", "instrument", "specs"))
@pytest.mark.parametrize("action", ("list", "show", "validate", "add"))
def test_catalog_cli(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    entity: str,
    action: str,
) -> None:
    @asynccontextmanager
    async def connect(_dsn: str) -> AsyncGenerator[object]:
        assert action != "validate"
        yield object()

    def dsn(_root: Path) -> str:
        assert action != "validate"
        return "fixture"

    source = workspace / "catalog.toml"
    source.write_text(DECLARATIONS[entity])
    result = {"show": DETAIL[entity], "list": LIST[entity], "add": ADD[entity]}.get(action)
    original = deepcopy(result)
    mock_add = AsyncMock(return_value=result)
    mock_browse = AsyncMock(return_value=result)
    monkeypatch.setattr(catalog, "database_connection", connect)
    monkeypatch.setattr(catalog, "database_dsn", dsn)
    monkeypatch.setattr(catalog.service, "add", mock_add)
    monkeypatch.setattr(catalog.service, "browse", mock_browse)
    command = ["catalog", entity, action]
    if action == "show":
        command.append("venue" if entity == "exchange" else REFERENCE)
    if action in {"validate", "add"}:
        command.extend(["--file" if action == "validate" else "--from", str(source)])
    if action != "validate":
        command.append("--testing")
    prefix = ["--project-root", str(workspace)]
    assert main([*prefix, *command]) == 0
    captured = capsys.readouterr()
    out = captured.out
    assert "MarketForge · Catalog" in out and not captured.err and "\x1b" not in out
    assert '"id":' not in out and '"definition":' not in out
    assert ("TESTING" in out) == (action != "validate")
    if action == "list":
        assert "1 displayed" in out and "No current specifications" not in out
    if action == "validate":
        assert "Local declaration checks passed" in out and "catalog.toml" in out
        mock_add.assert_not_called()
        mock_browse.assert_not_called()
    if action == "add":
        assert "REGISTERED" in out and "Created" not in out and "Updated" not in out
        assert f"mf catalog {entity} show" in out
    if entity == "exchange":
        assert "Échange [paper]" in out
    if entity == "specs" and action == "show":
        assert "Historically verified" in out and "Assumed applicable" in out
        assert "Declared valid" not in out and "BTC.USD/reference" in out
        assert "Historical instrument ID" in out and "Minimum quantity" in out
        assert "0.000000000000000001" in out
    assert main([*prefix, "--json", *command]) == 0
    captured = capsys.readouterr()
    machine = json.loads(captured.out)
    assert not captured.err
    if action == "validate":
        assert machine == {
            "valid": True,
            "declaration": catalog.models.load(entity, source).model_dump(mode="json"),
        }
    else:
        assert machine == json.loads(json.dumps(result, default=str))
    assert result == original


@pytest.mark.parametrize("width", (55, 80, 120))
def test_catalog_width_and_literal_evidence(tmp_path: Path, width: int) -> None:
    for action, payload in (("show", DETAIL["specs"]), ("list", LIST["specs"])):
        output = StringIO()
        console = create_console(PresentationSettings(color="never"), stream=output)
        console.width = width
        view = project("specs", action, payload, compact=width < 100)
        projected = repr(view.sections)
        assert SYMBOL in projected and "0.000000000000000001" in projected
        render(console, view, tmp_path)
        text = output.getvalue()
        assert "Current Specification" in text and "Historical" in text
        assert "BTC.USD" in text and "[test]" in text
        if action == "show":
            assert "[metadata]" in text and "10000000000000001.5" in text
        assert all(cell_len(line) <= width for line in text.splitlines())
        assert "\x1b" not in text


@pytest.mark.parametrize("current,evidence", ((False, True), (True, False), (False, False)))
def test_independent_missing_evidence(current: bool, evidence: bool) -> None:
    detail = {
        **DETAIL["specs"],
        "current": CURRENT if current else None,
        "evidence": [EVIDENCE] if evidence else [],
    }
    view = project("specs", "show", detail)
    notes = {section.note for section in view.sections}
    assert ("No current specification returned." in notes) == (not current)
    assert ("No historical specification evidence returned." in notes) == (not evidence)


def test_empty_and_errors(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        "mf catalog exchange add --help"
        in project("exchange", "list", {"exchanges": []}).sections[0].note
    )
    assert (
        "No instruments registered"
        in project("instrument", "list", {"instruments": []}).sections[0].note
    )
    assert all(
        section.note
        for section in project(
            "specs", "list", {"current_specifications": [], "specifications": []}
        ).sections
    )
    source = workspace / "invalid[fixture].toml"
    source.write_text('code = "UPPER"\nname = "Example"\n')
    args = ["catalog", "exchange", "validate", "--file", str(source)]
    prefix = ["--project-root", str(workspace)]
    assert main([*prefix, *args]) == 2
    captured = capsys.readouterr()
    assert not captured.out and source.name in captured.err and "code:" in captured.err
    assert main([*prefix, "--json", *args]) == 2
    captured = capsys.readouterr()
    assert not captured.out and json.loads(captured.err)["schema_version"] == 1
    assert "code:" in json.loads(captured.err)["error"]["message"]
