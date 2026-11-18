from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from forge.catalog.models import Exchange, Specification, reference
from forge.cli.main import main
from forge.historical.imports.models import ImportError, load_import

EXAMPLES = Path(__file__).resolve().parents[4] / "docs/content/examples/catalogue"


def test_catalogue_offline_cli_validation(capsys: pytest.CaptureFixture[str]) -> None:
    for kind, name in (("exchange", "exchange"), ("instrument", "instrument"), ("specs", "specs")):
        assert (
            main(["--json", "catalog", kind, "validate", "--file", str(EXAMPLES / f"{name}.toml")])
            == 0
        )
        assert '"valid": true' in capsys.readouterr().out


def test_reference_and_keys() -> None:
    assert reference("test.equity.equity.BRK.B").symbol == "BRK.B"
    with pytest.raises(ValidationError):
        reference("test.equity.spot.SYN")
    with pytest.raises(ValidationError):
        Exchange.model_validate({"code": "new", "name": "New", "id": 38})
    with pytest.raises(ValidationError):
        Exchange(code="not-allowed", name="New")


@pytest.mark.parametrize(
    "field,value",
    [
        ("tick_size", 0.01),
        ("tick_size", "0"),
        ("tick_size", "NaN"),
        ("min_qty", "-1"),
        ("qty_step", "0"),
        ("quantity_type", "contracts"),
        ("contract_value", "1"),
        ("provenance", " "),
    ],
)
def test_invalid_economics(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Specification.model_validate(
            {
                "instrument": "test.equity.equity.SYN",
                "quantity_type": "base",
                "tick_size": "0.01",
                "valid_from": datetime(2024, 1, 1, tzinfo=UTC),
                "valid_to": datetime(2024, 1, 2, tzinfo=UTC),
                "provenance": "Test fixture",
                field: value,
            }
        )


def test_raw_only_import_refuses_submitted_binding(tmp_path: Path) -> None:
    path = tmp_path / "import.toml"
    path.write_text("""
source_key = "test"
root = "."
files = ["test.csv"]
data_type = "trades"
[identity]
exchange = "example_exchange"
instrument_type = "equity"
market_category = "equity"
symbol = "SYN"
[binding.readiness]
normalization_handoff = "ready"
""")
    with pytest.raises(ImportError, match="raw-only"):
        load_import(path)
