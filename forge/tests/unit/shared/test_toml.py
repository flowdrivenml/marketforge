"""Check generated TOML through the same contracts used by Forge."""

import json
import tomllib
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import tomlkit
from tomlkit.exceptions import ConvertError

from forge.catalog import models as catalogue
from forge.cli.main import main
from forge.historical.archives.config import ArchiveRequest, load_acquisition
from forge.historical.archives.sources.exchanges import validate_request
from forge.historical.authoring import create_with_requests, request_addition
from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.engine.runtime import read_document
from forge.historical.imports.models import load_import
from forge.historical.merge.service import load_recipe as load_merge
from forge.historical.normalization.recipes import load_configuration
from forge.historical.pipelines.config import load_recipe_file
from forge.historical.profiles.service import inspect as inspect_profile
from forge.historical.registration.models import load as load_registration
from forge.shared.config.loader import load_settings
from forge.shared.config.models import SettingsModel
from forge.shared.config.recipe_editing import update_recipe
from forge.shared.config.toml import dumps
from forge.shared.paths import ProjectPaths

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES = ROOT / "docs/content/examples"
INTERVAL = {
    "start": datetime.fromisoformat("2026-11-20T00:00:00+00:00"),
    "end": datetime.fromisoformat("2026-11-23T00:00:00+00:00"),
}
TRADE = ArchiveRequest.model_validate(
    {
        "exchange": "bybit",
        "instrument_type": "spot",
        "market_category": "spot",
        "data_type": "trades",
        "scope": {"kind": "instrument", "identifier": "SOLUSDT"},
    }
)
DEPTH = ArchiveRequest.model_validate({**TRADE.model_dump(), "data_type": "order_book_l2"})
ACQUISITION = """\
version = 1

[interval]
start = 2026-11-20T00:00:00Z
end = 2026-11-23T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = { kind = "instrument", identifier = "SOLUSDT" }
"""


@pytest.mark.parametrize("requests", [(TRADE,), (TRADE, DEPTH)])
def test_solusdt_authoring(
    tmp_path: Path, requests: tuple[ArchiveRequest, ...], capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").write_text('[project]\nname = "marketforge"\n')
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    authored = create_with_requests(
        paths.acquisition_config_dir,
        "solusdt",
        requests,
        pipeline=False,
        start=INTERVAL["start"].isoformat(),
        end=INTERVAL["end"].isoformat(),
    ).path
    text = authored.read_text()
    assert text.startswith(ACQUISITION)
    assert text.count("[[requests]]") == len(requests)
    assert text.count('data_type = "order_book_l2"') == len(requests) - 1
    loaded = load_acquisition(authored)
    assert loaded.requests == list(requests)
    for request in loaded.requests:
        validate_request(request)
    assert dumps(loaded.model_dump(exclude_none=True)) == text
    assert (
        main(["--project-root", str(tmp_path), "--json", "acquisition", "validate", "solusdt"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["request_count"] == len(requests)


def _normalization(path: Path) -> SettingsModel:
    return load_configuration(path, retain=False)


def _merge(path: Path) -> SettingsModel:
    return load_merge(path, retain=False)


def _pipeline(path: Path) -> SettingsModel:
    return load_recipe_file(path, path.read_bytes())


def _conversion(path: Path) -> SettingsModel:
    # Loading a bound recipe also resolves its registry publication. This
    # structural test deliberately uses a synthetic publication identity.
    return ConversionRecipe.model_validate(tomllib.loads(path.read_text()))


def _catalogue(kind: str) -> Callable[[Path], SettingsModel]:
    return lambda path: catalogue.load(kind, path)


@pytest.mark.parametrize(
    ("loader", "document", "header"),
    [
        pytest.param(
            _normalization,
            {
                "interval": INTERVAL,
                "requests": [TRADE.model_dump(exclude_none=True)],
                "ranking_policy": "turnover",
            },
            "[[requests]]",
            id="dynamic-normalization",
        ),
        pytest.param(
            _normalization,
            {
                "registry_id": "12345678-1234-1234-1234-123456789abc",
                "archive_pins": [
                    {"archive_id": 2, "selection_sha256": "a" * 64},
                    {
                        "archive_id": 1,
                        "selection_sha256": "b" * 64,
                        "profile_id": "bybit.spot.spot.trades",
                        "profile_sha256": "c" * 64,
                    },
                ],
            },
            "[[archive_pins]]",
            id="exact-normalization",
        ),
        pytest.param(
            _normalization,
            {
                "job_id": 2**64 - 1,
                "dataset_id": 7,
                "content_type": "trades",
                "inputs": [{"task_id": 1, "archive_id": 2, "instrument_id": 3}],
            },
            "[[inputs]]",
            id="prepared-normalization",
        ),
        pytest.param(
            _merge,
            {
                "version": 1,
                "interval": INTERVAL,
                "timeline_ordering": "causal",
                "selection": {"source": "upstream", "publication_ids": [83, 79]},
                "stream_ranks": {"bybit.spot.SOLUSDT/trades": 2, "second": 1},
            },
            "[selection]",
            id="derived-merge",
        ),
        pytest.param(
            _merge,
            {
                "dataset_ids": [83, 79],
                "start_ns": 1727683200000000001,
                "end_ns": 1727769600000000003,
                "stream_ranks": {"bybit.spot.SOLUSDT/trades": 2, "second": 1},
            },
            "[stream_ranks]",
            id="direct-merge",
        ),
        pytest.param(
            _conversion,
            {
                "version": 1,
                "source": {
                    "kind": "publication",
                    "registry_publication_id": 22,
                    "registry_id": "12345678-1234-1234-1234-123456789abc",
                    "environment": "testing",
                    "engine_publication_id": str(2**64 - 1),
                    "manifest_sha256": "a" * 64,
                    "processing_job_sha256": "b" * 64,
                    "publication_checksums_sha256": "c" * 64,
                    "interval": {"start_ns": 1727683200000000001, "end_ns": 1727769600000000003},
                },
                "interval": {"start_ns": 1727683200000000001, "end_ns": 1727769600000000003},
                "selection": {"whole_publication": True},
                "conversion": {"target_quote": "USDT", "max_staleness_ns": 9007199254740993},
                "reference_stream_ids": ["sol-usd-trades", "usd-usdt-trades"],
                "reference_publication_id": 73,
            },
            "[source.interval]",
            id="conversion",
        ),
        pytest.param(
            _pipeline,
            {
                "version": 1,
                "interval": INTERVAL,
                "requests": [request.model_dump(exclude_none=True) for request in (TRADE, DEPTH)],
                "normalize": {"ranking_policy": "turnover"},
                "merge": {"timeline_ordering": "causal"},
                "convert": {
                    "target_quote": "USDT",
                    "max_staleness_ns": 9007199254740993,
                    "resources": {"workers": 2, "max_open_files": 32},
                },
            },
            "[convert.resources]",
            id="pipeline",
        ),
        pytest.param(
            load_import,
            {
                "version": 1,
                "source_key": "recording",
                "root": ".",
                "files": ["trades.csv"],
                "identity": {
                    "exchange": "bybit",
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "symbol": "SOLUSDT",
                },
                "data_type": "trades",
                "interval": INTERVAL,
                "specifications": {
                    "as_of": INTERVAL["start"],
                    "base_asset": "SOL",
                    "quote_asset": "USDT",
                    "quantity_type": "base",
                    "tick_size": "0.00000001000",
                },
            },
            "[specifications]",
            id="import",
        ),
        pytest.param(
            load_registration,
            tomllib.loads((EXAMPLES / "catalogue/registration.toml").read_text()),
            "[[inputs]]",
            id="registration",
        ),
        *[
            pytest.param(
                _catalogue(kind),
                tomllib.loads((EXAMPLES / f"catalogue/{kind}.toml").read_text()),
                field,
                id=kind,
            )
            for kind, field in (
                ("exchange", "code ="),
                ("instrument", "symbol ="),
                ("specs", "tick_size ="),
            )
        ],
        pytest.param(
            load_settings, {"version": 1, "http": {"max_attempts": 2}}, "[http]", id="general"
        ),
    ],
)
def test_schema_round_trip(
    tmp_path: Path,
    loader: Callable[[Path], SettingsModel],
    document: dict[str, object],
    header: str,
) -> None:
    path = tmp_path / "recipe.toml"
    # Seed the authoritative parser with a valid baseline layout.
    path.write_text(tomlkit.dumps(document))
    expected = loader(path)
    configured = tomllib.loads(path.read_text())
    canonical = dumps(configured)
    assert header in canonical
    path.write_text(canonical)
    parsed = loader(path)
    assert parsed == expected
    assert tomllib.loads(canonical) == configured
    assert dumps(tomllib.loads(canonical)) == canonical
    from_model = dumps(parsed.model_dump(exclude_none=True, exclude_unset=True))
    path.write_text(from_model)
    reloaded = loader(path)
    assert reloaded == parsed
    assert dumps(reloaded.model_dump(exclude_none=True, exclude_unset=True)) == from_model


@pytest.mark.parametrize(
    ("operation", "settings"),
    [
        ("normalize", {"resources": {"workers": 2, "memory_budget_bytes": 2**34}}),
        ("merge", {"resources": {"workers": 4, "memory_limit_mb": 4096}}),
        ("convert", {"parquet": {"row_group_target_mb": 16, "file_target_mb": 64}}),
        ("pipeline", {"normalize": {"resources": {"workers": 2}}}),
    ],
)
def test_runtime_round_trip(operation: str, settings: dict[str, object]) -> None:
    path = Path(f"{operation}.toml")
    before = {"schema_version": 1, **settings}
    text = dumps(before)
    assert text.startswith("schema_version = 1\n\n[")
    assert read_document(path, text.encode()) == before
    assert dumps(read_document(path, text.encode())) == text


@pytest.mark.parametrize("kind", ["trades", "depth"])
def test_profile_round_trip(tmp_path: Path, kind: str) -> None:
    name = f"example_exchange.equity.equity.{kind}.toml"
    source = EXAMPLES / "catalogue" / name
    expected = inspect_profile(source)["definition"]
    canonical = dumps(expected)
    path = tmp_path / name
    path.write_text(canonical)
    parsed = inspect_profile(path)["definition"]
    assert parsed == expected
    assert "[normalization.event_timestamp]" in canonical
    assert dumps(parsed) == canonical


def test_nested_tables_and_exact_scalar_types() -> None:
    text = dumps(
        {
            "inputs": [
                {"name": "one", "children": [{"value": 2**64 - 1}]},
                {"name": "two", "children": [{"value": 1}]},
            ],
            "dotted.key": {"date": date(2026, 11, 20), "decimal": Decimal("0.00000100")},
            "long": list(range(40)),
            "offset": datetime.fromisoformat("2026-11-20T06:00:00.123456+05:30"),
            "optional": None,
        }
    )
    parsed = tomllib.loads(text)
    assert parsed["inputs"] == [
        {"name": "one", "children": [{"value": 2**64 - 1}]},
        {"name": "two", "children": [{"value": 1}]},
    ]
    assert parsed["dotted.key"] == {"date": date(2026, 11, 20), "decimal": "0.00000100"}
    assert "offset = 2026-11-20T06:00:00.123456+05:30" in text
    assert "long = [\n    0," in text
    assert "optional" not in parsed
    assert dumps(parsed) == text
    with pytest.raises(ConvertError):
        dumps({"unsupported": [None]})
    with pytest.raises(ValueError, match="whole minutes"):
        dumps({"unsupported": datetime.fromisoformat("2026-11-20T00:00:00+00:00:01")})
    precise = "timestamp = 2026-11-20T00:00:00.123456789Z\n"
    assert dumps({"timestamp": tomlkit.parse(precise).item("timestamp")}) == precise


def test_patch_preserves_comments_order_and_timestamp_lexemes() -> None:
    source = (
        ("# Keep the research interval.\n" + ACQUISITION)
        .replace("00:00:00Z", "00:00:00.123456789+00:00", 1)
        .replace('identifier = "SOLUSDT" }', 'identifier = "SOLUSDT" } # Venue symbol')
    )
    before = tomllib.loads(source)
    after = tomllib.loads(source.replace('"SOLUSDT"', '"BTCUSDT"'))
    assert update_recipe(source.encode(), before, after) == source.replace('"SOLUSDT"', '"BTCUSDT"')
    appended, _ = request_addition(source.encode(), (DEPTH,), pipeline=False)
    assert appended.startswith(source)
    assert '\n\n[[requests]]\nexchange = "bybit"' in appended


def test_legacy_inline_request_edit_and_append() -> None:
    source = (
        "version = 1\ninterval = { start = 2026-11-20T00:00:00Z, end = 2026-11-23T00:00:00Z }\n"
        'requests = [{ exchange = "bybit", instrument_type = "spot", market_category = "spot", '
        'data_type = "trades", scope = { kind = "instrument", identifier = "SOLUSDT" } }] # Keep\n'
    )
    before = tomllib.loads(source)
    after = tomllib.loads(source.replace('"SOLUSDT"', '"BTCUSDT"'))
    assert update_recipe(source.encode(), before, after) == source.replace('"SOLUSDT"', '"BTCUSDT"')
    appended, _ = request_addition(source.encode(), (DEPTH,), pipeline=False)
    assert appended.endswith("] # Keep\n")
    assert len(tomllib.loads(appended)["requests"]) == 2


def test_patch_edit_and_append_to_existing_requests() -> None:
    source = ACQUISITION.replace("[[requests]]", "[[requests]] # first request")
    before = tomllib.loads(source)
    after = tomllib.loads(source.replace('"SOLUSDT"', '"BTCUSDT"'))
    after["requests"].append(DEPTH.model_dump(exclude_none=True))
    updated = update_recipe(source.encode(), before, after)
    assert updated.startswith(source.replace('"SOLUSDT"', '"BTCUSDT"'))
    assert tomllib.loads(updated) == after
