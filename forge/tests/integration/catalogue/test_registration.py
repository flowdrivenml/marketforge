"""Exercise the guarded database, actual Rust compiler and managed copy handoff."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.catalog.models import Exchange, Instrument, Specification
from forge.catalog.service import add, browse
from forge.historical.profiles import service as profiles
from forge.historical.registration import service as registration
from forge.markets.models import obj
from forge.shared.config.toml import dumps
from forge.shared.database.testing import testing_connection as db_connection
from forge.shared.database.testing import testing_dsn as db_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES = ROOT / "docs/content/examples/catalogue"
pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.engine]


@pytest.mark.asyncio
async def test_prerequisites_registration_reuse_and_immutable_specs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    try:
        dsn = db_dsn(ROOT)
    except MarketForgeError:
        pytest.skip("Testing database credentials unavailable")
    executable = ROOT / ".bin/marketforge-normalize"
    if not executable.is_file():
        pytest.skip("Run mf engine build normalize for the integration test")
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    venue = "fixture_" + uuid4().hex[:12]
    reference = f"{venue}.equity.equity.SYN"
    tests_root = tmp_path / "inputs"
    tests_root.mkdir()
    trades = tests_root / "trade.csv"
    trades.write_text((EXAMPLES / "synthetic-trades.csv").read_text())
    (tests_root / "depth.jsonl").write_text((EXAMPLES / "synthetic-depth.jsonl").read_text())
    start = datetime(2024, 10, 28, tzinfo=UTC)
    end = start + timedelta(days=1)
    recipe = tmp_path / "archive.toml"
    document: dict[str, Any] = {
        "registration": {"version": 1, "name": venue},
        "source": {
            "key": venue,
            "root": str(tests_root),
            "provider": "test-recorder",
            "product": "synthetic",
            "provenance": "Synthetic test files.",
        },
        "coverage": {"start": start, "end": end},
        "inputs": [
            {
                "instrument": reference,
                "profile": f"{venue}.equity.equity.trades",
                "content": "trades",
                "files": [trades.name],
            },
            {
                "instrument": reference,
                "profile": f"{venue}.equity.equity.depth",
                "content": "depth",
                "files": ["depth.jsonl"],
            },
        ],
    }
    recipe.write_text(dumps(document))
    spec = Specification(
        instrument=reference,
        quantity_type="base",
        tick_size=Decimal("0.01000000000000000001"),
        valid_from=start,
        valid_to=end,
        provenance="Synthetic test only",
    )
    equity = Instrument(
        exchange=venue,
        instrument_type="equity",
        market_category="equity",
        symbol="SYN",
        base_asset="SYN",
        quote_asset="USD",
    )

    class Rollback(Exception):
        pass

    async with db_connection(dsn) as c:

        @asynccontextmanager
        async def existing(_: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
            yield c

        def test_dsn(_: Path) -> str:
            return dsn

        def engine(_: ProjectPaths) -> Path:
            return executable

        monkeypatch.setattr(registration, "testing_connection", existing)
        monkeypatch.setattr(registration, "testing_dsn", test_dsn)
        monkeypatch.setattr(profiles, "resolve", engine)
        monkeypatch.setattr("forge.shared.database.testing.testing_connection", existing)
        monkeypatch.setattr("forge.shared.database.testing.testing_dsn", test_dsn)

        async def blocked(reason: str) -> None:
            with pytest.raises(MarketForgeError, match=reason):
                await registration.apply(paths, recipe)
            assert not paths.raw_dir.exists()
            assert await (
                await c.execute(
                    "SELECT count(*) FROM historical.raw_sources WHERE source_key=%s", (venue,)
                )
            ).fetchone() == (0,)

        with pytest.raises(Rollback):
            async with c.transaction():
                await blocked("Exchange.*missing")
                with pytest.raises(MarketForgeError, match="Exchange.*missing"):
                    await profiles.register(paths, f"{venue}.equity.equity.trades", recipe)
                with pytest.raises(MarketForgeError, match="Exchange.*missing"):
                    await add(c, equity)
                declaration = Exchange(code=venue, name="Fixture Venue")
                exchange = await add(c, declaration)
                assert exchange["id"] > 0 and exchange == await add(c, declaration)
                with pytest.raises(MarketForgeError, match="conflicts"):
                    await add(c, Exchange(code=venue, name="Different"))
                await blocked("Instrument.*missing")
                with pytest.raises(MarketForgeError, match="Instrument.*missing"):
                    await add(c, spec)
                instrument = await add(c, equity)
                assert instrument == await add(c, equity)
                assert instrument["id"] > 0 and instrument["historical_instrument_id"] > 0
                assert instrument["namespace"] == "marketforge:testing:historical:v1"
                with pytest.raises(MarketForgeError, match="conflicts"):
                    await add(c, equity.model_copy(update={"quote_asset": "EUR"}))
                await blocked("No applicable")
                current = Specification(
                    instrument=reference,
                    quantity_type="base",
                    tick_size=Decimal("0.02"),
                    fetched_at=start,
                    provenance="Synthetic current observation",
                )
                now = await add(c, current)
                assert now["definition"]["record_kind"] == "current_observation"
                await blocked("No applicable")
                first = await add(c, spec)
                assert first == await add(c, spec)
                assert Decimal(first["definition"]["tick_size"]) == spec.tick_size
                assert first["source_observed_at"] is None
                with pytest.raises(MarketForgeError, match="overlapping"):
                    await add(c, spec.model_copy(update={"tick_size": Decimal("0.03")}))
                later = await add(
                    c,
                    spec.model_copy(
                        update={
                            "valid_from": end,
                            "valid_to": end + timedelta(days=1),
                            "tick_size": Decimal("0.04"),
                        }
                    ),
                )
                assert first["id"] != later["id"]
                shown = await browse(c, "specs", reference)
                assert shown["current"]["tick_size"] == Decimal("0.02")
                assert {item["id"] for item in shown["evidence"]} == {
                    first["id"],
                    later["id"],
                    now["id"],
                }
                await blocked("Profile.*missing")
                for kind in ("trades", "depth"):
                    name = f"{venue}.equity.equity.{kind}"
                    path = tmp_path / f"{name}.toml"
                    text = (EXAMPLES / f"example_exchange.equity.equity.{kind}.toml").read_text()
                    text = text.replace("example_exchange", venue).replace(
                        "EXAMPLE-EXCHANGE", venue.upper().replace("_", "-")
                    )
                    path.write_text(text)
                    receipt = await profiles.register(paths, name, path)
                    duplicate = await profiles.register(paths, name, path)
                    assert duplicate["publication"] == "reused"
                    assert duplicate["engine"]["fingerprint"] == receipt["engine"]["fingerprint"]
                    path.write_text(
                        text.replace(
                            'ordering = "chronological"', 'ordering = "timestamp_sortable"'
                        )
                    )
                    with pytest.raises(MarketForgeError, match="Different interpretation"):
                        await profiles.register(paths, name, path)
                depth_id = f"{venue}.equity.equity.depth"
                installed = profiles.installed(paths, depth_id)
                definition = installed.read_text()
                installed.write_text(definition.replace("schema_version = 2", "schema_version = 9"))
                await blocked("expected profile schema_version")
                installed.write_text(definition)
                document["inputs"][0]["specification"] = "f" * 64
                recipe.write_text(dumps(document))
                await blocked("with fingerprint")
                document["inputs"][0]["specification"] = first["fingerprint"]
                recipe.write_text(dumps(document))
                trades.write_text('{"bad":"layout"}\n')
                await blocked("source structure does not match")
                trades.write_text((EXAMPLES / "synthetic-trades.csv").read_text())
                document["coverage"]["end"] = end + timedelta(hours=1)
                recipe.write_text(dumps(document))
                await blocked("No applicable")
                document["coverage"]["end"] = end
                recipe.write_text(dumps(document))
                assert (await registration.apply(paths, recipe, planning=True))[
                    "status"
                ] == "planned"
                assert not paths.raw_dir.exists()

                # An identical raw-only copy can acquire a validated binding.
                from forge.historical.imports.models import freeze_inputs
                from forge.historical.imports.service import import_file
                from forge.historical.registration.models import groups, load
                from forge.storage.models import StorageFilters
                from forge.storage.registry import parameters, scan

                raw = groups(load(recipe), paths)[0].model_copy(
                    update={"interval": None, "format_profile": None}
                )
                original = await import_file(
                    c, raw, freeze_inputs(raw)[0], paths.raw_dir, "raw-only"
                )
                assert original["normalization_readiness"] == "requires_profile"
                recorded: dict[str, Any] = await registration.apply(paths, recipe)
                assert recorded["status"] == "registered"
                assert len(recorded["copies"]) == 2
                assert recorded["copies"][0]["status"] == "reused"
                assert recorded["copies"][0]["local_archive_id"] == original["local_archive_id"]
                external = await scan(
                    c,
                    parameters(
                        paths.raw_dir,
                        StorageFilters(exchange=venue, instrument_type="equity", market="equity"),
                    ),
                    0,
                )
                assert len(external) == 2
                for item in external:
                    provenance = obj(item["import_provenance"])
                    binding = obj(provenance["binding"])
                    assert obj(binding["readiness"])["normalization_handoff"] == "ready"
                assert all(
                    binding["specification_evidence_id"] == first["id"]
                    for binding in recorded["bindings"]
                )
                repeated = await registration.apply(paths, recipe)
                assert [receipt["status"] for receipt in repeated["copies"]] == ["reused", "reused"]
                assert [receipt["local_archive_id"] for receipt in recorded["copies"]] == [
                    receipt["local_archive_id"] for receipt in repeated["copies"]
                ]
                raise Rollback
