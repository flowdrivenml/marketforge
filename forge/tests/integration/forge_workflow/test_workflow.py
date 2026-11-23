"""Small real Rust/PostgreSQL workflow in disposable schemas and storage roots.

Enable explicitly: MARKETFORGE_FORGE_INTEGRATION=1 pytest ...
No shared schema is migrated; each run owns three random schema names.
"""

import asyncio
import json
import os
import re
import shutil
import zipfile
from collections import Counter
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forge.historical.engine.managed import checksum
from forge.historical.normalization import datasets, execution
from forge.historical.normalization.batch import prepare_batch
from forge.historical.normalization.config import NormalizationRecipe
from forge.historical.profiles.service import register as register_profile
from forge.historical.registration import service as registration
from forge.shared.config.toml import dumps
from forge.shared.database.migrations import load_migrations
from forge.shared.database.testing import testing_connection as db_connection
from forge.shared.database.testing import testing_dsn as db_dsn
from forge.shared.paths import ProjectPaths

ROOT = Path(__file__).resolve().parents[4]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.database,
    pytest.mark.engine,
    pytest.mark.skipif(
        os.environ.get("MARKETFORGE_FORGE_INTEGRATION") != "1", reason="explicit opt-in required"
    ),
]


@pytest.fixture
def isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Iterator[Any]:
    if getattr(request, "param", None) == "persistent":
        tmp_path = ROOT / "data/tests/historical/normalize" / f"byod-{uuid4().hex}"
        tmp_path.mkdir(parents=True)
    prefix = "fi_" + uuid4().hex[:12]
    mapping = {name: prefix + "_" + name for name in ("catalog", "historical", "ranking")}
    dsn = db_dsn(ROOT)

    def rewrite(query: Any) -> Any:
        if isinstance(query, str):
            for name, replacement in mapping.items():
                query = re.sub(r"\b" + name + r"\b", replacement, query)
            query = query.replace(
                f"marketforge:testing:{mapping['historical']}:v1",
                "marketforge:testing:historical:v1",
            )
        return query

    class Cursor(psycopg.AsyncCursor[Any]):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            try:
                return await super().execute(rewrite(query), params, **kwargs)
            except psycopg.Error as error:
                print(type(error).__name__, error.diag.message_primary)
                raise

    @asynccontextmanager
    async def connection(_: str) -> AsyncGenerator[Any]:
        async with db_connection(dsn) as c:
            c.cursor_factory = Cursor
            yield c

    async def initialize() -> None:
        async with connection(dsn) as c:
            async with c.transaction():
                for migration in load_migrations():
                    await c.execute(migration.sql)
                    if migration.version == 12:
                        # Legacy remote/external ID=1 collisions must preserve both
                        # copy locations and map onto distinct stable source objects.
                        await c.execute(
                            "INSERT INTO catalog.exchanges(code,name) VALUES('binance','synthetic legacy') ON CONFLICT DO NOTHING"
                        )
                        await c.execute("""INSERT INTO historical.remote_archives(exchange_id,scope_key,scope_json,data_type,source_object_key,filename,start_at,end_at)
                        VALUES((SELECT id FROM catalog.exchanges WHERE code='binance'),'legacy-remote','{"instrument_type":"spot","market_category":"spot","scope_kind":"instrument","scope_identifier":"OTHER"}','trades','old-remote','old.zip','2026-01-01T00:00:00Z','2026-01-02T00:00:00Z')""")
                        await c.execute(
                            "INSERT INTO historical.external_sources(source_key,source_kind,root_uri) VALUES('legacy-local','recording','/original/location')"
                        )
                        await c.execute("""INSERT INTO historical.external_archives(external_source_id,source_object_key,sha256,bytes_expected,identity_json,data_type,normalization_readiness,import_provenance)
                        VALUES(1,'old-local',repeat('a',64),1,'{"exchange":"binance","instrument_type":"spot","market_category":"spot","symbol":"OTHER"}','trades','requires_profile','{}')""")
                        await c.execute(
                            "INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path) VALUES(1,'legacy-root','unchanged/remote.zip')"
                        )
                        await c.execute(
                            "INSERT INTO historical.local_archives(external_archive_id,storage_root_key,relative_path) VALUES(1,'legacy-root','unchanged/local.csv')"
                        )

    asyncio.run(initialize())
    # All production service queries use real PostgreSQL, redirected only at the
    # cursor boundary to operation-owned schemas. Environment guards stay real.
    for module in (
        registration,
        datasets,
        __import__("forge.historical.normalization.batch", fromlist=["testing_connection"]),
    ):
        monkeypatch.setattr(module, "database_connection", connection)

    def selected_dsn(_: Path) -> str:
        return dsn

    monkeypatch.setattr(registration, "database_dsn", selected_dsn)
    monkeypatch.setattr("forge.shared.database.connection.database_connection", connection)
    monkeypatch.setattr("forge.shared.database.connection.database_dsn", selected_dsn)
    monkeypatch.setattr(datasets, "database_dsn", selected_dsn)
    monkeypatch.setattr("forge.historical.normalization.batch.database_dsn", selected_dsn)
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    paths.bin_dir.mkdir()
    shutil.copyfile(ROOT / ".bin/marketforge-normalize", paths.bin_dir / "marketforge-normalize")
    (paths.bin_dir / "marketforge-normalize").chmod(0o755)
    (paths.bin_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "targets": {
                    "normalize": {"sha256": checksum(paths.bin_dir / "marketforge-normalize")}
                },
            }
        )
    )
    paths.engine_config_dir.mkdir(parents=True)
    runtime = (
        (ROOT / "engine/src/shared/historical/config/fixtures/runtime.toml")
        .read_text()
        .replace("workers = 8", "workers = 1")
        .replace("memory_budget_bytes = 68719476736", "memory_budget_bytes = 536870912")
        .replace('scratch_path = "data/.work/normalize"', f'scratch_path = "{tmp_path}/scratch"')
    )
    runtime = (
        runtime.replace("row_group_target_bytes = 268435456", "row_group_target_bytes = 65536")
        .replace("file_target_bytes = 2147483648", "file_target_bytes = 131072")
        .replace("reconciliation_bytes = 8589934592", "reconciliation_bytes = 16777216")
        .replace("metric_entries = 65536", "metric_entries = 128")
        .replace("depth_levels = 100000", "depth_levels = 100")
        .replace("record_bytes = 4194304", "record_bytes = 65536")
        .replace("workbook_bytes = 134217728", "workbook_bytes = 1048576")
        .replace("workbook_xml_bytes = 268435456", "workbook_xml_bytes = 1048576")
        .replace("workbook_rows = 250000", "workbook_rows = 100")
    )
    (paths.config_dir / "engine/normalize.toml").write_text(runtime)
    (paths.root / "forge").mkdir()
    (paths.root / "forge/pyproject.toml").write_text("")
    (paths.root / "engine").mkdir()
    try:
        yield paths, connection
    finally:

        async def cleanup() -> None:
            async with db_connection(dsn) as c:
                for schema in reversed(list(mapping.values())):
                    await c.execute(
                        sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                    )

        asyncio.run(cleanup())


def test_trade_depth_registration_prepare_edit_execute_retry(
    isolated: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, connection = isolated

    async def legacy_mapping() -> None:
        async with connection("") as c:
            cur = await c.execute(
                "SELECT legacy_origin,legacy_id,raw_object_id FROM historical.legacy_raw_identity_map ORDER BY legacy_origin"
            )
            entries = await cur.fetchall()
            assert len(entries) == 2 and entries[0][1] == entries[1][1] == 1
            assert entries[0][2] != entries[1][2]
            cur = await c.execute("SELECT relative_path FROM historical.local_archives ORDER BY id")
            assert await cur.fetchall() == [("unchanged/remote.zip",), ("unchanged/local.csv",)]

    asyncio.run(legacy_mapping())

    async def exchange_registry_compatibility() -> None:
        from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
        from forge.historical.archives.models import DiscoveredArchive
        from forge.historical.archives.registry import register_remote_archive

        async with connection("") as c:
            request = ArchiveRequest.model_validate(
                {
                    "exchange": "binance",
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "data_type": "trades",
                    "scope": {"kind": "instrument", "identifier": "OTHER"},
                }
            )
            archive = DiscoveredArchive(
                request=request,
                source_object_key="new-remote",
                filename="new.zip",
                interval=AcquisitionInterval(
                    start=datetime(2026, 1, 1, tzinfo=UTC), end=datetime(2026, 1, 2, tzinfo=UTC)
                ),
            )
            async with c.transaction():
                registered = await register_remote_archive(c, archive)
            cur = await c.execute(
                "SELECT o.acquisition_method,s.provider FROM historical.remote_archives r JOIN historical.raw_objects o ON o.id=r.id JOIN historical.raw_sources s ON s.id=o.raw_source_id WHERE r.id=%s",
                (registered.id,),
            )
            assert await cur.fetchone() == ("exchange_download", "binance")

    asyncio.run(exchange_registry_compatibility())

    # Manually authored fixture definitions. No production profile inference.
    trade = (ROOT / "docs/content/examples/forge/binance.spot.spot.trades.toml").read_text()
    depth = (ROOT / "docs/content/examples/forge/binance.spot.spot.depth.toml").read_text()
    for profile_id, text in (
        ("binance.spot.spot.trades", trade),
        ("binance.spot.spot.depth", depth),
    ):
        source = tmp_path / f"{profile_id}.toml"
        source.write_text(text)
        assert (
            asyncio.run(register_profile(paths, profile_id, source))["publication"] == "installed"
        )
        assert asyncio.run(register_profile(paths, profile_id, source))["publication"] == "reused"
    source = tmp_path / "inputs"
    source.mkdir()
    with zipfile.ZipFile(source / "trades.zip", "w") as archive:
        archive.writestr(
            "trades.csv",
            "1,100,1,100,1767225600000000,false,true\n2,101,1,101,1767225601000000,true,true\n",
        )
    with zipfile.ZipFile(source / "depth.zip", "w") as archive:
        archive.writestr(
            "depth.jsonl",
            '{"timestamp":1767225600,"asks":[[101,1]],"bids":[[99,1]]}\n{"timestamp":1767225601,"asks":[[102,1]],"bids":[[100,1]]}\n',
        )
    document: dict[str, Any] = {
        "registration": {"version": 1, "name": "synthetic"},
        "source": {
            "key": "synthetic",
            "provider": "fixture-recorder",
            "product": "explicit-trades-depth",
            "root": str(source),
            "provenance": "Synthetic assertions for testing only",
        },
        "inputs": [
            {
                "instrument": "binance.spot.spot.BTCUSDT",
                "profile": "binance.spot.spot.trades",
                "content": "trades",
                "files": ["trades.zip"],
            },
            {
                "instrument": "binance.spot.spot.BTCUSDT",
                "profile": "binance.spot.spot.depth",
                "content": "depth",
                "files": ["depth.zip"],
            },
        ],
        "coverage": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
    }
    document["coverage"] = {
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "end": datetime(2026, 1, 2, tzinfo=UTC),
    }
    recipe = tmp_path / "registration.toml"
    recipe.write_text(dumps(document))

    async def declare_catalogue() -> None:
        from forge.catalog.models import Instrument, Specification
        from forge.catalog.service import add as add_catalogue

        async with connection("") as c:
            await add_catalogue(
                c,
                Instrument(
                    exchange="binance",
                    symbol="BTCUSDT",
                    instrument_type="spot",
                    market_category="spot",
                    base_asset="BTC",
                    quote_asset="USDT",
                ),
            )
            await add_catalogue(
                c,
                Specification(
                    instrument="binance.spot.spot.BTCUSDT",
                    quantity_type="base",
                    valid_from=datetime(2026, 1, 1, tzinfo=UTC),
                    valid_to=datetime(2026, 1, 2, tzinfo=UTC),
                    tick_size=Decimal("0.01"),
                    provenance="Synthetic historical fixture",
                ),
            )

    asyncio.run(declare_catalogue())
    first = asyncio.run(registration.apply(paths, recipe))
    assert first["status"] == "registered"
    second = asyncio.run(registration.apply(paths, recipe))
    assert all(item["status"] == "reused" for item in second["copies"])
    request = NormalizationRecipe.model_validate(
        {
            "version": 1,
            "interval": document["coverage"],
            "requests": [
                {
                    "exchange": "binance",
                    "market_category": "spot",
                    "instrument_type": "spot",
                    "data_type": kind,
                    "scope": {"kind": "instrument", "identifier": "BTCUSDT"},
                }
                for kind in ("trades", "order_book_l2")
            ],
        }
    )
    report = asyncio.run(prepare_batch(paths, request))
    assert report["engine_ready"], report
    batch_path = Path(str(report["batch_path"]))
    batch = json.loads(batch_path.read_bytes())
    assert batch["schema_version"] == 4
    # A user edit is preserved; Forge must not rebuild identities/ranking/outputs.
    batch["run_id"] = "manually-edited-submission"
    batch_path = tmp_path / "explicit-submission.json"
    batch_path.write_text(json.dumps(batch, indent=1) + "\n")
    submitted = batch_path.read_bytes()
    result = execution.execute(paths, batch_path)
    assert Path(result["submission_path"]).read_bytes() == submitted
    assert result["status"] == "complete", result
    for outcome in result["registrations"]:
        assert outcome["registration"] == "registered", outcome
        repeated = asyncio.run(datasets.register(paths, Path(outcome["path"])))
        assert repeated["catalogue_dataset_id"] == outcome["catalogue_dataset_id"]

    async def check() -> None:
        async with connection("") as c:
            cur = await c.execute(
                "SELECT count(*) FROM historical.datasets WHERE status='complete'"
            )
            assert (await cur.fetchone())[0] == 2
            cur = await c.execute("SELECT count(*) FROM historical.dataset_raw_inputs")
            assert (await cur.fetchone())[0] == 2

    asyncio.run(check())
    assert all(
        Path(outcome["path"]).is_relative_to(paths.datasets_dir / "normalized")
        for outcome in result["registrations"]
    )

    # Direct Rust invocation remains database-independent. Change only execution
    # identities/destinations in a new submitted document, retaining metadata.
    direct = json.loads(submitted)
    direct["jobs"] = [direct["jobs"][0]]
    direct_job = direct["jobs"][0]
    direct_job["job_id"] = 991
    direct_job["dataset_id"] = 992
    direct_job["output"]["dataset_path"] = str(tmp_path / "direct-dataset")
    direct_job["output"]["staging_path"] = str(tmp_path / ".direct-staging")
    direct_path = tmp_path / "direct.json"
    direct_path.write_text(json.dumps(direct))
    from forge.historical.engine.managed import supervised

    response = supervised(
        [
            str(paths.bin_dir / "marketforge-normalize"),
            "--batch",
            str(direct_path),
            "--project-root",
            str(paths.root),
        ],
        cwd=paths.root,
    )
    assert response.returncode == 0, response.stdout
    direct_dataset = tmp_path / "direct-dataset"
    original_connection = datasets.database_connection
    from forge.shared.database.testing import TestingDatabaseError

    @asynccontextmanager
    async def unavailable(_: str) -> AsyncGenerator[Any]:
        raise TestingDatabaseError("simulated testing database outage")
        yield

    monkeypatch.setattr(datasets, "database_connection", unavailable)
    pending = asyncio.run(datasets.register(paths, direct_dataset))
    assert pending["publication"] == "committed" and pending["registration"] == "pending"
    assert "submitted_sha256" not in json.dumps(pending["execution"])
    before = checksum(direct_dataset / "manifest.json")
    monkeypatch.setattr(datasets, "database_connection", original_connection)
    retry = asyncio.run(datasets.register(paths, direct_dataset))
    assert retry["registration"] == "registered"
    assert checksum(direct_dataset / "manifest.json") == before

    # Concurrent retries converge on one authoritative catalogue row.
    async def concurrent_retry() -> None:
        left, right = await asyncio.gather(
            datasets.register(paths, direct_dataset), datasets.register(paths, direct_dataset)
        )
        assert left["registration"] == right["registration"] == "registered"
        assert left["catalogue_dataset_id"] == right["catalogue_dataset_id"]

    asyncio.run(concurrent_retry())

    # An interpretation conflict cannot overwrite a manually installed profile.
    submitted_profile = tmp_path / "conflicting" / "binance.spot.spot.trades.toml"
    submitted_profile.parent.mkdir()
    submitted_profile.write_text(trade.replace("unix_microseconds", "unix_milliseconds"))
    from forge.shared.errors import MarketForgeError

    with pytest.raises(MarketForgeError, match="Different interpretation"):
        asyncio.run(register_profile(paths, "binance.spot.spot.trades", submitted_profile))
    assert (
        paths.config_dir / "engine/normalize/profiles/trades/binance.spot.spot.trades.toml"
    ).read_text() == trade


def test_catalogue_enrichment_immutable_idempotent_and_family_authority(isolated: Any) -> None:
    """Fresh/upgrade migrations and inference use real isolated PostgreSQL schemas."""
    from forge.historical.metadata.policy import NAMESPACE
    from forge.historical.metadata.service import enrich

    paths, connection = isolated

    async def check() -> None:
        async with connection("") as c:
            await c.execute(
                "INSERT INTO catalog.exchanges(code,name) VALUES('okx','synthetic OKX') ON CONFLICT DO NOTHING"
            )
            for symbol, kind, market in [
                ("BTC-USDT-SWAP", "perpetual", "linear"),
                ("BTC-USD_UM-261225", "future", "linear"),
                ("BTC-USD_UM-260925", "future", "linear"),
            ]:
                cur = await c.execute(
                    """INSERT INTO catalog.instruments(exchange_id,symbol,instrument_type,market_category,base_asset,quote_asset,settlement_asset,expiry_ns)
                 VALUES((SELECT id FROM catalog.exchanges WHERE code='okx'),%s,%s,%s,'BTC','USD','BTC',1790323200000000000) RETURNING id""",
                    (symbol, kind, market),
                )
                row = await cur.fetchone()
                assert row
                instrument_id = row[0]
                # One expired member deliberately lacks specs and uses its BTC reference.
                if symbol.endswith("261225") or kind == "perpetual":
                    await c.execute(
                        """INSERT INTO catalog.instrument_specs(instrument_id,quantity_type,contract_value,contract_value_asset,tick_size,fetched_at) VALUES(%s,'contracts',0.01,'BTC',0.100000000000000001,'2026-11-05T00:00:00Z')""",
                        (instrument_id,),
                    )
                await c.execute(
                    "INSERT INTO catalog.instrument_api_raw(instrument_id,fetched_at,raw_json) VALUES(%s,'2026-11-05T00:00:00Z','{}')",
                    (instrument_id,),
                )
            before = await c.execute("SELECT count(*) FROM historical.local_archives")
            raw_count = await before.fetchone()
            first = await enrich(c, paths, discover_archives=False)
            assert first["concrete_instruments"]["catalogue_inferred"] == 2
            assert first["concrete_instruments"]["reference_contract_inferred"] == 1
            cur = await c.execute(
                "SELECT id,namespace,instrument_id,fingerprint,definition FROM historical.specification_evidence ORDER BY id"
            )
            original = await cur.fetchall()
            assert len(original) == 3
            assert all(
                row[1] == NAMESPACE and not row[4]["historically_verified"] for row in original
            )
            cur = await c.execute("SELECT count(*) FROM historical.family_membership_evidence")
            members = await cur.fetchone()
            assert members == (3,)
            await enrich(c, paths, discover_archives=False)
            cur = await c.execute(
                "SELECT id,namespace,instrument_id,fingerprint,definition FROM historical.specification_evidence ORDER BY id"
            )
            assert await cur.fetchall() == original
            cur = await c.execute("SELECT count(*) FROM historical.family_membership_evidence")
            assert await cur.fetchone() == members
            cur = await c.execute("SELECT count(*) FROM historical.local_archives")
            assert await cur.fetchone() == raw_count
            import psycopg

            with pytest.raises(psycopg.Error, match="immutable"):
                async with c.transaction():
                    await c.execute("UPDATE historical.specification_evidence SET definition='{}'")

    asyncio.run(check())


@pytest.mark.parametrize("isolated", ["persistent"], indirect=True)
def test_databento_recordings(
    isolated: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Publish both supplied JSONL files through the testing registry and Rust."""
    from pyarrow import parquet

    from forge.cli.commands import catalog
    from forge.cli.main import main

    paths, connection = isolated
    example = ROOT / "docs/content/examples/bring-your-own-data/databento"
    (paths.root / ".recordings").symlink_to(ROOT / ".recordings", target_is_directory=True)
    monkeypatch.setattr(catalog, "database_connection", connection)
    monkeypatch.setattr(catalog, "database_dsn", lambda _: "")

    def cli(*arguments: str) -> dict[str, Any]:
        status = main(["--project-root", str(paths.root), "--json", *arguments])
        output = capsys.readouterr()
        assert status == 0, (arguments, output.out, output.err)
        return json.loads(output.out)

    for entity in ("exchange", "instrument", "specs"):
        cli("catalog", entity, "add", "--from", str(example / f"{entity}.toml"), "--testing")
    for kind in ("trades", "depth"):
        profile = f"xnas.equity.equity.{kind}"
        definition = str(example / f"{profile}.toml")
        cli("normalize", "profiles", "validate", "--file", definition)
        cli("normalize", "profiles", "add", profile, "--from", definition, "--testing")

    recipe = str(example / "registration.toml")
    cli("acquisition", "register", "plan", recipe, "--testing")
    assert cli("acquisition", "register", "run", recipe, "--testing")["status"] == "registered"
    repeated = cli("acquisition", "register", "run", recipe, "--testing")
    assert len(repeated["copies"]) == 2
    assert all(copy["status"] == "reused" for copy in repeated["copies"])

    async def check_registry() -> None:
        async with connection("") as c:
            sources = await c.execute(
                "SELECT provider, product, provenance->>'declaration' "
                "FROM historical.raw_sources WHERE source_key=%s",
                ("databento-xnas-itch-aapl-20241028",),
            )
            ((provider, product, provenance),) = await sources.fetchall()
            assert (provider, product) == ("Databento", "XNAS.ITCH")
            assert "Publisher ID 2" in provenance
            bindings = await c.execute(
                """SELECT o.filename, o.venue_code, o.start_at, o.end_at,
                          i.sha256, i.format_profile, i.specifications_json,
                          i.import_provenance->'binding'
                   FROM historical.raw_sources s
                   JOIN historical.raw_objects o ON o.raw_source_id=s.id
                   JOIN historical.raw_interpretations i ON i.raw_object_id=o.id
                   WHERE s.source_key=%s""",
                ("databento-xnas-itch-aapl-20241028",),
            )
            expected = {"AAPL_trades.jsonl": "trades", "AAPL_orderbook_L2.jsonl": "depth"}
            rows = await bindings.fetchall()
            assert {row[0] for row in rows} == set(expected)
            assert len(rows) == 2
            for name, venue, start, end, digest, profile_id, specs, binding in rows:
                assert venue == "xnas"
                assert start == datetime(2024, 10, 28, 14, 30, tzinfo=UTC)
                assert end == datetime(2024, 10, 28, 14, 40, tzinfo=UTC)
                assert digest == checksum(ROOT / ".recordings" / name)
                assert profile_id == f"xnas.equity.equity.{expected[name]}"
                assert specs["quantity_type"] == "base"
                assert Decimal(specs["qty_step"]) == 1
                assert Decimal(specs["tick_size"]) == Decimal("0.01")
                assert len(binding["profile_fingerprint"]) == 64
                assert len(binding["specification_fingerprint"]) == 64

    asyncio.run(check_registry())
    report = cli("normalize", "prepare", str(example / "normalization.toml"), "--testing")
    assert report["engine_ready"], report
    result = cli("normalize", "run", str(report["batch_path"]), "--testing")
    assert result["status"] == "complete", result
    assert result["counts"]["verified_publications"] == 2
    assert result["registrations"]
    assert all(item["registration"] == "registered" for item in result["registrations"])

    def read_source(filename: str) -> list[dict[str, Any]]:
        with (ROOT / ".recordings" / filename).open() as source:
            return [json.loads(line) for line in source]

    def timestamp_ns(value: str) -> int:
        whole, fraction = value.removesuffix("Z").split(".")
        delta = datetime.fromisoformat(whole).replace(tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        return (delta.days * 86400 + delta.seconds) * 10**9 + int(fraction.ljust(9, "0"))

    def event_identity(record: dict[str, Any]) -> tuple[Any, ...]:
        return (
            record["hd"]["ts_event"],
            record["hd"]["instrument_id"],
            record["sequence"],
            record["price"],
            record["size"],
            record["side"],
        )

    trades = read_source("AAPL_trades.jsonl")
    depth = read_source("AAPL_orderbook_L2.jsonl")
    assert len(trades) == 1977
    assert len(depth) == 39605
    assert Counter(map(event_identity, trades)) == Counter(
        event_identity(record) for record in depth if record["action"] == "T"
    )
    manifests = {}
    for job in result["jobs"]:
        root = Path(job["path"])
        manifest = json.loads((root / "manifest.json").read_bytes())
        manifests[manifest["content_type"]] = (root, manifest)
    assert set(manifests) == {"trades", "depth"}
    expected_parts = {
        str(directory / file["path"])
        for directory, manifest in manifests.values()
        for file in manifest["files"]
    }
    registered_parts = [item["path"] for item in result["registrations"]]
    assert len(registered_parts) == len(expected_parts)
    assert set(registered_parts) == expected_parts

    def verify_dataset(kind: str, records: list[dict[str, Any]]) -> tuple[Any, ...]:
        directory, manifest = manifests[kind]
        assert manifest["integrity_status"] == "clean"
        metrics = manifest["processing_metrics"]
        assert metrics["counters"]["records_read"] == len(records)
        assert metrics["counters"]["records_processed"] == len(records)
        assert metrics["counters"]["records_rejected"] == 0
        assert metrics["scoped_integrity"]["global"]["sequence_gap"] == {
            "eligible": 0,
            "violations": 0,
        }
        assert manifest["processing_quality"]["logical_source_events"] == len(records)
        (instrument,) = [entry["instrument"] for entry in manifest["resolved_instruments"]]
        assert (instrument["exchange"], instrument["symbol"]) == ("xnas", "AAPL")
        assert instrument["instrument_type"] == instrument["market_category"] == "equity"
        assert instrument["quote_asset"] == "USD"
        rows = [
            row
            for file in manifest["files"]
            for row in parquet.read_table(directory / file["path"]).to_pylist()
        ]
        assert len(rows) == manifest["events_written"]
        assert {(r["exchange"], r["symbol"], r["instrument_id"]) for r in rows} == {
            ("xnas", "AAPL", instrument["instrument_id"])
        }
        return directory, manifest, rows, instrument["instrument_id"]

    _, trade_manifest, trade_rows, trade_instrument = verify_dataset("trades", trades)
    assert not trade_manifest["processing_quality"]["continuity_proven"]
    assert Counter(row["side"] for row in trade_rows) == {
        "buy": 1089,
        "sell": 726,
        "unknown": 162,
    }
    assert Counter(
        (
            r["event_timestamp_ns"],
            r["system_timestamp_ns"],
            r["sequence"],
            r["price"],
            r["quantity_base"],
            r["side"],
        )
        for r in trade_rows
    ) == Counter(
        (
            timestamp_ns(r["hd"]["ts_event"]),
            timestamp_ns(r["ts_recv"]),
            r["sequence"],
            Decimal(r["price"]),
            r["size"],
            {"A": "sell", "B": "buy", "N": "unknown"}[r["side"]],
        )
        for r in trades
    )

    directory, manifest, rows, depth_instrument = verify_dataset("depth", depth)
    assert trade_instrument == depth_instrument
    assert len(rows) == 42619
    quality = manifest["processing_quality"]
    assert quality["supplied_observations_preserved"]
    assert not quality["continuity_proven"]
    assert not quality["external_historical_coverage_established"]

    (stream,) = manifest["streams"]
    depth_dir = directory / stream["directory"]
    boundary = json.loads((depth_dir / "boundary.json").read_text())
    (source,) = boundary["sources"]
    for key in ("initial", "final_state"):
        assert source["boundary"][key]["coverage"] == {
            "depth_per_side": 10,
            "bid_levels": 10,
            "ask_levels": 10,
        }
    events = parquet.read_table(depth_dir / "events.parquet").to_pylist()
    assert len(events) == len(depth)
    offset = 0
    book = {}
    previous_sequence = None
    repeats = 0
    for ordinal, (event, raw) in enumerate(zip(events, depth, strict=True), start=1):
        assert event["event_ordinal"] == ordinal
        assert event["event_timestamp_ns"] == timestamp_ns(raw["hd"]["ts_event"])
        assert event["system_timestamp_ns"] == timestamp_ns(raw["ts_recv"])
        assert event["sequence_start"] == event["sequence_end"] == raw["sequence"]
        if previous_sequence is not None:
            assert raw["sequence"] >= previous_sequence
            repeats += raw["sequence"] == previous_sequence
        previous_sequence = raw["sequence"]
        assert event["source_operation"] == 3  # BoundedObservation
        assert event["canonical_row_offset"] == offset
        end = offset + event["canonical_row_count"]
        for row in rows[offset:end]:
            key = row["side"], row["price"]
            if row["quantity_base"]:
                book[key] = row["quantity_base"], row["order_count"]
            else:
                del book[key]
        offset = end
        assert len(raw["levels"]) == 10
        assert book == {
            (side, Decimal(level[f"{side}_px"])): (level[f"{side}_sz"], level[f"{side}_ct"])
            for level in raw["levels"]
            for side in ("bid", "ask")
        }
    assert offset == len(rows)
    assert repeats == 938


def test_concurrent_registration_only(isolated: Any) -> None:
    """Exercise advisory-lock retries in isolated schemas with no engine work."""
    paths, connection = isolated

    async def check() -> None:
        output = str(paths.data_root / "synthetic-merge")
        files = [
            {
                "path": name,
                "rows": 1,
                "size_bytes": 1,
                "start_timestamp_ns": 1,
                "end_timestamp_ns": 1,
            }
            for name in ("timeline/first.parquet", "timeline/second.parquet")
        ]
        evidence = {
            "operation": "merge",
            "content_type": "trades",
            "path": output,
            "manifest_path": output + "/manifest.json",
            "manifest_sha256": "b" * 64,
            "processing_job_sha256": "c" * 64,
            "engine_dataset_id": "61",
            "instrument_namespace": "fixture",
            "manifest": {"files": files},
            "resolved_job": {"job_id": 61},
            "inventory_sha256": {item["path"]: "d" * 64 for item in files},
            "merge_inputs": [],
        }
        results = await asyncio.gather(
            *[
                datasets.register_verified(
                    paths, [evidence], receipt_directory=paths.runs_dir / attempt
                )
                for attempt in ("first", "second")
            ]
        )
        assert all(result["registration"] == "registered" for result in results)
        assert results[0]["catalogue_dataset_ids"] == results[1]["catalogue_dataset_ids"]
        assert sorted(result["counts"]["new"] for result in results) == [0, 2]
        async with connection("") as c:
            cur = await c.execute("SELECT count(*) FROM historical.dataset_publications")
            assert await cur.fetchone() == (1,)
            cur = await c.execute(
                "SELECT count(*) FROM historical.datasets WHERE publication_id IS NOT NULL"
            )
            assert await cur.fetchone() == (2,)

    asyncio.run(check())
