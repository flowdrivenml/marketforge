"""Joined identities and all-record nominal coverage in guarded PostgreSQL."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import (
    AcquisitionInterval,
    ArchiveRequest,
    ArchiveScope,
    ArchiveSourceOptions,
)
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import register_remote_archive
from forge.historical.archives.storage.acquire import root_key
from forge.markets.models import Instrument, array, obj
from forge.markets.repository import json_value, save_instrument
from forge.storage.browsing import coverage, list_archives
from forge.storage.models import StorageFilters

pytestmark = [pytest.mark.integration, pytest.mark.database]
BASE = datetime(2026, 10, 1, tzinfo=UTC)


@dataclass
class RegistryFixture:
    connection: AsyncConnection[TupleRow]
    root: Path
    tag: str
    serial: int = 0

    async def local(
        self,
        *,
        remote: int | None = None,
        external: int | None = None,
        root: Path | None = None,
        status: str = "complete",
    ) -> int:
        self.serial += 1
        cur = await self.connection.execute(
            """INSERT INTO historical.local_archives
            (remote_archive_id,external_archive_id,storage_root_key,relative_path,status,bytes_written,sha256,downloaded_at,last_verified_at)
            VALUES(%s,%s,%s,%s,%s,10,%s,now(),now()) RETURNING id""",
            (
                remote,
                external,
                root_key(root or self.root),
                f"{self.tag}/{self.serial}.bin",
                status,
                "a" * 64,
            ),
        )
        row = await cur.fetchone()
        assert row is not None
        return row[0]

    async def remote(
        self, request: ArchiveRequest, *, start: int = 0, end: int = 1
    ) -> tuple[int, int]:
        archive = DiscoveredArchive(
            request=request,
            source_object_key=f"{self.tag}/{self.serial}/{uuid4().hex}",
            filename="not-an-identity.bin",
            interval=AcquisitionInterval(
                start=BASE + timedelta(days=start), end=BASE + timedelta(days=end)
            ),
        )
        record = await register_remote_archive(self.connection, archive)
        return record.id, await self.local(remote=record.id)

    async def external(self, *, source_key: str, symbol: str, declared: bool = True) -> int:
        cur = await self.connection.execute(
            "INSERT INTO historical.external_sources(source_key,source_kind,root_uri) VALUES(%s,'recording','file:///fixture') RETURNING id",
            (self.tag + source_key,),
        )
        source = await cur.fetchone()
        assert source is not None
        object_cursor = await self.connection.execute(
            """INSERT INTO historical.raw_objects
               (acquisition_method,raw_source_id,venue_code,scope_key,scope_json,data_type,
                source_object_key,filename,start_at,end_at,remote_status)
               VALUES('local_copy',%s,'gateio',%s,'{}','order_book_l2',%s,'capture.bin',%s,%s,'unknown')
               RETURNING id""",
            (
                source[0],
                symbol,
                self.tag + source_key,
                BASE if declared else None,
                BASE + timedelta(days=2) if declared else None,
            ),
        )
        raw = await object_cursor.fetchone()
        assert raw is not None
        cur = await self.connection.execute(
            """INSERT INTO historical.raw_interpretations
            (raw_object_id,external_source_id,source_object_key,sha256,bytes_expected,identity_json,specifications_json,data_type,
             declared_start_at,declared_end_at,normalization_readiness,import_provenance)
            VALUES(%s,%s,%s,%s,10,%s,%s,'order_book_l2',%s,%s,'requires_profile','{}') RETURNING id""",
            (
                raw[0],
                source[0],
                self.tag + source_key,
                "b" * 64,
                json_value(
                    {
                        "exchange": "gateio",
                        "market_category": "linear",
                        "instrument_type": "perpetual",
                        "symbol": symbol,
                    }
                ),
                json_value({"base_asset": "ETH", "quote_asset": "BTC"}),
                BASE if declared else None,
                BASE + timedelta(days=2) if declared else None,
            ),
        )
        record = await cur.fetchone()
        assert record is not None
        return await self.local(external=record[0])


def request(
    symbol: str, *, family: bool = False, gate: bool = False, snapshot: bool = False
) -> ArchiveRequest:
    return ArchiveRequest(
        exchange="gateio" if gate else "okx",
        market_category="linear",
        instrument_type="perpetual" if gate else "future",
        data_type="order_book_l2" if gate else "trades",
        scope=ArchiveScope(kind="family" if family else "instrument", identifier=symbol),
        source_options=ArchiveSourceOptions(feed="snapshot" if snapshot else "incremental")
        if gate
        else None,
    )


@pytest.mark.asyncio
async def test_identity_assets_family_external_and_root_isolation(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = RegistryFixture(database, tmp_path / "raw", "ST" + uuid4().hex[:12].upper())
    symbol = f.tag + "ETH_USDT"
    await save_instrument(
        database,
        Instrument(
            exchange="okx",
            market="linear",
            instrument_type="future",
            symbol=symbol,
            base="ETH",
            quote="USDT",
        ),
        BASE,
    )
    await save_instrument(
        database,
        Instrument(
            exchange="okx",
            market="linear",
            instrument_type="perpetual",
            symbol=symbol,
            base="OTHER",
            quote="OTHER",
        ),
        BASE,
    )
    remote, local = await f.remote(request(symbol), start=0, end=31)
    await f.local(remote=remote, root=tmp_path / "elsewhere")
    family = f.tag + "FAMILY"
    for suffix, quote in (("A", "USDT"), ("B", "BTC")):
        await save_instrument(
            database,
            Instrument(
                exchange="okx",
                market="linear",
                instrument_type="future",
                symbol=f.tag + suffix,
                base="ETH",
                quote=quote,
                raw={"instFamily": family},
            ),
            BASE,
        )
    await f.remote(request(family, family=True))
    unknown = f.tag + "UNKNOWN_FAMILY"
    await f.remote(request(unknown, family=True))
    imported = await f.external(source_key="one", symbol=symbol)
    await f.external(source_key="two", symbol=symbol, declared=False)
    # All browsing is SELECT-only; paths deliberately do not exist.
    await database.execute("SET LOCAL transaction_read_only=on")
    result = await list_archives(database, f.root, StorageFilters())
    rows = [obj(row) for row in array(result["archives"])]
    assert len(rows) == 5 and len({r["id"] for r in rows}) == 5
    native = next(row for row in rows if row["id"] == local)
    assert (
        native["base_asset"] == "ETH"
        and native["quote_asset"] == "USDT"
        and native["candidate_count"] == 1
    )
    assert native["local_id"] == f"local:{local}" and native["resolved_path"]
    aggregate = next(row for row in rows if row["scope_identifier"] == family)
    assert (
        aggregate["base_asset"] == "ETH"
        and aggregate["quote_asset"] is None
        and aggregate["candidate_count"] == 2
    )
    assert next(row for row in rows if row["scope_identifier"] == unknown)["base_asset"] is None
    external = next(row for row in rows if row["id"] == imported)
    assert (
        external["origin"] == "external"
        and external["quote_asset"] == "BTC"
        and external["identity_json"]
    )
    assert external["base_asset_source"] == "external_historical_specification"
    exact = await list_archives(
        database,
        f.root,
        StorageFilters(
            base_asset="eTh",
            quote_asset="uSdT",
            instrument_type="future",
            match=f.tag.lower() + "eTh_*",
            origin="remote",
        ),
    )
    assert [obj(r)["id"] for r in array(exact["archives"])] == [local]
    assert (
        array(
            (await list_archives(database, f.root, StorageFilters(match=f.tag + "ETHX*")))[
                "archives"
            ]
        )
        == []
    )
    suffix = await list_archives(
        database, f.root, StorageFilters(match="*usdt", base_asset="eth", scope_kind="instrument")
    )
    assert len(array(suffix["archives"])) == 3
    groups = await coverage(database, f.root, StorageFilters())
    assert len(array(groups["coverage"])) == 5
    unknown_interval = next(
        obj(row)
        for row in array(groups["coverage"])
        if obj(row).get("external_source_key") == f.tag + "two"
    )
    assert unknown_interval["gap_count"] is None and unknown_interval["unknown_interval_count"] == 1
    bounded = await list_archives(
        database,
        f.root,
        StorageFilters(start=BASE + timedelta(days=5), end=BASE + timedelta(days=6)),
    )
    assert [obj(r)["id"] for r in array(bounded["archives"])] == [local]
    assert obj(bounded["totals"])["unusable_intervals_before_date_filter"] == 1
    edge = await list_archives(database, f.root, StorageFilters(start=BASE + timedelta(days=31)))
    assert array(edge["archives"]) == []
    edge = await list_archives(database, f.root, StorageFilters(end=BASE))
    assert array(edge["archives"]) == []
    assert not f.root.exists()


@pytest.mark.asyncio
async def test_feed_variants_and_pagination_do_not_truncate_coverage(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = RegistryFixture(database, tmp_path / "raw", "SC" + uuid4().hex[:12].upper())
    symbol = f.tag + "ETH_USDT"
    await save_instrument(
        database,
        Instrument(exchange="gateio", market="linear", symbol=symbol, base="ETH", quote="USDT"),
        BASE,
    )
    remote, _ = await f.remote(request(symbol, gate=True))
    # Force coverage aggregation to account for more than a thousand duplicate local copies.
    await database.execute(
        """INSERT INTO historical.local_archives
        (remote_archive_id,storage_root_key,relative_path,status,bytes_written,sha256,downloaded_at)
        SELECT %s,%s,%s||n::text,'complete',10,%s,now() FROM generate_series(1,1001) n""",
        (remote, root_key(f.root), f.tag + "/copy", "a" * 64),
    )
    await f.remote(request(symbol, gate=True), start=3, end=4)
    await f.remote(request(symbol, gate=True, snapshot=True), start=0, end=4)
    unknown, _ = await f.remote(request(symbol, gate=True), start=1, end=2)
    await database.execute(
        "UPDATE historical.remote_archives SET scope_json=scope_json||'{\"source_feed\":null}'::jsonb WHERE id=%s",
        (unknown,),
    )
    await f.local(remote=remote, status="failed")
    await database.execute("SET LOCAL transaction_read_only=on")
    filters = StorageFilters(start=BASE - timedelta(days=1), end=BASE + timedelta(days=5), limit=1)
    result = await coverage(database, f.root, filters, include_gaps=True)
    assert obj(result["pagination"])["total"] == 3
    # Unknown feed sorts separately; find incremental via its exact filter.
    incremental = await coverage(
        database, f.root, filters.model_copy(update={"feed": "incremental"}), include_gaps=True
    )
    row = obj(array(incremental["coverage"])[0])
    assert row["local_file_count"] == 1003 and row["unique_registered_source_count"] == 2
    assert row["gap_count"] == 3 and row["covered_seconds"] == 2 * 86400
    assert obj(incremental["totals"])["recorded_bytes"] == 10030
    snapshot = await coverage(database, f.root, StorageFilters(feed="snapshot"), include_gaps=True)
    assert obj(array(snapshot["coverage"])[0])["gap_count"] == 0
    unknown_result = await coverage(database, f.root, StorageFilters(feed="unknown"))
    assert obj(array(unknown_result["coverage"])[0])["feed"] is None
    first = await list_archives(database, f.root, StorageFilters(limit=2))
    second = await list_archives(database, f.root, StorageFilters(limit=2, offset=2))
    a = [int(str(obj(row)["id"])) for row in array(first["archives"])]
    b = [int(str(obj(row)["id"])) for row in array(second["archives"])]
    assert not set(a) & set(b)
    first_rows = [obj(row) for row in array(first["archives"])]
    latest, prior = (row["start_at"] for row in first_rows)
    assert isinstance(latest, datetime) and isinstance(prior, datetime) and latest > prior
    smallest = await list_archives(
        database, f.root, StorageFilters(sort="bytes_written", order="asc", limit=1)
    )
    assert obj(array(smallest["archives"])[0])["bytes_written"] == 10
    largest_group = await coverage(database, f.root, StorageFilters(sort="files", limit=1))
    assert obj(array(largest_group["coverage"])[0])["local_file_count"] == 1003
    assert obj(first["pagination"])["has_more"] is True


@pytest.mark.asyncio
async def test_native_globs_treat_sql_wildcards_literally(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = RegistryFixture(database, tmp_path / "raw", "GLOB" + uuid4().hex[:10].upper())
    for suffix in ("ETH_USDT", "ETHXUSDT", "ETH%USDT", "ETHBTC"):
        await f.remote(request(f.tag + suffix))
    await database.execute("SET LOCAL transaction_read_only=on")
    for suffix in ("ETH_USDT", "ETH%USDT"):
        value = await list_archives(
            database, f.root, StorageFilters(match=f.tag.lower() + suffix.lower())
        )
        assert [obj(row)["scope_identifier"] for row in array(value["archives"])] == [
            f.tag + suffix
        ]
    prefix = await list_archives(database, f.root, StorageFilters(match=f.tag + "eTh*", limit=2))
    assert obj(prefix["pagination"])["total"] == 4 and obj(prefix["pagination"])["has_more"]
    suffix_result = await coverage(database, f.root, StorageFilters(match="*bTc"))
    assert [obj(row)["scope_identifier"] for row in array(suffix_result["coverage"])] == [
        f.tag + "ETHBTC"
    ]


@pytest.mark.asyncio
async def test_workspace_raw_groups_keep_source_windows_and_exact_archive_ids(
    database: AsyncConnection[TupleRow], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager

    from forge.datasets import workspace_inventory as inventory
    from forge.shared.paths import ProjectPaths

    f = RegistryFixture(database, tmp_path / "raw", "WS" + uuid4().hex[:12].upper())
    symbol = f.tag + "BTCUSDT"
    await save_instrument(
        database,
        Instrument(
            exchange="gateio",
            market="linear",
            instrument_type="perpetual",
            symbol=symbol,
            base="BTC",
            quote="USDT",
        ),
        BASE,
    )
    _, first = await f.remote(request(symbol, gate=True), start=0, end=1)
    _, adjacent = await f.remote(request(symbol, gate=True), start=1, end=2)
    _, separated = await f.remote(request(symbol, gate=True), start=3, end=4)
    _, snapshot = await f.remote(request(symbol, gate=True, snapshot=True), start=0, end=1)
    external = await f.external(source_key="imported", symbol=symbol, declared=False)
    second_symbol = f.tag + "ETHUSDT"
    await save_instrument(
        database,
        Instrument(
            exchange="gateio",
            market="linear",
            instrument_type="perpetual",
            symbol=second_symbol,
            base="ETH",
            quote="USDT",
        ),
        BASE,
    )
    await database.execute(
        """
        WITH bindings AS (
            INSERT INTO historical.raw_interpretations (
                raw_object_id,external_source_id,source_object_key,sha256,bytes_expected,
                identity_json,specifications_json,data_type,normalization_readiness,import_provenance,
                binding_fingerprint
            )
            SELECT a.raw_object_id,a.external_source_id,a.source_object_key,a.sha256,a.bytes_expected,
                a.identity_json || jsonb_build_object('symbol',candidate.symbol),
                a.specifications_json,a.data_type,a.normalization_readiness,
                '{"binding":{"readiness":{"normalization_handoff":"ready"}}}'::jsonb,
                encode(sha256(convert_to(candidate.symbol,'UTF8')),'hex')
            FROM historical.local_archives l JOIN historical.raw_interpretations a
                ON a.id=l.external_archive_id
            CROSS JOIN unnest(%s::text[]) candidate(symbol)
            WHERE l.id=%s
            RETURNING id
        )
        INSERT INTO historical.raw_bindings(local_archive_id,interpretation_id)
            SELECT %s,id FROM bindings
        """,
        ([symbol, second_symbol], external, external),
    )

    @asynccontextmanager
    async def fixture_connection(_: ProjectPaths) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(inventory, "readonly", fixture_connection)
    paths = ProjectPaths(tmp_path, tmp_path)
    rows, more = await inventory.stage_page(paths, "Raw", 0)
    assert not more and len(rows) == 4
    window = next(row for row in rows if row["file_count"] == 2)
    assert window["archives"] == [f"local:{first}", f"local:{adjacent}"]
    assert (
        window["exchange"],
        window["symbol"],
        window["instrument_type"],
        window["market_category"],
        window["data_type"],
        window["feed"],
    ) == (
        "gateio",
        symbol,
        "perpetual",
        "linear",
        "order_book_l2",
        "incremental",
    )
    assert window["instrument"] and window["source"] == "marketforge.exchange.gateio"
    assert (window["start"], window["finish"], window["recorded_bytes"], window["state"]) == (
        BASE,
        BASE + timedelta(days=2),
        20,
        "complete",
    )
    by_archive = {ref: row for row in rows for ref in row["archives"]}
    assert by_archive[f"local:{separated}"]["file_count"] == 1
    assert by_archive[f"local:{snapshot}"]["feed"] == "snapshot"
    multi = by_archive[f"local:{external}"]
    assert multi["start"] is None and multi["symbol"] == "Multiple (2)"
    assert {item["symbol"] for item in multi["instruments"]} == {symbol, second_symbol}
    assert (multi["file_count"], multi["recorded_bytes"]) == (1, 10)
    records = await inventory.archive_group(paths, [first, adjacent])
    assert [row["local_id"] for row in records] == window["archives"]
    assert all(row["sha256"] == "a" * 64 and row["resolved_path"] for row in records)
