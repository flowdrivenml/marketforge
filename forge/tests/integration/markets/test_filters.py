"""Metadata filters execute in PostgreSQL before ordering/limiting the catalogue."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument
from forge.markets.repository import browse, save_instrument

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_metadata_and_globs_before_first_page_limit(
    database: AsyncConnection[TupleRow],
) -> None:
    tag = "FILTER" + uuid4().hex[:10].upper()
    now = datetime.now(UTC)
    symbols = [f"A{i:03}" for i in range(55)] + [
        "ETHUSDT",
        "ETH_USDT",
        "ETHXUSDT",
        "ETH%USDT",
        "ETHBTC",
    ]
    for symbol in symbols:
        await save_instrument(
            database,
            Instrument(
                exchange="bybit",
                market="spot",
                symbol=tag + symbol,
                base="ETH" if symbol.startswith("ETH") else "FILLER",
                quote="BTC" if symbol.endswith("BTC") else "USDT",
            ),
            now,
        )
    await save_instrument(
        database,
        Instrument(
            exchange="bybit",
            market="linear",
            symbol=tag + "ETH-USDT-SWAP",
            base="ETH",
            quote="USDT",
        ),
        now,
    )
    first = await browse(database, exchange="bybit", market="spot", match=tag + "*", limit=50)
    assert not any(str(row["symbol"]).startswith(tag + "ETH") for row in first)
    for sort in ("identity", "freshness", "turnover"):
        result = await browse(
            database,
            exchange="bybit",
            market="spot",
            base_asset="eTh",
            quote_asset="uSdT",
            match=tag.lower() + "eTh*",
            sort=sort,
            limit=1,
        )
        assert (
            len(result) == 1
            and result[0]["base_asset"] == "ETH"
            and result[0]["quote_asset"] == "USDT"
        )
    assert [row["symbol"] for row in await browse(database, match=tag + "ETH_USDT")] == [
        tag + "ETH_USDT"
    ]
    assert [row["symbol"] for row in await browse(database, match=tag + "ETH%USDT")] == [
        tag + "ETH%USDT"
    ]
    question = await browse(database, match=tag + "ETH?USDT")
    assert {row["symbol"] for row in question} == {
        tag + "ETH_USDT",
        tag + "ETHXUSDT",
        tag + "ETH%USDT",
    }
    assert [
        row["symbol"]
        for row in await browse(database, base_asset="eth", quote_asset="btc", match=tag + "*bTc")
    ] == [tag + "ETHBTC"]
    exact = await browse(
        database,
        symbol=tag + "ETHUSDT",
        match=tag.lower() + "eth*",
        base_asset="ETH",
        quote_asset="USDT",
    )
    assert [row["symbol"] for row in exact] == [tag + "ETHUSDT"]
    assert await browse(database, symbol=(tag + "ETHUSDT").lower(), match=tag + "*") == []
    quoted = await browse(database, match=tag + "*", quote_asset="USDT")
    assert tag + "ETH-USDT-SWAP" in {row["symbol"] for row in quoted}
    suffix = await browse(database, match=tag + "*uSdT", quote_asset="USDT")
    assert tag + "ETH-USDT-SWAP" not in {row["symbol"] for row in suffix}
