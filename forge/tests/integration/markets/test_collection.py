"""Failed metadata pagination preserves stored identities and records failure."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from forge.markets.models import Instrument, Market, MarketDataError
from forge.markets.providers.public import PublicProvider
from forge.markets.repository import save_instrument
from forge.markets.service import collect
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection as connect_test
from forge.shared.database.testing import testing_dsn as load_test_dsn

pytestmark = [pytest.mark.integration, pytest.mark.database]
ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.asyncio
async def test_failed_metadata_pages_do_not_remove_existing_catalogue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = load_test_dsn(ROOT)
    symbol = "PAGES_" + uuid4().hex
    async with connect_test(dsn) as c:
        identity = await save_instrument(
            c,
            Instrument(exchange="bybit", market="spot", symbol=symbol, base="BTC", quote="USDT"),
            datetime.now(UTC),
        )

    async def broken_pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        assert metadata and market == "spot"
        yield []
        raise MarketDataError("Controlled second-page failure")

    monkeypatch.setattr(PublicProvider, "pages", broken_pages)
    result: dict[str, object] = {}
    try:
        result = await collect(
            dsn, HttpSettings(), metadata=True, exchanges=["bybit"], markets=["spot"], symbol=symbol
        )
        assert result["status"] == "partial"
        async with connect_test(dsn) as c:
            cur = await c.execute("SELECT status FROM catalog.instruments WHERE id=%s", (identity,))
            assert await cur.fetchone() == ("active",)
            cur = await c.execute(
                "SELECT status, source_outcomes->0->>'error' FROM catalog.instrument_syncs WHERE id=%s",
                (result["refresh_id"],),
            )
            assert await cur.fetchone() == ("partial", "Controlled second-page failure")
    finally:
        async with connect_test(dsn) as c, c.transaction():
            await c.execute(
                "DELETE FROM catalog.instrument_api_raw WHERE instrument_id=%s", (identity,)
            )
            await c.execute(
                "DELETE FROM catalog.instrument_specs WHERE instrument_id=%s", (identity,)
            )
            await c.execute("DELETE FROM catalog.instruments WHERE id=%s", (identity,))
            if result:
                await c.execute(
                    "DELETE FROM catalog.instrument_syncs WHERE id=%s", (result["refresh_id"],)
                )
