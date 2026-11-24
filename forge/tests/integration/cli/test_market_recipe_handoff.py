"""Exercise the Markets handoff against a real verified testing registry."""

import asyncio
import gzip
import os
import tomllib
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

import httpx
import pytest
from textual.widgets import Checkbox, Input, Select

from forge.catalog.models import Instrument
from forge.catalog.service import add
from forge.historical.archives.config import (
    AcquisitionConfig,
    AcquisitionInterval,
    ArchiveRequest,
    load_acquisition,
)
from forge.historical.archives.discovery import discover, scan_archives
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.planning import plan
from forge.historical.archives.sources.exchanges import ExchangeSource, validate_request
from forge.historical.archives.storage.acquire import acquire
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.pipelines.config import load_recipe
from forge.shared.database.testing import testing_connection as registry_connection
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter
from forge.startup.context import build_context
from forge.tui.app import ForgeApp
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.market_recipes import MarketRecipeDialog
from forge.tui.presentation import Results

pytestmark = [pytest.mark.integration, pytest.mark.database]


class FixtureArchiveSource:
    version = ExchangeSource.version
    validate = staticmethod(validate_request)

    async def discover(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        yield DiscoveredArchive(
            request=request,
            interval=interval,
            source_object_key=f"fixture/{request.scope.identifier}/{request.data_type}",
            filename=f"{request.data_type}.csv.gz",
            download_url=f"https://exchange.test/{request.data_type}.csv.gz",
        )


async def verify_acquisition(dsn: str, config: AcquisitionConfig, root: Path) -> None:
    async with registry_connection(dsn) as connection:
        before = await plan(connection, config, root)
        assert before["sampled_archives"] == []
        assert not await asyncio.to_thread(lambda: any(root.rglob("*.csv.gz")))
        source = FixtureArchiveSource()
        async with open_http_client(
            limiter=RequestRateLimiter(10),
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, stream=httpx.ByteStream(gzip.compress(request.url.path.encode()))
                )
            ),
        ) as http:
            for request in config.requests[1:]:
                scan = await discover(connection, source, request, config.interval)
                (remote,) = await scan_archives(connection, scan.scan_id, config.interval)
                assert remote.source_object_key.endswith(f"/{request.data_type}")
                result = await acquire(connection, http, remote, root)
                assert result.status == "completed"
                assert (root / result.relative_path).is_file()
        after = await plan(connection, config, root, verify_local=True)
        archives = cast(list[dict[str, object]], after["sampled_archives"])
        assert [item["request_index"] for item in archives] == list(range(1, len(config.requests)))
        assert all(item["state"] == "verified_reusable" for item in archives)


@pytest.mark.parametrize(
    ("exchange", "instrument_type", "category", "kinds"),
    [
        ("binance", "future", "inverse", ("trades",)),
        ("bybit", "perpetual", "linear", ("trades", "order_book_l2")),
    ],
)
@pytest.mark.asyncio
async def test_registered_market_into_existing_recipes(
    workspace: Path,
    exchange: str,
    instrument_type: Literal["future", "perpetual"],
    category: Literal["inverse", "linear"],
    kinds: tuple[str, ...],
) -> None:
    dsn = os.environ.get("TEST_DATABASE_KEY")
    if not dsn or os.environ.get("MARKETFORGE_TEST_DISPOSABLE_REGISTRY") != "1":
        pytest.skip("Requires an explicitly disposable migrated testing registry.")

    base = f"MF{uuid4().hex[:12].upper()}"
    symbol = f"{base}USD_270326" if exchange == "binance" else f"{base}USDT"
    async with registry_connection(dsn) as connection:
        await add(
            connection,
            Instrument(
                exchange=exchange,
                instrument_type=instrument_type,
                market_category=category,
                symbol=symbol,
                base_asset=base,
                quote_asset="USD" if exchange == "binance" else "USDT",
                settlement_asset=base if exchange == "binance" else "USDT",
            ),
        )
        await connection.execute(
            """INSERT INTO catalog.instrument_metrics
                   (instrument_id, turnover_24h, turnover_denomination, measured_at)
               SELECT i.id, %s, 'USD', now() FROM catalog.instruments i
               JOIN catalog.exchanges e ON e.id=i.exchange_id
               WHERE e.code=%s AND i.symbol=%s""",
            (Decimal("370930.00000001"), exchange, symbol),
        )

    context = build_context(project_root=workspace, testing=True)
    app = ForgeApp(context)
    app.initial_area, app.initial_view = "Markets", "Instruments"
    paths = context.paths
    conversion = (
        '\n[normalize]\nranking_policy = "custom"\n[merge]\ntimeline_ordering = "causal"\n'
        '[convert]\ntarget_quote = "EUR"\nmax_staleness_ns = 200\n'
        'reference_stream_ids = ["reference.trades"]\n[convert.resources]\nworkers = 2\n'
    )
    async with app.run_test(size=(100, 32)) as pilot:
        app.query_one("#filter", Input).value = symbol
        async with asyncio.timeout(5):
            while not app.database_id or not any(
                row.name == symbol for row in app.state.rows.values()
            ):
                await pilot.pause(0.05)
        for index, family in enumerate(("acquisition", "pipeline")):
            directory = paths.workflow_dir(family)
            directory.mkdir(parents=True)
            path = directory / "existing.toml"
            original = ACQUISITION_TEMPLATE + (conversion if family == "pipeline" else "")
            path.write_text(original)
            await pilot.press("f2")
            assert isinstance(app.screen, ActionMenu)
            assert app.screen.heading == f"{symbol} · {exchange.title()}"
            await pilot.click(f"#menu-{index}")
            await pilot.pause()
            assert isinstance(app.screen, MarketRecipeDialog)
            selection = cast(Select[str], app.screen.query_one("#market-recipe-target", Select))
            selection.value = "existing"
            await pilot.pause()
            assert app.screen.query_one("#order_book_l2", Checkbox).disabled == (len(kinds) == 1)
            for kind in kinds:
                checkbox = app.screen.query_one(f"#{kind}", Checkbox)
                checkbox.focus()
                await pilot.pause()
                assert await pilot.click(checkbox)
            await pilot.click("#market-recipe-save")
            await pilot.pause()
            assert isinstance(app.screen, Inspection)
            assert app.screen.heading == "Recipe updated"
            await pilot.press("escape")
            app.query_one("#results", Results).focus()
            assert app.controller.current is None
            saved = (
                load_recipe(path.read_bytes()) if family == "pipeline" else load_acquisition(path)
            )
            expected = (
                load_recipe(original.encode())
                if family == "pipeline"
                else (AcquisitionConfig.model_validate(tomllib.loads(original)))
            )
            assert saved.model_dump(exclude={"requests"}) == expected.model_dump(
                exclude={"requests"}
            )
            assert saved.requests[:1] == expected.requests
            assert [
                (
                    request.exchange,
                    request.instrument_type,
                    request.market_category,
                    request.scope.kind,
                    request.scope.identifier,
                    request.data_type,
                )
                for request in saved.requests[1:]
            ] == [
                (exchange, instrument_type, category, "instrument", symbol, kind) for kind in kinds
            ]
            if isinstance(saved, AcquisitionConfig) and len(kinds) == 2:
                await verify_acquisition(dsn, saved, paths.raw_dir)
