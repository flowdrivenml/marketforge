"""Pilot coverage for the shared existing-recipe instrument dialog."""

from pathlib import Path

import pytest
from textual.widgets import Button, Checkbox, Static

from forge.historical.archives.config import load_acquisition
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.tests.unit.tui.test_interaction import context
from forge.tui.actions import Values
from forge.tui.app import ForgeApp
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.market_recipes import MarketRecipeDialog
from forge.tui.workflows import ApplicationContext, Edit, Progress

REGISTRY = "00000000-0000-4000-8000-000000000001"


def instrument(symbol: str, type_: str = "perpetual", exchange: str = "bybit") -> dict[str, object]:
    return {
        "id": f"{exchange}-{type_}-{symbol}",
        "reference": f"{exchange}.linear.{type_}.{symbol}",
        "symbol": symbol,
        "exchange": exchange,
        "instrument_type": type_,
        "market_category": "linear",
    }


@pytest.mark.parametrize(
    ("size", "kinds", "basket", "initial_symbol"),
    [
        ((80, 24), ("trades",), (), "ETHUSDT"),
        ((100, 30), ("order_book_l2",), (), "ETHUSDT"),
        ((120, 32), ("trades", "order_book_l2"), (), "ETHUSDT"),
        ((160, 48), ("trades", "order_book_l2"), (), "BTCUSDT"),
        ((80, 24), ("trades", "order_book_l2"), ("perpetual", "future"), "ETHUSDT"),
        ((100, 30), ("trades",), ("perpetual", "binance"), "ETHUSDT"),
    ],
)
@pytest.mark.asyncio
async def test_market_data_types(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    size: tuple[int, int],
    kinds: tuple[str, ...],
    basket: tuple[str, ...],
    initial_symbol: str,
) -> None:
    ctx = context(tmp_path)
    directory = ctx.paths.workflow_dir("acquisition")
    directory.mkdir(parents=True)
    path = directory / "historical_crypto.toml"
    path.write_text(ACQUISITION_TEMPLATE)
    before = load_acquisition(path)
    row_values = {
        "perpetual": instrument(initial_symbol),
        "future": instrument(initial_symbol, "future"),
        "binance": instrument(initial_symbol, exchange="binance"),
    }

    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        assert key == "markets list"
        return {"instruments": list(row_values.values())}

    async def verify_registry(_root: Path) -> str:
        return REGISTRY

    monkeypatch.setattr("forge.tui.app.selected_registry_id", verify_registry)
    app = ForgeApp(ctx, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    app.database_id = REGISTRY
    app.initial_area, app.initial_view = "Markets", "Instruments"
    async with app.run_test(size=size) as pilot:
        await pilot.pause(0.3)
        row = app.state.rows[str(row_values["perpetual"]["reference"])]
        for item in basket:
            reference = str(row_values[item]["reference"])
            app.state.basket[reference] = app.state.rows[reference]
        app.row_menu(row)
        assert isinstance(app.screen, ActionMenu)
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, MarketRecipeDialog)
        dialog = app.screen
        assert len(dialog.instruments) == max(1, len(basket))
        assert dialog.names == ("historical_crypto",)
        trade = dialog.query_one("#trades", Checkbox)
        depth = dialog.query_one("#order_book_l2", Checkbox)
        assert not trade.value and not depth.value
        assert depth.disabled == ("binance" in basket)
        if depth.disabled:
            assert "Binance supports historical trades only." in " ".join(
                str(widget.content) for widget in dialog.query(".market-warning").results(Static)
            )
        await pilot.press("h", "i", "s", "enter")
        await pilot.press("enter", "down", "enter")
        await pilot.pause()
        assert dialog.destination and dialog.destination.path == path
        save = dialog.query_one("#market-recipe-save", Button)
        assert save.disabled
        assert "Select at least one" in str(
            dialog.query_one("#market-recipe-preview", Static).content
        )
        for kind in kinds:
            checkbox = dialog.query_one(f"#{kind}", Checkbox)
            checkbox.focus()
            await pilot.pause()
            if kind == "trades":
                await pilot.press("space")
            else:
                assert await pilot.click(checkbox)
        await pilot.pause()
        expected = [
            (str(row_values[item]["exchange"]), str(row_values[item]["instrument_type"]), kind)
            for item in basket or ("perpetual",)
            for kind in kinds
            if (initial_symbol, item, kind) != ("BTCUSDT", "perpetual", "trades")
        ]
        preview = str(dialog.query_one("#market-recipe-preview", Static).content)
        if initial_symbol == "BTCUSDT":
            assert "= BTCUSDT · Trades" in preview
            assert "Already included" in preview
        count = len(expected)
        assert f"{count} {'request' if count == 1 else 'requests'} to add" in preview
        assert save.region.bottom <= size[1]
        assert await pilot.click(save)
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "Recipe updated"
        assert app.controller.current is None

    after = load_acquisition(path)
    assert after.model_dump(exclude={"requests"}) == before.model_dump(exclude={"requests"})
    assert after.requests[:1] == before.requests
    assert [(r.exchange, r.instrument_type, r.data_type) for r in after.requests[1:]] == expected
    assert all(
        (r.market_category, r.scope.kind, r.scope.identifier)
        == ("linear", "instrument", initial_symbol)
        for r in after.requests[1:]
    )
