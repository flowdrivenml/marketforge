from __future__ import annotations

from decimal import Decimal

from rich import box
from rich.table import Table

from marketforge.cli.ui.console import console
from marketforge.market_metrics.models import InstrumentMarketView


def print_instruments(
    instruments: list[InstrumentMarketView],
    *,
    details: bool = False,
) -> None:
    """Render catalog instruments and current market metrics."""

    console.print()

    if not instruments:
        console.print(
            "No instruments found.",
            style="muted",
        )
        return

    if details:
        _print_detailed(instruments)
    else:
        _print_compact(instruments)

    console.print()
    console.print(
        f"{len(instruments):,} instrument(s)",
        style="muted",
    )


def _print_compact(
    instruments: list[InstrumentMarketView],
) -> None:
    table = Table(
        title="MarketForge Instruments",
        title_style="heading",
        box=box.SIMPLE_HEAVY,
        header_style="bold",
    )

    table.add_column(
        "Exchange",
        style="cyan",
    )

    table.add_column(
        "Symbol",
        style="bold",
    )

    table.add_column("Type")

    table.add_column("Market")

    table.add_column(
        "Price",
        justify="right",
    )

    table.add_column(
        "Turnover 24h",
        justify="right",
    )

    table.add_column(
        "OI",
        justify="right",
    )

    for item in instruments:
        table.add_row(
            item.exchange,
            item.symbol,
            item.instrument_type,
            item.market_category,
            _number(item.last_price),
            _metric(
                item.turnover_24h,
                item.turnover_denomination,
            ),
            _number(item.open_interest_value),
        )

    console.print(table)


def _print_detailed(
    instruments: list[InstrumentMarketView],
) -> None:
    table = Table(
        title="MarketForge Instruments — Detailed",
        title_style="heading",
        box=box.SIMPLE_HEAVY,
        header_style="bold",
    )

    table.add_column("Exchange")
    table.add_column("Symbol")
    table.add_column("Type")
    table.add_column("Market")

    table.add_column(
        "Last",
        justify="right",
    )

    table.add_column(
        "High 24h",
        justify="right",
    )

    table.add_column(
        "Low 24h",
        justify="right",
    )

    table.add_column(
        "Change",
        justify="right",
    )

    table.add_column(
        "Turnover",
        justify="right",
    )

    table.add_column(
        "Open Interest",
        justify="right",
    )

    table.add_column(
        "Funding",
        justify="right",
    )

    for item in instruments:
        table.add_row(
            item.exchange,
            item.symbol,
            item.instrument_type,
            item.market_category,
            _number(item.last_price),
            _number(item.high_24h),
            _number(item.low_24h),
            _percentage(item.price_change_pct_24h),
            _metric(
                item.turnover_24h,
                item.turnover_denomination,
            ),
            _number(item.open_interest_value),
            _percentage(item.funding_rate),
        )

    console.print(table)


def _number(
    value: Decimal | None,
) -> str:
    if value is None:
        return "—"

    return _compact_decimal(value)


def _metric(
    value: Decimal | None,
    denomination: str | None,
) -> str:
    if value is None:
        return "—"

    rendered = _compact_decimal(value)

    if denomination is None:
        return rendered

    return f"{rendered} {denomination}"


def _percentage(
    value: Decimal | None,
) -> str:
    if value is None:
        return "—"

    return f"{value * 100:.3f}%"


def _compact_decimal(
    value: Decimal,
) -> str:
    absolute = abs(value)

    thresholds = (
        (
            Decimal("1000000000000"),
            "T",
        ),
        (
            Decimal("1000000000"),
            "B",
        ),
        (
            Decimal("1000000"),
            "M",
        ),
        (
            Decimal("1000"),
            "K",
        ),
    )

    for threshold, suffix in thresholds:
        if absolute >= threshold:
            scaled = value / threshold
            return f"{scaled:.2f}{suffix}"

    if absolute >= 1:
        return f"{value:.2f}"

    return f"{value:.6f}"
