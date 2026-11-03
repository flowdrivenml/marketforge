from __future__ import annotations

from rich import box
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from marketforge.cli.ui.console import console
from marketforge.models import AcquisitionPlan, BaseCoin, DownloadAction


def print_plan(
    plan: AcquisitionPlan,
) -> None:
    """Render an acquisition plan."""

    request = plan.request
    target = request.target

    if isinstance(target, BaseCoin):
        if plan.downloads:
            instrument_type = plan.downloads[0].remote_file.instrument_type.value
        else:
            instrument_type = "option"

    else:
        instrument_type = target.instrument_type.value

    _print_header(
        exchange=target.exchange.value,
        symbol=target.symbol,
        instrument_type=instrument_type,
        market_category=target.market_category.value,
        data_type=request.data_type.value,
        start=request.start.isoformat(),
        end=request.end.isoformat(),
    )

    _print_summary(plan)

    if not plan.downloads:
        console.print()
        console.print(
            "No remote archives found.",
            style="muted",
        )
        return

    console.print()

    _print_archives(plan)


def _print_header(
    *,
    exchange: str,
    symbol: str,
    instrument_type: str,
    market_category: str,
    data_type: str,
    start: str,
    end: str,
) -> None:
    title = Text()

    title.append(
        exchange.upper(),
        style="bold cyan",
    )

    title.append(
        "  ·  ",
        style="muted",
    )

    title.append(
        symbol,
        style="bold white",
    )

    title.append(
        "  ·  ",
        style="muted",
    )

    title.append(
        instrument_type.upper(),
        style="bold",
    )

    title.append(
        "  ·  ",
        style="muted",
    )

    title.append(
        market_category.upper(),
        style="bold",
    )

    title.append(
        "  ·  ",
        style="muted",
    )

    title.append(
        data_type.upper(),
        style="bold magenta",
    )

    body = Text()

    body.append(
        "Range  ",
        style="label",
    )

    body.append(
        start,
        style="value",
    )

    body.append(
        "  →  ",
        style="muted",
    )

    body.append(
        end,
        style="value",
    )

    body.append(
        "  [exclusive]",
        style="muted",
    )

    console.print()

    console.print(
        Panel(
            body,
            title=title,
            title_align="left",
            border_style="cyan",
            padding=(1, 2),
        )
    )


def _print_summary(
    plan: AcquisitionPlan,
) -> None:
    table = Table(
        box=None,
        show_header=False,
        pad_edge=False,
        padding=(0, 2),
    )

    table.add_column(
        style="label",
    )

    table.add_column(
        justify="right",
        style="value",
    )

    table.add_row(
        "Archives",
        str(plan.file_count),
    )

    table.add_row(
        "Download",
        str(plan.download_count),
    )

    table.add_row(
        "Reuse",
        str(plan.reuse_count),
    )

    if plan.known_size_bytes:
        known_size = _human_size(plan.known_size_bytes)
    else:
        known_size = "—"

    table.add_row(
        "Known size",
        known_size,
    )

    table.add_row(
        "Unknown size",
        str(plan.unknown_size_count),
    )

    console.print()

    console.print(
        Panel(
            table,
            title="Acquisition Plan",
            title_align="left",
            border_style="dim",
            padding=(1, 2),
        )
    )


def _print_archives(
    plan: AcquisitionPlan,
) -> None:
    table = Table(
        title="Archives",
        title_style="heading",
        box=box.SIMPLE_HEAVY,
        header_style="bold",
        show_lines=False,
    )

    table.add_column(
        "Action",
        width=10,
    )

    table.add_column(
        "Archive",
        overflow="fold",
    )

    table.add_column(
        "Size",
        justify="right",
        no_wrap=True,
    )

    table.add_column(
        "Destination",
        style="path",
        overflow="fold",
    )

    for item in plan.downloads:
        remote = item.remote_file

        if item.action == DownloadAction.REUSE:
            action = Text(
                "REUSE",
                style="reuse",
            )
        else:
            action = Text(
                "DOWNLOAD",
                style="download",
            )

        size = (
            _human_size(remote.size_bytes)
            if remote.size_bytes is not None
            else "unknown"
        )

        table.add_row(
            action,
            remote.filename,
            size,
            str(item.destination),
        )

    console.print(table)


def _human_size(
    size: int,
) -> str:
    """Format a byte count for terminal display."""

    value = float(size)

    units = (
        "B",
        "KiB",
        "MiB",
        "GiB",
        "TiB",
    )

    for unit in units:
        if value < 1024.0:
            if unit == "B":
                return f"{int(value)} {unit}"

            return f"{value:.1f} {unit}"

        value /= 1024.0

    return f"{value:.1f} PiB"
