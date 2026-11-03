from __future__ import annotations

from rich import box
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from marketforge.cli.ui.console import console
from marketforge.models import DownloadResult, DownloadStatus


def print_results(
    results: list[DownloadResult],
) -> None:
    """Render completed acquisition results."""

    downloaded = sum(result.status == DownloadStatus.DOWNLOADED for result in results)

    reused = sum(result.status == DownloadStatus.REUSED for result in results)

    table = Table(
        box=box.SIMPLE_HEAVY,
        header_style="bold",
    )

    table.add_column(
        "Status",
        width=12,
    )

    table.add_column(
        "Archive",
    )

    table.add_column(
        "Size",
        justify="right",
    )

    table.add_column(
        "SHA-256",
        overflow="fold",
    )

    for result in results:
        if result.status == DownloadStatus.DOWNLOADED:
            status = Text(
                "DOWNLOADED",
                style="success",
            )
        else:
            status = Text(
                "REUSED",
                style="reuse",
            )

        table.add_row(
            status,
            result.remote_file.filename,
            _human_size(result.bytes_written),
            result.sha256,
        )

    console.print()
    console.print(table)

    summary = Text()

    summary.append(
        "Downloaded  ",
        style="label",
    )
    summary.append(
        str(downloaded),
        style="success",
    )

    summary.append(
        "    Reused  ",
        style="label",
    )
    summary.append(
        str(reused),
        style="reuse",
    )

    summary.append(
        "    Files  ",
        style="label",
    )
    summary.append(
        str(len(results)),
        style="value",
    )

    console.print()

    console.print(
        Panel(
            summary,
            title="Complete",
            title_align="left",
            border_style="green",
            padding=(1, 2),
        )
    )


def _human_size(
    size: int,
) -> str:
    value = float(size)

    for unit in (
        "B",
        "KiB",
        "MiB",
        "GiB",
        "TiB",
    ):
        if value < 1024.0:
            if unit == "B":
                return f"{int(value)} {unit}"

            return f"{value:.1f} {unit}"

        value /= 1024.0

    return f"{value:.1f} PiB"
