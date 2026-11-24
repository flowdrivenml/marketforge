"""Bounded Rich presentation for durable pipeline snapshots."""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from forge.historical.pipelines.progress import PipelineSnapshot
from forge.historical.pipelines.service import PipelineRun
from forge.historical.pipelines.terminal import STAGE_LABELS
from forge.shared.terminal import palette

COLORS = {
    "complete": palette.GREEN,
    "prepared": palette.GREEN,
    "running": palette.CYAN,
    "failed": palette.RED,
    "blocked": palette.YELLOW,
    "interrupted": palette.YELLOW,
    "interrupting": palette.YELLOW,
    "stopped": palette.YELLOW,
    "pending": palette.FG,
}


def elapsed(start: str | None, end: str | None = None) -> str:
    if start is None:
        return "—"
    stop = datetime.fromisoformat(end) if end else datetime.now(UTC)
    seconds = max(0, int((stop - datetime.fromisoformat(start)).total_seconds()))
    return f"{seconds // 3600:02}:{seconds // 60 % 60:02}:{seconds % 60:02}"


def render(run: PipelineSnapshot, width: int, height: int) -> RenderableType:
    def utc(value: datetime) -> str:
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%SZ")

    header = Text("MarketForge / Pipeline", style=f"bold {palette.BLUE}")
    header.append(f"  {run.state.upper()}", style=COLORS.get(run.state, palette.FG))
    name = Text(f"{run.name} · {run.run_id} · {run.environment}", overflow="ellipsis", no_wrap=True)
    duration = elapsed(run.started_at, run.finished_at)
    window = Text(f"{utc(run.interval[0])} – {utc(run.interval[1])} · {duration}", overflow="fold")
    stages = Table(box=None, expand=True, padding=(0, 1), header_style=palette.BLUE)
    stages.add_column("Stage", no_wrap=True)
    stages.add_column("State")
    stages.add_column("Phase")
    stages.add_column("Elapsed", no_wrap=True)
    if width >= 72:
        stages.add_column("Output")
    if width >= 88:
        stages.add_column("Terminal")
    for stage in run.stages:
        cells = [
            Text(STAGE_LABELS[stage.name]),
            Text(stage.state, style=COLORS.get(stage.state, palette.FG)),
            Text(stage.phase.replace("_", " "), overflow="ellipsis"),
            Text(elapsed(stage.started_at, stage.finished_at)),
        ]
        if width >= 72:
            cells.append(Text(stage.summary or "—", overflow="ellipsis"))
        if width >= 88:
            cells.append(Text(stage.terminal or "—"))
        stages.add_row(*cells)

    details: list[Text] = []
    active = next((stage for stage in run.stages if stage.name == run.active_stage), None)
    if active and active.name == "acquire" and run.acquisition is not None:
        acquisition = run.acquisition
        suffix = "· discovery in progress" if acquisition.provisional else "discovered"
        details.extend(
            (
                Text(
                    f"{acquisition.requests.finished} / {acquisition.total_requests} requests finished"
                ),
                Text(f"{acquisition.known_total} files {suffix}"),
                Text(
                    f"{acquisition.files.completed} downloaded · {acquisition.files.reused} reused"
                    f" · {acquisition.files.failed} failed"
                ),
            )
        )
        if height >= 20 and acquisition.current:
            details.append(Text(acquisition.current, overflow="ellipsis", no_wrap=True))
            if acquisition.activity:
                details.append(Text(acquisition.activity, overflow="ellipsis", no_wrap=True))
        if acquisition.failures:
            details.append(
                Text(
                    acquisition.failures[-1],
                    style=palette.YELLOW,
                    overflow="ellipsis",
                    no_wrap=True,
                )
            )
    elif active:
        details.append(Text(f"{STAGE_LABELS[active.name]} · {active.phase.replace('_', ' ')}"))
        if active.terminal:
            details.append(Text(f"Separate {active.terminal} window"))
        if active.verified_jobs is not None:
            done, total = active.verified_jobs
            details.append(Text(f"{done} / {total} jobs verified"))
        elif active.summary:
            details.append(Text(active.summary))
    if run.error:
        details.insert(0, Text(run.error, style=palette.RED, overflow="ellipsis", no_wrap=True))
    if not details:
        details.append(Text("Opening testing registry and freezing settings"))

    footer = Text(
        f"Ctrl+C interrupt · Run {run.run_id[:8]} · report.json",
        style=palette.FG,
        overflow="ellipsis",
        no_wrap=True,
    )
    if height < 15 or width < 46:
        return Group(header, name, *details[:2], footer)
    overview = Panel(stages, title="Stages", title_align="left", border_style=palette.MUTED)
    current = Panel(
        Group(*details), title="Active stage", title_align="left", border_style=palette.MUTED
    )
    if width >= 110:
        side = Table.grid(expand=True, padding=(0, 1))
        side.add_column(ratio=3)
        side.add_column(ratio=2)
        side.add_row(overview, current)
        panels: list[RenderableType] = [side]
    else:
        panels = [overview, current]
    if height >= 23 and run.recent:
        activity = Group(
            *(
                Text(f"{item.stage} · {item.phase.replace('_', ' ')} · {item.state}")
                for item in run.recent[-min(5, height - 20) :]
            )
        )
        panels.append(
            Panel(activity, title="Recent activity", title_align="left", border_style=palette.MUTED)
        )
    return Group(header, name, window, *panels, footer)


async def present(run: PipelineRun, console: Console, *, tui: bool, stop: bool) -> dict[str, Any]:
    if not tui or not console.is_terminal:
        return await run.run(stop=stop)
    live = Live(
        console=console,
        auto_refresh=False,
        transient=True,
        redirect_stdout=False,
        redirect_stderr=False,
    )

    async def refresh() -> None:
        try:
            live.start()
            while True:
                size = console.size
                live.update(render(run.snapshot(), size.width, size.height), refresh=True)
                await asyncio.sleep(0.25)
        except Exception:
            with suppress(Exception):
                console.print("Pipeline dashboard unavailable. Execution continues in plain mode.")
        finally:
            with suppress(Exception):
                live.stop()

    task = asyncio.create_task(refresh())
    try:
        return await run.run(stop=stop)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
