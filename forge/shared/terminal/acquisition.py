"""One owner for bounded acquisition progress and permanent summaries."""

from time import monotonic
from types import TracebackType
from typing import Self

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

from forge.historical.archives.config import AcquisitionInterval
from forge.historical.archives.progress import ProgressSnapshot


def _short(value: str, width: int) -> str:
    return value if len(value) <= width else value[: max(0, width - 1)] + "…"


class AcquisitionDisplay:
    """Consume snapshots; rendering failures disable the view, never acquisition.

    Live refresh runs at four frames/second. Redirected output emits only terminal
    request/file transitions, never unchanged counters or per-chunk updates.
    """

    def __init__(
        self,
        console: Console,
        *,
        download: bool,
        name: str,
        interval: AcquisitionInterval,
        silent: bool = False,
    ) -> None:
        self.console = console
        self.download = download
        self.name = name
        self.interval = interval
        self.silent = silent
        self.started = monotonic()
        self.live: Live | None = None
        self.disabled = False
        self.snapshot: ProgressSnapshot | None = None

    def __enter__(self) -> Self:
        if not self.silent and self.console.file.isatty():
            try:
                self.live = Live(
                    _CurrentView(self),
                    console=self.console,
                    refresh_per_second=4,
                    transient=True,
                )
                self.live.start()
            except Exception:
                self._disable()
        return self

    def _disable(self) -> None:
        self.disabled = True
        if self.live is not None:
            try:
                self.live.stop()
            except Exception:
                pass
            self.live = None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.live is not None:
            try:
                self.live.stop()
            except Exception:
                self.disabled = True
            finally:
                self.live = None

    def update(self, snapshot: ProgressSnapshot) -> None:
        self.snapshot = snapshot
        if self.silent or self.disabled:
            return
        try:
            if self.live is not None:
                self.live.update(_CurrentView(self))
            elif snapshot.transition:
                # Explicitly disable styling even when color='always' is configured.
                self.console.file.write(_short(snapshot.transition, 240) + "\n")
                self.console.file.flush()
        except Exception:
            self._disable()

    def render(self, snapshot: ProgressSnapshot) -> Panel:
        width = max(16, self.console.width - 6)
        rows: list[Text] = [
            Text(_short(f"Configuration: {self.name}", width), style="mf.secondary"),
            Text(
                _short(
                    f"UTC [{self.interval.start.isoformat()}, {self.interval.end.isoformat()})",
                    width,
                ),
                style="mf.secondary",
            ),
        ]
        if self.download:
            files = snapshot.files
            total = max(snapshot.known_total, files.queued)
            provisional = (
                " · total provisional (discovery ongoing)"
                if snapshot.provisional
                else " · total final"
            )
            rows.append(
                Text(
                    f"Files resolved {files.resolved}/{total}{provisional}",
                    style="mf.heading",
                )
            )
            rows.append(
                Text(
                    f"Downloaded {files.completed} · reused {files.reused} · failed {files.failed} · cancelled {files.cancelled}"
                )
            )
            rows.append(Text(f"Remaining {max(0, total - files.resolved)}", style="mf.secondary"))
            for _, identity, activity in snapshot.active:
                rows.append(Text(_short(f"{activity}: {identity}", width), style="mf.identifier"))
        else:
            counts = snapshot.requests
            rows.append(
                Text(
                    f"Requests finished {counts.finished}/{snapshot.total_requests} · remaining {snapshot.total_requests - counts.finished}",
                    style="mf.heading",
                )
            )
            rows.append(
                Text(
                    f"Successful {counts.successful} (cached {counts.cached}, empty {counts.empty}; subsets)"
                )
            )
            rows.append(
                Text(
                    f"Unsupported {counts.unsupported} · failed {counts.failed} · cancelled {counts.cancelled}"
                )
            )
            rows.append(Text(f"Archives discovered {counts.archives}"))
        for _, identity, activity in snapshot.active_requests:
            rows.append(Text(_short(identity, width), style="mf.identifier"))
            rows.append(Text(_short(activity, width), style="mf.secondary"))
        rows.append(
            Text(
                f"Elapsed {int(monotonic() - self.started)}s · Ctrl+C to cancel",
                style="mf.secondary",
            )
        )
        for result in snapshot.recent[-(3 if width < 70 else 6) :]:
            style = (
                "mf.failure"
                if result.startswith("failed")
                else "mf.warning"
                if result.startswith(("unsupported", "cancelled", "empty"))
                else "mf.success"
            )
            rows.append(Text(_short(result, width), style=style))
        for failure in snapshot.failures[-2:]:
            if failure not in snapshot.recent:
                rows.append(Text(_short(failure, width), style="mf.failure"))
        return Panel(
            Group(*rows),
            title=Text(
                "MarketForge · " + ("Download" if self.download else "Mapping"), style="mf.heading"
            ),
            border_style="mf.secondary",
            padding=(0, 1),
        )

    def summary(self, console: Console, *, status: str, report: str) -> None:
        """Permanent operation-specific summary after the live display has stopped."""
        snapshot = self.snapshot
        if snapshot is None:
            return
        label = "completed with failures" if status == "failed" else status
        lines = [f"MarketForge · {'Download' if self.download else 'Mapping'} {label}"]
        if self.download:
            f = snapshot.files
            lines.append(
                f"Downloaded {f.completed} · reused {f.reused} · failed {f.failed} · cancelled {f.cancelled}"
            )
        else:
            r = snapshot.requests
            lines.append(
                f"Requests {r.finished}/{snapshot.total_requests}: successful {r.successful} (cached {r.cached}, empty {r.empty}; subsets), unsupported {r.unsupported}, failed {r.failed}, cancelled {r.cancelled}; archives {r.archives}"
            )
        if not self.download:
            lines.append(
                "Discovery success does not establish complete market-data coverage; see interval observations in report."
            )
        lines.extend(snapshot.failures)
        lines.append(f"Report: {report}")
        # A final renderer failure also cannot change the recorded operation outcome.
        try:
            for line in lines:
                if console.file.isatty():
                    style = (
                        "mf.heading"
                        if line.startswith("MarketForge")
                        else "mf.failure"
                        if line.startswith("failed:")
                        else "mf.warning"
                        if line.startswith(("unsupported:", "cancelled:"))
                        else "mf.body"
                    )
                    console.print(line, style=style, overflow="fold")
                else:
                    console.file.write(line + "\n")
            console.file.flush()
        except Exception:
            self._disable()


class _CurrentView:
    """Rich evaluates this on each bounded refresh, even during silent waits."""

    def __init__(self, display: AcquisitionDisplay) -> None:
        self.display = display

    def __rich__(self) -> Panel | Text:
        if self.display.snapshot is None:
            return Text("Preparing acquisition…", style="mf.identifier")
        try:
            return self.display.render(self.display.snapshot)
        except Exception:
            return Text("Progress display unavailable; operation continues.")
