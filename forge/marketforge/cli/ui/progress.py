from __future__ import annotations

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from marketforge.cli.ui.console import console
from marketforge.models import PlannedDownload


class DownloadProgress:
    """Rich presentation adapter for MarketForge download progress."""

    def __init__(self) -> None:
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.fields[filename]}"),
            BarColumn(),
            TaskProgressColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            expand=True,
        )

        self._tasks: dict[
            PlannedDownload,
            int,
        ] = {}

    def __enter__(
        self,
    ) -> DownloadProgress:
        self._progress.start()
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self._progress.stop()

    def callback(
        self,
        planned: PlannedDownload,
        bytes_written: int,
        total_bytes: int | None,
    ) -> None:
        task_id = self._tasks.get(planned)

        if task_id is None:
            task_id = self._progress.add_task(
                "",
                filename=(planned.remote_file.filename),
                total=total_bytes,
            )

            self._tasks[planned] = task_id

        self._progress.update(
            task_id,
            completed=bytes_written,
            total=total_bytes,
        )
