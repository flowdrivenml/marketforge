"""Rich-free projections of acquisition configuration and retained operation results."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from forge.historical.archives.config import AcquisitionConfig, load_acquisition
from forge.orchestration.registry import rows, status_of
from forge.orchestration.run_summary import mapping, when
from forge.shared.config.library import ConfigurationNameError, resolve_config_file
from forge.shared.config.loader import ConfigurationLoadError

if TYPE_CHECKING:
    from forge.historical.archives.operation import OperationReport

DISPLAY_LIMIT = 20


def diagnostic(value: object) -> str:
    message = str(value or "")
    if re.search(
        r"https?://|(?:signature|token|password|credential|authorization)\s*[=:]", message, re.I
    ):
        return "Sensitive upstream details withheld. Inspect retained diagnostics locally."
    return " ".join(message.split())[:300]


def count(value: object) -> str:
    return f"{value:,}" if type(value) is int and value >= 0 else "Unknown"


@dataclass(frozen=True, slots=True)
class RequestResult:
    exchange: str
    scope: str
    market: str
    data: str
    status: str
    discovery: str
    archives: str
    cache: str
    error: str
    nominal: str


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    run_id: str
    action: str
    recipe: str
    environment: str
    period: str
    status: str
    requests: tuple[RequestResult, ...]
    counts: tuple[tuple[str, str], ...]
    coverage: str
    report: str
    warning: str
    registered_archives: int | None

    @property
    def displayed_requests(self) -> tuple[RequestResult, ...]:
        ordered = sorted(self.requests, key=lambda row: (not row.error, row.status == "complete"))
        return tuple(ordered[:DISPLAY_LIMIT])

    @property
    def issues(self) -> tuple[RequestResult, ...]:
        return tuple(row for row in self.requests if row.error)

    @classmethod
    def from_report(
        cls, report: OperationReport, *, download: bool, recipe: str
    ) -> AcquisitionResult:
        requests: list[RequestResult] = []
        for item in report["requests"]:
            request = mapping(item.get("request"))
            scope = mapping(request.get("scope"))
            kind = str(scope.get("kind") or "")
            identifier = str(scope.get("identifier") or "Unknown")
            market = str(request.get("market_category") or "Unknown")
            instrument = str(request.get("instrument_type") or "Unknown")
            if instrument != market:
                market = f"{'Perp' if instrument == 'perpetual' else instrument.title()}/{market.title()}"
            else:
                market = market.title()
            discovery = str(item.get("discovery") or "unknown")
            if discovery == "failed":
                observed = item.get("archives_found")
                saved = mapping(item.get("candidate_diagnostics")).get("registered")
                if download and type(item.get("scan_id")) is int and type(observed) is int:
                    discovery = "archives" if observed else "empty"
                elif type(saved) is int and saved > 0:
                    discovery = "partial"
            requests.append(
                RequestResult(
                    str(request.get("exchange") or "Unknown").title(),
                    identifier
                    if kind in {"", "instrument"}
                    else f"{identifier} ({kind.replace('_', ' ')})",
                    market,
                    {"trades": "Trades", "order_book_l2": "Depth"}.get(
                        str(request.get("data_type")), "Unknown"
                    ),
                    str(item.get("status") or "unknown"),
                    discovery,
                    count(item.get("archives_found")),
                    {"reused": "Cached", "refreshed": "Refreshed"}.get(
                        str(item.get("cache")), "Unknown"
                    ),
                    diagnostic(item.get("error")),
                    str(item.get("archive_interval_coverage") or ""),
                )
            )
        interval = mapping(report.get("interval"))
        start, end = when(interval.get("start")), when(interval.get("end"))
        period = format_period(start, end) if start and end else "Unknown"
        action = "download" if download else "map"
        registered_archives = None
        if download:
            selected = rows(mapping(report.get("selection")).get("archives"))
            ids = {
                row["local_archive_id"]
                for row in selected
                if type(row.get("local_archive_id")) is int
            }
            if "archives" in report["selection"]:
                registered_archives = len(ids)
            bytes_written = report.get("bytes_written")
            counts = (
                ("Completed downloads", count(report["file_counts"].get("completed"))),
                ("Reused files", count(report["file_counts"].get("reused"))),
                ("Failed downloads", count(report["file_counts"].get("failed"))),
                ("Cancelled downloads", count(report["file_counts"].get("cancelled"))),
                (
                    "Bytes in completed downloads",
                    count(bytes_written),
                ),
            )
        else:
            stats = report["request_counts"]
            counts = (
                ("Successful requests", f"{count(stats.get('successful'))} / {len(requests):,}"),
                ("Archives reported", count(stats.get("archives"))),
                ("Cached discoveries", count(stats.get("cached"))),
                ("Empty discoveries", count(stats.get("empty"))),
                ("Failed requests", count(stats.get("failed"))),
                ("Unsupported requests", count(stats.get("unsupported"))),
                ("Cancelled requests", count(stats.get("cancelled"))),
            )
        nominal = [request.nominal for request in requests]
        coverage = (
            "Some recorded archive intervals do not span the requested period."
            if any(value.startswith("incomplete:") for value in nominal)
            else "Recorded archive intervals nominally span all requests."
            if nominal and all(value.startswith("nominal intervals span") for value in nominal)
            else "Nominal interval evidence is incomplete."
        )
        return cls(
            report["run_id"],
            action,
            str(report.get("recipe") or recipe),
            report.get("environment", "unknown"),
            period,
            status_of(report, action),
            tuple(requests),
            counts,
            coverage,
            report["report"],
            report.get("execution_registry_warning", ""),
            registered_archives,
        )


def format_period(start: datetime, end: datetime, *, compact: bool = False) -> str:
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if (
        compact
        and start == start.replace(hour=0, minute=0, second=0, microsecond=0)
        and end == end.replace(hour=0, minute=0, second=0, microsecond=0)
    ):
        if start.date().replace(day=1) == end.date().replace(day=1):
            return f"{start.day}–{end.day} {end:%b %Y}"
        return f"{start:%d %b %Y} → {end:%d %b %Y}"

    def timestamp(value: datetime) -> str:
        detail = value.strftime("%H:%M")
        if value.second or value.microsecond:
            detail += value.strftime(":%S")
        if value.microsecond:
            detail += f".{value.microsecond:06d}".rstrip("0")
        return f"{value:%d %b %Y} {detail}"

    return f"{timestamp(start)} → {timestamp(end)}" + ("" if compact else " UTC")


@dataclass(frozen=True, slots=True)
class ConfigurationResult:
    name: str
    source: Path
    request_count: int | None = None
    period: str = ""
    error: str = ""

    @classmethod
    def from_config(
        cls,
        source: Path,
        config: AcquisitionConfig,
        *,
        compact: bool = False,
        name: str | None = None,
    ) -> ConfigurationResult:
        return cls(
            name or source.stem,
            source,
            len(config.requests),
            format_period(config.interval.start, config.interval.end, compact=compact),
        )


def configuration_inventory(
    directory: Path, names: tuple[str, ...]
) -> tuple[ConfigurationResult, ...]:
    inventory: list[ConfigurationResult] = []
    for name in names:
        path = directory / f"{name}.toml"
        try:
            source = resolve_config_file(directory, name)
            inventory.append(
                ConfigurationResult.from_config(
                    source, load_acquisition(source), compact=True, name=name
                )
            )
        except (ConfigurationLoadError, ConfigurationNameError) as error:
            inventory.append(ConfigurationResult(name, path, error=str(error)))
    return tuple(inventory)
