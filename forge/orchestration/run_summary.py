"""Inventory fields derived from retained run evidence."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from forge.shared.errors import MarketForgeError


@dataclass(frozen=True)
class RunSummary:
    operation: str
    recipe: str
    state: str
    exchanges: tuple[str, ...]
    scope: tuple[str, ...]
    start: datetime | None
    end: datetime | None
    output: bool
    result: str


def mapping(value: object) -> Mapping[str, object]:
    return cast(Mapping[str, object], value) if isinstance(value, dict) else {}


def when(value: object) -> datetime | None:
    if value:
        try:
            date = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        return date.astimezone(UTC) if date.tzinfo is not None else None
    return None


def summarize(report: Mapping[str, object], directory: Path | None = None) -> RunSummary:
    requests = report.get("requests")
    if directory is not None and report.get("kind") == "historical_pipeline":
        path = directory / "resolved-plan.json"
        try:
            if path.is_file() and not path.is_symlink() and path.stat().st_size <= 256 * 1024:
                plan: object = json.loads(path.read_bytes())
                requests = mapping(mapping(plan).get("recipe")).get("requests")
        except (OSError, ValueError):
            pass
    entries = (
        [mapping(value) for value in cast(list[object], requests)]
        if isinstance(requests, list)
        else []
    )
    outputs = (*mapping(report.get("stages")).values(), report)
    state = str(report.get("state") or report.get("status") or "")
    registration = report.get("registration")
    registered = any(mapping(value).get("primary_file_ids") for value in outputs)
    if registration == "registered" or registered:
        result = "Registered files"
    elif registration == "failed":
        result = "Registration failed"
    elif report.get("publication"):
        result = "Committed output"
    else:
        result = {
            "valid": "Validation report",
            "validated": "Validation report",
            "stopped": "Prepared jobs",
            "prepared": "Prepared jobs",
            "materialized": "Unpublished output",
            "failed": "Failure report",
            "blocked": "Blocked report",
            "interrupted": "Retained evidence",
            "running": "Awaiting outcome",
        }.get(state, "")
    return RunSummary(
        "pipeline"
        if report.get("kind") == "historical_pipeline"
        else str(report.get("operation") or report.get("kind") or ""),
        str(report.get("name") or report.get("recipe") or ""),
        str(report.get("state") or report.get("status") or ""),
        tuple(sorted({str(entry["exchange"]) for entry in entries if entry.get("exchange")})),
        tuple(
            sorted(
                {
                    str(scope["identifier"])
                    for item in entries
                    if (scope := mapping(item.get("scope"))).get("identifier")
                }
            )
        ),
        when(report.get("started_at")),
        when(report.get("finished_at")),
        any(
            mapping(value).get(key)
            for value in outputs
            for key in ("primary_file_ids", "publications", "publication", "catalogue_dataset_ids")
        ),
        result,
    )


def matches(summary: RunSummary, identity: str, filters: Mapping[str, str]) -> bool:
    values = {
        "exchange": summary.exchanges,
        "scope": summary.scope,
        "operation": (summary.operation,),
        "recipe": (summary.recipe,),
        "state": (summary.state,),
        "identity": (identity,),
    }
    for key, field in values.items():
        if (target := filters.get(key)) and not any(
            target.casefold() in value.casefold() for value in field
        ):
            return False
    for key, operation in (("start", "after"), ("end", "before")):
        if value := filters.get(key):
            stamp = when(value)
            if stamp is None:
                raise MarketForgeError(
                    "Run start bounds require ISO-8601 timestamps with a timezone."
                )
            if summary.start is None or (
                summary.start < stamp if operation == "after" else summary.start >= stamp
            ):
                return False
    seconds = (
        (summary.end - summary.start).total_seconds() if summary.end and summary.start else None
    )
    if (minimum := filters.get("minimum_duration")) and (
        seconds is None or seconds < float(minimum)
    ):
        return False
    availability = filters.get("output")
    return not availability or summary.output == (availability == "present")


def value_for_sort(summary: RunSummary, column: str) -> str | float | None:
    if column == "Started UTC":
        return summary.start.timestamp() if summary.start else None
    if column == "Duration":
        return (
            (summary.end - summary.start).total_seconds() if summary.start and summary.end else None
        )
    return {
        "Operation": summary.operation,
        "Recipe": summary.recipe,
        "Outcome": summary.state,
        "Exchange": ", ".join(summary.exchanges),
        "Scope": ", ".join(summary.scope),
    }.get(column) or None
