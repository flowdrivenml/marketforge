"""Bounded run inspection; incomplete/missing reports remain explicit."""

import json
import os
from pathlib import Path
from uuid import UUID

from forge.shared.errors import MarketForgeError


def list_runs(root: Path, limit: int = 50) -> list[dict[str, object]]:
    if not 1 <= limit <= 1000:
        raise MarketForgeError("Run limit must be 1..1000.")
    if not root.exists():
        return []
    items: list[tuple[float, str]] = []
    with os.scandir(root) as entries:
        for index, entry in enumerate(entries):
            if index >= 100000:
                raise MarketForgeError("Run inventory exceeds bounded scan limit.")
            if not entry.is_dir(follow_symlinks=False):
                continue
            try:
                UUID(entry.name)
            except ValueError:
                continue
            items.append((entry.stat().st_mtime, entry.name))
            items = sorted(items, reverse=True)[:limit]
    return [read_report(root, name) for _, name in items]


def run_root(root: Path, run_id: str) -> Path:
    try:
        name = str(UUID(run_id))
    except ValueError:
        raise MarketForgeError("Run ID must be a UUID.") from None
    path = root / name
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise MarketForgeError("Unsafe run artifact location.")
    return path


def read_report(root: Path, run_id: str) -> dict[str, object]:
    path = run_root(root, run_id)
    report = path / "report.json"
    if not report.is_file():
        return {
            "run_id": run_id,
            "status": "report_missing_or_interrupted",
            "events_present": (path / "events.jsonl").is_file(),
        }
    if report.stat().st_size > 4 * 1024 * 1024:
        raise MarketForgeError("Run report exceeds 4 MiB inspection limit.")
    try:
        with report.open(encoding="utf-8") as source:
            value: object = json.load(source)
        from forge.markets.models import obj

        return obj(value)
    except (ValueError, UnicodeError):
        return {"run_id": run_id, "status": "report_incomplete_or_invalid"}


def read_logs(root: Path, run_id: str, limit: int = 100) -> dict[str, object]:
    if not 1 <= limit <= 1000:
        raise MarketForgeError("Log limit must be 1..1000.")
    path = run_root(root, run_id) / "events.jsonl"
    if not path.is_file():
        return {"run_id": run_id, "events": [], "status": "events_missing"}
    with path.open("rb") as source:
        data = source.read(1024 * 1024 + 1)
    lines = data[: 1024 * 1024].decode("utf-8", errors="replace").splitlines()
    events: list[object] = []
    for line in lines[:limit]:
        try:
            events.append(json.loads(line))
        except ValueError:
            events.append({"status": "incomplete_event_line"})
    return {
        "run_id": run_id,
        "events": events,
        "truncated": len(data) > 1024 * 1024 or len(lines) > limit,
    }


def read_managed_report(root: Path, run_id: str) -> dict[str, object]:
    """Read a known child execution receipt by its safe directory identity."""
    import re

    if not re.fullmatch(
        r"(?:normalize|merge|convert)-(?:execute|prepare|retry|register|recovery)-[0-9a-f-]{1,64}",
        run_id,
    ):
        return read_report(root, run_id)
    directory = root / run_id
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise MarketForgeError("Unsafe managed execution directory.")
    for filename in (
        "execution-result.json",
        "registration-result.json",
        "prepare-report.json",
        "execution-intent.json",
    ):
        path = directory / filename
        if path.is_file() and not path.is_symlink():
            if path.stat().st_size > 4 * 1024 * 1024:
                raise MarketForgeError("Managed report exceeds 4 MiB inspection limit.")
            from forge.markets.models import obj

            result = obj(json.loads(path.read_bytes()))
            return {**result, "run_id": run_id, "resolved_report": str(path), "liveness": "unknown"}
    return {"run_id": run_id, "status": "report_missing_or_interrupted"}


def run_page(
    root: Path, limit: int, offset: int, search: str = "", *, pipeline_only: bool = False
) -> tuple[list[dict[str, object]], bool]:
    """Search retained report identities before applying a bounded result page."""
    if not 1 <= limit <= 100 or offset < 0:
        raise MarketForgeError("Invalid retained run page.")
    if not root.is_dir():
        return [], False
    candidates: list[tuple[float, str]] = []
    with os.scandir(root) as items:
        for index, entry in enumerate(items):
            if index >= 100000:
                raise MarketForgeError("Run inventory exceeds bounded scan limit.")
            if not entry.is_dir(follow_symlinks=False):
                continue
            try:
                UUID(entry.name)
                candidates.append((entry.stat().st_mtime, entry.name))
            except (ValueError, FileNotFoundError):
                continue
    result: list[dict[str, object]] = []
    matched = 0
    for _, name in sorted(candidates, reverse=True)[
        offset if not search and not pipeline_only else 0 :
    ]:
        try:
            report = read_report(root, name)
        except (MarketForgeError, OSError) as error:
            report = {"status": "unavailable", "error": str(error)}
        if pipeline_only and report.get("kind") != "historical_pipeline":
            continue
        haystack = " ".join(
            str(report.get(key, "")) for key in ("name", "kind", "operation", "state", "status")
        )
        if search.casefold() not in f"{name} {haystack}".casefold():
            continue
        if (search or pipeline_only) and matched < offset:
            matched += 1
            continue
        result.append({**report, "run_id": name})
        if len(result) > limit:
            break
    return result[:limit], len(result) > limit
