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
