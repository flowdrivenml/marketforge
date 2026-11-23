"""Bounded run inspection; incomplete/missing reports remain explicit."""

import json
import os
import re
from dataclasses import asdict
from functools import lru_cache
from pathlib import Path

from forge.orchestration.run_summary import RunSummary, matches, summarize, value_for_sort
from forge.orchestration.run_visibility import hidden_names
from forge.shared.errors import MarketForgeError
from forge.shared.identity import parse_run_id


def list_runs(root: Path, limit: int = 50) -> list[dict[str, object]]:
    if not 1 <= limit <= 1000:
        raise MarketForgeError("Run limit must be 1..1000.")
    if not root.exists():
        return []
    items: list[tuple[float, str]] = []
    hidden = hidden_names(root)
    with os.scandir(root) as entries:
        for index, entry in enumerate(entries):
            if index >= 100000:
                raise MarketForgeError("Run inventory exceeds bounded scan limit.")
            if entry.name in hidden or not entry.is_dir(follow_symlinks=False):
                continue
            try:
                parse_run_id(entry.name)
            except ValueError:
                continue
            items.append((entry.stat().st_mtime, entry.name))
            items = sorted(items, reverse=True)[:limit]
    return [read_report(root, name) for _, name in items]


def run_root(root: Path, run_id: str) -> Path:
    try:
        name = str(parse_run_id(run_id))
    except ValueError:
        raise MarketForgeError("Expected an operation-prefixed run ID or legacy UUID.") from None
    path = root / name
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise MarketForgeError("Unsafe run artifact location.")
    return path


def read_report(root: Path, run_id: str) -> dict[str, object]:
    path = run_root(root, run_id)
    report = path / "report.json"
    if report.is_symlink():
        raise MarketForgeError("Unsafe retained report location.")
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
    return read_events(run_root(root, run_id) / "events.jsonl", run_id, limit)


def read_events(path: Path, run_id: str, limit: int) -> dict[str, object]:
    if not 1 <= limit <= 1000:
        raise MarketForgeError("Log limit must be 1..1000.")
    if path.is_symlink():
        raise MarketForgeError("Unsafe retained event location.")
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


MANAGED_RUN = re.compile(
    r"(?:normalize|merge|convert)-(?:execute|prepare|retry|register|recovery)-(?:[0-9]{1,20}|[0-9a-f]{32}|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})"
)


def read_managed_report(root: Path, run_id: str) -> dict[str, object]:
    """Read a known child execution receipt by its safe directory identity."""
    if not MANAGED_RUN.fullmatch(run_id):
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

            try:
                result = obj(json.loads(path.read_bytes()))
            except (ValueError, UnicodeError):
                return {"run_id": run_id, "status": "report_incomplete_or_invalid"}
            return {**result, "run_id": run_id, "resolved_report": str(path), "liveness": "unknown"}
    return {"run_id": run_id, "status": "report_missing_or_interrupted"}


def run_signatures(root: Path, name: str, managed: bool) -> tuple[tuple[str, int, int], ...]:
    files = (
        (
            "execution-result.json",
            "registration-result.json",
            "prepare-report.json",
            "execution-intent.json",
        )
        if managed
        else ("report.json", "resolved-plan.json")
    )
    signatures: list[tuple[str, int, int]] = []
    for label in files:
        try:
            entry = (root / name / label).lstat()
        except OSError:
            continue
        signatures.append((label, entry.st_size, entry.st_mtime_ns))
    return tuple(signatures)


@lru_cache(maxsize=2048)
def indexed_run(
    root: Path, name: str, managed: bool, _signatures: tuple[tuple[str, int, int], ...]
) -> tuple[dict[str, object], RunSummary]:
    report = read_managed_report(root, name) if managed else read_report(root, name)
    summary = summarize(report, root / name)
    fields = ("kind", "operation", "name", "recipe", "state", "status", "started_at", "finished_at")
    return {key: report[key] for key in fields if key in report}, summary


def run_page(
    root: Path,
    limit: int,
    offset: int,
    search: str = "",
    *,
    pipeline_only: bool = False,
    include_managed: bool = False,
    managed_prefix: str | None = None,
    filters: dict[str, str] | None = None,
    sort: str = "",
    descending: bool = False,
) -> tuple[list[dict[str, object]], bool]:
    """Search retained report identities before applying a bounded result page."""
    if not 1 <= limit <= 100 or offset < 0:
        raise MarketForgeError("Invalid retained run page.")
    if not root.is_dir():
        return [], False
    candidates: list[tuple[float, str]] = []
    hidden = hidden_names(root)
    with os.scandir(root) as items:
        for index, entry in enumerate(items):
            if index >= 100000:
                raise MarketForgeError("Run inventory exceeds bounded scan limit.")
            if entry.name in hidden or not entry.is_dir(follow_symlinks=False):
                continue
            if managed_prefix is not None and not entry.name.startswith(managed_prefix):
                continue
            try:
                if not (include_managed and MANAGED_RUN.fullmatch(entry.name)):
                    parse_run_id(entry.name)
                candidates.append((entry.stat().st_mtime, entry.name))
            except (ValueError, FileNotFoundError):
                continue
    result: list[dict[str, object]] = []
    matched = 0
    full = bool(filters or sort)
    sort_keys: dict[str, str | float] = {}
    for _, name in sorted(candidates, reverse=True)[
        offset if not search and not pipeline_only and not full else 0 :
    ]:
        summary: RunSummary | None = None
        try:
            if full:
                managed = include_managed and bool(MANAGED_RUN.fullmatch(name))
                report, summary = indexed_run(
                    root, name, managed, run_signatures(root, name, managed)
                )
            else:
                report = (
                    read_managed_report(root, name) if include_managed else read_report(root, name)
                )
        except (MarketForgeError, OSError, ValueError) as error:
            report = {"status": "unavailable", "error": str(error)}
            if full:
                summary = summarize(report)
        if pipeline_only and report.get("kind") != "historical_pipeline":
            continue
        if summary is not None and not matches(summary, name, filters or {}):
            continue
        haystack = " ".join(
            str(report.get(key, "")) for key in ("name", "kind", "operation", "state", "status")
        )
        if search.casefold() not in f"{name} {haystack}".casefold():
            continue
        if not full and (search or pipeline_only) and matched < offset:
            matched += 1
            continue
        if summary is not None:
            if (sort_value := value_for_sort(summary, sort)) is not None:
                sort_keys[name] = sort_value
            result.append({**report, "run_id": name, "run_context": asdict(summary)})
        else:
            result.append({**report, "run_id": name})
        if not full and len(result) > limit:
            break
    if full:
        if sort:

            def key(item: dict[str, object]) -> str | float:
                return sort_keys.get(str(item["run_id"]), "")

            present = [item for item in result if key(item) != ""]
            missing = [item for item in result if key(item) == ""]
            present.sort(key=lambda item: str(item["run_id"]))
            present.sort(key=key, reverse=descending)
            result = present + missing
        return result[offset : offset + limit], len(result) > offset + limit
    return result[:limit], len(result) > limit


def read_managed_logs(
    root: Path,
    run_id: str,
    *,
    truncated: set[str] | None = None,
    supplemental: dict[str, str] | None = None,
) -> dict[str, object]:
    """Read a bounded tail from the fixed engine log names."""
    if not MANAGED_RUN.fullmatch(run_id):
        raise MarketForgeError("Expected a managed execution identity.")
    directory = root / run_id
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise MarketForgeError("Unsafe managed execution directory.")
    logs: dict[str, object] = {}
    names = [
        f"{prefix}-{stream}.log"
        for prefix in ("engine", "validation")
        for stream in ("stdout", "stderr")
    ]
    if supplemental is not None:
        names.append("engine-tui.log")
    for name in names:
        path = directory / name
        if path.is_file() and not path.is_symlink():
            with path.open("rb") as handle:
                end = handle.seek(0, os.SEEK_END)
                if truncated is not None and end > 12288:
                    truncated.add(name)
                handle.seek(max(0, end - 12288))
                content = handle.read(12288).decode(errors="replace")
            if name == "engine-tui.log" and supplemental is not None:
                supplemental[name] = content
            else:
                logs[name] = content
    return logs or {"status": "No retained engine logs"}
