"""Read-only presentation of configured recipe values and source evidence."""

import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import cast

from rich.text import Text

from forge.shared.terminal.palette import CYAN, MUTED
from forge.tui.clipboard import field_text, safe_text
from forge.tui.controller import document
from forge.tui.design import SECONDARY
from forge.tui.interval import split_timestamp
from forge.tui.recipe_forms import GROUPS, Payload, model_for, summarize


def format_value(key: str, value: object) -> str:
    if key in {"start_ns", "end_ns"} and type(value) is int:
        date, clock = split_timestamp(value, nanoseconds=True)
        return f"{date} {clock} UTC ({value} ns)"
    if key == "max_staleness_ns" and type(value) is int:
        seconds = Decimal(value).scaleb(-9).normalize()
        return f"{seconds:f} seconds ({value} ns)"
    if key == "price_source":
        return field_text(key, value).replace("_", " ").capitalize()
    if key in {"start", "end"}:
        try:
            date, clock = split_timestamp(value, nanoseconds=False)
        except ValueError:
            return str(value)
        return f"{date} {clock} UTC"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if key in {"data_type", "content_type", "kind"}:
        return {
            "trades": "Trades",
            "trade": "Trades",
            "order_book_l2": "Depth",
            "depth": "Depth",
        }.get(str(value), str(value))
    return field_text(key, value)


def configuration(data: Mapping[str, object]) -> Text:
    result = Text()

    def lines(values: Mapping[str, object], indent: str = "") -> None:
        for key, item in values.items():
            label = safe_text(key.replace("_", " ").capitalize())
            if field_text(key, "") == "[redacted]":
                result.append(f"{indent}{label}  [redacted]\n", MUTED)
                continue
            if key in {"start", "start_ns", "end", "end_ns"}:
                label = (
                    "Start UTC (inclusive)" if key.startswith("start") else "End UTC (exclusive)"
                )
            if isinstance(item, dict):
                result.append(f"{indent}{label}\n", CYAN)
                lines(document(cast(object, item)), indent + "  ")
            elif isinstance(item, list) and any(
                isinstance(x, dict) for x in cast(list[object], item)
            ):
                result.append(f"{indent}{label}\n", CYAN)
                for index, entry in enumerate(cast(list[object], item), 1):
                    result.append(f"{indent}  {index}.\n", MUTED)
                    lines(document(entry), indent + "    ")
            else:
                rendered = (
                    ", ".join(format_value(key, x) for x in cast(list[object], item)) or "Empty"
                    if isinstance(item, list)
                    else format_value(key, item)
                )
                result.append(f"{indent}{label}  ", SECONDARY)
                result.append(
                    f"{rendered}\n", CYAN if key in {"symbol", "instrument", "instruments"} else ""
                )

    lines(data)
    return result


def retained_summary(root: Path, run_id: str, attempt_id: str, path: Path) -> Mapping[str, object]:
    from forge.orchestration.inspection import run_root

    directory = run_root(root, run_id)
    path = path.resolve()
    if path != directory / "attempts" / attempt_id / "outcome.json":
        return {}
    try:
        with path.open("rb") as stream:
            payload = document(json.loads(stream.read(128 * 1024)))
    except (OSError, ValueError):
        return {}
    if payload.get("run_id") != run_id or payload.get("attempt_id") != attempt_id:
        return {}
    return document(payload.get("summary"))


def sections(family: str, data: Payload, state: str, path: str) -> dict[str, object]:
    groups: dict[str, Payload] = {}
    for key, value in data.items():
        group = GROUPS.get(key, "Processing options")
        groups.setdefault(group, {})[key] = value
    options = Text()
    ordered = [
        "Historical interval",
        "Declared coverage",
        "Acquisition requests",
        "Source selection",
    ]
    if "requests" in data:
        groups["Processing options"].pop("requests")
        groups["Acquisition requests"] = {"requests": data["requests"]}
    for name in dict.fromkeys([*ordered, *groups]):
        if values := groups.get(name):
            options.append(f"{name}\n", f"bold {CYAN}")
            options.append_text(configuration(values))
            options.append("\n")
    if (model := model_for(family, data)) is not None:
        omitted = [name.replace("_", " ") for name in model.model_fields if name not in data]
        if omitted:
            options.append("Unset settings\n", f"bold {CYAN}")
            options.append(", ".join(omitted) + "\n", MUTED)
    return {
        "Summary": {"Name": Path(path).stem, "Workflow": family, **summarize(data, state, path)},
        "Configuration": options,
    }
