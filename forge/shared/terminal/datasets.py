"""Grouped file listings and instrument coverage from the current registry page."""

from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any

from rich import box
from rich.cells import cell_len
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from forge.shared.terminal.markets import state
from forge.shared.terminal.storage import size

Row = dict[str, Any]
IDENTITY_FIELDS = ("exchange", "symbol", "instrument_type", "market_category", "instrument_id")
CONTENT = {
    "trade": "Trades",
    "trades": "Trades",
    "l2": "L2 depth",
    "depth": "L2 depth",
    "trade_l2": "Combined",
    "combined": "Combined",
}
OPERATIONS = {"normalize": "Normalized", "merge": "Merged"}
INTEGRITY = {"clean": "mf.success", "degraded": "mf.warning", "failed": "mf.failure"}


def _label(value: object) -> str:
    return "Unknown" if value is None else str(value)


def _identity(member: Row) -> tuple[Any, ...]:
    return tuple(member.get(key) for key in IDENTITY_FIELDS)


def _members(row: Row) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        sorted({_identity(m) for m in row.get("summary", {}).get("memberships", ())}, key=str)
    )


def _market(identity: tuple[Any, ...]) -> str:
    return (
        " / ".join(
            dict.fromkeys(str(value).replace("_", " ").title() for value in identity[2:4] if value)
        )
        or "Unknown market"
    )


def _instrument(identity: tuple[Any, ...], *, exchange: bool = True) -> Text:
    text = Text()
    if exchange:
        text.append(_label(identity[0]).upper() + " / ", "mf.identifier")
    text.append(_label(identity[1]), "bold mf.body")
    return text


def _timestamps(value: int | None, *, short: bool, precise: bool = False) -> str:
    if value is None:
        return "Unknown"
    seconds, nanos = divmod(value, 10**9)
    date = datetime.fromtimestamp(seconds, UTC)
    text = date.strftime("%m-%d %H:%M:%S" if short else "%Y-%m-%d %H:%M:%S")
    return f"{text}.{nanos:09d}" if precise else text


def _year(rows: list[Row]) -> int | None:
    years = {
        datetime.fromtimestamp(value // 10**9, UTC).year
        for row in rows
        for key in ("start_timestamp_ns", "end_timestamp_ns")
        if (value := row.get(key)) is not None
    }
    return next(iter(years)) if len(years) == 1 else None


def _events(count: int | None) -> str:
    if count is None:
        return "Unknown"
    for divisor, label in ((10**12, "T"), (10**9, "B"), (10**6, "M"), (10**3, "K")):
        if count >= divisor:
            return f"{count / divisor:.2f}".rstrip("0").rstrip(".") + label
    return str(count)


def _integrity(row: Row) -> Text:
    summary = row.get("summary", {})
    integrity = summary.get("integrity")
    value = state(row.get("status") or "Unknown status")
    value.append(" · ")
    value.append(_label(integrity), INTEGRITY.get(integrity, "mf.secondary"))
    if scope := summary.get("integrity_scope"):
        value.append(f" ({scope})", "mf.secondary")
    return value


def _table(*headers: str) -> Table:
    table = Table(
        box=box.SIMPLE,
        padding=(0, 1),
        pad_edge=False,
        header_style="mf.heading",
        border_style="mf.secondary",
    )
    for header in headers:
        table.add_column(header, overflow="fold", no_wrap=header in {"START (UTC)", "END (UTC)"})
    return table


def _heading(console: Console, title: str) -> None:
    console.print(Text(f"MARKETFORGE  /  {title}", style="mf.section"))
    console.print(Rule(style="mf.secondary"))


def _footer(console: Console, result: Row, *, coverage: bool = False) -> None:
    console.print(Rule(style="mf.secondary"))
    count, offset = len(result["datasets"]), result["offset"]
    message = f"Files on this page {count} · Offset {offset}"
    if result.get("has_more"):
        message += f" · Next page --offset {offset + result['limit']}"
    console.print(Text(message, style="mf.secondary"))
    if coverage:
        console.print(Text("Observed UTC envelopes · Continuity unknown", style="mf.secondary"))
    else:
        console.print(
            Text(
                "Observed UTC endpoints are inclusive · Query end is exclusive",
                style="mf.secondary",
            )
        )


def _print_interval(console: Console, first: str, last: str) -> None:
    interval = f"    {first} → {last}"
    if cell_len(interval) <= console.width:
        console.print(Text(interval))
    else:
        console.print(Text(f"    {first}\n    → {last}"))


def _file_group(
    console: Console, rows: list[Row], *, wide: bool, precise: bool, shared_state: bool
) -> None:
    year = _year(rows)
    console.print(Text(f"{year} · UTC" if year is not None else "UTC", style="mf.secondary"))
    cells: list[tuple[Text, ...]] = []
    for row in rows:
        summary = row.get("summary", {})
        path = row.get("primary_file")
        file = PurePosixPath(path).name if path else "Unknown file"
        if (
            file.startswith("part-")
            and file.endswith(".parquet")
            and file[5:-8].isdigit()
            and not wide
        ):
            file = file[5:-8]
        cells.append(
            (
                Text(str(row["id"]), style="mf.dataset_id"),
                Text((path or file) if wide else file),
                Text(
                    _timestamps(
                        row.get("start_timestamp_ns"), short=year is not None, precise=precise
                    )
                ),
                Text(
                    _timestamps(
                        row.get("end_timestamp_ns"), short=year is not None, precise=precise
                    )
                ),
                Text(_events(summary.get("rows")), style="mf.success"),
                Text(size(summary.get("size_bytes")), style="mf.secondary"),
                _integrity(row),
            )
        )

    headers = ["ID", "FILE" if wide else "PART", "START (UTC)", "END (UTC)", "EVENTS", "SIZE"]
    if not shared_state:
        headers.append("STATUS / INTEGRITY")
    # Reserve minimum space for foldable paths, and keep both timestamps intact.
    minimum = sum(
        max(len(label), 14 if index == 1 else max(len(row[index]) for row in cells))
        for index, label in enumerate(headers)
    ) + 2 * len(headers)
    if console.width >= max(120, minimum):
        table = _table(*headers)
        for index in (0, 4, 5):
            table.columns[index].justify = "right"
        for row in cells:
            table.add_row(*row[: len(headers)])
        console.print(table)
        return
    for identity, file, start, end, events, stored, integrity in cells:
        console.print(Text.assemble("#", identity, "  ", file))
        _print_interval(console, start.plain, end.plain)
        console.print(Text.assemble("    ", events, " events · ", stored))
        if not shared_state:
            console.print(Text.assemble("    ", integrity))


def render_datasets(
    console: Console, result: Row, *, wide: bool = False, precise: bool = False
) -> None:
    _heading(console, "DATASETS")
    publications: dict[tuple[Any, ...], list[Row]] = defaultdict(list)
    for row in result["datasets"]:
        publications[(row.get("instrument_namespace"), row.get("engine_dataset_id"))].append(row)
    if publications:
        console.print(
            Text(
                f"{len(result['datasets'])} files · {len(publications)} publications on this page",
                style="mf.secondary",
            )
        )
    else:
        console.print(Text("No files on this page", style="mf.warning"))
    repeated_ids = Counter(key[1] for key in publications)
    for (namespace, publication), rows in publications.items():
        console.print()
        operation = rows[0].get("operation")
        operation_label = OPERATIONS.get(operation, _label(operation))
        kinds = {row.get("data_type") for row in rows}
        if len(kinds) == 1:
            content = next(iter(kinds))
            operation_label += f" / {CONTENT.get(content, _label(content))}"
        console.print(Text(operation_label, style="mf.section"))
        label = Text(f"Publication {_label(publication)}", style="mf.section")
        if repeated_ids[publication] > 1:
            label.append(f" · {_label(namespace)}", "mf.secondary")
        console.print(label)
        states = {
            (
                row.get("status"),
                row.get("summary", {}).get("integrity"),
                row.get("summary", {}).get("integrity_scope"),
            )
            for row in rows
        }
        shared_state = len(states) == 1 and next(iter(states))[2] == "publication"
        if shared_state:
            console.print(_integrity(rows[0]))

        exchanges: dict[str, dict[tuple[Any, ...], list[Row]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for row in rows:
            members = _members(row)
            streams = {
                m.get("stream_id") for m in row.get("summary", {}).get("memberships", ())
            } - {None}
            exchange = (
                _label(members[0][0]).upper()
                if len(members) == 1 and len(streams) <= 1
                else "Multi-stream / mixed membership"
                if members
                else "Unresolved membership"
            )
            exchanges[exchange][(members, row.get("data_type"))].append(row)
        for exchange, identities in exchanges.items():
            console.print()
            console.print(Text(exchange, style="mf.identifier"))
            for (members, content), group in identities.items():
                if len(kinds) > 1:
                    console.print(
                        Text(CONTENT.get(content, _label(content)), style="mf.identifier_detail")
                    )
                for member in members:
                    label = _instrument(
                        member, exchange=exchange == "Multi-stream / mixed membership"
                    )
                    label.append(f" · {_market(member)}", "mf.identifier_detail")
                    if member[-1] is not None:
                        label.append(f" · Instrument {member[-1]}", "mf.secondary")
                    console.print(label)
                _file_group(console, group, wide=wide, precise=precise, shared_state=shared_state)
    if not precise and publications:
        console.print(
            Text("Displayed to whole seconds · Use --precise for nanoseconds", style="mf.secondary")
        )
    _footer(console, result)


def coverage_groups(rows: list[Row], filters: Row) -> list[Row]:
    """Project matched file rows onto canonical identities without asserting continuity."""
    groups: dict[tuple[Any, ...], Row] = {}
    identity_filters = any(
        filters.get(key) is not None
        for key in ("exchange", "instrument", "instrument_type", "market_category")
    )
    for row in rows:
        members = row.get("summary", {}).get("memberships", ())
        for member in members or ({},):
            if identity_filters and not _matches(member, filters):
                continue
            key = (row.get("instrument_namespace"), *_identity(member), row.get("data_type"))
            entry = groups.setdefault(
                key,
                {
                    "identity": _identity(member),
                    "data_type": row.get("data_type"),
                    "files": set(),
                    "start_timestamp_ns": None,
                    "end_timestamp_ns": None,
                },
            )
            entry["files"].add(row["id"])
            for endpoint, combine in (("start_timestamp_ns", min), ("end_timestamp_ns", max)):
                value = member.get(endpoint) if members else row.get(endpoint)
                if value is not None:
                    old = entry[endpoint]
                    entry[endpoint] = value if old is None else combine(old, value)
    return [groups[key] for key in sorted(groups, key=str)]


def _matches(member: Row, filters: Row) -> bool:
    for key in ("exchange", "instrument_type", "market_category"):
        if filters.get(key) is not None and member.get(key) != filters[key]:
            return False
    instrument = filters.get("instrument")
    if instrument is not None and instrument.lower() not in {
        str(member.get("symbol") or "").lower(),
        str(member.get("instrument_id") or ""),
    }:
        return False
    start, end = filters.get("start"), filters.get("end")
    first, last = member.get("start_timestamp_ns"), member.get("end_timestamp_ns")
    return (start is None or (last is not None and last >= start)) and (
        end is None or (first is not None and first < end)
    )


def render_coverage(console: Console, result: Row, *, filters: Row) -> None:
    _heading(console, "DATA COVERAGE")
    groups = coverage_groups(result["datasets"], filters)
    if not groups:
        console.print(Text("No observed envelopes on this page", style="mf.warning"))
    else:
        console.print(
            Text(
                f"{len(groups)} instrument / content groups on this file page", style="mf.secondary"
            )
        )
    table = _table("EXCHANGE", "INSTRUMENT", "MARKET", "TYPE", "FILES", "OBSERVED UTC ENVELOPE")
    table.columns[4].justify = "right"
    table.columns[5].min_width = 21
    # Long symbols get their own wrapped line when a table would interrupt them.
    longest_identity = max(
        (
            cell_len(_label(group["identity"][1])) + len(_label(group["identity"][-1])) + 3
            for group in groups
        ),
        default=0,
    )
    tabular = console.width >= max(120, longest_identity + 82)
    for group in groups:
        identity = group["identity"]
        exchange = Text(_label(identity[0]).upper(), "mf.identifier")
        instrument = _instrument(identity, exchange=False)
        if identity[-1] is not None:
            instrument.append(f" · {identity[-1]}", "mf.secondary")
        market = Text(_market(identity), "mf.identifier_detail")
        content = CONTENT.get(group["data_type"], _label(group["data_type"]))
        first = _timestamps(group["start_timestamp_ns"], short=False)
        last = _timestamps(group["end_timestamp_ns"], short=False)
        files = Text(str(len(group["files"])), "mf.success")
        if tabular:
            table.add_row(exchange, instrument, market, content, files, Text(f"{first}\n→ {last}"))
        else:
            console.print(Text.assemble(exchange, " / ", instrument))
            console.print(
                Text.assemble(
                    "    ",
                    market,
                    f" · {content} · ",
                    files,
                    " file" if len(group["files"]) == 1 else " files",
                )
            )
            _print_interval(console, first, last)
    if groups and tabular:
        console.print(table)
    _footer(console, result, coverage=True)
