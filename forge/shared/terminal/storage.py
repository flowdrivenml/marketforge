"""Tokyo Night storage tables and details; no data access or revalidation."""

from rich.console import Console
from rich.table import Table
from rich.text import Text

from forge.markets.models import array, obj
from forge.shared.terminal.markets import state, table, timestamp


def size(value: object) -> str:
    if value is None:
        return "unknown"
    amount = float(str(value))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:,.0f} {unit}" if unit == "B" else f"{amount:,.1f} {unit}"
        amount /= 1024
    return "unknown"


def utc(value: object) -> str:
    return "unknown" if value is None else timestamp(value)


def record_state(value: object) -> Text:
    if value in {"missing", "corrupt", "failed"}:
        return Text(str(value), style="mf.failure")
    return state(value)


def known(value: object) -> str:
    return "unknown" if value is None else str(value)


def identity(row: dict[str, object]) -> Text:
    name = known(row.get("scope_identifier"))
    if row.get("scope_kind") != "instrument":
        name = known(row.get("scope_kind")) + ": " + name
    result = Text(name, style="mf.identifier")
    origin = str(row.get("origin", ""))
    if row.get("external_source_key"):
        origin += ":" + str(row["external_source_key"])
    result.append("\n" + origin, style="mf.secondary")
    return result


def product(row: dict[str, object]) -> str:
    return known(row.get("instrument_type")) + "/" + known(row.get("market_category"))


def content(row: dict[str, object]) -> str:
    kind = known(row.get("data_type"))
    return kind + (
        "/" + known(row.get("feed"))
        if row.get("data_type") == "order_book_l2" or row.get("feed")
        else ""
    )


def numeric_columns(t: Table) -> None:
    for column in t.columns:
        if column.header in {"Files", "Gaps", "Size"}:
            column.justify = "right"


def render_storage(
    console: Console, action: str, value: dict[str, object], *, wide: bool = False
) -> None:
    console.print(Text("Storage · " + action, style="mf.heading"))
    if action == "show":
        render_details(console, obj(array(value["archives"])[0]))
        return
    selected = obj(value.get("filters", {}))
    active = [
        f"{key}={val}"
        for key, val in selected.items()
        if val is not None and key not in {"limit", "offset", "start", "end"}
    ]
    if active:
        console.print(Text("Filters: " + ", ".join(active), style="mf.secondary"))
    narrow = console.width < 110
    rows = [
        obj(row) for row in array(value["coverage"] if action == "coverage" else value["archives"])
    ]
    if not rows and obj(value["pagination"])["total"]:
        console.print("No rows on this page; reduce --offset to browse matching results.")
    elif not rows:
        console.print(
            Text(
                "No matching complete local archive groups."
                if action == "coverage"
                else "No matching local archives.",
                style="mf.warning",
            )
        )
    elif action == "coverage":
        t = table(
            *(
                ["Source / instrument", "Data / interval (UTC, end exclusive)", "Files", "Gaps"]
                if narrow
                else [
                    "Exchange",
                    "Type / market",
                    "Instrument / family",
                    "Data / feed",
                    "Earliest UTC",
                    "Latest UTC (exclusive)",
                    "Files",
                    "Gaps",
                ]
            )
        )
        for row in rows:
            source = identity(row)
            gap = row["gap_count"]
            gap_text = Text(known(gap), style="mf.success" if gap == 0 else "mf.warning")
            if narrow:
                source.append(
                    "\n" + known(row["exchange"]) + " · " + product(row), style="mf.secondary"
                )
                t.add_row(
                    source,
                    content(row)
                    + "\n"
                    + utc(row["earliest_start"])
                    + "\n→ "
                    + utc(row["latest_end_exclusive"]),
                    str(row["local_file_count"]),
                    gap_text,
                )
            else:
                t.add_row(
                    known(row["exchange"]),
                    product(row),
                    source,
                    content(row),
                    utc(row["earliest_start"]),
                    utc(row["latest_end_exclusive"]),
                    str(row["local_file_count"]),
                    gap_text,
                )
        numeric_columns(t)
        console.print(t)
        for row in rows:
            if selected.get("start") or selected.get("end"):
                console.print(
                    Text(
                        known(row["scope_identifier"])
                        + " · "
                        + content(row)
                        + " evaluated ["
                        + utc(row["evaluation_start"])
                        + ", "
                        + utc(row["evaluation_end_exclusive"])
                        + ")",
                        style="mf.secondary",
                    )
                )
            if row.get("metadata_conflicts"):
                console.print(
                    Text(
                        known(row["scope_identifier"])
                        + ": inconsistent asset metadata: "
                        + ", ".join(str(key) for key in array(row["metadata_conflicts"])),
                        style="mf.warning",
                    )
                )
            if row.get("diagnostics"):
                console.print(
                    Text(
                        known(row["scope_identifier"]) + ": " + "; ".join(obj(row["diagnostics"])),
                        style="mf.warning",
                    )
                )
            for raw in array(row.get("gaps") or []):
                gap = obj(raw)
                console.print(
                    Text(
                        known(row["scope_identifier"])
                        + " · "
                        + content(row)
                        + " missing ["
                        + utc(gap["start"])
                        + ", "
                        + utc(gap["end_exclusive"])
                        + ")",
                        style="mf.warning",
                    )
                )
    else:
        t = table(
            *(
                ["Local ID / instrument", "Data / interval UTC (end exclusive)", "Size / status"]
                if narrow
                else [
                    "Local ID",
                    "Exchange",
                    "Type / market",
                    "Instrument / family",
                    "Data / feed",
                    "Start UTC",
                    "End UTC (exclusive)",
                    "Size",
                    "Status",
                ]
            )
        )
        if narrow:
            t.columns[0].min_width = 16
            t.columns[1].min_width = 23
            t.columns[2].justify = "right"
        for row in rows:
            local = Text(str(row["local_id"]), style="mf.identifier")
            source = identity(row)
            if narrow:
                local.append("\n")
                local.append(source)
                local.append(
                    "\n" + known(row["exchange"]) + " · " + product(row), style="mf.secondary"
                )
                status_text = Text(size(row["bytes_written"]) + "\n")
                status_text.append(record_state(row["status"]))
                cells = [
                    local,
                    content(row) + "\n" + utc(row["start_at"]) + "\n→ " + utc(row["end_at"]),
                    status_text,
                ]
            else:
                cells = [
                    local,
                    known(row["exchange"]),
                    product(row),
                    source,
                    content(row),
                    utc(row["start_at"]),
                    utc(row["end_at"]),
                    size(row["bytes_written"]),
                    record_state(row["status"]),
                ]
            t.add_row(*cells)
        numeric_columns(t)
        console.print(t)
        if wide:
            for row in rows:
                console.print(
                    Text.assemble(
                        Text(str(row["local_id"]), style="mf.identifier"),
                        Text(
                            " · filename: "
                            + known(row.get("filename") or row.get("external_object")),
                            style="mf.secondary",
                        ),
                    )
                )
    paging = obj(value["pagination"])
    totals = obj(value["totals"])
    console.print(
        f"Displayed {paging['returned']} of {paging['total']} matching {'groups' if action == 'coverage' else 'archives'} · offset {paging['offset']} · "
        + ("more available" if paging["has_more"] else "end of results")
    )
    console.print(
        f"{totals['total']} {'complete ' if action == 'coverage' else ''}local files · recorded bytes {size(totals['recorded_bytes'])} · unknown sizes {totals['unknown_size_count']}"
    )
    selected = obj(value.get("filters", {}))
    if selected.get("start") or selected.get("end"):
        console.print(
            "Requested ["
            + known(selected.get("start"))
            + ", "
            + known(selected.get("end"))
            + ") · missing bounds use the observed envelope."
        )
        console.print(
            f"Unusable intervals omitted by date filters: {totals['unusable_intervals_before_date_filter']}"
        )
    console.print(Text(str(value["semantics"]), style="mf.secondary"))
    if action == "coverage":
        console.print(
            Text(
                "Gaps are inside the observed envelope unless bounded; use --gaps for missing intervals. Coverage includes every matching file before group pagination.",
                style="mf.secondary",
            )
        )


def render_details(console: Console, row: dict[str, object]) -> None:
    console.print(Text(str(row["local_id"]), style="mf.identifier"))
    sections: list[tuple[str, tuple[str, ...]]] = [
        (
            "Source identity and scope",
            (
                "origin",
                "external_source_key",
                "external_source_root",
                "source_object_key",
                "exchange",
                "instrument_type",
                "market_category",
                "scope_kind",
                "scope_identifier",
                "base_asset",
                "base_asset_source",
                "quote_asset",
                "quote_asset_source",
            ),
        ),
        (
            "Nominal interval [start, end)",
            ("data_type", "feed", "feed_source", "start_at", "end_at"),
        ),
        ("Local file", ("resolved_path", "relative_path", "filename", "bytes_written")),
        (
            "Checksum and source validators",
            (
                "sha256",
                "registered_source_sha256",
                "source_etag",
                "source_last_modified",
                "registered_source_etag",
                "registered_source_last_modified",
            ),
        ),
        (
            "Stored timestamps (no new verification)",
            ("downloaded_at", "last_verified_at", "created_at", "updated_at"),
        ),
        (
            "Status and readiness",
            ("status", "failure_message", "normalization_readiness", "format_profile"),
        ),
    ]
    for heading, fields in sections:
        console.print(Text(heading, style="mf.heading"))
        t = table("Field", "Value")
        for key in fields:
            raw = row.get(key)
            text = (
                size(raw)
                if key == "bytes_written"
                else utc(raw)
                if key.endswith("_at") or "last_modified" in key
                else known(raw)
            )
            style = (
                "mf.warning"
                if raw is None
                else "mf.failure"
                if key == "failure_message"
                else "mf.identifier"
                if key in {"sha256", "resolved_path"}
                else "mf.body"
            )
            t.add_row(
                key.replace("_", " "),
                record_state(raw) if key == "status" else Text(text, style=style),
            )
        console.print(t)
    for item in array(row.get("diagnostics", [])):
        console.print(Text(str(item), style="mf.warning"))
    console.print(
        Text(
            "Stored registry details only; no file, checksum, or event verification performed.",
            style="mf.secondary",
        )
    )
