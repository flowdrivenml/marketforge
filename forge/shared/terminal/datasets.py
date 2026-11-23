"""Terminal tables for individual dataset inspection."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any, cast

from rich import box
from rich.cells import cell_len
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from forge.shared.database.connection import database_flag
from forge.shared.terminal.markets import state
from forge.shared.terminal.storage import size

Row = dict[str, Any]
IDENTITY_FIELDS = ("exchange", "symbol", "instrument_type", "market_category", "instrument_id")
CONTENT = {
    "trade": "Trades",
    "trades": "Trades",
    "l2": "Depth (L2)",
    "depth": "Depth (L2)",
    "order_book_l2": "Depth (L2)",
    "trade_l2": "Trades + Depth",
    "combined": "Trades + Depth",
}
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
        table.add_column(
            header,
            overflow="fold",
            no_wrap=header in {"START (UTC)", "END (UTC)", "TIME (UTC)", "OBSERVED INTERVAL"},
        )
    return table


def _heading(console: Console, title: str) -> None:
    console.print(Text(f"MARKETFORGE  /  {title}", style="mf.section"))
    console.print(Rule(style="mf.secondary"))


def _section(console: Console, title: str) -> None:
    console.print()
    console.print(Text(title, "mf.heading"))


def _details(console: Console, fields: list[tuple[str, str | Text]]) -> None:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="mf.secondary", no_wrap=True)
    grid.add_column(overflow="fold")
    for key, value in fields:
        grid.add_row(key, value if isinstance(value, Text) else Text(value))
    console.print(grid)


def _content(value: object) -> str:
    return CONTENT.get(str(value), _label(value))


def _file_identity(console: Console, file: Row) -> None:
    identities = _members(file)
    if not identities:
        console.print(Text("Membership unresolved", "mf.warning"))
    elif len(identities) > 1:
        console.print(
            Text(f"Mixed membership · {len(identities)} canonical identities", "mf.secondary")
        )
    for identity in identities:
        label = _instrument(identity)
        label.append(f" · {_market(identity)}", "mf.identifier_detail")
        console.print(label)
    console.print(Text(_content(file.get("data_type")), "mf.secondary"))


def _responsive_table(console: Console, headers: tuple[str, ...], rows: list[list[Text]]) -> None:
    if not rows:
        console.print(Text("No records", "mf.secondary"))
        return
    needed = (
        sum(
            max(
                cell_len(header),
                *(
                    max((cell_len(line) for line in row[i].plain.splitlines()), default=0)
                    for row in rows
                ),
            )
            for i, header in enumerate(headers)
        )
        + len(headers) * 2
    )
    if needed <= console.width:
        table = _table(*headers)
        for row in rows:
            table.add_row(*row)
        console.print(table)
    else:
        for row in rows:
            _details(console, list(zip(headers, row, strict=True)))
            console.print()


def _identity_cell(members: tuple[tuple[Any, ...], ...]) -> Text:
    if not members:
        return Text("Unresolved membership", "mf.warning")
    result = Text()
    for index, member in enumerate(members):
        if index:
            result.append("\n")
        result.append_text(_instrument(member))
        result.append(f" · {_market(member)}", "mf.identifier_detail")
    return result


def _observed(value: int | None, *, precise: bool = False) -> str:
    text = _timestamps(value, short=False, precise=precise)
    return f"{text} UTC" if value is not None else text


def _show(console: Console, result: Row) -> None:
    file, publication, counts = result["file"], result["publication"], result["counts"]
    summary = file.get("summary", {})
    _file_identity(console, file)
    _section(console, "STATUS")
    console.print(_integrity(file))
    _section(console, "DATA")
    path = file.get("primary_file")
    _details(
        console,
        [
            ("File", PurePosixPath(path).name if path else "Unknown"),
            (
                "Format",
                "Apache Parquet"
                if file.get("storage_format") == "parquet"
                else _label(file.get("storage_format")),
            ),
            (
                "Rows",
                Text(
                    f"{summary['rows']:,}" if summary.get("rows") is not None else "Unknown",
                    "mf.success",
                ),
            ),
            ("Size", size(summary.get("size_bytes"))),
        ],
    )
    _section(console, "OBSERVED INTERVAL")
    _details(
        console,
        [
            ("Start", _observed(file.get("start_timestamp_ns"), precise=True)),
            ("End", _observed(file.get("end_timestamp_ns"), precise=True)),
            ("Continuity", "Unknown"),
        ],
    )
    _section(console, "PUBLICATION")
    _details(
        console,
        [
            ("Database ID", _label(publication.get("publication_id"))),
            ("Engine ID", _label(publication.get("engine_dataset_id"))),
            ("Operation", _label(file.get("operation"))),
        ],
    )
    _section(console, "PROVENANCE")
    _details(
        console,
        [
            ("Raw inputs", f"{counts['raw']:,} proven file contributions"),
            ("Parent datasets", f"{counts['datasets']:,} proven file contributions"),
            ("Streams", str(len(summary.get("memberships", [])))),
            ("Membership scope", _label(summary.get("membership_scope"))),
        ],
    )
    _section(console, "ACTIONS")
    for action in ("streams", "lineage", "schema", "head", "verify"):
        console.print(Text(f"mf datasets {action} {file['id']} {database_flag()}", "mf.identifier"))


def _streams(console: Console, result: Row) -> None:
    members = result["streams"]
    console.print(
        Text(
            f"{len(members)} stream {'membership' if len(members) == 1 else 'memberships'}"
            f" · Scope {result['scope']}",
            "mf.secondary",
        )
    )
    data_type = result["file"].get("data_type")
    rows = [
        [
            _instrument(_identity(member)),
            Text(_market(_identity(member)), "mf.identifier_detail"),
            Text(_content(member.get("kind") or (None if data_type == "trade_l2" else data_type))),
            Text(_label(member.get("instrument_id")), "mf.dataset_id"),
            Text(
                _observed(member.get("start_timestamp_ns"))
                + "\n"
                + _observed(member.get("end_timestamp_ns"))
            ),
            Text(_label(member.get("stream_id")), "mf.identifier"),
        ]
        for member in members
    ]
    _responsive_table(
        console,
        ("INSTRUMENT", "MARKET", "DATA", "INSTRUMENT ID", "OBSERVED PERIOD", "STREAM ID"),
        rows,
    )


def _archive_extent(archive: Row) -> str:
    times = [
        value.astimezone(UTC).isoformat(timespec="seconds") if value is not None else "Unknown"
        for value in (archive.get("start_at"), archive.get("end_at"))
    ]
    return " → ".join(times)


def _lineage_rows(console: Console, raw: list[Row], parents: list[Row], *, checksums: bool) -> None:
    if raw:
        _section(console, "RAW ARCHIVES")
        for archive in raw:
            console.print(
                Text.assemble(
                    (f"#{archive['id']}", "mf.dataset_id"),
                    " · ",
                    (_label(archive.get("exchange")).upper(), "mf.identifier"),
                    " · ",
                    _label(archive.get("filename")),
                )
            )
            scope = f"{_label(archive.get('scope_kind'))} {_label(archive.get('scope'))}"
            console.print(
                Text(f"    {scope} · Source interval {_archive_extent(archive)}", "mf.secondary")
            )
            if checksums:
                console.print(Text(f"    SHA-256 {_label(archive.get('sha256'))}", "mf.secondary"))
    if parents:
        _section(console, "INPUT DATASETS")
        for parent in parents:
            path = parent.get("primary_file")
            filename = PurePosixPath(path).name if path else "Unresolved file"
            console.print(
                Text.assemble(
                    (f"#{parent['id']}", "mf.dataset_id"),
                    f" · {_content(parent.get('data_type'))} · {filename}",
                )
            )
            console.print(
                _identity_cell(
                    tuple(
                        _identity(member)
                        for member in cast(list[Row], parent.get("memberships") or [])
                    )
                )
            )


def _lineage(console: Console, result: Row, *, include_identity: bool) -> None:
    if include_identity:
        _file_identity(console, result["file"])
    _section(console, "FILE-LEVEL CONTRIBUTIONS")
    counts = result["counts"]
    console.print(
        Text(
            f"{counts['raw']:,} raw archives · {counts['datasets']:,} parent datasets", "mf.success"
        )
    )
    _lineage_rows(console, result["raw"], result["parents"], checksums=result["checksums"])
    _section(console, "PUBLICATION-LEVEL SELECTION")
    selection = result["selected"]
    console.print(
        Text(
            f"Publication #{result['file']['publication_id']} · "
            f"{_label(selection.get('raw'))} raw archives · "
            f"{_label(selection.get('datasets'))} selected datasets"
        )
    )
    console.print(
        Text(
            "Selection is shared publication evidence. File contributions are listed separately.",
            "mf.secondary",
        )
    )
    if "publication_raw" in result:
        _lineage_rows(
            console,
            result["publication_raw"],
            result["publication_datasets"],
            checksums=result["checksums"],
        )
    else:
        console.print(Text("Use --publication to expand the selection.", "mf.identifier"))
    console.print(
        Text(
            f"Up to {result['limit']} rows per section · Adjust --limit or use global --json for complete evidence",
            "mf.secondary",
        )
    )


def _schema(console: Console, result: Row) -> None:
    columns = result["columns"]
    rows: list[list[Text]] = []
    for column in columns:
        name, type_name = column["name"], column["type"]
        rows.append(
            [
                Text(
                    name,
                    "mf.identifier"
                    if name.endswith("_timestamp_ns") or type_name.startswith("timestamp")
                    else "bold mf.body",
                ),
                Text(
                    type_name,
                    "mf.success" if type_name.startswith("decimal") else "mf.identifier_detail",
                ),
                Text(
                    "Yes" if column["nullable"] else "No",
                    "mf.secondary" if column["nullable"] else "mf.success",
                ),
            ]
        )
    _responsive_table(console, ("COLUMN", "ARROW TYPE", "NULLABLE"), rows)
    console.print(Text(f"{len(columns)} columns", "mf.secondary"))


def _preview_value(value: Any) -> str:
    if isinstance(value, Decimal):
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    if isinstance(value, dict):
        return "\n".join(
            f"{key}  {_preview_value(item)}" for key, item in cast(dict[str, Any], value).items()
        )
    if isinstance(value, (list, tuple)):
        return "\n".join(
            f"[{i}]  {_preview_value(item)}"
            for i, item in enumerate(cast(list[Any] | tuple[Any, ...], value))
        )
    return "—" if value is None else str(value)


def _preview(console: Console, result: Row, *, include_identity: bool) -> None:
    file, records = result["file"], result["rows"]
    if include_identity:
        _file_identity(console, file)
    total = file.get("summary", {}).get("rows")
    console.print(
        Text(
            f"{len(records)} of {total:,} stored rows"
            if total is not None
            else f"{len(records)} stored rows",
            "mf.secondary",
        )
    )
    if not records:
        console.print(Text("No rows in preview", "mf.warning"))
        return
    names: list[str] = result["projection"] or list(records[0])
    if result["compact"]:
        names = [
            name
            for name in names
            if name not in {"quantity_base", "quantity_quote", "quantity_contracts"}
            or any(record.get(name) is not None for record in records)
        ]
        # Keep another denomination visible if the base quantity is absent on any row.
        if "quantity_base" in names and all(
            record.get("quantity_base") is not None for record in records
        ):
            names = [name for name in names if name not in {"quantity_quote", "quantity_contracts"}]
    labels = {
        "event_timestamp_ns": "TIME (UTC)",
        "side": "SIDE",
        "price": "PRICE",
        "quantity_base": "BASE QTY",
        "quantity_quote": "QUOTE QTY",
        "quantity_contracts": "CONTRACTS",
        "trade_id": "TRADE ID",
        "order_count": "ORDERS",
        "source_operation": "SOURCE OP",
        "sequence_start": "FIRST SEQ",
        "sequence_end": "LAST SEQ",
        "canonical_row_count": "SOURCE ROWS",
        "event_kind": "CONTENT",
        "trade": "TRADE",
    }
    rows: list[list[Text]] = []
    for record in records:
        cells: list[Text] = []
        for name in names:
            value = record.get(name)
            if name.endswith("_timestamp_ns") and isinstance(value, int):
                cells.append(Text(_observed(value, precise=result["precise"]), "mf.body"))
            elif result["compact"] and name == "event_kind":
                cells.append(Text({0: "Trades", 1: "Depth (L2)"}.get(value, _preview_value(value))))
            elif result["compact"] and name == "trade" and isinstance(value, dict):
                payload = cast(dict[str, Any], value)
                cells.append(
                    Text(
                        _preview_value(
                            {
                                key: payload[key]
                                for key in ("side", "price", "quantity_base", "trade_id")
                                if payload.get(key) is not None
                            }
                        )
                    )
                )
            elif name == "side" and isinstance(value, str):
                cells.append(
                    Text(
                        value.upper(),
                        {
                            "buy": "mf.success",
                            "bid": "mf.success",
                            "sell": "mf.failure",
                            "ask": "mf.failure",
                        }.get(value.lower(), "mf.body"),
                    )
                )
            else:
                cells.append(
                    Text(
                        _preview_value(value),
                        "mf.success" if isinstance(value, Decimal) else "mf.body",
                    )
                )
        rows.append(cells)
    headers = tuple(labels.get(name, name) if result["compact"] else name for name in names)
    _responsive_table(console, headers, rows)
    if not result["precise"] and any(name.endswith("_timestamp_ns") for name in names):
        console.print(
            Text("Displayed to whole seconds · Use --precise for nanoseconds", "mf.secondary")
        )


def render_inspection(
    console: Console, action: str, result: Row, *, show_header: bool = True
) -> None:
    if result.get("status") == "superseded":
        if show_header:
            _heading(console, f"DATASET #{result['legacy_dataset_id']}")
        else:
            console.print(Text(f"Former registry file reference · {result['legacy_dataset_id']}"))
        console.print(Text("Superseded aggregate · Select a replacement file ID", "mf.warning"))
        rows = [
            [Text(str(file["id"]), "mf.dataset_id"), Text(file["primary_file"])]
            for file in result["replacement_files"]
        ]
        _responsive_table(console, ("ID", "PRIMARY FILE"), rows)
        return
    dataset_id = result["id"] if "id" in result else result["file"]["id"]
    suffix = {
        "show": "",
        "streams": " / STREAMS",
        "lineage": " / LINEAGE",
        "schema": " / PARQUET SCHEMA",
        "head": " / PREVIEW",
    }[action]
    if show_header:
        _heading(console, f"DATASET #{dataset_id}{suffix}")
    if action == "lineage":
        _lineage(console, result, include_identity=show_header)
    elif action == "head":
        _preview(console, result, include_identity=show_header)
    else:
        {"show": _show, "streams": _streams, "schema": _schema}[action](console, result)
