"""Tokyo Night cleanup previews and per-target application outcomes."""

from datetime import datetime

from rich.console import Console
from rich.text import Text

from forge.markets.models import array, obj
from forge.shared.terminal.markets import table
from forge.shared.terminal.storage import content, identity, product, size, utc


def display_time(value: object) -> str:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return "unknown"
    return utc(value)


def render_cleanup(console: Console, value: dict[str, object]) -> None:
    console.print(Text(f"Storage · {value['operation']} · {value['mode']}", style="mf.heading"))
    summary = obj(value["totals"])
    if value.get("interactive_review") and summary["eligible"]:
        console.print(
            "Interactive prune shows all selected targets before confirmation; display pagination does not restrict selection.",
            style="mf.secondary",
        )
    console.print(
        f"Selected {summary['selected']} · {summary['eligible']} records eligible for retirement · {summary.get('partial_files_found', 0)} partial files found · blocked {summary['blocked']} · reclaimable {size(summary['reclaimable_bytes'])}"
    )
    if value["mode"] == "apply":
        for item in array(value["outcomes"]):
            row = obj(item)
            status = str(row["status"])
            console.print(
                Text.assemble(
                    Text(str(row["local_id"]), style="mf.identifier"),
                    Text(
                        " · " + status + " · reclaimed " + size(row.get("reclaimed_bytes")),
                        style="mf.success"
                        if status in {"deleted", "already_deleted", "removed_partial", "absent"}
                        else "mf.failure",
                    ),
                )
            )
            if row.get("reason"):
                console.print(Text(str(row["reason"]), style="mf.warning"))
            if row.get("recovery"):
                console.print(Text(str(row["recovery"]), style="mf.warning"))
        if value.get("result_totals"):
            actual = obj(value["result_totals"])
            if value["operation"] == "delete":
                console.print(
                    f"{actual.get('records_deleted', 0)} records deleted · {actual.get('archive_files_removed', 0)} archive files removed · {actual.get('partial_files_removed', 0)} partial files removed · {size(actual['reclaimed_bytes'])} reclaimed"
                )
            else:
                console.print(
                    f"{actual['retired_failed_reservations']} failed records retired · {actual.get('partial_files_removed', actual.get('removed_partials', 0))} partial files removed · {size(actual['reclaimed_bytes'])} reclaimed"
                )
        console.print(Text("Receipt: " + str(value["receipt_path"]), style="mf.secondary"))
        return
    rows = [obj(row) for row in array(value["targets"])]
    paging = obj(value["pagination"])
    start = int(str(paging["offset"]))
    limit = int(str(paging["limit"])) if paging["limit"] is not None else None
    shown = rows[start:] if limit is None else rows[start : start + limit]
    if not rows:
        console.print(
            "No eligible failed records to retire."
            if value["operation"] == "prune"
            else "No matching targets."
        )
        return
    narrow = console.width < 110
    t = table(
        *(
            ["Local ID / instrument", "Full UTC interval / data", "Bytes / eligibility"]
            if narrow
            else [
                "Local ID / instrument",
                "Full UTC interval / data",
                "Actual / reclaimable",
                "Eligibility",
            ]
        )
    )
    t.columns[2].justify = "right"
    if narrow:
        t.columns[0].min_width = 14
        t.columns[1].min_width = 23
    for row in shown:
        record = obj(row["record"])
        local = Text(f"local:{row['local_id']}", style="mf.identifier")
        local.append("\n")
        local.append(identity(record))
        local.append(
            "\n" + str(record.get("exchange")) + " · " + product(record), style="mf.secondary"
        )
        state = str(row["state"])
        state_text = Text(state, style="mf.success" if state == "eligible" else "mf.warning")
        bounds = (
            content(record)
            + "\n"
            + display_time(record.get("start_at"))
            + "\n→ "
            + display_time(record.get("end_at"))
            + " (exclusive)"
        )
        sizes = Text(size(row["actual_bytes"]) + "\n" + size(row["reclaimable_bytes"]))
        if narrow:
            sizes.append("\n")
            sizes.append(state_text)
            t.add_row(local, bounds, sizes)
        else:
            t.add_row(local, bounds, sizes, state_text)
    console.print(t)
    for row in shown:
        console.print(
            Text(
                f"local:{row['local_id']} · " + str(row.get("path") or "path unknown"),
                style="mf.identifier",
            )
        )
        record = obj(row["record"])
        console.print(
            Text(
                "Full interval: ["
                + display_time(record.get("start_at"))
                + ", "
                + display_time(record.get("end_at"))
                + ") · "
                + content(record),
                style="mf.secondary",
            )
        )
        console.print(Text("Action: " + str(row["action"]), style="mf.secondary"))
        for blocker in array(row.get("blockers", [])):
            console.print(Text(str(blocker), style="mf.warning"))
    console.print(
        f"Displayed {len(shown)} of {len(rows)} selected targets. Selection is independent of display pagination."
    )
    if summary["eligible"] and not value["review_complete"]:
        console.print(
            Text(
                "Apply is blocked: some targets were hidden. Repeat the preview with --all or --json.",
                style="mf.warning",
            )
        )
    if not rows:
        console.print("No matching targets; no files removed.")
    console.print(
        Text(
            "Date selection: " + str(value["date_mode"]) + "; full archive bounds are shown.",
            style="mf.secondary",
        )
    )
    console.print(Text(str(value["byte_semantics"]), style="mf.secondary"))
    if not summary["eligible"]:
        console.print("No eligible targets; no files removed or records retired.")
        return
    if value["operation"] == "delete":
        console.print(Text(str(value["dependency_check"]), style="mf.warning"))
    console.print(Text("Preview saved: " + str(value["preview_path"]), style="mf.secondary"))
    console.print(Text("After reviewing: " + str(value["apply_command"]), style="mf.identifier"))
