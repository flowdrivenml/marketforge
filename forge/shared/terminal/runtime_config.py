"""Rich views of runtime settings and changes. All evidence comes from the service."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.text import Text

from forge.shared.documentation import CONVERSION_RUNTIME, NORMALIZATION_RUNTIME
from forge.shared.presentation.normalization import binary_size
from forge.shared.terminal.presentation import (
    data_table,
    display_path,
    documentation_link,
    header,
    reference,
    section,
    status_label,
    summary,
)

GROUPS = (
    ("Performance", ("merge_fan_in", "scratch_format", "scratch_codec", "resources.workers")),
    ("Resources", ("resources.",)),
    ("Parquet", ("resources.parquet.", "parquet.")),
    ("Partitioning", ("partitioning.",)),
    ("Diagnostics", ("diagnostics.",)),
    ("Integrity", ("integrity_policy.",)),
    ("Reconciliation and timestamp regression", ("reconciliation.", "timestamp_regression.")),
)
OVERVIEW_LABELS = {
    "resources.workers": "Workers",
    "merge_fan_in": "Merge fan-in",
    "scratch_format": "Scratch format",
    "scratch_codec": "Scratch codec",
    "resources.memory_budget_bytes": "Memory budget",
    "resources.scratch_budget_bytes": "Scratch budget",
    "resources.scratch_path": "Scratch directory",
    "resources.normalization_limits.open_files": "Open files",
    "resources.parquet.row_group_target_bytes": "Row group target",
    "resources.parquet.file_target_bytes": "File target",
    "diagnostics.mode": "Mode",
    "diagnostics.interval_ms": "Interval",
    "integrity_policy.enabled": "Enabled",
    "integrity_policy.parse_failure.action": "Parse failure",
    "integrity_policy.invalid_record.action": "Invalid record",
    "integrity_policy.sequence_gap.action": "Sequence gap",
    "integrity_policy.timestamp_regression.action": "Timestamp regression",
    "reconciliation.trades.overlap_policy": "Trades overlap",
    "reconciliation.depth.overlap_policy": "Depth overlap",
    "timestamp_regression.trades.policy": "Trade timestamps",
    "timestamp_regression.depth.policy": "Depth timestamps",
}


def group(path: str) -> str:
    # Parquet is a separate UI group nested under the TOML resources table.
    if path.startswith("resources.parquet."):
        return "Parquet"
    for label, prefixes in GROUPS:
        if any(path.startswith(key) if key.endswith(".") else path == key for key in prefixes):
            return label
    return "Schema identity"


def format_value(value: object, kind: str, *, compact: bool = False) -> str:
    if value is None:
        return "—"
    if type(value) is bool:
        return ("Yes" if value else "No") if compact else str(value).lower()
    if type(value) is int:
        if kind == "mib":
            return f"{value:,} MiB"
        if kind == "bytes":
            formatted = binary_size(value)
            return formatted if compact else f"{value:,} ({formatted})"
        if kind == "milliseconds":
            return f"{value:,} ms"
        return f"{value:,}"
    return str(value).replace("_", " ").capitalize() if compact and kind == "enum" else str(value)


def state(field: dict[str, Any]) -> str:
    if field["active"] is None:
        return "Unavailable"
    if field["configured_path"] is None and field["effective"] is None:
        return "Absent"
    if field["active"] is False:
        return "Inactive"
    return "Configured" if field["configured_path"] else "Default"


def inspection_table(console: Console, fields: Sequence[dict[str, Any]], *, schema: bool) -> None:
    table = data_table()
    headings = (
        ("Field", "Type", "Default", "State")
        if schema
        else ("Field", "Configured", "Effective", "Default", "State")
    )
    for label in headings:
        table.add_column(label, overflow="fold", ratio=4 if label == "Field" else 1)
    for field in fields:
        values = [field["path"]]
        if schema:
            values.append(field["kind"])
        else:
            values.extend(
                format_value(field[key], field["kind"]) for key in ("configured", "effective")
            )
        values.extend((format_value(field["default"], field["kind"]), state(field)))
        table.add_row(*(Text(value) for value in values))
    console.print(table)
    if not schema:
        reasons = dict.fromkeys(
            reason
            for field in fields
            if field["configured_path"]
            for reason in field["inactive_reasons"]
        )
        for reason in reasons:
            console.print(Text(f"Activation condition · {reason}", style="mf.secondary"))


def field_detail(console: Console, field: dict[str, Any]) -> None:
    kind = field["kind"]
    if field["required"]:
        required = (
            f"When {field['required_when']} is present" if field.get("required_when") else "Yes"
        )
    else:
        required = "No"
    lines = [
        ("Field", field["path"]),
        ("Type", "Integer (MiB)" if kind == "mib" else kind.capitalize()),
        ("Purpose", field["purpose"]),
        ("Configured", format_value(field["configured"], kind)),
        (
            "Effective",
            format_value(field["effective"], kind) if field["effective_known"] else "Unavailable",
        ),
        (
            "Default",
            format_value(field["default"], kind) if field["default"] is not None else "None",
        ),
        ("State", state(field)),
        ("Required", required),
        ("Mutable", "Yes" if field["mutable"] else "No"),
        ("Risk", field["risk"]),
    ]
    if field["configured_path"]:
        lines.append(("Source field", field["configured_path"]))
    if field["choices"]:
        lines.append(("Accepted values", ", ".join(field["choices"])))
    if field["constraint"]:
        lines.append(("Constraint", field["constraint"]))
    for dependency in field["dependencies"]:
        values = dependency.get("values")
        description = f" = {' or '.join(values)}" if values else " must be present"
        lines.append(("Effective when", dependency["path"] + description))
    summary(console, lines)


def render_changes(console: Console, report: dict[str, Any]) -> None:
    changes = report.get("changes", [])
    if not changes:
        if report.get("status") in {"ready_to_apply", "applied"}:
            console.print("TOML structure, formatting, or comments changed.")
        return
    section(console, "Changes")
    table = data_table()
    for column in ("Setting", "Current", "Proposed", "Risk"):
        table.add_column(column, overflow="fold")
    for change in changes:
        row = [change["path"]]
        for key in ("before", "after"):
            side = change[key]
            value = side["configured"] if side["configured_path"] else side["effective"]
            label = format_value(value, change["kind"])
            if not side["configured_path"] and side["effective_known"]:
                label += " (default)" if value is not None else " (absent)"
            if side["active"] is False and value is not None:
                label += " (inactive)"
            row.append(label)
        row.append(change["risk"])
        table.add_row(*(Text(value) for value in row))
    console.print(table)
    purposes: list[str] = list(
        dict.fromkeys(change["purpose"] for change in changes if change["configured_changed"])
    )
    if purposes:
        section(console, "Impact")
        for purpose in purposes[:5]:
            console.print(Text(purpose))
    if report.get("requires_risk_acknowledgement"):
        console.print(
            "Integrity-critical changes require explicit acknowledgement.", style="mf.warning"
        )


def render_runtime(
    console: Console, report: dict[str, Any], *, root: Path, documentation: bool = True
) -> None:
    action = report["action"]
    operation = report["kind"].removesuffix("_runtime_configuration")
    merge = operation == "merge"
    convert = operation == "convert"
    title = {
        "show": "Normalization Runtime",
        "get": "Runtime Setting",
        "schema": "Normalization Runtime Schema",
        "validate": "Normalization Runtime Validation",
    }.get(action, "Runtime Configuration Change")
    if merge or convert:
        title = f"{operation.title()} · {title.replace('Normalization ', '')}"
    header(console, title)
    details: list[tuple[str, str | Text]] = [
        ("Configuration", display_path(Path(report["configuration_path"]), root))
    ]
    if merge or convert:
        details.append(("Scope", "Base runtime settings"))
    if "runtime_schema_version" in report:
        details.append(("Schema", str(report["runtime_schema_version"])))
    status = report["status"]
    style = "complete" if status in {"applied", "unchanged", "valid"} else status
    details.append(("Status", status_label(style, label=status.replace("_", " ").upper())))
    summary(console, details)
    if merge and action in {"show", "schema"}:
        console.print(
            "Prepared jobs and pipeline stages can use pinned runtime snapshots and overrides.",
            style="mf.secondary",
        )
    if report.get("error"):
        console.print(Text(report["error"]["message"], style="mf.failure"))
    if report.get("warning"):
        console.print(Text(report["warning"], style="mf.warning"))
    if "fields" in report:
        fields = report["fields"]
        if len(fields) == 1 and action in {"schema", "get"}:
            console.print()
            field_detail(console, fields[0])
        else:
            for label in ("Schema identity", *(label for label, _ in GROUPS)):
                selected = [field for field in fields if group(field["path"]) == label]
                if not selected:
                    continue
                section(console, label)
                if report["view"] == "overview":
                    summary(
                        console,
                        (
                            (
                                OVERVIEW_LABELS[field["path"]],
                                format_value(
                                    field["configured"]
                                    if field["configured_path"]
                                    else field["effective"],
                                    field["kind"],
                                    compact=True,
                                )
                                + (" (inactive)" if field["active"] is False else ""),
                            )
                            for field in selected
                        ),
                    )
                else:
                    inspection_table(console, selected, schema=action == "schema")
    if report.get("validation", {}).get("rust") and action in {"validate", "set", "unset", "edit"}:
        section(console, "Validation")
        console.print("✓ Complete runtime configuration accepted by Rust", style="mf.success")
    if action in {"set", "unset", "edit"}:
        render_changes(console, report)
    if report.get("apply_command"):
        console.print()
        reference(console, "Apply", report["apply_command"])
    if report.get("draft_path"):
        console.print()
        reference(console, "Retained draft", report["draft_path"])
    if convert and action in {"show", "schema"}:
        console.print(
            "Changes apply to future preparation. Prepared jobs retain their settings.",
            style="mf.secondary",
        )
    if documentation and convert:
        documentation_link(console, CONVERSION_RUNTIME)
    if documentation and not (merge or convert):
        documentation_link(console, NORMALIZATION_RUNTIME)
