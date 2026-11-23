"""Terminal presentation for the shared Doctor report."""

from rich.console import Console
from rich.table import Table
from rich.text import Text

from forge.startup.diagnostics import (
    CATEGORIES,
    STATUSES,
    Category,
    Check,
    DiagnosticReport,
    Readiness,
    Status,
)

STYLE: dict[Status, tuple[str, str, str]] = {
    "passed": ("✓", "mf.success", "Passed"),
    "warning": ("!", "mf.warning", "Warnings"),
    "failed": ("✗", "mf.failure", "Failed"),
    "unknown": ("?", "mf.secondary", "Unknown"),
}


def badge(status: Status) -> Text:
    glyph, style, _ = STYLE[status]
    return Text(glyph, style=style)


def highest_status(checks: list[Check]) -> Status:
    for status in ("failed", "unknown", "warning"):
        if any(check.status == status for check in checks):
            return status
    return "passed"


def category_summary(category: Category, checks: list[Check]) -> str:
    passed = {check.id: check.observed for check in checks if check.status == "passed"}
    if category == "Rust Engines":
        working = [check.label for check in checks if check.status == "passed"]
        return (
            "Checksum verified · " + ", ".join(working)
            if working
            else "Managed installations require attention"
        )
    if category == "PostgreSQL Tools":
        return (
            "Available · " + ", ".join(check.label for check in checks if check.status == "passed")
            if passed
            else "Backup and restore tools missing"
        )
    if category == "Environment":
        return (
            " · ".join(
                filter(
                    None,
                    (
                        f"Python {passed['environment.python']}"
                        if "environment.python" in passed
                        else "",
                        passed.get("environment.workspace"),
                        passed.get("environment.config"),
                    ),
                )
            )
            or "Workspace prerequisites require attention"
        )
    if category == "Database":
        return (
            " · ".join(
                passed[key]
                for key in (
                    "database.connection",
                    "database.identity",
                    "database.registry",
                    "database.migrations",
                )
                if key in passed
            )
            or "PostgreSQL readiness could not be established"
        )
    return (
        " · ".join(
            filter(
                None,
                (
                    passed.get("storage.root"),
                    f"{passed['storage.capacity']} available"
                    if "storage.capacity" in passed
                    else "",
                ),
            )
        )
        or "Data filesystem requires attention"
    )


def render_doctor(console: Console, report: DiagnosticReport) -> None:
    with console:
        header = Table.grid(expand=True)
        header.width = min(console.width, 100)
        header.add_column()
        header.add_column(justify="right")
        header.add_row(
            Text("MarketForge · Doctor", style="mf.heading"),
            Text(report.environment.upper(), style="mf.identifier"),
        )
        console.print(header)
        console.print(
            Text(report.checked_at.strftime("Checked  %Y-%m-%d %H:%M UTC"), style="mf.secondary")
        )
        if report.project_root:
            console.print(Text(f"Project  {report.project_root}"))
        console.print()

        categories: dict[Category, list[Check]] = {
            category: [check for check in report.checks if check.category == category]
            for category in CATEGORIES
        }
        overview = Table.grid(padding=(0, 2))
        overview.add_column(width=1)
        overview.add_column(min_width=16)
        overview.add_column()
        for category, checks in categories.items():
            overview.add_row(
                badge(highest_status(checks)),
                Text(category, style="mf.identifier"),
                Text(category_summary(category, checks)),
            )
        console.print(overview)

        for category, checks in categories.items():
            details = [
                check
                for check in checks
                if check.status != "passed"
                and (check.status != "unknown" or check.action or check.detail)
            ]
            if not details:
                continue
            console.print()
            console.print(Text(category, style="mf.section"))
            table = Table.grid(padding=(0, 2))
            table.add_column(width=1)
            table.add_column(min_width=16, max_width=22)
            table.add_column()
            for check in details:
                observation = Text(check.observed, style=STYLE[check.status][1])
                if check.detail:
                    observation.append(f"\n{check.detail}", style="mf.body")
                table.add_row(badge(check.status), Text(check.label), observation)
            console.print(table)

        console.print()
        console.print(Text("Diagnostic summary", style="mf.heading"))
        counts = Text("  ")
        summary = report.summary
        for status in STATUSES:
            if status == "unknown" and not summary[status]:
                continue
            glyph, style, label = STYLE[status]
            if status == "warning" and summary[status] == 1:
                label = "Warning"
            counts.append(f"{glyph} {summary[status]} {label}", style=style)
            counts.append("    ")
        counts.rstrip()
        console.print(counts)
        readiness_styles: dict[Readiness, Status] = {
            "ready": "passed",
            "ready_with_warnings": "warning",
            "not_ready": "failed",
            "indeterminate": "unknown",
        }
        style = STYLE[readiness_styles[report.readiness]][1]
        console.print(Text(f"  Status: {report.readiness.replace('_', ' ').upper()}", style=style))

        actions = dict.fromkeys(check.action for check in report.checks if check.action)
        if actions:
            console.print()
            console.print(Text("Next steps", style="mf.heading"))
            for index, action in enumerate(actions, 1):
                first, *rest = action.splitlines()
                console.print(Text(f"  {index}. {first}"))
                for line in rest:
                    console.print(
                        Text(
                            f"     {line}",
                            style="mf.identifier" if line.startswith("mf ") else "mf.body",
                        )
                    )
