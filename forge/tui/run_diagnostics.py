"""The searchable diagnostic panel embedded in a retained run inspection."""

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

from rich.text import Text
from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.timer import Timer
from textual.widgets import Button, Input, Select, Static

from forge.historical.archives.storage.files import disk
from forge.orchestration.run_diagnostics import DiagnosticPage, DiagnosticReader
from forge.shared.presentation.run_diagnostics import Diagnostic, explain
from forge.shared.terminal.palette import BLUE, CYAN, MUTED, RED, YELLOW
from forge.tui.clipboard import contains_credentials, plain, safe_text
from forge.tui.editor_inspection import OpenInspection, RetainedFile
from forge.tui.presentation import status
from forge.tui.screens import Detail, WorkspaceHost

STAGE_LABELS = {
    "acquire": "Acquisition",
    "acquisition": "Acquisition",
    "normalize": "Normalization",
    "merge": "Merge",
    "convert": "Conversion",
}
COLORS = {"error": RED, "warning": YELLOW, "info": CYAN}
VIEW_PAGE = 20


def stage_label(stage: str) -> str:
    return STAGE_LABELS.get(stage, stage.replace("_", " ").capitalize())


def severity_label(entry: Diagnostic) -> str:
    return "OUTPUT" if entry.source.path.suffix == ".log" else entry.severity.upper()


def technical(entry: Diagnostic) -> str:
    pairs: dict[str, object] = {
        "Run ID": entry.source.run_id,
        "Operation": entry.source.operation,
        "Source file": str(entry.source.path),
    }
    for key, value in (
        ("Stage", stage_label(entry.source.stage)),
        ("Attempt ID", entry.source.attempt),
        ("Recorded time", entry.timestamp),
        ("JSON pointer", entry.pointer),
        ("Log line", entry.line),
    ):
        if value:
            pairs[key] = value
    return plain({**pairs, **dict(entry.metadata)})


def searchable_entries(entries: tuple[Diagnostic, ...]) -> list[tuple[Diagnostic, str]]:
    return [
        (
            entry,
            safe_text(
                f"{severity_label(entry)} {entry.severity} {entry.source.stage} "
                f"{entry.message}\n{technical(entry)}\n{explain(entry)}"
            ).casefold(),
        )
        for entry in entries
    ]


class ErrorsPanel(Vertical):
    def __init__(
        self,
        reader: DiagnosticReader,
        load: Callable[[], Awaitable[DiagnosticPage]],
        child: Callable[[str], object],
        updated: Callable[[str], None],
    ) -> None:
        super().__init__(id="run-errors-panel")
        self.reader, self.load, self.child, self.updated = reader, load, child, updated
        self.entries: list[tuple[Diagnostic, str]] = []
        self.page: DiagnosticPage | None = None
        self.timer: Timer | None = None
        self.page_offset = 0
        self.visible_entries: list[Diagnostic] = []
        self.occurrences: dict[int, list[Diagnostic]] = {}
        self.loading = False
        self.failure = ""

    def compose(self) -> ComposeResult:
        yield Static(id="errors-summary", markup=False)
        with Horizontal(id="errors-filters"):
            yield Input(placeholder="Search loaded diagnostics", id="errors-search")
            yield Select(
                [
                    ("Errors + warnings", "issues"),
                    ("All diagnostics", "all"),
                    ("Errors", "error"),
                    ("Warnings", "warning"),
                    ("Info / output", "info"),
                ],
                value="issues",
                allow_blank=False,
                id="errors-severity",
            )
            yield Select([("All stages", "all")], value="all", allow_blank=False, id="errors-stage")
        yield Static(id="errors-coverage", markup=False)
        with VerticalScroll(id="errors-scroll"):
            yield Vertical(id="errors-cards")
        with Horizontal(id="errors-navigation"):
            yield Button("Previous", id="errors-previous", classes="compact")
            yield Button("Next", id="errors-next", classes="compact")
            yield Button("Load more evidence", id="errors-load", classes="compact")
            yield Static(id="errors-range", markup=False)

    def activate(self) -> None:
        if self.page is None and not self.loading and not self.failure:
            self.read_page()
        else:
            self.render_page()

    @work(group="retained-diagnostics", exit_on_error=False)
    async def read_page(self) -> None:
        if self.loading:
            return
        self.loading = True
        try:
            self.query_one("#errors-load", Button).disabled = True
            self.query_one("#errors-coverage", Static).update("Reading retained evidence…")
            self.page = await self.load()
            self.entries.extend(await disk(searchable_entries, self.page.entries))
            select = cast(Select[str], self.query_one("#errors-stage", Select))
            current = select.value
            stages = dict.fromkeys(
                [stage.name for stage in self.page.stages]
                + [entry.source.stage for entry, _ in self.entries if entry.source.stage]
            )
            select.set_options(
                [("All stages", "all"), *((stage_label(stage), stage) for stage in stages)]
            )
            if isinstance(current, str) and current in stages:
                select.value = current
        except Exception as error:
            self.failure = safe_text(str(error))
        finally:
            self.loading = False
        self.render_page()

    @work(exclusive=True, group="diagnostic-cards", exit_on_error=False)
    async def render_page(self) -> None:
        await self.show_page()

    async def show_page(self) -> None:
        page = self.page
        errors = sum(entry.severity == "error" for entry, _ in self.entries)
        warnings = sum(entry.severity == "warning" for entry, _ in self.entries)
        complete = page is not None and not page.more and not page.notices and not self.failure
        self.updated(f"Errors ({errors})" if complete else "Errors")
        heading = Text(
            f"{self.reader.status.replace('_', ' ').upper()}  ·  {self.reader.name}", BLUE
        )
        error_count = f"{errors} error" + ("" if errors == 1 else "s")
        warning_count = f"{warnings} warning" + ("" if warnings == 1 else "s")
        heading.append(f"\n{error_count} · {warning_count}" + ("" if complete else " loaded"))
        if self.reader.environment:
            heading.append(f"  ·  {self.reader.environment.title()}", MUTED)
        self.query_one("#errors-summary", Static).update(heading)
        notices = page.notices if page else ()
        coverage = "More retained evidence is available." if page and page.more else ""
        if notices:
            coverage = " ".join(filter(None, (coverage, notices[-1], "See coverage details.")))
        self.query_one("#errors-coverage", Static).update(self.failure or coverage)
        search = self.query_one("#errors-search", Input).value.casefold().strip()
        severity = cast(Select[str], self.query_one("#errors-severity", Select)).value
        stage = cast(Select[str], self.query_one("#errors-stage", Select)).value
        matches = [
            entry
            for entry, text in self.entries
            if (not search or search in text)
            and (
                severity == "all"
                or severity == entry.severity
                or (
                    severity == "issues"
                    and (entry.severity != "info" or entry.source.path.suffix != ".log")
                )
            )
            and (stage == "all" or stage == entry.source.stage)
        ]
        if stage == "all" and page and page.stages:
            order = {item.name: i + 1 for i, item in enumerate(page.stages)}
            matches.sort(
                key=lambda entry: (
                    order.get(entry.source.stage, len(order) + 1) if entry.source.stage else 0
                )
            )
        # The attempt's Pipeline report declares the same canonical report path.
        # Group only that known mirror pair and retain both artifact references.
        groups: dict[tuple[tuple[str, str], str, Path], list[Diagnostic]] = {}
        for entry in matches:
            if (
                entry.explanation
                and entry.explanation.evidence
                and entry.explanation.mirror_identity
            ):
                key = (
                    entry.explanation.mirror_identity,
                    entry.message,
                    entry.explanation.evidence.path,
                )
                groups.setdefault(key, []).append(entry)
        grouped: dict[int, list[Diagnostic]] = {}
        hidden: set[int] = set()
        for group in groups.values():
            canonical = [entry for entry in group if entry.source.path.name == "report.json"]
            mirrors = [
                entry
                for entry in group
                if entry.source.path.name == "pipeline-report.json"
                and "attempts" in entry.source.path.parts
            ]
            if len(canonical) == len(mirrors) == 1:
                grouped[id(canonical[0])] = [canonical[0], mirrors[0]]
                hidden.add(id(mirrors[0]))
        matches = [entry for entry in matches if id(entry) not in hidden]
        self.page_offset = min(
            self.page_offset, (max(0, len(matches) - 1) // VIEW_PAGE) * VIEW_PAGE
        )
        visible = matches[self.page_offset : self.page_offset + VIEW_PAGE]
        self.query_one("#errors-range", Static).update(
            f"{self.page_offset + 1}–{self.page_offset + len(visible)} of {len(matches)} loaded matches"
            if visible
            else ""
        )
        self.query_one("#errors-next", Button).disabled = self.page_offset + VIEW_PAGE >= len(
            matches
        )
        self.query_one("#errors-previous", Button).disabled = self.page_offset == 0
        self.query_one("#errors-load", Button).disabled = self.loading or not bool(
            page and page.more
        )
        cards = self.query_one("#errors-cards", Vertical)
        await cards.remove_children()
        widgets: list[Vertical | Static | Button] = []
        if page:
            for number, item in enumerate(page.stages):
                if stage != "all" and stage != item.name:
                    continue
                row = Static(
                    Text.assemble(
                        (f"{stage_label(item.name)}  ·  ", CYAN),
                        status(item.state.replace("_", " ").upper()),
                        (f"  ·  {item.phase.replace('_', ' ')}" if item.phase else "", MUTED),
                    ),
                    classes="error-stage-label",
                )
                if item.child and item.child in self.reader.child_ids:
                    widgets.append(
                        Vertical(
                            row,
                            Button(
                                f"Inspect child {item.child}",
                                id=f"errors-child-{number}",
                                classes="compact",
                            ),
                            classes="error-stage",
                        )
                    )
                else:
                    widgets.append(row)
        self.visible_entries = visible
        self.occurrences = {
            index: grouped[id(entry)] for index, entry in enumerate(visible) if id(entry) in grouped
        }
        for index, entry in enumerate(visible):
            widgets.append(self.card(entry, index))
        if not visible:
            state = self.reader.status.casefold()
            if search or severity not in {"all", "issues"} or stage != "all":
                message = "No loaded diagnostics match these filters."
            elif page and page.more:
                message = (
                    "This page has no recorded diagnostic messages. Load more evidence to continue."
                )
            elif state in {"failed", "blocked", "interrupted", "cancelled", "partial"}:
                message = (
                    "No detailed diagnostics were found in the available evidence for this outcome."
                )
            else:
                message = "No recorded errors or warnings in the available evidence."
            widgets.append(Static(message, classes="error-empty", markup=False))
            if targets := tuple(self.reader.files.values())[:3]:
                widgets.append(
                    Vertical(
                        *(
                            OpenInspection(
                                RetainedFile(
                                    "Original evidence",
                                    target.path,
                                    self.reader.root,
                                    target.sha256,
                                ),
                                f"Open {target.path.name}",
                                id=f"errors-empty-source-{i}",
                            )
                            for i, target in enumerate(targets)
                        ),
                        classes="error-empty-evidence",
                    )
                )
        if notices:
            widgets.append(Button("Coverage details", id="errors-notices", classes="compact"))
        await cards.mount(*widgets)
        self.query_one("#errors-scroll", VerticalScroll).scroll_home(animate=False)

    def card(self, entry: Diagnostic, index: int) -> Vertical:
        label = severity_label(entry)
        stage = stage_label(entry.source.stage)
        title = Text(
            f"{label}  ·  {stage or entry.source.operation.capitalize()}",
            style=f"bold {COLORS[entry.severity]}",
        )
        raw = safe_text(entry.message)
        preview = "\n".join(raw.split("\n")[:5])[:650]
        if preview != raw:
            preview += "\n… Open details for the full retained message."
        metadata = "  ·  ".join(
            filter(
                None,
                (
                    entry.timestamp,
                    *(
                        f"{key.replace('_', ' ')} {plain(value)}"
                        for key, value in entry.metadata
                        if key
                        in {
                            "exchange",
                            "symbol",
                            "scope_identifier",
                            "scope",
                            "instrument",
                            "archive_id",
                            "local_id",
                            "remote_archive_id",
                            "job_id",
                            "task_id",
                            "publication_id",
                            "code",
                        }
                    ),
                    entry.source.path.name,
                ),
            )
        )
        if contains_credentials(entry.message):
            metadata += "  ·  Protected content redacted"
        evidence = RetainedFile(
            "Original diagnostic evidence", entry.source.path, self.reader.root, entry.source.sha256
        )
        explanation = explain(entry)
        sections = Text()
        if entry.severity != "info":

            def section(heading: str, value: str) -> None:
                if value:
                    sections.append(f"{heading}\n", f"bold {CYAN}")
                    sections.append(safe_text(value) + "\n")

            section("What happened", explanation.happened)
            if entry.severity == "error":
                section(
                    "Root cause",
                    "\n".join(explanation.cause)
                    if explanation.root_cause
                    else "Root cause not established by retained evidence.",
                )
                if explanation.cause and not explanation.root_cause:
                    section("Underlying diagnostics", "\n".join(explanation.cause))
            section(
                "Affected inputs",
                "\n".join(
                    f"{key.replace('_', ' ')}  {plain(value)}"
                    for key, value in explanation.affected
                ),
            )
            section("Impact", explanation.impact)
            section("Next step", explanation.resolution)
            section("Evidence limitation", explanation.limitation)
            sections.append("Original diagnostic", f"bold {CYAN}")
        related = explanation.evidence
        return Vertical(
            Static(title, markup=False),
            Static(safe_text(metadata), classes="error-meta", markup=False),
            *((Static(sections, classes="error-explanation", markup=False),) if sections else ()),
            Static(preview, classes="error-message", markup=False),
            Horizontal(
                Button("Details", id=f"errors-detail-{index}", classes="compact"),
                OpenInspection(
                    evidence,
                    "Open original log"
                    if evidence.path.suffix != ".json"
                    else "Open original JSON",
                    id=f"errors-source-{index}",
                ),
                classes="error-actions",
            ),
            *(
                (
                    OpenInspection(
                        RetainedFile(
                            "Preparation report", related.path, self.reader.root, related.sha256
                        ),
                        "Open preparation report"
                        if related.path.name == "prepare-report.json"
                        else "Open related report",
                        id=f"errors-preparation-{index}",
                    ),
                )
                if related
                else ()
            ),
            *(
                (
                    Button(
                        "View all recorded occurrences",
                        id=f"errors-occurrences-{index}",
                        classes="compact",
                    ),
                )
                if index in self.occurrences
                else ()
            ),
            classes="error-card",
        )

    @on(Input.Changed, "#errors-search")
    @on(Select.Changed, "#errors-severity, #errors-stage")
    def filter_changed(self) -> None:
        self.page_offset = 0
        if self.timer:
            self.timer.stop()
        self.timer = self.set_timer(0.18, self.render_page)

    @on(Button.Pressed)
    def pressed(self, event: Button.Pressed) -> None:
        key = event.button.id or ""
        if not key.startswith("errors-"):
            return
        event.stop()
        if key == "errors-load":
            self.read_page()
        elif key in {"errors-next", "errors-previous"}:
            self.page_offset += VIEW_PAGE if key == "errors-next" else -VIEW_PAGE
            self.render_page()
        elif key == "errors-notices" and self.page:
            source = self.reader.coverage_source
            cast(WorkspaceHost, self).app.push_screen(
                Detail(
                    "Evidence coverage",
                    "\n\n".join(self.page.notices),
                    raw=True,
                    editor_target=RetainedFile(
                        "Original evidence", source.path, self.reader.root, source.sha256
                    )
                    if source
                    else None,
                )
            )
        elif key.startswith("errors-child-") and self.page:
            self.child(self.page.stages[int(key.removeprefix("errors-child-"))].child)
        elif key.startswith("errors-occurrences-"):
            group = self.occurrences[int(key.removeprefix("errors-occurrences-"))]
            cast(WorkspaceHost, self).app.push_screen(
                Detail(
                    "Recorded occurrences",
                    "\n\n".join(
                        f"{safe_text(entry.message)}\n{technical(entry)}" for entry in group
                    ),
                    raw=True,
                    editor_target=RetainedFile(
                        "Attempt report",
                        group[-1].source.path,
                        self.reader.root,
                        group[-1].source.sha256,
                    ),
                )
            )
        elif key.startswith("errors-detail-"):
            entry = self.visible_entries[int(key.removeprefix("errors-detail-"))]
            message = safe_text(entry.message)
            redaction = (
                "\nProtected content was redacted." if contains_credentials(entry.message) else ""
            )
            cast(WorkspaceHost, self).app.push_screen(
                Detail(
                    f"{severity_label(entry)} · Recorded diagnostic",
                    f"{message}\n\nRecorded context\n{technical(entry)}{redaction}",
                    raw=True,
                    editor_target=RetainedFile(
                        "Original diagnostic evidence",
                        entry.source.path,
                        self.reader.root,
                        entry.source.sha256,
                    ),
                )
            )

    async def on_unmount(self) -> None:
        if self.timer:
            self.timer.stop()
        await disk(self.reader.close)
