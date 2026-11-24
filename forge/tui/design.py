"""MarketForge's Textual surfaces and semantic Rich styles."""

from rich.markup import escape
from rich.style import Style
from rich.text import Text
from textual.app import RenderResult
from textual.theme import Theme
from textual.widgets import Static

from forge.shared.terminal.palette import BG, BLUE, CYAN, FG, GREEN, MUTED, RED, YELLOW

PANEL = "#202231"
FIELD = "#24283b"
OUTLINE = "#363c57"
SECONDARY = "#a1aacf"
TEXT_SELECTION = Style(color=BG, bgcolor=CYAN, reverse=False)

STATUS_COLORS = {
    **dict.fromkeys(
        (
            "complete",
            "completed",
            "success",
            "valid",
            "validated",
            "verified",
            "reused",
            "published",
            "registered",
            "installed",
            "ready",
            "rust validated",
            "available",
        ),
        GREEN,
    ),
    **dict.fromkeys(("failed", "corrupt", "missing", "error", "invalid"), RED),
    **dict.fromkeys(
        (
            "blocked",
            "partial",
            "stale",
            "interrupted",
            "stopped",
            "pending",
            "queued",
            "modified",
            "degraded",
            "warning",
        ),
        YELLOW,
    ),
    **dict.fromkeys(("prepared", "running", "executing", "interrupting"), CYAN),
    **dict.fromkeys(("waiting", "skipped", "unknown", "unavailable", "not_available", "—"), MUTED),
}


def utilization(percent: float | None) -> str:
    if percent is None:
        return MUTED
    if percent >= 97:
        return RED
    return YELLOW if percent >= 90 else FG


THEME = Theme(
    name="marketforge",
    primary=BLUE,
    secondary=CYAN,
    accent=CYAN,
    success=GREEN,
    warning=YELLOW,
    error=RED,
    background=BG,
    foreground=FG,
    surface=FIELD,
    panel=PANEL,
    variables={"screen-selection-background": CYAN, "screen-selection-foreground": BG},
)


def success_message(message: str) -> str:
    """Use Textual's standard toast with an explicit success indicator."""
    return f"[{GREEN}]✓[/] {escape(message)}"


class WorkspaceTitle(Static):
    """Emphasize the area within the existing one-line workspace title."""

    def render(self) -> RenderResult:
        if not isinstance(self.content, str):
            return super().render()
        brand, separator, area = self.content.partition(" · ")
        return Text.assemble((brand, f"bold {BLUE}"), (separator, MUTED), (area, FG))
