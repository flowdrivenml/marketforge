"""MarketForge's Textual surfaces and semantic Rich styles."""

from rich.markup import escape
from rich.text import Text
from textual.app import RenderResult
from textual.theme import Theme
from textual.widgets import Static

from forge.shared.terminal.palette import BG, BLUE, CYAN, FG, GREEN, RED, YELLOW

PANEL = "#202231"
FIELD = "#24283b"
OUTLINE = "#363c57"
SECONDARY = "#a1aacf"

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
        return Text.assemble((brand, f"bold {FG}"), (separator, SECONDARY), (area, f"bold {BLUE}"))
