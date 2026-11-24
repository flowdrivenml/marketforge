"""Mouse selection over the visible terminal cells of a Textual table."""

from typing import Self

from rich.cells import split_graphemes
from rich.segment import Segment
from rich.text import Text
from textual import events
from textual.coordinate import Coordinate
from textual.geometry import Offset
from textual.strip import Strip
from textual.widgets import DataTable

from forge.tui.design import TEXT_SELECTION


class SelectableTable(DataTable[Text]):
    """Keep table activation on click and select displayed text on drag."""

    ALLOW_SELECT = False
    text_selection_enabled = True
    _origin: Offset | None = None
    _span: tuple[int, int, int] | None = None
    _dragged = False

    def clear_text_selection(self) -> None:
        if self._origin is not None:
            self.release_mouse()
        self._origin = None
        self._dragged = False
        if self._span is not None:
            self._span = None
            self.refresh()

    def clear(self, columns: bool = False) -> Self:
        self.clear_text_selection()
        super().clear(columns)
        return self

    def on_mouse_down(self, event: events.MouseDown) -> None:
        if not self.text_selection_enabled or event.button != 1:
            return
        self.clear_text_selection()
        row, column = event.style.meta.get("row"), event.style.meta.get("column")
        if (
            isinstance(row, int)
            and isinstance(column, int)
            and self.is_valid_coordinate(Coordinate(row, column))
        ):
            self._origin = event.get_content_offset(self)
            if self._origin is not None:
                self.focus()
                self.capture_mouse()

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if self._origin is None:
            return
        offset = event.get_content_offset_capture(self)
        if offset != self._origin:
            self._dragged = True
        if self._dragged:
            self._select_to(offset.x)

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if event.button == 1 and self._origin is not None:
            offset = event.get_content_offset_capture(self)
            if offset != self._origin or self._dragged:
                self._dragged = True
                self._select_to(offset.x)
            self._origin = None
            self.release_mouse()

    async def _on_click(self, event: events.Click) -> None:
        self.consume_drag_click(event)

    def consume_drag_click(self, event: events.Click) -> bool:
        if self._dragged and event.button == 1:
            self._dragged = False
            event.stop()
            event.prevent_default()
            return True
        return False

    def _select_to(self, end: int) -> None:
        assert self._origin is not None
        x, y = self._origin
        line = super().render_line(y)
        left, right = sorted((x, min(line.cell_length - 1, max(0, end))))
        position = 0
        for _, _, width in split_graphemes(line.text)[0]:
            if position <= left < position + width:
                left = position
            if position <= right < position + width:
                right = position + width - 1
                break
            position += width
        self._span = y, left, right + 1
        self.refresh()

    @property
    def selected_text(self) -> str:
        if self._span is None:
            return ""
        y, left, right = self._span
        return super().render_line(y).crop(left, right).text

    def render_line(self, y: int) -> Strip:
        line = super().render_line(y)
        if self._span is None or self._span[0] != y:
            return line
        _, left, right = self._span
        return Strip.join(
            (
                line.crop(0, left),
                Strip(Segment.apply_style(line.crop(left, right), post_style=TEXT_SELECTION)),
                line.crop(right),
            )
        )

    def watch_scroll_x(self, old_value: float, new_value: float) -> None:
        if int(old_value) != int(new_value):
            self.clear_text_selection()
        super().watch_scroll_x(old_value, new_value)

    def watch_scroll_y(self, old_value: float, new_value: float) -> None:
        if int(old_value) != int(new_value):
            self.clear_text_selection()
        super().watch_scroll_y(old_value, new_value)
