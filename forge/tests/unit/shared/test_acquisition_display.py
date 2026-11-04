"""Live/redirected acquisition presentation consumes bounded progress snapshots."""

from datetime import UTC, datetime
from io import StringIO

import pytest
from rich.console import Console

from forge.historical.archives.config import AcquisitionInterval
from forge.historical.archives.progress import ProgressEvent, ProgressState
from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.acquisition import AcquisitionDisplay
from forge.shared.terminal.console import create_console


@pytest.fixture
def interval() -> AcquisitionInterval:
    return AcquisitionInterval(
        start=datetime(2026, 11, 1, tzinfo=UTC), end=datetime(2026, 11, 4, tzinfo=UTC)
    )


def test_narrow_render_and_provisional_totals(interval: AcquisitionInterval) -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = 38
    display = AcquisitionDisplay(console, download=True, name="test", interval=interval)
    state = ProgressState(1)
    state.apply(ProgressEvent("queued"))
    snapshot = state.apply(
        ProgressEvent(
            "file_active",
            file_id=1,
            identity="bybit " + "long-file" * 100,
            activity="Verifying local archive",
        )
    )
    console.print(display.render(snapshot))
    assert all(len(line) <= 38 for line in output.getvalue().splitlines())
    assert "provisional" in output.getvalue()
    assert "100%" not in output.getvalue()
    assert "Ctrl+C" in output.getvalue()


@pytest.mark.parametrize("silent", [False, True])
def test_redirected_output_has_only_transitions_and_permanent_summary(
    interval: AcquisitionInterval, silent: bool
) -> None:
    output = StringIO()
    console = create_console(PresentationSettings(color="always"), stream=output)
    display = AcquisitionDisplay(
        console, download=False, name="test", interval=interval, silent=silent
    )
    state = ProgressState(1)
    with display:
        display.update(
            state.apply(ProgressEvent("request_started", identity="bybit spot BTCUSDT trades"))
        )
        display.update(state.apply(ProgressEvent("activity", activity="Fetching listing")))
        display.update(
            state.apply(
                ProgressEvent(
                    "request_finished",
                    identity="bybit BTCUSDT",
                    request_outcome="empty",
                    cached=True,
                )
            )
        )
    display.summary(console, status="complete", report="report.json")
    result = output.getvalue()
    assert "\x1b" not in result
    assert "Mapping complete" in result
    assert "(cached 1, empty 1; subsets)" in result
    assert result.count("empty: bybit") == (0 if silent else 1)
    assert "Files queued" not in result


class TerminalBuffer(StringIO):
    def isatty(self) -> bool:
        return True


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_live_display_stops_on_failure_or_cancellation(
    interval: AcquisitionInterval, monkeypatch: pytest.MonkeyPatch, error: type[BaseException]
) -> None:
    calls: list[str] = []

    class FakeLive:
        def __init__(
            self, renderable: object, *, console: Console, refresh_per_second: int, transient: bool
        ) -> None:
            assert refresh_per_second == 4

        def start(self) -> None:
            calls.append("start")

        def stop(self) -> None:
            calls.append("stop")

        def update(self, renderable: object) -> None:
            calls.append("update")

    monkeypatch.setattr("forge.shared.terminal.acquisition.Live", FakeLive)
    console = create_console(stream=TerminalBuffer())
    with (
        pytest.raises(error),
        AcquisitionDisplay(console, download=False, name="test", interval=interval) as display,
    ):
        display.update(ProgressState(1).apply(ProgressEvent("request_started")))
        raise error()
    assert calls == ["start", "update", "stop"]


def test_broken_renderer_is_disabled_without_propagating(interval: AcquisitionInterval) -> None:
    class BrokenBuffer(StringIO):
        def write(self, text: str) -> int:
            raise OSError("broken terminal")

    console: Console = create_console(stream=BrokenBuffer())
    display = AcquisitionDisplay(console, download=False, name="test", interval=interval)
    display.update(
        ProgressState(1).apply(ProgressEvent("request_finished", request_outcome="failed"))
    )
    assert display.disabled


def test_real_live_restores_cursor_with_color_disabled(
    interval: AcquisitionInterval, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TERM", "xterm-256color")
    output = TerminalBuffer()
    console = create_console(PresentationSettings(color="never"), stream=output)
    console.width = 60
    state = ProgressState(1)
    with AcquisitionDisplay(console, download=False, name="test", interval=interval) as display:
        display.update(
            state.apply(ProgressEvent("request_started", identity="bybit BTCUSDT trades"))
        )
        display.update(
            state.apply(
                ProgressEvent(
                    "request_finished",
                    identity="bybit BTCUSDT trades",
                    request_outcome="successful",
                    archives=3,
                )
            )
        )
    rendered = output.getvalue()
    assert "\x1b[?25l" in rendered
    assert "\x1b[?25h" in rendered
    assert "38;2" not in rendered  # No RGB styling with color='never'.
