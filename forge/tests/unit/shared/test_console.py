"""Terminal output policy tests."""

from io import StringIO
from typing import Literal

import pytest

from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.console import create_console


@pytest.mark.parametrize("color", ["auto", "never"])
def test_redirected_output_is_plain(
    color: Literal["auto", "never"],
) -> None:
    output = StringIO()
    console = create_console(
        PresentationSettings(color=color),
        stream=output,
    )

    console.print("Normalization complete", style="mf.success")

    assert output.getvalue() == "Normalization complete\n"


def test_explicit_color_emits_styling() -> None:
    output = StringIO()
    console = create_console(
        PresentationSettings(color="always"),
        stream=output,
    )

    console.print("Registration pending", style="mf.warning")

    rendered = output.getvalue()
    assert "\x1b[" in rendered
    assert "Registration pending" in rendered


def test_explicit_color_overrides_no_color(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    output = StringIO()
    console = create_console(
        PresentationSettings(color="always"),
        stream=output,
    )

    console.print("Complete", style="mf.success")

    assert "\x1b[" in output.getvalue()


def test_automatic_mode_respects_no_color(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TerminalBuffer(StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("NO_COLOR", "1")
    output = TerminalBuffer()
    console = create_console(stream=output)

    console.print("Registration pending", style="mf.warning")

    assert output.getvalue() == "Registration pending\n"


def test_user_text_is_not_interpreted_as_markup_or_emoji() -> None:
    output = StringIO()
    console = create_console(
        PresentationSettings(color="never"),
        stream=output,
    )

    message = "archive[1].zip :warning:"
    console.print(message)

    assert output.getvalue() == f"{message}\n"
