"""Construct consistently configured CLI consoles."""

from __future__ import annotations

import sys
from typing import Literal, TextIO

from rich.console import Console

from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.theme import build_theme


def create_console(
    settings: PresentationSettings | None = None,
    *,
    stderr: bool = False,
    stream: TextIO | None = None,
) -> Console:
    """Create a console for human-readable application output.

    Automatic mode respects NO_COLOR and disables terminal styling
    for redirected streams. Explicit color choices override detection.
    """
    selected = settings if settings is not None else PresentationSettings()
    output = stream if stream is not None else (sys.stderr if stderr else sys.stdout)

    color_system: Literal["auto", "truecolor"] | None = "auto"
    force_terminal = output.isatty()
    no_color: bool | None = None

    if selected.color == "always":
        color_system = "truecolor"
        force_terminal = True
        no_color = False
    elif selected.color == "never":
        color_system = None
        force_terminal = False
        no_color = True

    return Console(
        file=output,
        theme=build_theme(),
        style="mf.body",
        color_system=color_system,
        force_terminal=force_terminal,
        force_jupyter=False,
        no_color=no_color,
        markup=False,
        highlight=False,
        emoji=False,
    )
