"""Semantic terminal styles."""

from rich.theme import Theme

from forge.shared.terminal import palette


def build_theme() -> Theme:
    """Create the shared CLI theme."""
    return Theme(
        {
            "mf.body": palette.FG,
            "mf.heading": f"bold {palette.BLUE}",
            "mf.identifier": palette.CYAN,
            "mf.secondary": palette.MUTED,
            "mf.success": palette.GREEN,
            "mf.warning": palette.YELLOW,
            "mf.failure": palette.RED,
        },
    )
