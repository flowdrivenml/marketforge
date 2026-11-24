"""Clipboard transport and plain-text inspection formatting."""

import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from decimal import Decimal
from os import environ
from typing import cast

from rich.text import Text

_CONTROL = re.compile(
    r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]|[\x00-\x08\x0b-\x1f\x7f-\x9f]"
)
_SENSITIVE_FIELD = re.compile(
    r"(?i)password|credential|authorization|secret|token|(?:^|_)dsn$|database_url|api[_-]?key"
)
_SECRET = re.compile(
    r"(?i)(?:[a-z][a-z0-9+.-]*://[^/\s]+:[^@\s]+@|\b(?:password|secret|token|api[_-]?key|dsn)\s*[:=]\s*\S+)"
)


def safe_text(value: str) -> str:
    return _SECRET.sub("[redacted]", _CONTROL.sub("", value))


def field_text(key: str, value: object) -> str:
    return "[redacted]" if _SENSITIVE_FIELD.search(key) else plain(value)


def plain(value: object) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Text):
        return safe_text(value.plain)
    if isinstance(value, Mapping):
        return "\n".join(
            f"{name.replace('_', ' ').capitalize()}  " + plain(item).replace("\n", "\n  ")
            for key, item in cast(Mapping[object, object], value).items()
            if not _SENSITIVE_FIELD.search(name := str(key))
        )
    if isinstance(value, (list, tuple)):
        return "\n\n".join(plain(item) for item in cast(list[object] | tuple[object, ...], value))
    return safe_text(str(value)) if value is not None else "—"


def providers(environment: Mapping[str, str], platform: str) -> list[tuple[str, ...]]:
    candidates: list[tuple[str, ...]] = []
    if platform == "darwin":
        candidates.append(("pbcopy",))
    if environment.get("WAYLAND_DISPLAY"):
        candidates.append(("wl-copy",))
    if environment.get("DISPLAY"):
        candidates.extend(
            (("xclip", "-selection", "clipboard"), ("xsel", "--clipboard", "--input"))
        )
    if environment.get("TMUX"):
        candidates.append(("tmux", "load-buffer", "-w", "-"))
    return [command for command in candidates if shutil.which(command[0])]


def deliver(value: str) -> str | None:
    for command in providers(environ, sys.platform):
        try:
            result = subprocess.run(
                command,
                input=value.encode("utf-8"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=3,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            return command[0]
    return None
