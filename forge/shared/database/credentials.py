"""Resolve environment-specific PostgreSQL credentials without executing dotenv data."""

from __future__ import annotations

import os
import re
from pathlib import Path

from forge.shared.errors import MarketForgeError
from forge.shared.paths import resolve_project_root

TEST_DATABASE_KEY = "TEST_DATABASE_KEY"
PRODUCTION_DATABASE_KEY = "PRODUCTION_DATABASE_KEY"
_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")


class DatabaseCredentialsError(MarketForgeError):
    """Credentials are missing or invalid; messages never include their values."""


class MissingDatabaseCredentials(DatabaseCredentialsError):
    """The selected key has no configured value."""


def _dotenv_value(text: str, *, key: str, line: int) -> str:
    text = text.strip()
    if not text or text[0] not in "\"'":
        # A URL fragment is data; an unquoted comment starts after whitespace.
        return re.split(r"\s+#", text, maxsplit=1)[0].strip() if not text.startswith("#") else ""
    quote = text[0]
    result: list[str] = []
    index = 1
    escapes = {"\\": "\\", quote: quote}
    if quote == '"':
        escapes.update({"n": "\n", "r": "\r", "t": "\t"})
    while index < len(text):
        character = text[index]
        if character == quote:
            tail = text[index + 1 :].strip()
            if not tail or tail.startswith("#"):
                return "".join(result)
            break
        if character == "\\" and index + 1 < len(text):
            following = text[index + 1]
            result.append(escapes.get(following, "\\" + following))
            index += 2
        else:
            result.append(character)
            index += 1
    raise DatabaseCredentialsError(
        f"Invalid quoted assignment for {key} in .secrets/postgres.env at line {line}."
    )


def resolve_database_dsn(*, testing: bool, project_root: Path | None = None) -> str:
    """Use a nonblank environment value, then the same key in the project file.

    Empty environment values do not mask file credentials. The two environments
    never substitute for each other. Values are literal: no shell, interpolation,
    environment mutation, connection attempt or cached credential state.
    """
    key = TEST_DATABASE_KEY if testing else PRODUCTION_DATABASE_KEY
    environment = os.environ.get(key)
    if environment is not None and environment.strip():
        return environment.strip()

    source = resolve_project_root(project_root) / ".secrets" / "postgres.env"
    value: str | None = None
    try:
        with source.open(encoding="utf-8-sig") as stream:
            for number, line in enumerate(stream, 1):
                assignment = _ASSIGNMENT.match(line.rstrip("\r\n"))
                if assignment is not None and assignment[1] == key:
                    value = _dotenv_value(assignment[2], key=key, line=number)
    except FileNotFoundError:
        pass
    except (OSError, UnicodeError):
        raise DatabaseCredentialsError(
            f"Cannot read PostgreSQL credentials for {key} from .secrets/postgres.env."
        ) from None
    if value is None or not value.strip():
        raise MissingDatabaseCredentials(
            f"Set {key} to a nonempty value in the environment or in "
            "<project-root>/.secrets/postgres.env."
        )
    return value.strip()
