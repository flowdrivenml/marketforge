"""Small TOML writer for validated configuration documents (no extra dependency)."""

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import cast


def value(v: object) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return json.dumps(str(v))
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(value(x) for x in cast(list[object], v)) + "]"
    if isinstance(v, Mapping):
        return (
            "{"
            + ", ".join(
                json.dumps(str(k)) + " = " + value(x)
                for k, x in cast(Mapping[str, object], v).items()
                if x is not None
            )
            + "}"
        )
    raise ValueError("Unsupported TOML configuration value.")


def dumps(document: Mapping[str, object]) -> str:
    return (
        "\n".join(
            json.dumps(key) + " = " + value(v) for key, v in document.items() if v is not None
        )
        + "\n"
    )
