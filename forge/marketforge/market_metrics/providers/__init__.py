from __future__ import annotations

from decimal import Decimal
from typing import Any


def decimal_or_none(
    value: Any,
) -> Decimal | None:
    if value in (None, ""):
        return None

    return Decimal(str(value))


def percentage_or_none(
    value: Any,
) -> Decimal | None:
    result = decimal_or_none(value)

    if result is None:
        return None

    return result / Decimal(100)
