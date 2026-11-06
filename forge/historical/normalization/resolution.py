"""Validation of local preparation metadata; archive interpretation belongs to Rust."""

from decimal import Decimal
from typing import cast

from forge.shared.errors import MarketForgeError


def require(condition: bool, message: str) -> None:
    if not condition:
        raise MarketForgeError(message)


def positive_decimal(value: object, field: str) -> str:
    require(isinstance(value, Decimal), f"Missing authoritative {field} metadata.")
    number = cast(Decimal, value)
    require(number.is_finite() and number > 0, f"Invalid {field} metadata.")
    return str(number)
