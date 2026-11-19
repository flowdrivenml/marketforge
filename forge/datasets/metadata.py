"""Exact observed bounds and vocabulary shared by the research API."""

import re
from datetime import UTC, datetime, timedelta
from typing import Any

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CONTENT = {
    "trade": "trades",
    "trades": "trades",
    "l2": "depth",
    "depth": "depth",
    "order_book_l2": "depth",
    "combined": "combined",
    "trade_l2": "combined",
}


def content(value: str) -> str:
    if value not in CONTENT:
        raise ValueError(f"Unknown data type {value!r}; use trades, depth or combined.")
    return CONTENT[value]


def timestamp_ns(value: int | str | datetime) -> int:
    if type(value) is int:
        result = value
    else:
        fraction = ""
        if isinstance(value, str):
            if re.fullmatch(r"-?\d+", value):
                return timestamp_ns(int(value))
            match = re.fullmatch(
                r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
                value,
            )
            if not match:
                raise ValueError(
                    "Use a timezone-aware ISO timestamp with at most nine fractional digits."
                )
            value, fraction = datetime.fromisoformat(match[1] + match[3]), match[2] or ""
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("Use integer Unix nanoseconds or a timezone-aware timestamp.")
        delta = value - EPOCH
        result = (
            (delta.days * 86400 + delta.seconds) * 10**9
            + delta.microseconds * 1000
            + int(fraction.ljust(9, "0"))
        )
    if not -(2**63) <= result < 2**63:
        raise ValueError("Timestamp exceeds signed 64-bit nanoseconds.")
    return result


def utc(value: int | None) -> str:
    if value is None:
        return "Unknown"
    seconds, fraction = divmod(value, 10**9)
    return (EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S") + f".{fraction:09d}Z"


def bounds(start: Any, end: Any) -> tuple[int | None, int | None]:
    lo = timestamp_ns(start) if start is not None else None
    hi = timestamp_ns(end) if end is not None else None
    if lo is not None and hi is not None and lo >= hi:
        raise ValueError("Require start < end for a half-open interval.")
    return lo, hi


def interval_matches(
    first: int | None, last: int | None, lo: int | None, hi: int | None, mode: str
) -> bool:
    if lo is None and hi is None:
        return True
    if first is None or last is None:
        return False
    if mode == "overlap":
        return (lo is None or last >= lo) and (hi is None or first < hi)
    return (lo is None or first >= lo) and (hi is None or last < hi)
