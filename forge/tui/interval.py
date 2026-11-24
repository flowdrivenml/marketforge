"""Translate UTC date/time controls to the recipe's existing timestamp type."""

import re
from datetime import UTC, datetime, timedelta

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def split_timestamp(value: object, *, nanoseconds: bool) -> tuple[str, str]:
    if value is None:
        return "", "00:00:00"
    remainder = 0
    if nanoseconds:
        seconds, remainder = divmod(int(str(value)), 1_000_000_000)
        stamp = EPOCH + timedelta(seconds=seconds)
    else:
        stamp = datetime.fromisoformat(str(value))
        if stamp.tzinfo is None:
            raise ValueError("Stored interval timestamp is missing its UTC offset.")
        stamp = stamp.astimezone(UTC)
    clock = stamp.time().isoformat()
    if remainder:
        clock += f".{remainder:09d}".rstrip("0")
    return stamp.date().isoformat(), clock


def combine_timestamp(date: str, time: str, *, nanoseconds: bool) -> int | datetime:
    precision = 9 if nanoseconds else 6
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
        raise ValueError("Enter a UTC date as YYYY-MM-DD.")
    if not re.fullmatch(rf"[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(?:\.[0-9]{{1,{precision}}})?", time):
        raise ValueError(f"Enter HH:MM:SS with up to {precision} fractional digits.")
    clock, _, fraction = time.partition(".")
    try:
        stamp = datetime.fromisoformat(f"{date}T{clock}+00:00")
    except ValueError as error:
        raise ValueError(str(error).capitalize() + ".") from error
    if nanoseconds:
        delta = stamp - EPOCH
        return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int(fraction.ljust(9, "0"))
    return stamp.replace(microsecond=int(fraction.ljust(6, "0")))
