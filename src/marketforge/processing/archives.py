from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_ARCHIVE_SUFFIXES = (
    ".csv.gz",
    ".json.gz",
    ".jsonl.gz",
    ".zip",
    ".tar.gz",
    ".csv",
    ".json",
    ".jsonl",
)


_DATE_PATTERN = re.compile(
    r"(?<!\d)"
    r"(?P<year>\d{4})"
    r"[-_]"
    r"(?P<month>\d{2})"
    r"(?:[-_](?P<day>\d{2}))?"
    r"(?!\d)"
)


@dataclass(frozen=True)
class RawArchive:
    path: Path

    exchange: str
    instrument_type: str
    market_category: str
    data_type: str
    symbol: str

    filename: str

    start_date: date | None
    end_date: date | None


def scan_raw_archives(
    *,
    data_root: Path = Path("data"),
) -> list[RawArchive]:
    """
    Discover locally downloaded immutable raw archives.

    The filesystem hierarchy itself provides the archive's
    exchange/instrument/dataset identity.
    """

    root = data_root / "raw"

    if not root.exists():
        return []

    archives: list[RawArchive] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        if not _is_archive(path):
            continue

        relative = path.relative_to(root)

        parts = relative.parts

        if len(parts) != 6:
            continue

        (
            exchange,
            instrument_type,
            market_category,
            data_type,
            symbol,
            filename,
        ) = parts

        start_date, end_date = _extract_date_range(filename)

        archives.append(
            RawArchive(
                path=path,
                exchange=exchange,
                instrument_type=instrument_type,
                market_category=market_category,
                data_type=data_type,
                symbol=symbol,
                filename=filename,
                start_date=start_date,
                end_date=end_date,
            )
        )

    return sorted(
        archives,
        key=lambda archive: (
            archive.exchange,
            archive.instrument_type,
            archive.market_category,
            archive.data_type,
            archive.symbol,
            archive.start_date or date.min,
            archive.filename,
        ),
    )


def select_raw_archives(
    archives: list[RawArchive],
    *,
    exchange: str | None = None,
    instrument_type: str | None = None,
    market_category: str | None = None,
    data_type: str | None = None,
    symbol: str | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[RawArchive]:
    """
    Filter a raw archive inventory.

    Date filtering uses overlap semantics. Archives whose dates
    cannot be inferred from the filename are excluded when a date
    filter is supplied.
    """

    selected: list[RawArchive] = []

    for archive in archives:
        if exchange is not None and archive.exchange != exchange:
            continue

        if instrument_type is not None and archive.instrument_type != instrument_type:
            continue

        if market_category is not None and archive.market_category != market_category:
            continue

        if data_type is not None and archive.data_type != data_type:
            continue

        if symbol is not None and archive.symbol != symbol:
            continue

        if start is not None or end is not None:
            if archive.start_date is None or archive.end_date is None:
                continue

            if start is not None and archive.end_date < start:
                continue

            if end is not None and archive.start_date >= end:
                continue

        selected.append(archive)

    return selected


def delete_raw_archives(
    archives: Sequence[RawArchive],
) -> list[Path]:
    """
    Delete selected raw archive files.

    Only the explicitly supplied archive paths are removed.
    """

    deleted: list[Path] = []

    for archive in archives:
        path = archive.path

        if not path.is_file():
            continue

        path.unlink()
        deleted.append(path)

    return deleted


def _is_archive(
    path: Path,
) -> bool:
    name = path.name.lower()

    if name.endswith(".manifest.json") or name.endswith(".part"):
        return False

    return any(name.endswith(suffix) for suffix in _ARCHIVE_SUFFIXES)


def _extract_date_range(
    filename: str,
) -> tuple[date | None, date | None]:
    """
    Infer an archive's logical date range from its filename.

    Daily:
        YYYY-MM-DD
        YYYY_MM_DD

    Monthly:
        YYYY-MM
        YYYY_MM

    Monthly end_date is the final calendar day represented by
    the archive.
    """

    match = _DATE_PATTERN.search(filename)

    if match is None:
        return None, None

    year = int(match.group("year"))

    month = int(match.group("month"))

    day_value = match.group("day")

    if day_value is not None:
        value = date(
            year,
            month,
            int(day_value),
        )

        return value, value

    start = date(
        year,
        month,
        1,
    )

    if month == 12:
        next_month = date(
            year + 1,
            1,
            1,
        )
    else:
        next_month = date(
            year,
            month + 1,
            1,
        )

    from datetime import timedelta

    end = next_month - timedelta(days=1)

    return start, end
