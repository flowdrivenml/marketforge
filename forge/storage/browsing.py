"""Read-only local archive browsing and coverage across all matching registry pages."""

from pathlib import Path

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.files import contained
from forge.shared.errors import MarketForgeError
from forge.storage.coverage import GROUP_FIELDS, CoverageGroup
from forge.storage.models import StorageFilters, interval
from forge.storage.registry import counts, page, parameters, scan

SEMANTICS = "Nominal archive coverage from registered half-open intervals [start, end). Browsing does not verify file existence, checksum, or event-level completeness. Stored verification times are historical."


def pagination(total: int, filters: StorageFilters, returned: int) -> dict[str, object]:
    return {
        "limit": filters.limit,
        "offset": filters.offset,
        "total": total,
        "returned": returned,
        "has_more": filters.offset + returned < total,
    }


def archive_document(row: dict[str, object], root: Path) -> dict[str, object]:
    result = dict(row)
    result["local_id"] = "local:" + str(row["id"])
    diagnostics: list[str] = []
    try:
        result["resolved_path"] = str(contained(root, str(row["relative_path"])).resolve())
    except MarketForgeError as error:
        result["resolved_path"] = None
        diagnostics.append(str(error))
    _, issue = interval(row.get("start_at"), row.get("end_at"))
    if issue:
        diagnostics.append(issue)
    for key in (
        "exchange",
        "instrument_type",
        "market_category",
        "scope_kind",
        "scope_identifier",
        "base_asset",
        "quote_asset",
    ):
        if row.get(key) is None:
            diagnostics.append(f"unknown or ambiguous {key}")
    if row.get("data_type") == "order_book_l2" and row.get("feed") is None:
        diagnostics.append("unknown feed identity")
    result["diagnostics"] = diagnostics
    return result


async def list_archives(
    c: AsyncConnection[TupleRow],
    root: Path,
    filters: StorageFilters,
    *,
    archive_id: int | None = None,
) -> dict[str, object]:
    params = parameters(root, filters, archive_id)
    totals = await counts(c, params)
    rows = await page(c, params)
    return {
        "schema_version": 1,
        "storage_root": str(root),
        "archives": [archive_document(row, root) for row in rows],
        "pagination": pagination(int(str(totals["total"])), filters, len(rows)),
        "totals": totals,
        "filters": filters.model_dump(mode="json"),
        "verification": "Stored registry records; not reverified during browsing.",
        "semantics": SEMANTICS,
    }


async def coverage(
    c: AsyncConnection[TupleRow], root: Path, filters: StorageFilters, *, include_gaps: bool = False
) -> dict[str, object]:
    selected = filters.model_copy(update={"status": "complete"})
    params = parameters(root, selected)
    totals = await counts(c, params)
    groups: dict[tuple[object, ...], CoverageGroup] = {}
    after = 0
    while rows := await scan(c, params, after):
        for row in rows:
            key = tuple(row.get(key) for key in GROUP_FIELDS)
            group = groups.setdefault(
                key, CoverageGroup({key: row.get(key) for key in GROUP_FIELDS})
            )
            group.add(row)
        after = int(str(rows[-1]["id"]))
    ordered = sorted(
        groups.values(),
        key=lambda group: tuple(str(group.identity.get(key) or "") for key in GROUP_FIELDS),
    )
    shown = ordered[filters.offset : filters.offset + filters.limit]
    return {
        "schema_version": 1,
        "storage_root": str(root),
        "coverage": [group.document(selected, include_gaps=include_gaps) for group in shown],
        "pagination": pagination(len(ordered), selected, len(shown)),
        "totals": totals,
        "filters": selected.model_dump(mode="json"),
        "gaps_included": include_gaps,
        "range_semantics": "Missing bounds use each group's observed envelope. One supplied bound never implies coverage beyond the observed opposite bound. Groups require matching local records; absent scopes are not inferred.",
        "semantics": SEMANTICS,
    }
