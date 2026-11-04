"""Read-only inventory plus explicit verification/reconciliation of both origins."""

from pathlib import Path

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.acquire import reuse_registered, root_key
from forge.historical.archives.storage.files import contained, disk, fingerprint, validate_file
from forge.shared.errors import MarketForgeError


async def inventory(
    c: AsyncConnection[TupleRow],
    *,
    root: Path,
    limit: int = 100,
    archive_id: int | None = None,
    status: str | None = None,
    origin: str | None = None,
) -> list[dict[str, object]]:
    if not 1 <= limit <= 10000:
        raise MarketForgeError("Inventory limit must be 1..10000.")
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """SELECT l.*,CASE WHEN l.external_archive_id IS NULL THEN 'remote' ELSE 'external' END AS origin,
            r.filename,a.source_object_key AS external_object,a.normalization_readiness,a.identity_json
            FROM historical.local_archives l LEFT JOIN historical.remote_archives r ON r.id=l.remote_archive_id
            LEFT JOIN historical.external_archives a ON a.id=l.external_archive_id
            WHERE l.storage_root_key=%s AND (%s::bigint IS NULL OR l.id=%s) AND (%s::text IS NULL OR l.status=%s)
              AND (%s::text IS NULL OR (%s='external')=(l.external_archive_id IS NOT NULL))
            ORDER BY l.id DESC LIMIT %s""",
            (root_key(root), archive_id, archive_id, status, status, origin, origin, limit),
        )
        return list(await cur.fetchall())


async def maintain(
    c: AsyncConnection[TupleRow],
    *,
    root: Path,
    reconcile: bool,
    limit: int = 100,
    archive_id: int | None = None,
) -> dict[str, object]:
    rows = await inventory(c, root=root, limit=limit, archive_id=archive_id)
    outcomes: list[dict[str, object]] = []
    for row in rows:
        identity = int(str(row["id"]))
        path = contained(root, str(row["relative_path"]))
        result: dict[str, object] = {"id": identity}
        try:
            if reconcile:
                await reuse_registered(
                    c,
                    root,
                    remote_archive_id=int(str(row["remote_archive_id"]))
                    if row["remote_archive_id"] is not None
                    else None,
                    external_archive_id=int(str(row["external_archive_id"]))
                    if row["external_archive_id"] is not None
                    else None,
                    cleanup_partials=False,
                )
                cur = await c.execute(
                    "SELECT status FROM historical.local_archives WHERE id=%s", (identity,)
                )
                current = await cur.fetchone()
                result["status"] = current[0] if current else "missing_record"
                result["action"] = (
                    "Common reservation recovery; unverified partials retained; no network requests."
                )
            elif not await disk(path.is_file):
                result["verification"] = "missing"
                await c.execute(
                    "UPDATE historical.local_archives SET status='missing',updated_at=now() WHERE id=%s",
                    (identity,),
                )
            elif row["sha256"] is None or row["bytes_written"] is None:
                result["verification"] = "unverifiable: no committed checksum"
            else:
                size, checksum = await disk(fingerprint, path)
                matches = (size, checksum) == (row["bytes_written"], row["sha256"])
                if matches and row["remote_archive_id"] is not None:
                    await disk(validate_file, path)
                result["verification"] = "verified" if matches else "corrupt"
                await c.execute(
                    """UPDATE historical.local_archives SET status=CASE WHEN %s THEN status ELSE 'corrupt' END,
                    last_verified_at=CASE WHEN %s THEN now() ELSE last_verified_at END,updated_at=now() WHERE id=%s""",
                    (matches, matches, identity),
                )
        except Exception:
            result["error"] = (
                "Verification/recovery failed; inspect storage permissions and reserved bytes. Files retained."
            )
        outcomes.append(result)
    return {
        "outcomes": outcomes,
        "scope": "Only configured raw root; bounded selection; no redownload or data-file deletion.",
    }
