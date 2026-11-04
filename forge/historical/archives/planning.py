"""Cache-only acquisition planning: no HTTP, transactions writes, or file changes."""

from pathlib import Path

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.discovery import scan_archives
from forge.historical.archives.scans import find_fresh_discovery
from forge.historical.archives.sources.exchanges import ExchangeSource
from forge.historical.archives.storage.acquire import root_key
from forge.historical.archives.storage.files import contained, disk, fingerprint
from forge.shared.errors import MarketForgeError


async def plan(
    c: AsyncConnection[TupleRow],
    config: AcquisitionConfig,
    root: Path,
    *,
    verify_local: bool = False,
    limit: int = 100,
) -> dict[str, object]:
    if not 1 <= limit <= 10000:
        raise MarketForgeError("Plan limit must be 1..10000.")
    requests: list[dict[str, object]] = []
    archives: list[dict[str, object]] = []
    for index, request in enumerate(config.requests):
        scan = await find_fresh_discovery(
            c, request=request, interval=config.interval, adapter_version=ExchangeSource.version
        )
        outcome: dict[str, object] = {
            "index": index,
            "request": request.model_dump(mode="json"),
            "coverage": "unresolved: discovery required",
        }
        requests.append(outcome)
        if scan is None:
            continue
        outcome.update(
            scan_id=scan.id,
            archives_observed=scan.archives_found,
            coverage="Fresh source interpretation; market-data completeness remains unknown.",
        )
        if len(archives) >= limit:
            continue
        for remote in await scan_archives(c, scan.id, config.interval):
            if len(archives) >= limit:
                break
            cur = await c.execute(
                """SELECT id,relative_path,bytes_written,sha256 FROM historical.local_archives
                WHERE remote_archive_id=%s AND storage_root_key=%s AND status='complete' ORDER BY id DESC LIMIT 1""",
                (remote.id, root_key(root)),
            )
            local = await cur.fetchone()
            state = "download_required"
            local_id = None
            if local:
                local_id = local[0]
                state = "registered_unverified"
                if verify_local:
                    path = contained(root, local[1])
                    state = (
                        "verified_reusable"
                        if await disk(path.is_file)
                        and await disk(fingerprint, path) == (local[2], local[3])
                        else "missing_or_corrupt"
                    )
            archives.append(
                {
                    "request_index": index,
                    "remote_archive_id": remote.id,
                    "filename": remote.filename,
                    "local_archive_id": local_id,
                    "state": state,
                }
            )
    return {
        "cache_only": True,
        "requests": requests,
        "sampled_archives": archives,
        "limit": limit,
        "verification_scope": "Selected sample only; verification does not update registry state.",
    }
