"""Destructive checks use temporary managed files and a rollback-only guarded DB."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest, ArchiveScope
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import register_remote_archive
from forge.historical.archives.storage.acquire import root_key
from forge.markets.models import array, obj
from forge.markets.repository import json_value
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.storage import cleanup
from forge.storage.cleanup_plans import CleanupPlan, load_plan, save_plan
from forge.storage.models import StorageFilters

pytestmark = [pytest.mark.integration, pytest.mark.database]
BASE = datetime(2026, 10, 27, tzinfo=UTC)


@dataclass
class Fixture:
    c: AsyncConnection[TupleRow]
    paths: ProjectPaths
    symbol: str

    async def archive(
        self,
        *,
        start: datetime = BASE,
        end: datetime = BASE + timedelta(days=1),
        status: str = "failed",
        partial: bytes | None = None,
        final: bytes | None = None,
    ) -> tuple[int, Path, Path]:
        discovered = DiscoveredArchive(
            request=ArchiveRequest(
                exchange="bybit",
                instrument_type="spot",
                market_category="spot",
                data_type="trades",
                scope=ArchiveScope(kind="instrument", identifier=self.symbol),
            ),
            source_object_key=uuid4().hex,
            filename="archive.csv",
            interval=AcquisitionInterval(start=start, end=end),
        )
        remote = await register_remote_archive(self.c, discovered)
        relative = f"{remote.id}/{uuid4().hex}/archive.csv"
        path = self.paths.raw_dir / relative
        path.parent.mkdir(parents=True)
        temp = path.with_name(path.name + ".part")
        if partial is not None:
            temp.write_bytes(partial)
        if final is not None:
            path.write_bytes(final)
        cur = await self.c.execute(
            """INSERT INTO historical.local_archives
            (remote_archive_id,storage_root_key,relative_path,status,bytes_written,sha256,downloaded_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                remote.id,
                root_key(self.paths.raw_dir),
                relative,
                status,
                len(final) if status == "complete" and final is not None else None,
                "a" * 64 if status == "complete" else None,
                BASE if status == "complete" else None,
            ),
        )
        row = await cur.fetchone()
        assert row is not None
        return row[0], path, temp

    async def preview(
        self,
        operation: Literal["prune", "delete"],
        filters: StorageFilters | None = None,
        *,
        identity: int | None = None,
        overlap: bool = False,
        all_targets: bool = True,
    ) -> tuple[Path, CleanupPlan]:
        plan = await cleanup.preview(
            self.c,
            self.paths,
            filters or StorageFilters(status="failed"),
            operation,
            archive_id=identity,
            overlap=overlap,
            review_all=all_targets,
        )
        source = save_plan(self.paths, plan)
        return source, plan


def fixture(database: AsyncConnection[TupleRow], tmp_path: Path) -> Fixture:
    paths = ProjectPaths(root=tmp_path / "project", data_root=tmp_path / "project/data")
    return Fixture(database, paths, "CLEAN" + uuid4().hex[:12].upper())


@pytest.mark.asyncio
async def test_prune_failed_partial_absence_activity_and_changed_file(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = fixture(database, tmp_path)
    identity, final, part = await f.archive(partial=b"leftover")
    absent, _, _ = await f.archive()
    active, _, active_part = await f.archive(status="downloading", partial=b"active")
    source, plan = await f.preview("prune", StorageFilters(instrument=f.symbol))
    states = {target.local_id: target for target in plan.targets}
    assert states[identity].state == "eligible" and states[identity].reclaimable_bytes == 8
    assert states[absent].state == "eligible" and states[absent].reclaimable_bytes == 0
    absent_snapshot = states[absent].partial_file
    assert absent_snapshot is not None and not absent_snapshot.exists
    assert states[active].state == "blocked"
    assert part.exists() and active_part.exists() and not final.exists()
    part.write_bytes(b"changed after preview")
    applied = await cleanup.apply(database, f.paths, source, plan)
    outcomes = {obj(o)["local_id"]: obj(o) for o in array(applied["outcomes"])}
    assert outcomes[f"local:{identity}"]["status"] == "skipped_changed"
    assert part.exists() and active_part.exists()
    source, plan = await f.preview("prune")
    applied = await cleanup.apply(database, f.paths, source, plan)
    assert not part.exists() and active_part.exists()
    assert obj(array(applied["outcomes"])[0])["status"] == "deleted"
    cursor = await database.execute(
        "SELECT id,status FROM historical.local_archives WHERE id=ANY(%s) ORDER BY id",
        ([identity, absent, active],),
    )
    assert await cursor.fetchall() == [
        (identity, "deleted"),
        (absent, "deleted"),
        (active, "downloading"),
    ]
    again = await cleanup.apply(database, f.paths, source, plan)
    assert all(obj(row)["status"] == "already_deleted" for row in array(again["outcomes"]))


@pytest.mark.asyncio
async def test_delete_date_containment_and_completed_file_removal(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = fixture(database, tmp_path)
    ids: list[int] = []
    final_paths: list[Path] = []
    for start, end in (
        (BASE, BASE + timedelta(days=1)),
        (BASE + timedelta(days=1), BASE + timedelta(days=2)),
        (BASE + timedelta(days=2), BASE + timedelta(days=3)),
        (BASE.replace(day=1), datetime(2026, 11, 1, tzinfo=UTC)),
        (BASE - timedelta(days=1), BASE),
        (BASE + timedelta(days=3), BASE + timedelta(days=4)),
    ):
        local, path, _ = await f.archive(start=start, end=end, status="complete", final=b"archive")
        ids.append(local)
        final_paths.append(path)
    selected = StorageFilters(
        instrument=f.symbol, status="complete", start=BASE, end=BASE + timedelta(days=3), limit=1
    )
    source, plan = await f.preview("delete", selected)
    assert [t.local_id for t in plan.targets] == ids[:3]
    assert all(t.state == "eligible" and t.action == "delete_managed_archive" for t in plan.targets)
    _, overlapping = await f.preview("delete", selected, overlap=True)
    assert [t.local_id for t in overlapping.targets] == ids[:4]
    monthly = overlapping.targets[-1]
    assert str(monthly.record["start_at"]).startswith("2026-10-01") and str(
        monthly.record["end_at"]
    ).startswith("2026-11-01")
    assert [
        t.local_id
        for t in (await f.preview("delete", StorageFilters(start=BASE), identity=ids[0]))[1].targets
    ] == [ids[0]]
    assert all(t.final_file is not None and t.final_file.exists for t in plan.targets)
    result = await cleanup.apply(database, f.paths, source, plan)
    assert result["status"] == "complete"
    assert obj(result["result_totals"])["archive_files_removed"] == 3
    assert obj(result["result_totals"])["reclaimed_bytes"] == 3 * len(b"archive")
    assert all(not path.exists() for path in final_paths[:3])
    assert all(path.exists() for path in final_paths[3:])  # Monthly/adjacent files retained.
    result = await cleanup.apply(database, f.paths, source, plan)
    assert all(obj(row)["status"] == "already_deleted" for row in array(result["outcomes"]))


@pytest.mark.asyncio
async def test_delete_retirement_retries_after_unlink_and_registry_failure(
    database: AsyncConnection[TupleRow], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = fixture(database, tmp_path)
    identity, _, part = await f.archive(partial=b"unpublished")
    source, plan = await f.preview("delete", identity=identity)
    original = cleanup.retire_failed
    calls = 0

    async def fail_once(c: AsyncConnection[TupleRow], local_id: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise MarketForgeError("simulated registry update failure")
        await original(c, local_id)

    monkeypatch.setattr(cleanup, "retire_failed", fail_once)
    result = await cleanup.apply(database, f.paths, source, plan)
    assert not part.exists() and result["status"] == "partial"
    part.write_bytes(b"replacement after failed registration")
    changed = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(changed["outcomes"])[0])["status"] == "skipped_changed"
    assert part.read_bytes() == b"replacement after failed registration"
    part.unlink()
    result = await cleanup.apply(database, f.paths, source, plan)
    assert (
        result["status"] == "complete" and obj(array(result["outcomes"])[0])["status"] == "deleted"
    )
    cur = await database.execute(
        "SELECT status FROM historical.local_archives WHERE id=%s", (identity,)
    )
    assert await cur.fetchone() == ("deleted",)
    result = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(result["outcomes"])[0])["status"] == "already_deleted"


@pytest.mark.asyncio
async def test_old_plan_cannot_follow_status_change_or_hidden_targets(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = fixture(database, tmp_path)
    identity, _, part = await f.archive(partial=b"first")
    await f.archive(partial=b"second")
    source, plan = await f.preview(
        "prune", StorageFilters(status="failed", limit=1), all_targets=False
    )
    with pytest.raises(MarketForgeError, match="pagination"):
        load_plan(f.paths, source, "prune")
    with pytest.raises(MarketForgeError, match="visible"):
        await cleanup.apply(database, f.paths, source, plan)
    assert part.exists()
    source, plan = await f.preview("prune")
    await database.execute(
        "UPDATE historical.local_archives SET status='downloading' WHERE id=%s", (identity,)
    )
    applied = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(applied["outcomes"])[0])["status"] == "skipped_changed" and part.exists()
    with WorkspaceOwnership(f.paths):
        with pytest.raises(MarketForgeError, match="Another historical application"):
            await cleanup.apply(database, f.paths, source, plan)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe", ["legacy_path", "symlink", "hardlink", "durable", "shared_attempt", "external"]
)
async def test_ambiguous_or_recoverable_partial_ownership_is_blocked(
    database: AsyncConnection[TupleRow], tmp_path: Path, unsafe: str
) -> None:
    f = fixture(database, tmp_path)
    identity, final, part = await f.archive(partial=b"do not remove")
    original = tmp_path / "original-external-file"
    original.write_bytes(b"original")
    if unsafe == "legacy_path":
        relative = "legacy/archive.csv"
        destination = f.paths.raw_dir / relative
        destination.parent.mkdir(parents=True)
        moved = destination.with_name(destination.name + ".part")
        part.rename(moved)
        part = moved
        await database.execute(
            "UPDATE historical.local_archives SET relative_path=%s WHERE id=%s",
            (relative, identity),
        )
    elif unsafe in {"symlink", "hardlink"}:
        part.unlink()
        if unsafe == "symlink":
            part.symlink_to(original)
        else:
            part.hardlink_to(original)
    elif unsafe == "durable":
        await database.execute(
            "UPDATE historical.local_archives SET bytes_written=100,sha256=%s WHERE id=%s",
            ("a" * 64, identity),
        )
    elif unsafe == "shared_attempt":
        relative = str(final.relative_to(f.paths.raw_dir)) + ".part"
        await database.execute(
            """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,status)
            SELECT remote_archive_id,storage_root_key,%s,'downloading' FROM historical.local_archives WHERE id=%s""",
            (relative, identity),
        )
    elif unsafe == "external":
        cur = await database.execute(
            "INSERT INTO historical.external_sources(source_key,source_kind,root_uri) VALUES(%s,'recording',%s) RETURNING id",
            (f.symbol, str(tmp_path)),
        )
        source = await cur.fetchone()
        assert source is not None
        cur = await database.execute(
            """INSERT INTO historical.external_archives
            (external_source_id,source_object_key,sha256,bytes_expected,identity_json,data_type,normalization_readiness,import_provenance)
            VALUES(%s,'original',%s,8,%s,'trades','requires_profile','{}') RETURNING id""",
            (
                source[0],
                "b" * 64,
                json_value(
                    {
                        "exchange": "bybit",
                        "market_category": "spot",
                        "instrument_type": "spot",
                        "symbol": f.symbol,
                    }
                ),
            ),
        )
        external = await cur.fetchone()
        assert external is not None
        await database.execute(
            "UPDATE historical.local_archives SET remote_archive_id=NULL,external_archive_id=%s WHERE id=%s",
            (external[0], identity),
        )
    source, plan = await f.preview("prune", identity=identity)
    assert plan.targets[0].state == "blocked" and plan.targets[0].reclaimable_bytes == 0
    await cleanup.apply(database, f.paths, source, plan)
    assert part.exists() and original.read_bytes() == b"original"


@pytest.mark.asyncio
async def test_absent_retry_and_new_candidates_cannot_expand_review(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = fixture(database, tmp_path)
    identity, _, part = await f.archive(partial=b"old")
    source, plan = await f.preview("prune")
    _, _, new = await f.archive(partial=b"new target after review")
    part.unlink()  # A downloader retry already cleaned it up.
    result = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(result["outcomes"])[0])["status"] == "deleted"
    assert obj(result["result_totals"])["partial_files_removed"] == 0
    assert obj(result["result_totals"])["reclaimed_bytes"] == 0
    assert len(array(result["outcomes"])) == 1 and new.exists()
    cur = await database.execute(
        "SELECT status FROM historical.local_archives WHERE id=%s", (identity,)
    )
    assert await cur.fetchone() == ("deleted",)


@pytest.mark.asyncio
async def test_changed_registry_path_and_unknown_activity_are_not_applied(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    f = fixture(database, tmp_path)
    identity, _, part = await f.archive(partial=b"private")
    blocked = await cleanup.preview(
        database, f.paths, StorageFilters(status="failed"), "prune", inactive=False, review_all=True
    )
    assert blocked.targets[0].state == "blocked" and "lock" in blocked.targets[0].blockers[0]
    source, plan = await f.preview("prune")
    await database.execute(
        "UPDATE historical.local_archives SET relative_path=relative_path||'.changed' WHERE id=%s",
        (identity,),
    )
    result = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(result["outcomes"])[0])["status"] == "skipped_changed" and part.exists()


@pytest.mark.asyncio
async def test_symbol_time_and_combined_delete_previews_never_apply_completed_archives(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    f = fixture(database, tmp_path)
    november = datetime(2026, 11, 1, tzinfo=UTC)
    daily: list[int] = []
    files: list[Path] = []
    for day in range(7):
        identity, path, _ = await f.archive(
            start=november + timedelta(days=day),
            end=november + timedelta(days=day + 1),
            status="complete",
            final=b"November archive",
        )
        daily.append(identity)
        files.append(path)
    monthly, path, _ = await f.archive(
        start=november, end=datetime(2026, 12, 1, tzinfo=UTC), status="complete", final=b"monthly"
    )
    files.append(path)
    other = Fixture(database, f.paths, f.symbol + "OTHER")
    another, path, _ = await other.archive(
        start=november + timedelta(days=14),
        end=november + timedelta(days=15),
        status="complete",
        final=b"other symbol",
    )
    files.append(path)
    _, by_symbol = await f.preview("delete", StorageFilters(instrument=f.symbol))
    assert [t.local_id for t in by_symbol.targets] == [*daily, monthly]
    assert all(t.state == "eligible" for t in by_symbol.targets)
    _, by_lower = await f.preview("delete", StorageFilters(start=november + timedelta(days=5)))
    assert [t.local_id for t in by_lower.targets] == [*daily[5:], another]
    _, by_upper = await f.preview("delete", StorageFilters(end=november + timedelta(days=2)))
    assert [t.local_id for t in by_upper.targets] == daily[:2]
    selected = StorageFilters(
        exchange="bybit", instrument=f.symbol, start=november + timedelta(days=5), limit=1
    )
    _, combined = await f.preview("delete", selected)
    assert [t.local_id for t in combined.targets] == daily[5:]
    _, overlap = await f.preview(
        "delete", selected.model_copy(update={"end": november + timedelta(days=7)}), overlap=True
    )
    assert [t.local_id for t in overlap.targets] == [*daily[5:], monthly]
    assert all(path.read_bytes() for path in files)
    cursor = await database.execute(
        "SELECT status FROM historical.local_archives WHERE id=ANY(%s)",
        ([*daily, monthly, another],),
    )
    assert all(row == ("complete",) for row in await cursor.fetchall())


@pytest.mark.asyncio
async def test_prune_retires_absent_failed_records_with_zero_reclaimed_bytes(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    f = fixture(database, tmp_path)
    identities: list[int] = []
    for _ in range(13):
        identity, _, _ = await f.archive()
        identities.append(identity)
    await database.execute(
        "UPDATE historical.local_archives SET failure_message='original failed attempt' WHERE id=ANY(%s)",
        (identities,),
    )
    source, plan = await f.preview("prune")
    counts = cleanup.totals(plan.targets)
    assert counts["records_eligible_for_retirement"] == 13
    assert counts["partial_files_found"] == 0 and counts["reclaimable_bytes"] == 0
    applied = await cleanup.apply(database, f.paths, source, plan)
    result = obj(applied["result_totals"])
    assert result["failed_records_retired"] == 13
    assert result["partial_files_removed"] == 0 and result["reclaimed_bytes"] == 0
    cursor = await database.execute(
        "SELECT id,status,failure_message FROM historical.local_archives WHERE id=ANY(%s) ORDER BY id",
        (identities,),
    )
    assert await cursor.fetchall() == [
        (identity, "deleted", "original failed attempt") for identity in identities
    ]
    _, again = await f.preview("prune")
    assert again.targets == []


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency", ["dataset", "job", "preparation"])
async def test_completed_archive_concrete_dependencies_block_before_and_after_preview(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    dependency: str,
) -> None:
    f = fixture(database, tmp_path)
    identity, final, _ = await f.archive(status="complete", final=b"retained")
    source, plan = await f.preview("delete", StorageFilters(), identity=identity)
    assert plan.targets[0].state == "eligible"
    if dependency == "dataset":
        cur = await database.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='historical' AND table_name='datasets' AND column_name='operation')"
        )
        evolved = await cur.fetchone()
        if evolved == (True,):
            cur = await database.execute(
                "INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,storage_format,status,operation) VALUES('trade',1,2,'parquet','pending','normalize') RETURNING id"
            )
        else:
            cur = await database.execute(
                "INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,storage_format,status) VALUES('trade',1,2,'parquet','pending') RETURNING id"
            )
        dataset = await cur.fetchone()
        assert dataset is not None
        await database.execute(
            "INSERT INTO historical.dataset_raw_inputs(dataset_id,local_archive_id,input_sha256) VALUES(%s,%s,%s)",
            (dataset[0], identity, "a" * 64),
        )
    elif dependency == "job":
        import json

        folder = f.paths.normalization_jobs_dir
        folder.mkdir(parents=True)
        (folder / "known.json").write_text(json.dumps({"tasks": [{"input_path": str(final)}]}))
    else:
        folder = f.paths.normalization_config_dir
        folder.mkdir(parents=True)
        (folder / "known.toml").write_text(f"[[inputs]]\narchive_id = {identity}\n")
    _, blocked = await f.preview("delete", StorageFilters(), identity=identity)
    assert blocked.targets[0].state == "blocked" and blocked.targets[0].blockers
    applied = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(applied["outcomes"])[0])["status"] == "skipped_changed"
    assert final.read_bytes() == b"retained"


@pytest.mark.asyncio
async def test_completed_file_registry_failure_recovers_and_does_not_delete_replacement(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = fixture(database, tmp_path)
    identity, final, _ = await f.archive(status="complete", final=b"completed")
    source, plan = await f.preview("delete", StorageFilters(), identity=identity)
    original = cleanup.retire_archive
    calls = 0

    async def fail_once(c: AsyncConnection[TupleRow], local_id: int, status: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise MarketForgeError("simulated archive registry failure")
        await original(c, local_id, status)

    monkeypatch.setattr(cleanup, "retire_archive", fail_once)
    result = await cleanup.apply(database, f.paths, source, plan)
    assert result["status"] == "partial" and not final.exists()
    assert obj(result["result_totals"])["archive_files_removed"] == 1
    final.write_bytes(b"new replacement")
    changed = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(changed["outcomes"])[0])["status"] == "skipped_changed"
    assert final.read_bytes() == b"new replacement"
    final.unlink()
    recovered = await cleanup.apply(database, f.paths, source, plan)
    assert recovered["status"] == "complete"
    assert obj(recovered["result_totals"])["records_deleted"] == 1
    assert obj(recovered["result_totals"])["archive_files_removed"] == 0
    cursor = await database.execute(
        "SELECT status FROM historical.local_archives WHERE id=%s", (identity,)
    )
    assert await cursor.fetchone() == ("deleted",)


@pytest.mark.asyncio
async def test_completed_file_staleness_and_active_managed_operation_block(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    f = fixture(database, tmp_path)
    identity, final, _ = await f.archive(status="complete", final=b"original archive")
    active = await cleanup.preview(
        database,
        f.paths,
        StorageFilters(),
        "delete",
        archive_id=identity,
        inactive=False,
        review_all=True,
    )
    assert active.targets[0].state == "blocked" and "lock" in active.targets[0].blockers[0]
    source, plan = await f.preview("delete", StorageFilters(), identity=identity)
    final.write_bytes(b"changed archive")
    result = await cleanup.apply(database, f.paths, source, plan)
    assert obj(array(result["outcomes"])[0])["status"] == "skipped_changed"
    assert final.read_bytes() == b"changed archive"
    with WorkspaceOwnership(f.paths):
        with pytest.raises(MarketForgeError, match="Another historical application"):
            await cleanup.apply(database, f.paths, source, plan)


@pytest.mark.asyncio
async def test_completed_external_copy_deletion_preserves_original(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    f = fixture(database, tmp_path)
    identity, copy, _ = await f.archive(status="complete", final=b"copy")
    original = tmp_path / "original.bin"
    original.write_bytes(b"original")
    cur = await database.execute(
        "INSERT INTO historical.external_sources(source_key,source_kind,root_uri) VALUES(%s,'recording',%s) RETURNING id",
        (f.symbol, str(tmp_path)),
    )
    source = await cur.fetchone()
    assert source is not None
    cur = await database.execute(
        """INSERT INTO historical.external_archives
        (external_source_id,source_object_key,sha256,bytes_expected,identity_json,data_type,normalization_readiness,import_provenance)
        VALUES(%s,'original.bin',%s,8,%s,'trades','requires_profile','{}') RETURNING id""",
        (
            source[0],
            "b" * 64,
            json_value(
                {
                    "exchange": "bybit",
                    "market_category": "spot",
                    "instrument_type": "spot",
                    "symbol": f.symbol,
                }
            ),
        ),
    )
    external = await cur.fetchone()
    assert external is not None
    await database.execute(
        "UPDATE historical.local_archives SET remote_archive_id=NULL,external_archive_id=%s WHERE id=%s",
        (external[0], identity),
    )
    source_path, plan = await f.preview("delete", StorageFilters(), identity=identity)
    assert plan.targets[0].state == "eligible"
    result = await cleanup.apply(database, f.paths, source_path, plan)
    assert result["status"] == "complete" and not copy.exists()
    assert original.read_bytes() == b"original"
