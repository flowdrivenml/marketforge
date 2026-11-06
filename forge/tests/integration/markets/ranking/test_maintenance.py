"""Isolated fixture publications exercise cleanup without deleting user ranking history."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import LiteralString, cast
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.normalization.batch import ranking_positions
from forge.markets.models import Instrument, Observation, array, obj
from forge.markets.ranking.coordination import RANKING_LOCK, ranking_lock
from forge.markets.ranking.deletion import delete_snapshots
from forge.markets.ranking.service import find_latest_snapshot, latest_snapshot
from forge.markets.repository import json_value, save_instrument, save_observation
from forge.shared.database.testing import testing_connection as connect_testing
from forge.shared.database.testing import testing_dsn as guarded_dsn
from forge.shared.paths import ProjectPaths

ROOT = Path(__file__).resolve().parents[5]
pytestmark = [pytest.mark.integration, pytest.mark.database]


async def seed(
    c: AsyncConnection[TupleRow],
    policy: str,
    *,
    now: datetime | None = None,
    published: bool = True,
) -> tuple[UUID, int, int, int]:
    now = now or datetime(2001, 1, 1, tzinfo=UTC)
    instrument = Instrument(
        exchange="bybit",
        market="spot",
        symbol="MAINT_" + uuid4().hex,
        base="BTC",
        quote="USDT",
        tick_size=Decimal("0.1"),
    )
    identity = await save_instrument(c, instrument, now)
    cur = await c.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    observation = await save_observation(
        c,
        row[0],
        identity,
        Observation(
            instrument=instrument,
            fetched_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="fetch_time",
            turnover=Decimal(1),
            turnover_unit="USDT",
            raw={},
            interpretation={
                "activity_scope": "exchange_local",
                "base_volume_scope": "exchange_local",
            },
        ),
    )
    snapshot = uuid4()
    await c.execute(
        """INSERT INTO ranking.snapshots(id,policy_name,policy_json,policy_sha256,algorithm_version,
        comparison_currency,comparison_basis,created_at,status,partial_universe,universe_count,included_count)
        VALUES(%s,%s,%s,%s,'maintenance-fixture','USD','{}',%s,'draft',true,1,1)""",
        (snapshot, policy, json_value({"metric": "turnover_24h"}), "a" * 64, now),
    )
    cur = await c.execute(
        """INSERT INTO ranking.conversions(snapshot_id,evidence_sha256,method,source_currency,
        comparison_currency,target_exchange,rate,numerator,denominator,evidence_json)
        VALUES(%s,%s,'direct','USDT','USD','bybit',1,'1','1','{}') RETURNING id""",
        (snapshot, "b" * 64),
    )
    row = await cur.fetchone()
    assert row is not None
    conversion = int(row[0])
    await c.execute(
        "INSERT INTO ranking.conversion_inputs(conversion_id,observation_id,leg_index) VALUES(%s,%s,0)",
        (conversion, observation),
    )
    await c.execute(
        """INSERT INTO ranking.entries(snapshot_id,instrument_id,instrument_reference,observation_id,
        position,score,conversion_id,native_turnover,native_unit) VALUES(%s,%s,%s,%s,1,1,%s,1,'USDT')""",
        (snapshot, identity, instrument.reference, observation, conversion),
    )
    if published:
        await c.execute("UPDATE ranking.snapshots SET status='published' WHERE id=%s", (snapshot,))
    return snapshot, identity, observation, conversion


@pytest.mark.asyncio
async def test_name_lookup_shared_with_normalization_accepts_old_partial_and_ties(
    database: AsyncConnection[TupleRow], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pin the name argument for the turnover convenience loader to this isolated policy.
    policy = "maint_" + uuid4().hex
    first, _, _, _ = await seed(database, policy)
    second, identity, _, _ = await seed(database, policy)
    expected = max(first, second)
    assert await latest_snapshot(database, policy) == expected
    assert await find_latest_snapshot(database, policy) == expected

    async def lookup(c: AsyncConnection[TupleRow], name: str) -> UUID | None:
        assert name == "turnover"
        return await find_latest_snapshot(c, policy)

    monkeypatch.setattr("forge.historical.normalization.batch.find_latest_snapshot", lookup)
    snapshot, positions = await ranking_positions(database)
    assert snapshot == str(expected) and len(positions) == 1
    if expected == second:
        assert positions == {identity: 1}


@pytest.mark.asyncio
async def test_preview_apply_cleanup_and_ordinary_guards(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    policy = "maint_" + uuid4().hex
    one, instrument, observation, conversion = await seed(database, policy)
    two, _, _, _ = await seed(database, policy)
    unrelated, _, _, _ = await seed(database, policy + "_other")
    checks: list[tuple[LiteralString, tuple[object, ...]]] = [
        ("DELETE FROM ranking.snapshots WHERE id=%s", (one,)),
        ("UPDATE ranking.snapshots SET partial_universe=false WHERE id=%s", (one,)),
        ("DELETE FROM ranking.entries WHERE snapshot_id=%s", (one,)),
        ("DELETE FROM ranking.conversion_inputs WHERE conversion_id=%s", (conversion,)),
        ("DELETE FROM catalog.metric_observations WHERE id=%s", (observation,)),
    ]
    for query, parameters in checks:
        with pytest.raises(psycopg.Error, match="immutable"):
            async with database.transaction():
                await database.execute(query, parameters)
    preview = await delete_snapshots(database, paths, policy)
    assert preview["matched_count"] == 2 and preview["deleted_count"] == 0
    assert {obj(row)["id"] for row in array(preview["snapshots"])} == {str(one), str(two)}
    result = await delete_snapshots(database, paths, str(one), apply=True)
    assert result["deleted_ids"] == [str(one)]
    for table, _column, value in [
        ("entries", "snapshot_id", one),
        ("conversions", "snapshot_id", one),
        ("conversion_inputs", "conversion_id", conversion),
    ]:
        # Fixed fixture identifiers only; production queries remain parameterized.
        queries: dict[str, LiteralString] = {
            "entries": "SELECT count(*) FROM ranking.entries WHERE snapshot_id=%s",
            "conversions": "SELECT count(*) FROM ranking.conversions WHERE snapshot_id=%s",
            "conversion_inputs": "SELECT count(*) FROM ranking.conversion_inputs WHERE conversion_id=%s",
        }
        cur = await database.execute(queries[table], (value,))
        assert await cur.fetchone() == (0,)
    cur = await database.execute("SELECT id FROM catalog.instruments WHERE id=%s", (instrument,))
    assert await cur.fetchone() == (instrument,)
    cur = await database.execute(
        "SELECT id FROM catalog.metric_observations WHERE id=%s", (observation,)
    )
    assert await cur.fetchone() == (observation,)
    assert await latest_snapshot(database, policy + "_other") == unrelated
    absent = await delete_snapshots(database, paths, str(one), apply=True)
    assert absent["status"] == "already_absent"
    result = await delete_snapshots(database, paths, policy, apply=True)
    assert result["deleted_ids"] == [str(two)]
    assert (await delete_snapshots(database, paths, policy))["matched_count"] == 0
    # The local maintenance permission was restored, including for another publication.
    with pytest.raises(psycopg.Error, match="immutable"):
        async with database.transaction():
            await database.execute("DELETE FROM ranking.snapshots WHERE id=%s", (unrelated,))


@pytest.mark.asyncio
async def test_mixed_selection_blocks_managed_job_and_dataset_references(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    policy = "maint_" + uuid4().hex
    blocked, _, _, _ = await seed(database, policy)
    eligible, _, _, _ = await seed(database, policy)
    run = paths.runs_dir / "fixture"
    run.mkdir(parents=True)
    (run / "normalize-batch.json").write_text(
        json.dumps({"ranking_snapshot": str(blocked), "jobs": []})
    )
    await database.execute(
        """INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,storage_format,status,operation,operation_details)
        VALUES('trade',0,1,'parquet','pending','normalize',%s)""",
        (json_value({"ranking_snapshot": str(blocked)}),),
    )
    result = await delete_snapshots(database, paths, policy, apply=True)
    assert result["status"] == "partial" and result["deleted_ids"] == [str(eligible)]
    assert result["blocked_ids"] == [str(blocked)]
    row = obj(next(r for r in array(result["snapshots"]) if obj(r)["id"] == str(blocked)))
    assert any("Dataset" in str(reason) for reason in array(row["blockers"]))
    assert any("Managed artifact" in str(reason) for reason in array(row["blockers"]))
    draft, _, _, _ = await seed(database, policy, published=False)
    assert (await delete_snapshots(database, paths, str(draft)))["matched_count"] == 1
    assert (await delete_snapshots(database, paths, policy))["matched_count"] == 1
    # --all preview covers every status/policy; it never deletes user snapshots.
    everything = await delete_snapshots(database, paths, all_snapshots=True)
    assert str(blocked) in {obj(r)["id"] for r in array(everything["snapshots"])}
    assert str(draft) in {obj(r)["id"] for r in array(everything["snapshots"])}
    assert everything["deleted_count"] == 0


@pytest.mark.asyncio
async def test_active_preparation_lock_blocks_deletion_until_publication_boundary(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    async with connect_testing(guarded_dsn(ROOT)) as other:
        async with ranking_lock(database, shared=True):
            cur = await other.execute("SELECT pg_try_advisory_lock(%s)", (RANKING_LOCK,))
            assert await cur.fetchone() == (False,)
            task = asyncio.create_task(delete_snapshots(other, paths, str(uuid4()), apply=True))
            # Let the real database receive the deletion request; it must remain blocked.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.05)
        result = await asyncio.wait_for(task, timeout=3)
        assert result["status"] == "already_absent"


@pytest.mark.asyncio
async def test_isolated_policy_automatic_creation_and_cleanup_uses_real_workflow(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from forge.historical.normalization.ranking import ensure_ranking
    from forge.markets.ranking import workflow
    from forge.shared.config.models import HttpSettings

    policy = "maintenance_auto_" + uuid4().hex
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    cur = await database.execute("SELECT id FROM ranking.snapshots")
    original = {row[0] for row in await cur.fetchall()}
    old, instrument, observation, _ = await seed(database, policy, now=datetime.now(UTC))
    assert (await delete_snapshots(database, paths, policy, apply=True))["deleted_ids"] == [
        str(old)
    ]
    cur = await database.execute(
        "SELECT symbol FROM catalog.instruments WHERE id=%s", (instrument,)
    )
    row = await cur.fetchone()
    assert row is not None
    tmp_path.joinpath(policy + ".toml").write_text(f"""version=1
comparison_currency="USDT"
instruments=["bybit.spot.{row[0]}"]
allow_partial_universe=true
""")
    cur = await database.execute(
        "SELECT refresh_id FROM catalog.metric_observations WHERE id=%s", (observation,)
    )
    row = await cur.fetchone()
    assert row is not None
    collect = AsyncMock(return_value={"status": "complete", "refresh_id": row[0], "sources": []})

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(workflow, "collect", collect)
    monkeypatch.setattr(workflow, "testing_connection", connect)
    async with ranking_lock(database):
        created = await ensure_ranking(
            database, "fixture", tmp_path, policy, HttpSettings(), dry_run=False
        )
        assert created.details["decision"] == "created" and created.snapshot is not None
        reused = await ensure_ranking(
            database, "fixture", tmp_path, policy, HttpSettings(), dry_run=False
        )
        assert reused.details["decision"] == "reused" and reused.snapshot == created.snapshot
        assert collect.await_count == 1
    result = await delete_snapshots(database, paths, policy, apply=True)
    assert result["deleted_ids"] == [str(created.snapshot)]
    assert tmp_path.joinpath(policy + ".toml").exists()
    cur = await database.execute("SELECT id FROM ranking.snapshots")
    assert {row[0] for row in await cur.fetchall()} == original


@pytest.mark.asyncio
async def test_competing_preparations_recheck_and_create_only_once(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock

    from forge.historical.normalization import ranking
    from forge.shared.config.models import HttpSettings

    # Only coordination is live DB work; publication/collection state is controlled.
    # Thus no committed fixture history or network access is needed for the race.
    state: list[UUID] = []
    builds: list[str] = []
    cursor = AsyncMock()
    cursor.__aenter__.return_value = cursor
    cursor.fetchone.return_value = {
        "metric": "turnover_24h",
        "created_at": "2001-01-01",
        "partial_universe": True,
    }
    mock = AsyncMock()

    def cursor_factory(**kwargs: object) -> AsyncMock:
        return cursor

    mock.cursor = cursor_factory
    metadata = cast(AsyncConnection[TupleRow], mock)
    tmp_path.joinpath("turnover.toml").write_bytes(
        (ROOT / "data/.config/markets/rankings/turnover.toml").read_bytes()
    )

    async def latest(c: AsyncConnection[TupleRow], policy: str) -> UUID | None:
        return state[0] if state else None

    async def publish(
        dsn: str, settings: HttpSettings, directory: Path, names: tuple[str, ...]
    ) -> dict[str, object]:
        builds.extend(names)
        await asyncio.sleep(0.02)
        state.append(uuid4())
        return {
            "rankings": [{"status": "published", "snapshot_id": str(state[0])}],
            "status": "complete",
        }

    async def prepare(c: AsyncConnection[TupleRow]) -> ranking.RankingSelection:
        async with ranking_lock(c):
            return await ranking.ensure_ranking(
                metadata, "controlled", tmp_path, "turnover", HttpSettings(), dry_run=False
            )

    monkeypatch.setattr(ranking, "find_latest_snapshot", latest)
    monkeypatch.setattr(ranking, "refresh_and_publish", publish)
    async with connect_testing(guarded_dsn(ROOT)) as other:
        first, second = await asyncio.gather(prepare(database), prepare(other))
    assert builds == ["turnover"]
    assert {first.details["decision"], second.details["decision"]} == {"created", "reused"}
    assert first.snapshot == second.snapshot == state[0]
