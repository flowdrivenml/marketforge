"""Recipe batches resolve real registry/catalog rows; all fixture writes roll back."""

import gzip
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import (
    AcquisitionConfig,
    AcquisitionInterval,
    ArchiveRequest,
    ArchiveScope,
)
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import register_remote_archive
from forge.historical.archives.storage.acquire import root_key
from forge.historical.normalization.batch import plan_batch, publish_batch
from forge.markets.models import Instrument, array, obj
from forge.markets.repository import save_instrument
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]
ROOT = Path(__file__).resolve().parents[5]
BASE = datetime(2026, 10, 27, tzinfo=UTC)


@pytest.mark.asyncio
async def test_two_forge_jobs_three_tasks_deduplicated_and_no_engine(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    symbol = "MF_RECIPE_" + uuid4().hex
    await save_instrument(
        database,
        Instrument(
            exchange="bybit",
            market="spot",
            symbol=symbol,
            base="BTC",
            quote="USDT",
            tick_size=Decimal("0.0100"),
        ),
        BASE,
    )
    requests = [
        ArchiveRequest(
            exchange="bybit",
            instrument_type="spot",
            market_category="spot",
            data_type=content,
            scope=ArchiveScope(kind="instrument", identifier=symbol),
        )
        for content in ("trades", "order_book_l2")
    ]
    for request in requests:
        for day in range(3):
            bounds = AcquisitionInterval(
                start=BASE + timedelta(days=day), end=BASE + timedelta(days=day + 1)
            )
            remote = await register_remote_archive(
                database,
                DiscoveredArchive(
                    request=request,
                    source_object_key=uuid4().hex,
                    filename="fixture",
                    interval=bounds,
                ),
            )
            relative = f"{remote.id}/input.bin"
            file = paths.raw_dir / relative
            file.parent.mkdir(parents=True)
            if request.data_type == "trades":
                file.write_bytes(gzip.compress(b"fixture", mtime=0))
            else:
                with zipfile.ZipFile(file, "w") as z:
                    z.writestr("native-symbol.data", "{}\n")
                    z.writestr("another-contract.data", "{}\n")
            await database.execute(
                """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,
                status,bytes_written,sha256,downloaded_at) VALUES(%s,%s,%s,'complete',%s,%s,%s)""",
                (remote.id, root_key(paths.raw_dir), relative, file.stat().st_size, "a" * 64, BASE),
            )
    recipe = AcquisitionConfig(
        interval=AcquisitionInterval(start=BASE, end=BASE + timedelta(days=3)),
        requests=[*requests, requests[0]],
    )
    plan = await plan_batch(database, paths, recipe)
    repeated = await plan_batch(database, paths, recipe)
    assert plan.jobs == repeated.jobs and plan.report == repeated.report
    assert not paths.runs_dir.exists()
    assert plan.report["status"] == "prepared"
    assert len(plan.jobs) == 2
    ids: set[object] = set()
    for job in plan.jobs.values():
        ids.update([job["job_id"], job["dataset_id"]])
        assert "protocol_version" not in job and job["operation"] == "normalize"
        assert job["ordering"] == {"primary": "event_timestamp_ns", "tie_break": "stream_rank"}
        tasks = [obj(task) for task in array(job["tasks"])]
        assert [t["task_id"] for t in tasks] == [1, 2, 3]
        assert all(t["symbol"] == symbol for t in tasks)
        assert all(
            t["exchange"] == "bybit"
            and t["instrument_type"] == "spot"
            and t["market_category"] == "spot"
            for t in tasks
        )
        assert all(t["base_asset"] == "BTC" and t["quote_asset"] == "USDT" for t in tasks)
        assert all(
            not {
                "instrument_id",
                "format_profile",
                "profile_fingerprint",
                "archive_member",
                "source_compression",
            }
            & t.keys()
            for t in tasks
        )
        assert obj(tasks[0]["instrument"])["tick_size"] == "0.0100"
        assert "streams" not in job
        assert all(t["stream_rank"] == 1 for t in tasks)
        assert len({t["stream_id"] for t in tasks}) == 1
        content = obj(job["output"])["content_type"]
        assert str(tasks[0]["stream_id"]).endswith("." + str(content))
        assert "resources" not in job and "integrity_policy" not in job
    assert len(ids) == 4
    report = obj(json.loads(json.dumps(plan.report, default=str)))
    publish_batch(paths, plan, report)
    assert sorted(p.name for p in plan.run.iterdir()) == [
        "normalize-batch.json",
        "prepare-report.json",
    ]
    wrapper = obj(json.loads((plan.run / "normalize-batch.json").read_text()))
    assert wrapper["kind"] == "normalization_batch" and wrapper["schema_version"] == 3
    assert array(wrapper["jobs"]) == list(plan.jobs.values())
    # Published IDs are unreserved but a later batch avoids their on-disk collisions.
    next_plan = await plan_batch(database, paths, recipe)
    assert set(next_plan.jobs).isdisjoint(plan.jobs)
    assert not (paths.bin_dir / "marketforge-normalize").exists()
    # Missing one input is diagnosed; resolved tasks in both content jobs survive.
    next(paths.raw_dir.rglob("input.bin")).unlink()
    partial = await plan_batch(database, paths, recipe)
    assert partial.report["status"] == "partial"
    assert len(partial.jobs) == 2
    assert sorted(len(array(j["tasks"])) for j in partial.jobs.values()) == [2, 3]


@pytest.mark.asyncio
async def test_family_assets_use_consensus_without_contract_specs(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    family = "MF_FAMILY_" + uuid4().hex
    for index in range(2):
        await save_instrument(
            database,
            Instrument(
                exchange="okx",
                market="linear",
                instrument_type="future",
                symbol=f"{family}-{index}",
                base="BTC",
                quote="USD",
                tick_size=Decimal(str(index + 1)),
                quantity_type="contracts",
                contract_value=Decimal(str(index + 10)),
                contract_unit="BTC",
                raw={"instFamily": family},
            ),
            BASE,
        )
    request = ArchiveRequest(
        exchange="okx",
        instrument_type="future",
        market_category="linear",
        data_type="trades",
        scope=ArchiveScope(kind="family", identifier=family),
    )
    remote = await register_remote_archive(
        database,
        DiscoveredArchive(
            request=request,
            source_object_key=uuid4().hex,
            filename="family.zip",
            interval=AcquisitionInterval(start=BASE, end=BASE + timedelta(days=1)),
        ),
    )
    file = paths.raw_dir / "family.zip"
    file.parent.mkdir(parents=True)
    with zipfile.ZipFile(file, "w") as z:
        z.writestr("first-contract.csv", "first")
        z.writestr("second-contract.csv", "second")
    await database.execute(
        """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,
        status,bytes_written,sha256,downloaded_at) VALUES(%s,%s,%s,'complete',%s,%s,%s)""",
        (remote.id, root_key(paths.raw_dir), file.name, file.stat().st_size, "b" * 64, BASE),
    )
    recipe = AcquisitionConfig(
        interval=AcquisitionInterval(start=BASE, end=BASE + timedelta(days=1)),
        requests=[request, request],
    )
    plan = await plan_batch(database, paths, recipe)
    assert plan.report["status"] == "prepared"
    assert len(plan.jobs) == 1
    job = next(iter(plan.jobs.values()))
    tasks = array(job["tasks"])
    assert len(tasks) == 1
    task = obj(tasks[0])
    assert task["scope"] == {"kind": "family", "identifier": family}
    assert task["symbol"] is None and task["instrument"] is None
    assert task["base_asset"] == "BTC" and task["quote_asset"] == "USD"
    assert task["stream_rank"] == 1
    assert (
        not {
            "format_profile",
            "profile_fingerprint",
            "source_compression",
            "archive_member",
            "instrument_id",
        }
        & task.keys()
    )
    assert not paths.config_dir.exists()


@pytest.mark.asyncio
async def test_bitget_pair_preserves_provenance_and_uses_resolved_ranking(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    unique = uuid4().hex
    base = "Mf" + unique
    native = "ZZ_NATIVE_" + unique
    historical = f"{base.swapcase()}/uSdT"
    resolved_id = await save_instrument(
        database,
        Instrument(
            exchange="bitget",
            market="spot",
            symbol=native,
            base=base,
            quote="UsDt",
            tick_size=Decimal("0.0100"),
        ),
        BASE,
    )
    exact = "AA_EXACT_" + unique
    await save_instrument(
        database,
        Instrument(
            exchange="bitget",
            market="spot",
            symbol=exact,
            base="OTHER_" + unique,
            quote="BTC",
            tick_size=Decimal("0.1"),
        ),
        BASE,
    )
    requests = [
        ArchiveRequest(
            exchange="bitget",
            instrument_type="spot",
            market_category="spot",
            data_type="trades",
            scope=ArchiveScope(kind="instrument", identifier=identifier),
        )
        for identifier in (historical, exact)
    ]
    for index, request in enumerate(requests):
        remote = await register_remote_archive(
            database,
            DiscoveredArchive(
                request=request,
                source_object_key=uuid4().hex,
                filename="unchanged.zip",
                interval=AcquisitionInterval(start=BASE, end=BASE + timedelta(days=1)),
            ),
        )
        file = paths.raw_dir / f"{index}/unchanged.zip"
        file.parent.mkdir(parents=True)
        file.write_bytes(b"Complete archive contents belong to the engine")
        await database.execute(
            """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,
            status,bytes_written,sha256,downloaded_at) VALUES(%s,%s,%s,'complete',%s,%s,%s)""",
            (
                remote.id,
                root_key(paths.raw_dir),
                str(file.relative_to(paths.raw_dir)),
                file.stat().st_size,
                "c" * 64,
                BASE,
            ),
        )
    calls: list[str] = []

    async def ranking(connection: AsyncConnection[TupleRow]) -> tuple[str, dict[int, int]]:
        calls.append("resolved")
        return "frozen-fixture-snapshot", {resolved_id: 100}

    monkeypatch.setattr("forge.historical.normalization.batch.ranking_positions", ranking)
    recipe = AcquisitionConfig(
        interval=AcquisitionInterval(start=BASE, end=BASE + timedelta(days=1)),
        requests=[*requests, requests[0]],
    )
    plan = await plan_batch(database, paths, recipe)
    assert calls == ["resolved"]
    assert plan.report["status"] == "prepared" and plan.batch["schema_version"] == 3
    tasks = [obj(t) for t in array(next(iter(plan.jobs.values()))["tasks"])]
    assert len(tasks) == 2
    # The fallback stream ranks ahead of the lexically earlier unranked stream.
    assert [(t["symbol"], t["stream_rank"]) for t in tasks] == [(native, 1), (exact, 2)]
    assert tasks[0]["input_path"] == str(paths.raw_dir / "0/unchanged.zip")
    assert tasks[0]["base_asset"] == base and tasks[0]["quote_asset"] == "UsDt"
    assert obj(tasks[0]["instrument"])["tick_size"] == "0.0100"
    assert tasks[0]["stream_id"] == f"bitget.spot.spot.{native}.trades"
    report = obj(json.loads(json.dumps(plan.report, default=str)))
    selected = obj(array(obj(array(report["requests"])[0])["selected_archives"])[0])
    assert selected["instrument_id"] == resolved_id
    assert selected["identity_resolution"] == {
        "archive_identifier": historical,
        "method": "bitget_spot_asset_pair",
        "resolved_symbol": native,
        "parsed_assets": {"base_asset": base.swapcase(), "quote_asset": "uSdT"},
    }
    publish_batch(paths, plan, report)
    saved = obj(json.loads((plan.run / "prepare-report.json").read_text()))
    assert saved == report
    assert "identity_resolution" not in tasks[0]
