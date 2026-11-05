"""Recipe batches resolve real registry/catalog rows; all fixture writes roll back."""

import gzip
import json
import re
import shutil
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
async def test_two_v3_jobs_three_tasks_deduplicated_and_no_engine(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    for directory in ("profiles", "raw_schemas"):
        shutil.copytree(
            ROOT / "data/.config/historical" / directory,
            paths.config_dir / "historical" / directory,
        )
    symbol = "MF_RECIPE_" + uuid4().hex
    instrument_id = await save_instrument(
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
        assert job["protocol_version"] == 3 and job["operation"] == "normalize"
        loader = (ROOT / "engine/src/historical/config/job.rs").read_text()
        envelope = loader.split("pub struct NormalizationJobV3 {", 1)[1].split("\n}", 1)[0]
        assert set(job) == set(re.findall(r"pub (\w+):", envelope))
        task_shape = loader.split("pub struct ProfileTask {", 1)[1].split("\n}", 1)[0]
        for task in array(job["tasks"]):
            assert set(obj(task)) == set(re.findall(r"pub (\w+):", task_shape))
        assert job["ordering"] == {"primary": "event_timestamp_ns", "tie_break": "stream_rank"}
        tasks = [obj(task) for task in array(job["tasks"])]
        assert [t["task_id"] for t in tasks] == [1, 2, 3]
        assert all(t["instrument_id"] == instrument_id for t in tasks)
        assert all(t["profile_fingerprint"] is None for t in tasks)
        assert obj(tasks[0]["instrument"])["tick_size"] == "0.0100"
        assert array(job["streams"]) == [{"stream_id": tasks[0]["stream_id"], "stream_rank": 1}]
        assert len({t["stream_id"] for t in tasks}) == 1
        content = obj(job["output"])["content_type"]
        assert str(tasks[0]["stream_id"]).endswith("." + str(content))
        assert tasks[0]["source_compression"] == ("gzip" if content == "trades" else "zip")
        assert tasks[0]["archive_member"] == (None if content == "trades" else "native-symbol.data")
        assert "resources" not in job and "integrity_policy" not in job
    assert len(ids) == 4
    report = obj(json.loads(json.dumps(plan.report, default=str)))
    publish_batch(paths, plan, report)
    assert [p.name for p in plan.run.iterdir()] == ["normalize-batch.json"]
    wrapper = obj(json.loads((plan.run / "normalize-batch.json").read_text()))
    assert wrapper["kind"] == "normalization_batch" and wrapper["schema_version"] == 1
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
