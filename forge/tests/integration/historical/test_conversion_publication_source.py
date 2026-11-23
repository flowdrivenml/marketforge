"""Publication-bound conversion recipes use exact current registry selections."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from forge.historical.conversion import recipes, registry
from forge.historical.conversion.evidence import checked_binding, resolve_binding
from forge.historical.engine.managed import checksum
from forge.shared.database.connection import database_environment, registry_id
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_exact_publication_binding_and_eligibility(
    database: registry.Connection, tmp_path: Path
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.datasets_dir / "merged" / "501"
    root.mkdir(parents=True)
    manifest_path = root / "manifest.json"
    job_path = root / "merge-job.json"
    primary = root / "part.parquet"
    primary.write_bytes(b"fixture: only metadata is read for binding")
    time_range = {"start_ns": 1_793_000_000_000_000_003, "end_ns": 1_793_000_001_000_000_007}
    job = {
        "job_id": 501,
        "operation": "merge",
        "output": {"dataset_path": str(root)},
        "time_range": time_range,
    }
    entry = {
        "path": primary.name,
        "rows": 1,
        "start_timestamp_ns": time_range["start_ns"] + 4,
        "end_timestamp_ns": time_range["end_ns"] - 5,
    }
    manifest = {
        "files_written": 1,
        "events_written": 1,
        "trade_events": 1,
        "depth_events": 0,
        "job_id": 501,
        "protocol_version": 1,
        "physical_schema_version": 1,
        "mode": "trades",
        "time_range": time_range,
        "files": [entry],
        "start_timestamp_ns": time_range["start_ns"] + 4,
        "end_timestamp_ns": time_range["end_ns"] - 5,
    }
    manifest_path.write_text(json.dumps(manifest))
    job_path.write_text(json.dumps(job))
    inventory = {file.name: checksum(file) for file in (manifest_path, job_path, primary)}
    (root / "publication-checksums.json").write_text(json.dumps(inventory))
    namespace = uuid4().hex
    evidence = {
        "file_memberships": {primary.name: []},
        "manifest": manifest,
        "path": str(root),
        "instrument_namespace": namespace,
        "content_type": "trades",
        "engine_dataset_id": "501",
        "manifest_path": str(manifest_path),
        "resolved_job": job,
        "inventory_sha256": inventory,
        "manifest_sha256": inventory[manifest_path.name],
        "processing_job_sha256": inventory[job_path.name],
    }
    row = await (
        await database.execute(
            """INSERT INTO historical.dataset_publications
           (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
           VALUES(%s,'501','merge',%s,%s,%s) RETURNING id""",
            (namespace, str(root), str(manifest_path), Jsonb(evidence)),
        )
    ).fetchone()
    assert row is not None
    publication_id = int(row[0])
    with pytest.raises(MarketForgeError, match="no complete registered files"):
        await registry.selectable_publication(database, publication_id)
    await database.execute(
        """INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,
           storage_format,storage_path,status,completed_at,operation,storage_root_key,manifest_path,
           operation_details,publication_id,primary_file,file_sha256)
           VALUES('trade',%s,%s,'parquet',%s,'complete',now(),'merge',%s,%s,%s,%s,%s,%s)""",
        (
            manifest["start_timestamp_ns"],
            manifest["end_timestamp_ns"],
            str(primary),
            namespace,
            str(manifest_path),
            Jsonb({**entry, "integrity": "physical_validation", "memberships": []}),
            publication_id,
            primary.name,
            inventory[primary.name],
        ),
    )
    pub = await registry.selectable_publication(database, publication_id)
    selected_registry = await registry_id(database)
    binding = await checked_binding(database, pub, selected_registry)
    await database.execute(
        "UPDATE historical.datasets SET file_sha256=%s WHERE publication_id=%s",
        ("0" * 64, publication_id),
    )
    with pytest.raises(MarketForgeError, match="inconsistent committed evidence"):
        await checked_binding(database, pub, selected_registry)
    await database.execute(
        "UPDATE historical.datasets SET file_sha256=%s WHERE publication_id=%s",
        (inventory[primary.name], publication_id),
    )
    created = recipes.create_bound(paths, "target", binding, binding.interval, "USDT")
    local, _, _ = recipes.load_recipe(paths, Path(str(created["path"])), retain=False)
    assert local.source == binding and local.selection.whole_publication
    assert local.interval.model_dump() == time_range
    assert await resolve_binding(database, binding) == pub
    with (
        database_environment(testing=False),
        pytest.raises(MarketForgeError, match="Select --testing"),
    ):
        await resolve_binding(database, binding)
    with pytest.raises(MarketForgeError, match="different testing registry"):
        await resolve_binding(database, binding.model_copy(update={"registry_id": str(uuid4())}))
    with pytest.raises(MarketForgeError, match="not a registered merge"):
        await registry.selectable_publication(database, 2**63 - 1)
    job_path.write_text("{}")
    with pytest.raises(MarketForgeError, match="metadata differs"):
        await resolve_binding(database, binding)
    job_path.write_text(json.dumps(job))
    await database.execute(
        "UPDATE historical.dataset_publications SET operation='normalize' WHERE id=%s",
        (publication_id,),
    )
    with pytest.raises(MarketForgeError, match="not a registered merge"):
        await resolve_binding(database, binding)
    await database.execute(
        "UPDATE historical.dataset_publications SET operation='merge' WHERE id=%s",
        (publication_id,),
    )
    other = await (
        await database.execute(
            """INSERT INTO historical.dataset_publications
           (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
           VALUES(%s,'502','merge',%s,%s,'{}') RETURNING id""",
            (namespace, str(root / "other"), str(root / "other/manifest.json")),
        )
    ).fetchone()
    assert other is not None
    await database.execute(
        """INSERT INTO historical.publication_invalidations
           (publication_id,source_publication_id,cleanup_id,evidence) VALUES(%s,%s,%s,'{}')""",
        (publication_id, other[0], uuid4()),
    )
    with pytest.raises(MarketForgeError, match="invalidated"):
        await resolve_binding(database, binding)
    await database.execute(
        "DELETE FROM historical.publication_invalidations WHERE publication_id=%s",
        (publication_id,),
    )
    await database.execute(
        """INSERT INTO historical.publication_supersessions
           (publication_id,canonical_id,state,cleanup_id,evidence,removed_at)
           VALUES(%s,%s,'removed',%s,'{}',now())""",
        (publication_id, other[0], uuid4()),
    )
    with pytest.raises(MarketForgeError, match="superseded, retired"):
        await resolve_binding(database, binding)
