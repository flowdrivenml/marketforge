"""Exercise the publication cleanup transaction against two disposable registries."""

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest
from psycopg.types.json import Jsonb

from forge.datasets import deduplication, force_cleanup, publication_cleanup
from forge.historical.conversion.execution import require_live_prepared_inputs
from forge.historical.engine.managed import checksum
from forge.historical.engine.work import WorkIndex
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_environment,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

pytestmark = pytest.mark.asyncio


async def seed(
    paths: ProjectPaths,
    name: str,
    payload: bytes,
    *,
    op: str = "normalize",
    root: Path | None = None,
) -> tuple[int, int, dict[str, Any]]:
    dataset = root or paths.datasets_dir / "normalized" / name
    dataset.mkdir(parents=True, exist_ok=True)
    records = {"manifest.json": b"{}", "part.parquet": payload}
    for filename, body in records.items():
        (dataset / filename).write_bytes(body)
    index = dataset / "publication-checksums.json"
    index.write_bytes(b"fixture checksums")
    hashes = {filename: checksum(dataset / filename) for filename in records}
    async with database_connection(database_dsn(paths.root)) as connection:
        cursor = await connection.execute(
            """INSERT INTO historical.dataset_publications
               (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
               VALUES ('dedup-integration',%s,%s,%s,%s,%s) RETURNING id""",
            (
                name,
                op,
                str(dataset),
                str(dataset / "manifest.json"),
                Jsonb({"manifest_sha256": hashes["manifest.json"], "inventory_sha256": hashes}),
            ),
        )
        row = await cursor.fetchone()
        assert row is not None
        publication = row[0]
        cursor = await connection.execute(
            """INSERT INTO historical.datasets
               (data_type,start_timestamp_ns,end_timestamp_ns,storage_format,storage_path,status,
               completed_at,operation,storage_root_key,manifest_path,publication_id,primary_file,file_sha256)
               VALUES ('trade',1,2,'parquet',%s,'complete',clock_timestamp(),%s,%s,%s,%s,'part.parquet',%s)
               RETURNING id""",
            (
                str(dataset / "part.parquet"),
                op,
                str(dataset),
                str(dataset / "manifest.json"),
                publication,
                hashes["part.parquet"],
            ),
        )
        row = await cursor.fetchone()
        assert row is not None
    return (
        publication,
        row[0],
        {
            "path": str(dataset),
            "checksum_index_sha256": checksum(index),
            "manifest_sha256": hashes["manifest.json"],
            "inventory_sha256": hashes,
        },
    )


async def test_retirement_recovery_and_independent_protected_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.environ.get("MARKETFORGE_TEST_CLEANUP_DISPOSABLE") != "1":
        pytest.skip("Requires two explicitly disposable migrated registries.")
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    with database_environment(testing=True):
        fixture_root = paths.data_root / "tests/historical/normalize" / "pipeline-fixture"
        canonical, canonical_file, canonical_proof = await seed(
            paths, "101", b"same", root=fixture_root / "datasets/normalized/101"
        )
        intent = fixture_root / "runs/job/steps/normalize/attempt/execution-intent.json"
        intent.parent.mkdir(parents=True)
        intent.write_text("{}")
        pipeline = fixture_root / "runs/job/report.json"
        pipeline.write_text(
            json.dumps(
                {
                    "environment": "testing",
                    "registry_id": await selected_registry_id(paths.root),
                    "report_path": str(pipeline),
                    "stages": {
                        "normalize": {
                            "state": "complete",
                            "execution_intent": str(intent),
                            "primary_file_ids": [canonical_file],
                        }
                    },
                }
            )
        )

        redundant, redundant_file, proof = await seed(paths, "102", b"same")
        protected, protected_file, protected_proof = await seed(paths, "103", b"same")
        _, child, _ = await seed(paths, "105", b"consumer", op="merge")
        async with database_connection(database_dsn(paths.root)) as connection:
            await connection.execute(
                "INSERT INTO historical.dataset_inputs VALUES (%s,%s)", (child, protected_file)
            )
            # Keep the canonical preference anchored independently of scan order.
            await connection.execute(
                """INSERT INTO historical.publication_supersessions
                   (publication_id,canonical_id,state,cleanup_id,evidence)
                   VALUES (%s,%s,'retained','00000000-0000-0000-0000-000000000010','{}')""",
                (protected, canonical),
            )
        evidence = {canonical: canonical_proof, redundant: proof, protected: protected_proof}

        def verify(_: ProjectPaths, __: str, target: dict[str, Any]) -> dict[str, Any]:
            publication = target["publication_id"]
            exact = evidence[publication]
            assert (
                checksum(Path(exact["path"]) / "part.parquet")
                == exact["inventory_sha256"]["part.parquet"]
            )
            return {
                **exact,
                "work_key": "1" * 64,
                "engine_sha256": "2" * 64,
                "runtime_sha256": "3" * 64,
                "contract_sha256": "4" * 64,
                "verification": "verified",
                "registry_binding": "intent",
                "evidence_source": str(intent)
                if publication == canonical
                else f"/attempt/{publication}",
                "reuse_result": {"path": exact["path"], "execution_id": "first"},
            }

        monkeypatch.setattr(deduplication, "verify_candidate", verify)
        with database_environment(testing=False):
            foreign_publication, foreign_file, _ = await seed(
                paths, "foreign", b"same", root=Path(proof["path"])
            )
        shared = await deduplication.inventory(paths, "normalize", apply=False)
        assert not shared["groups"][0]["physical_removal_eligible"]
        with monkeypatch.context() as environment:
            environment.setenv("PRODUCTION_DATABASE_KEY", "dbname=marketforge_test")
            missing = await deduplication.inventory(paths, "normalize", apply=False)
            assert not missing["groups"][0]["physical_removal_eligible"]
            assert missing["other_registry_error"]
        assert any(
            "Other registry references" in reason
            for target in shared["groups"][0]["targets"]
            for reason in target["blockers"]
        )
        with database_environment(testing=False):
            async with database_connection(database_dsn(paths.root)) as connection:
                await connection.execute(
                    "DELETE FROM historical.datasets WHERE id=%s", (foreign_file,)
                )
                await connection.execute(
                    "DELETE FROM historical.dataset_publications WHERE id=%s",
                    (foreign_publication,),
                )
        preview = await deduplication.inventory(paths, "normalize", apply=False)
        assert len(preview["groups"]) == 1
        assert preview["groups"][0]["canonical_publication_id"] == canonical
        assert preview["groups"][0]["physical_removal_eligible"]
        assert await asyncio.to_thread(Path(proof["path"]).is_dir)
        removal = publication_cleanup.remove_reviewed_tree

        def interrupt(_: dict[str, Any]) -> None:
            raise OSError("interrupted after registry retirement")

        with monkeypatch.context() as injection:
            injection.setattr(publication_cleanup, "remove_reviewed_tree", interrupt)
            first = await deduplication.apply_inventory(paths, "normalize", preview=preview)
        assert first["status"] == "partial" and first["physical_files_removed"] == 0
        assert first["retired_publications"] == [redundant]
        assert (await deduplication.inspect_supersession(paths, redundant))["state"] == "retiring"
        assert await asyncio.to_thread(Path(proof["path"]).is_dir)
        async with database_connection(database_dsn(paths.root)) as connection:
            cursor = await connection.execute(
                "SELECT status FROM historical.datasets WHERE id=%s", (redundant_file,)
            )
            assert await cursor.fetchone() == ("superseded",)

        assert publication_cleanup.remove_reviewed_tree == removal
        second = await deduplication.apply_inventory(paths, "normalize")
        assert second["retired_publications"] == [redundant]
        assert second["physical_files_removed"] == 3
        assert second["status"] == "partial"
        assert not await asyncio.to_thread(Path(proof["path"]).exists)
        assert await asyncio.to_thread(
            Path(canonical_proof["path"]).is_dir
        ) and await asyncio.to_thread(Path(protected_proof["path"]).is_dir)
        assert (await deduplication.inspect_supersession(paths, redundant))["state"] == "removed"
        index = WorkIndex(paths.root, preview["registry_id"], "normalize", "verification")
        matches = await index.peek(["1" * 64])
        result = matches["1" * 64][2]
        assert result is not None and result["path"] == canonical_proof["path"]
        assert (await deduplication.inspect_supersession(paths, protected))["state"] == "retained"
        third = await deduplication.apply_inventory(paths, "normalize")
        assert third["physical_files_removed"] == 0
        _, grandchild_file, child_proof = await seed(paths, "106", b"transitive", op="merge")
        async with database_connection(database_dsn(paths.root)) as connection:
            await connection.execute(
                "INSERT INTO historical.dataset_inputs VALUES (%s,%s)", (grandchild_file, child)
            )
        affected, affected_file, affected_proof = await seed(
            paths, "107", b"old canonical consumer", op="merge"
        )
        async with database_connection(database_dsn(paths.root)) as connection:
            await connection.execute(
                "INSERT INTO historical.dataset_inputs VALUES (%s,%s)",
                (affected_file, canonical_file),
            )
        reviewed = await force_cleanup.preview(paths, "normalize")
        group = reviewed["groups"][0]
        plan = group["targets"][0]
        assert group["canonical_publication_id"] == protected
        assert reviewed["canonical_optimization"]["optimized_preserved"] == 2
        assert reviewed["canonical_optimization"]["baseline_preserved"] == 1
        assert plan["publication_id"] == canonical and plan["review"]
        assert plan["managed_storage"] == str(fixture_root)
        assert {node["publication_id"] for node in plan["dependency_graph"]["dependents"]} == {
            affected
        }
        assert await asyncio.to_thread(Path(canonical_proof["path"]).is_dir)
        draft = paths.runs_dir / "draft" / "merge-job.json"
        draft.parent.mkdir(parents=True)
        draft.write_text(f'{{"dataset_ids":[{canonical_file}]}}')
        impacted = await force_cleanup.preview(paths, "normalize")
        assert not impacted["groups"][0]["targets"][0]["review"]
        deduplication.save_preview(paths, reviewed)
        verified_plan = deduplication.load_preview(paths, Path(reviewed["report_path"]))
        rejected = await force_cleanup.apply(paths, "normalize", verified_plan)
        assert rejected["status"] == "blocked" and rejected["invalidated_downstream"] == []
        assert rejected["blocked_actions"][0]["publication_id"] == canonical
        draft.unlink()
        with monkeypatch.context() as injection:
            injection.setattr(publication_cleanup, "remove_reviewed_tree", interrupt)
            forced = await force_cleanup.apply(paths, "normalize", verified_plan)
        assert forced["physical_files_removed"] == 0
        assert forced["retired_publications"] == [canonical]
        assert {item["publication_id"] for item in forced["invalidated_downstream"]} == {
            affected,
        }
        assert (await deduplication.inspect_supersession(paths, affected))["state"] == "invalidated"
        with pytest.raises(MarketForgeError, match="retired testing publications"):
            await require_live_prepared_inputs(paths, reviewed["registry_id"], {affected})
        ordinary = await deduplication.apply_inventory(paths, "normalize")
        assert ordinary["physical_files_removed"] == 0
        resumed = await force_cleanup.apply(paths, "normalize", verified_plan)
        assert resumed["removed_publications"] == [canonical]
        assert resumed["primary_files_removed"] == 1
        assert not await asyncio.to_thread(Path(canonical_proof["path"]).exists)
        assert await asyncio.to_thread(Path(child_proof["path"]).is_dir)
        assert await asyncio.to_thread(Path(protected_proof["path"]).is_dir)
        assert await asyncio.to_thread(Path(affected_proof["path"]).is_dir)
        assert (await deduplication.inspect_supersession(paths, redundant))[
            "canonical_id"
        ] == protected
        result = (await index.peek(["1" * 64]))["1" * 64][2]
        assert result and result["path"] == protected_proof["path"]
        repeated = await force_cleanup.apply(paths, "normalize", verified_plan)
        assert repeated["physical_files_removed"] == 0


async def test_refuses_unreviewed_changes_and_multiple_links(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    root.mkdir()
    path = root / "reviewed"
    path.write_bytes(b"original")
    files, directories = await asyncio.to_thread(publication_cleanup.scan_tree, root)
    review = {
        "root": str(root),
        "files": {name: value.model_dump() for name, value in files.items()},
        "directories": {name: list(value) for name, value in directories.items()},
    }
    foreign = root / "foreign"
    foreign.write_bytes(b"unrelated")
    with pytest.raises(MarketForgeError, match="changed"):
        await asyncio.to_thread(publication_cleanup.remove_reviewed_tree, review)
    assert foreign.exists() and path.exists()
    foreign.unlink()
    link = root / "alias"
    link.hardlink_to(path)
    with pytest.raises(MarketForgeError, match="multiple hard links"):
        await asyncio.to_thread(publication_cleanup.scan_tree, root)


async def test_reviewed_legacy_clearance_and_explicit_risk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.environ.get("MARKETFORGE_TEST_CLEANUP_DISPOSABLE") != "1":
        pytest.skip("Requires a disposable legacy catalogue database.")
    from psycopg import AsyncConnection
    from psycopg.conninfo import make_conninfo

    dsn = make_conninfo(os.environ["TEST_DATABASE_KEY"], dbname="marketforge_fixture_legacy")
    monkeypatch.setenv("PRODUCTION_DATABASE_KEY", dsn)
    async with await AsyncConnection.connect(dsn) as legacy:
        await legacy.execute("CREATE SCHEMA catalog")
        await legacy.execute("CREATE TABLE catalog.schema_meta(key text PRIMARY KEY, value text)")
        await legacy.execute("INSERT INTO catalog.schema_meta VALUES ('schema_version','9')")
        await legacy.execute(
            """CREATE TABLE catalog.datasets(id bigint PRIMARY KEY, storage_path text,
               data_type text, storage_format text,status text)"""
        )
        await legacy.execute(
            """INSERT INTO catalog.datasets
               SELECT i,'data/processed/'||i,'trade','parquet','complete'
               FROM generate_series(1,45) i"""
        )
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    with database_environment(testing=True):
        proofs: dict[int, dict[str, Any]] = {}
        for name in ("legacy-a", "legacy-b", "exclusive-a", "exclusive-b"):
            identity, _, proof = await seed(
                paths,
                name,
                name.split("-")[0].encode(),
                op="merge",
                root=paths.datasets_dir / "merged" / name,
            )
            proofs[identity] = proof

        def verify(_: ProjectPaths, __: str, target: dict[str, Any]) -> dict[str, Any]:
            identity = target["publication_id"]
            proof = proofs[identity]
            path = proof["path"]
            group = Path(path).name.split("-")[0]
            return {
                **proof,
                "work_key": ("1" if group == "legacy" else "2") * 64,
                "engine_sha256": "3" * 64,
                "contract_sha256": "4" * 64,
                "verification": "verified",
                "started_at": Path(path).name,
                "evidence_source": f"/attempt/{identity}",
                "reuse_result": {
                    "path": path,
                    "execution_id": f"execute-{identity}",
                },
            }

        monkeypatch.setattr(deduplication, "verify_candidate", verify)
        preview = await force_cleanup.preview(paths, "merge")
        assert preview["foreign_ownership_evidence"]["unanchored_references"] == 45
        assert len(preview["legacy_risk_targets"]) == 2
        group = next(
            entry
            for entry in preview["groups"]
            if any(item["path"].endswith("/legacy-b") for item in entry["candidates"])
        )
        target = group["targets"][0]
        target_path = Path(proofs[target["publication_id"]]["path"])
        survivor_path = Path(proofs[group["canonical_publication_id"]]["path"])
        foreign = paths.datasets_dir / "merged/exclusive-b"
        # Protect the other group with a known reference even under acknowledgement.
        async with await AsyncConnection.connect(dsn) as legacy:
            await legacy.execute(
                "INSERT INTO catalog.datasets VALUES(46,%s,'trade','parquet','complete')",
                (str(foreign),),
            )
        preview = await force_cleanup.preview(paths, "merge")
        assert preview["legacy_risk_targets"] == [target["publication_id"]]
        assert any(
            item["ownership"]["state"] == "known_foreign_reference"
            for entry in preview["groups"]
            for item in entry["targets"]
        )
        deduplication.save_preview(paths, preview)
        unacknowledged = await force_cleanup.apply(paths, "merge", preview)
        assert unacknowledged["physical_files_removed"] == 0
        async with await AsyncConnection.connect(dsn) as legacy:
            await legacy.execute(
                "INSERT INTO catalog.datasets VALUES(47,%s,'trade','parquet','complete')",
                (str(target_path),),
            )
        blocked = await force_cleanup.apply(
            paths,
            "merge",
            preview,
            accept_unverified_legacy_references=True,
        )
        assert blocked["physical_files_removed"] == 0
        assert blocked["blocked_actions"] and await asyncio.to_thread(target_path.is_dir)
        async with await AsyncConnection.connect(dsn) as legacy:
            await legacy.execute("DELETE FROM catalog.datasets WHERE id=47")
        target_file = target_path / "part.parquet"
        await asyncio.to_thread(target_file.touch)
        changed = await force_cleanup.apply(
            paths,
            "merge",
            preview,
            accept_unverified_legacy_references=True,
        )
        assert changed["physical_files_removed"] == 0
        assert "inventory changed" in changed["blocked_actions"][0]["reason"]
        preview = await force_cleanup.preview(paths, "merge")
        deduplication.save_preview(paths, preview)

        def interrupt_legacy(_: dict[str, Any]) -> None:
            raise OSError("interrupt after acknowledgement and registry retirement")

        with monkeypatch.context() as interruption:
            interruption.setattr(publication_cleanup, "remove_reviewed_tree", interrupt_legacy)
            pending = await force_cleanup.apply(
                paths,
                "merge",
                preview,
                accept_unverified_legacy_references=True,
            )
        assert pending["risk_acknowledged_publications"] == [target["publication_id"]]
        assert target["publication_id"] in pending["retired_publications"]
        without_consent = await force_cleanup.apply(paths, "merge", preview)
        assert without_consent["physical_files_removed"] == 0
        assert await asyncio.to_thread(target_path.is_dir)
        result = await force_cleanup.apply(
            paths,
            "merge",
            preview,
            accept_unverified_legacy_references=True,
        )
        assert result["removed_publications"] == [target["publication_id"]]
        assert result["risk_acknowledged_publications"] == [target["publication_id"]]
        assert not await asyncio.to_thread(target_path.exists)
        assert await asyncio.to_thread(survivor_path.is_dir)
        assert await asyncio.to_thread(foreign.is_dir)
        repeated = await force_cleanup.apply(
            paths,
            "merge",
            preview,
            accept_unverified_legacy_references=True,
        )
        assert repeated["physical_files_removed"] == 0
        # With a separately declared, disjoint root and no known path conflict,
        # the remaining target can be removed without the acknowledgement.
        anchor = tmp_path / "original-production-root"
        anchor.mkdir()
        async with await AsyncConnection.connect(dsn) as legacy:
            await legacy.execute("DELETE FROM catalog.datasets WHERE id=46")
            await legacy.execute(
                "INSERT INTO catalog.schema_meta VALUES ('project_root',%s)", (str(anchor),)
            )
        exclusive = await force_cleanup.preview(paths, "merge")
        assert not exclusive["legacy_risk_targets"]
        deduplication.save_preview(paths, exclusive)
        approved = await force_cleanup.apply(paths, "merge", exclusive)
        assert approved["physical_files_removed"] == 3
        assert approved["risk_acknowledged_publications"] == []
        assert not await asyncio.to_thread(foreign.exists)


async def test_explicit_deletion_keeps_non_equivalent_survivor_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if os.environ.get("MARKETFORGE_TEST_CLEANUP_DISPOSABLE") != "1":
        pytest.skip("Requires disposable testing and production registries.")
    from forge.datasets import explicit_delete

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    with database_environment(testing=True):
        survivor, _, survivor_disk = await seed(paths, "keep", b"matching-content")
        retired, file_id, retired_disk = await seed(paths, "discard", b"matching-content")
        dependent, dependent_file, dependent_disk = await seed(paths, "consumer", b"dependent")
        async with database_connection(database_dsn(paths.root)) as connection:
            await connection.execute(
                "INSERT INTO historical.dataset_inputs VALUES (%s,%s)",
                (dependent_file, file_id),
            )
        proof = {survivor: survivor_disk, retired: retired_disk}

        def verify(_: ProjectPaths, __: str, candidate: dict[str, Any]) -> dict[str, Any]:
            identity = candidate["publication_id"]
            return {
                **proof[identity],
                "work_key": f"{identity:064x}",
                "engine_sha256": f"{identity:064x}",
                "contract_sha256": "5" * 64,
                "verification": "verified",
            }

        monkeypatch.setattr(deduplication, "verify_candidate", verify)
        audit = await deduplication.inventory(paths, "normalize", apply=False)
        assert any(
            group["classification"] == "physically_identical_but_semantics_unproven"
            and {candidate["publication_id"] for candidate in group["candidates"]}
            == {retired, survivor}
            for group in audit["groups"]
        )
        with pytest.raises(MarketForgeError, match="not registered"):
            await explicit_delete.preview(paths, [retired], [], [2**62], cascade=True)
        with pytest.raises(MarketForgeError, match="distinct"):
            await explicit_delete.preview(paths, [retired], [], [retired], cascade=True)
        no_cascade = await explicit_delete.preview(paths, [retired], [], [survivor], cascade=False)
        assert no_cascade["targets"][0]["review"] is None
        assert any("--cascade" in reason for reason in no_cascade["targets"][0]["blockers"])
        plan = await explicit_delete.preview(paths, [retired], [], [survivor], cascade=True)
        assert plan["status"] == "ready"
        assert await asyncio.to_thread(Path(retired_disk["path"]).is_dir)
        deduplication.save_preview(paths, plan)
        corrupted = {**plan, "cascade": False}
        with pytest.raises(MarketForgeError, match="exact reviewed"):
            await explicit_delete.apply(paths, corrupted)

        def interrupt(_: dict[str, Any]) -> None:
            raise OSError("interrupted before physical deletion")

        with monkeypatch.context() as injection:
            injection.setattr(publication_cleanup, "remove_reviewed_tree", interrupt)
            first = await explicit_delete.apply(paths, plan)
        assert first["status"] == "partial" and first["physical_files_removed"] == 0
        assert first["retired_publications"] == [retired]
        assert first["invalidated_downstream"] == [
            {"publication_id": dependent, "source_publication_id": retired}
        ]
        history = await deduplication.inspect_supersession(paths, retired)
        assert history["canonical_id"] is None and history["state"] == "retiring"
        assert "Explicit" in history["evidence"]["reason"]
        completed = await explicit_delete.apply(paths, plan)
        assert completed["removed_publications"] == [retired]
        assert completed["primary_files_removed"] == 1
        assert not await asyncio.to_thread(Path(retired_disk["path"]).exists)
        assert await asyncio.to_thread(Path(survivor_disk["path"]).is_dir)
        assert await asyncio.to_thread(Path(dependent_disk["path"]).is_dir)
        assert (await deduplication.inspect_supersession(paths, retired))["state"] == "removed"
        index = WorkIndex(paths.root, plan["registry_id"], "normalize", "retired-destination")
        with pytest.raises(MarketForgeError, match="explicitly retired"):
            await index.acquire({"b" * 64: {"path": retired_disk["path"]}})
        repeated = await explicit_delete.apply(paths, plan)
        assert repeated["physical_files_removed"] == 0


async def test_explicit_indexed_conversion_has_no_registry_publication_id(tmp_path: Path) -> None:
    if os.environ.get("MARKETFORGE_TEST_CLEANUP_DISPOSABLE") != "1":
        pytest.skip("Requires disposable testing and production registries.")
    from forge.datasets import explicit_delete

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    key = "e" * 64
    root = paths.datasets_dir / "converted" / "307"
    root.mkdir(parents=True)
    manifest = {
        "job_id": 307,
        "conversion": {"target_quote": "USDT"},
        "integrity": {"status": "pass"},
        "output": {
            "content_type": "trades",
            "files_written": 1,
            "first_timestamp_ns": 10,
            "last_timestamp_ns": 11,
            "files": [{"path": "part.parquet"}],
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    (root / "execution.json").write_text("{}")
    (root / "conversion-runtime.json").write_text("{}")
    (root / "part.parquet").write_bytes(b"converted fixture")
    hashes = {item.name: checksum(item) for item in root.iterdir()}
    with database_environment(testing=True):
        registry = await selected_registry_id(paths.root)
        result = {
            "registry_id": registry,
            "work_key": key,
            "status": "complete",
            "input": {},
            "publication": {"manifest": str(root / "manifest.json"), "fingerprints": hashes},
        }
        async with database_connection(database_dsn(paths.root)) as connection:
            await connection.execute(
                """INSERT INTO historical.resolved_work(registry_id,operation,work_key,owner,evidence,result)
                   VALUES (%s,'convert',%s,'convert-fixture',%s,%s)""",
                (registry, key, Jsonb({"path": str(root)}), Jsonb(result)),
            )
        plan = await explicit_delete.preview(paths, [], [key], [], cascade=False)
        assert plan["status"] == "ready"
        assert plan["targets"][0]["target"]["publication_id"] is None
        deduplication.save_preview(paths, plan)
        applied = await explicit_delete.apply(paths, plan)
        assert applied["removed_publications"] == []
        assert applied["removed_conversion_refs"] == [key]
        assert applied["physical_files_removed"] == 4
        assert not await asyncio.to_thread(root.exists)
        assert (await explicit_delete.inspect_conversion(paths, key))["state"] == "removed"
        with pytest.raises(MarketForgeError, match="not a complete indexed output"):
            await explicit_delete.preview(paths, [], [key], [], cascade=False)
        assert (await explicit_delete.apply(paths, plan))["physical_files_removed"] == 0
