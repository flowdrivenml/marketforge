"""Frozen local registration using the shared archive/copy authority."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.catalog.service import instrument
from forge.historical.archives.storage.acquire import root_key
from forge.historical.archives.storage.files import contained, disk
from forge.historical.engine.managed import checksum
from forge.historical.imports.formats import check_profile_sample
from forge.historical.imports.models import (
    FrozenInput,
    HistoricalSpecs,
    ImportConfig,
    freeze_inputs,
)
from forge.historical.imports.service import hash_source, import_file
from forge.historical.metadata.policy import NAMESPACE
from forge.historical.metadata.resolution import specification
from forge.historical.normalization.service import write_job
from forge.historical.profiles import service as profiles
from forge.historical.registration.models import Recipe, groups
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def inspect(recipe: Recipe, paths: ProjectPaths) -> dict[str, Any]:
    inventory: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for index, config in enumerate(groups(recipe, paths)):
        for item in freeze_inputs(config):
            key = (item.device, item.inode)
            if key in seen:
                raise MarketForgeError(
                    f"inputs.{index}: duplicate physical source selected: {item.reference}"
                )
            seen.add(key)
            inventory.append({"group": index, **asdict(item)})
    return {"recipe": recipe.model_dump(mode="json"), "inventory": inventory}


async def prerequisites(
    c: AsyncConnection[TupleRow], recipe: Recipe, paths: ProjectPaths, configs: list[ImportConfig]
) -> tuple[list[ImportConfig], dict[str, Any]]:
    bound: list[ImportConfig] = []
    validated: dict[str, Any] = {}
    for config, group in zip(configs, recipe.inputs, strict=True):
        entry = await instrument(c, config.identity)
        key = group.instrument.replace(".", "/", 3)
        canonical, definition, evidence = await specification(
            c,
            key,
            {"start_at": recipe.coverage.start, "end_at": recipe.coverage.end},
            declared_only=True,
            required_fingerprint=group.specification,
        )
        if canonical != entry["historical_instrument_id"]:
            raise MarketForgeError(
                f"{group.instrument}: historical and catalogue identities disagree."
            )
        if group.profile not in validated:
            source = profiles.installed(paths, group.profile)
            if not source.is_file():
                raise MarketForgeError(
                    f"Profile {group.profile} is missing. Use mf acquisition profiles add."
                )
            validated[group.profile] = await disk(profiles.validate, paths, source)
        profile = validated[group.profile]
        quantity = profile["definition"]["normalization"]["quantity_encoding"]
        if quantity != definition["quantity_type"]:
            raise MarketForgeError(
                f"Profile {group.profile} quantity encoding conflicts with historical specifications."
            )
        specs = HistoricalSpecs.model_validate(
            {
                name: value
                for name, value in definition.items()
                if name in HistoricalSpecs.model_fields
            }
        )
        binding: dict[str, Any] = {
            "namespace": NAMESPACE,
            "instrument_id": canonical,
            "specification_evidence_id": evidence["evidence_id"],
            "specification_fingerprint": evidence["fingerprint"],
            "profile_id": group.profile,
            "profile_fingerprint": profile["engine"]["fingerprint"],
            "readiness": {"normalization_handoff": "ready", "blockers": []},
            "provider": recipe.source.provider,
            "product": recipe.source.product,
            "provenance": recipe.source.provenance,
        }
        if group.native_identity is not None:
            binding["native_identity"] = group.native_identity.model_dump(mode="json")
        bound.append(config.model_copy(update={"binding": binding, "specifications": specs}))
    return bound, validated


async def validate(recipe: Recipe, paths: ProjectPaths) -> dict[str, Any]:
    async with testing_connection(testing_dsn(paths.root)) as c:
        return (await preflight(c, recipe, paths))[0]


async def preflight(
    c: AsyncConnection[TupleRow], recipe: Recipe, paths: ProjectPaths
) -> tuple[dict[str, Any], list[ImportConfig], list[list[FrozenInput]]]:
    configs, validated = await prerequisites(c, recipe, paths, groups(recipe, paths))
    result = await disk(inspect, recipe, paths)
    frozen_groups = [await disk(freeze_inputs, config) for config in configs]
    if [
        dict(group=index, **asdict(item))
        for index, items in enumerate(frozen_groups)
        for item in items
    ] != result["inventory"]:
        raise MarketForgeError("Source inventory changed during planning.")
    for index, items in enumerate(frozen_groups):
        group = recipe.inputs[index]
        config = configs[index]
        for item in items:
            await disk(
                check_profile_sample,
                Path(item.path),
                validated[group.profile]["definition"],
                config.identity.symbol,
                {
                    group.native_identity.publisher_field: group.native_identity.publisher_id,
                    group.native_identity.instrument_field: group.native_identity.instrument_id,
                }
                if group.native_identity
                else None,
            )
            cur = await c.execute(
                """SELECT venue_code,scope_json,data_type,start_at,end_at FROM historical.raw_objects o
                JOIN historical.raw_sources s ON s.id=o.raw_source_id
                WHERE s.source_key=%s AND o.source_object_key=%s AND o.acquisition_method='local_copy'""",
                (recipe.source.key, item.reference),
            )
            prior = await cur.fetchone()
            if prior and (
                prior[:3]
                != (
                    config.identity.exchange,
                    {
                        "scope_kind": "instrument",
                        "scope_identifier": config.identity.symbol,
                        "instrument_type": config.identity.instrument_type,
                        "market_category": config.identity.market_category,
                    },
                    config.data_type,
                )
                or (
                    prior[3] is not None
                    and prior[3:] != (recipe.coverage.start, recipe.coverage.end)
                )
            ):
                raise MarketForgeError(
                    f"{item.reference}: source object conflicts with its registered identity or coverage."
                )
    result["profiles"] = validated
    result["bindings"] = [config.binding for config in configs]
    result["readiness"] = {"normalization_handoff": "ready", "blockers": []}
    return result, configs, frozen_groups


async def apply(paths: ProjectPaths, source: Path, *, planning: bool = False) -> dict[str, Any]:
    submitted = await disk(source.read_bytes)
    recipe = Recipe.model_validate(tomllib.loads(submitted.decode()))
    async with testing_connection(testing_dsn(paths.root)) as c:
        result, configs, frozen_groups = await preflight(c, recipe, paths)
        run_id = f"register-{uuid4().hex}"
        operation = paths.runs_dir / run_id
        operation.mkdir(parents=True, exist_ok=False)
        (operation / "registration.toml").write_bytes(submitted)
        result.update(
            {
                "run_id": run_id,
                "environment": "testing",
                "recipe_sha256": hashlib.sha256(submitted).hexdigest(),
                "status": "planned" if planning else "applying",
            }
        )
        write_job(operation / "intent.json", result)
        actions: list[dict[str, Any]] = []
        for group_index, items in enumerate(frozen_groups):
            for item_index, item in enumerate(items):
                digest = await hash_source(item)
                safe_name = (
                    "".join(
                        ch if ch.isalnum() or ch in "._-" else "_" for ch in Path(item.path).name
                    )[-160:]
                    or "input"
                )
                relative = f"registered/{run_id}/{group_index}/{item_index}/{safe_name}"
                cur = await c.execute(
                    """SELECT l.id,l.relative_path,l.bytes_written,l.sha256 FROM historical.local_archives l
                JOIN historical.raw_objects o ON o.id=l.raw_object_id JOIN historical.raw_sources s ON s.id=o.raw_source_id
                WHERE s.source_key=%s AND o.source_object_key=%s AND l.sha256=%s AND l.storage_root_key=%s
                AND l.status='complete' ORDER BY l.id DESC LIMIT 1""",
                    (recipe.source.key, item.reference, digest, root_key(paths.raw_dir)),
                )
                prior = await cur.fetchone()
                candidate = contained(paths.raw_dir, str(prior[1])) if prior else None
                reuse = bool(
                    candidate and candidate.is_file() and await disk(checksum, candidate) == digest
                )
                actions.append(
                    {
                        "group": group_index,
                        "reference": item.reference,
                        "sha256": digest,
                        "action": "reuse_verified" if reuse else "copy_new_revision",
                        "planned_relative": str(prior[1]) if reuse and prior else relative,
                        "destination": str(
                            paths.raw_dir / (str(prior[1]) if reuse and prior else relative)
                        ),
                        "local_copy_id": prior[0] if reuse and prior else None,
                    }
                )
        result["intended_actions"] = actions
        write_job(operation / "resolved-intent.json", result)
        if planning:
            cur = await c.execute(
                "SELECT count(*) FROM historical.raw_objects WHERE raw_source_id IN (SELECT id FROM historical.raw_sources WHERE source_key=%s)",
                (recipe.source.key,),
            )
            row = await cur.fetchone()
            assert row is not None
            result["existing_source_objects"] = row[0]
            write_job(operation / "plan.json", result)
            return result
        for profile in result["profiles"].values():
            if await disk(checksum, Path(profile["location"])) != profile["sha256"]:
                raise MarketForgeError(
                    f"Installed profile {profile['id']} changed after validation."
                )
        # Reserve source provenance before copying bytes.
        async with c.transaction():
            cur = await c.execute(
                """INSERT INTO historical.raw_sources(source_key,source_kind,root_uri,provider,product,provenance)
            VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(source_key) DO UPDATE SET
            provider=EXCLUDED.provider, product=EXCLUDED.product, provenance=EXCLUDED.provenance
            WHERE raw_sources.source_kind=EXCLUDED.source_kind AND raw_sources.root_uri=EXCLUDED.root_uri
            AND ((raw_sources.provider IS NULL AND raw_sources.product IS NULL AND raw_sources.provenance='{}'::jsonb)
              OR (raw_sources.provider,raw_sources.product,raw_sources.provenance)=(EXCLUDED.provider,EXCLUDED.product,EXCLUDED.provenance))
            RETURNING id""",
                (
                    recipe.source.key,
                    recipe.source.kind,
                    configs[0].root,
                    recipe.source.provider,
                    recipe.source.product,
                    Jsonb({"declaration": recipe.source.provenance}),
                ),
            )
            if await cur.fetchone() is None:
                raise MarketForgeError(
                    "Source identity/provenance conflicts with existing registration."
                )
        receipts: list[dict[str, object]] = []
        for index, config in enumerate(configs):
            for item in frozen_groups[index]:
                intended = next(
                    action
                    for action in actions
                    if action["group"] == index and action["reference"] == item.reference
                )
                if await hash_source(item) != intended["sha256"]:
                    raise MarketForgeError(
                        "Source bytes changed after frozen registration planning."
                    )
                receipt = await import_file(
                    c,
                    config,
                    item,
                    paths.raw_dir,
                    run_id,
                    planned_relative=intended["planned_relative"]
                    if intended["action"] != "reuse_verified"
                    else None,
                )
                receipts.append(receipt)
                write_job(operation / f"copy-{len(receipts)}.json", receipt)
        result["copies"] = receipts
        result["status"] = "registered"
        async with c.transaction():
            await c.execute(
                "INSERT INTO historical.registrations(id,environment,recipe_sha256,evidence) VALUES(%s,'testing',%s,%s)",
                (run_id, result["recipe_sha256"], Jsonb(result)),
            )
    write_job(operation / "result.json", result)
    return result
