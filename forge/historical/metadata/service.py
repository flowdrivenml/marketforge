"""Idempotent metadata enrichment from retained PostgreSQL evidence and raw identities."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from forge.historical.archives.storage.files import disk
from forge.historical.metadata.discovery import discover
from forge.historical.metadata.policy import (
    NAMESPACE,
    POLICY_VERSION,
    evidence_fingerprint,
    fingerprint,
    identity_key,
    infer,
    native_family,
    option_identity,
    reference_family,
    select_evidence,
    valid_spec,
)
from forge.historical.normalization.service import write_job
from forge.historical.profiles.service import validate_catalogue
from forge.shared.database.testing import testing_connection
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.models import StorageFilters
from forge.storage.registry import parameters, scan

CATALOGUE = """SELECT i.*,e.code AS exchange,s.quantity_type,s.contract_value,s.contract_value_asset,
 s.tick_size,s.qty_step,s.min_qty,s.max_qty,s.min_notional,s.fetched_at AS observed_at,
 raw.id AS api_id,raw.raw_json AS raw,raw.id AS observation_order
 FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
 LEFT JOIN catalog.instrument_specs s ON s.instrument_id=i.id
 LEFT JOIN LATERAL(SELECT id,raw_json FROM catalog.instrument_api_raw WHERE instrument_id=i.id ORDER BY fetched_at DESC,id DESC LIMIT 1) raw ON true ORDER BY i.id"""


def serial(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, allow_nan=False))


async def records(
    c: AsyncConnection[TupleRow], query: str, args: Any = None
) -> list[dict[str, Any]]:
    from typing import LiteralString, cast

    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(cast(LiteralString, query), args)
        result: list[dict[str, Any]] = []
        while page := await cursor.fetchmany(500):
            result.extend(page)
            if len(result) > 200000:
                raise MarketForgeError(
                    "Historical metadata inventory exceeds 200000 records; narrow the supported catalogue."
                )
        return result


async def bulk(c: AsyncConnection[TupleRow], statement: str, rows: list[dict[str, Any]]) -> None:
    from typing import LiteralString, cast

    for offset in range(0, len(rows), 500):
        async with c.transaction():
            await c.execute(
                cast(LiteralString, statement), (Jsonb(serial(rows[offset : offset + 500])),)
            )


async def ensure_authority(
    c: AsyncConnection[TupleRow], rows: list[dict[str, Any]]
) -> dict[str, int]:
    values: list[dict[str, Any]] = [
        {
            "namespace": NAMESPACE,
            "identity_key": identity_key(row),
            "definition": {
                key: row[key]
                for key in ("exchange", "instrument_type", "market_category", "symbol")
            },
        }
        for row in rows
    ]
    await bulk(
        c,
        """INSERT INTO historical.instrument_authority(namespace,identity_key,definition)
      SELECT namespace,identity_key,definition FROM jsonb_to_recordset(%s) AS x(namespace text,identity_key text,definition jsonb)
      ON CONFLICT(namespace,identity_key) DO NOTHING""",
        values,
    )
    found = await records(
        c,
        "SELECT instrument_id,identity_key,definition FROM historical.instrument_authority WHERE namespace=%s",
        (NAMESPACE,),
    )
    expected = {value["identity_key"]: value["definition"] for value in values}
    for row in found:
        if row["identity_key"] in expected and any(
            row["definition"].get(k) != v for k, v in expected[row["identity_key"]].items()
        ):
            raise MarketForgeError(
                f"Canonical authority definition conflicts with identity {row['identity_key']}."
            )
    return {row["identity_key"]: row["instrument_id"] for row in found}


def covers(row: dict[str, Any], start: datetime | None, end: datetime | None) -> bool:
    d = row["definition"]
    inferred = row.get("evidence_kind") in {"catalogue_inferred", "reference_contract_inferred"}
    a = d.get("assumed_applicable_from") if inferred else d.get("valid_from")
    b = d.get("assumed_applicable_to") if inferred else d.get("valid_to")
    if (
        (not inferred and (a is None or b is None))
        or (start is not None and a is None)
        or (end is not None and b is None)
    ):
        return False
    return (
        a is None or start is None or datetime.fromisoformat(a.replace("Z", "+00:00")) <= start
    ) and (b is None or end is None or datetime.fromisoformat(b.replace("Z", "+00:00")) >= end)


async def load_evidence(c: AsyncConnection[TupleRow]) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in await records(
        c,
        "SELECT * FROM historical.specification_evidence WHERE namespace=%s ORDER BY id",
        (NAMESPACE,),
    ):
        result[row["instrument_id"]].append(row)
    return result


async def enrich(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, *, discover_archives: bool = True
) -> dict[str, Any]:
    run_id = "metadata-enrichment-" + uuid4().hex
    operation = paths.runs_dir / run_id
    operation.mkdir(parents=True, exist_ok=False)
    catalogue = await records(c, CATALOGUE)
    identities = await ensure_authority(c, catalogue)
    raw: list[dict[str, Any]] = []
    after = 0
    while page := await scan(
        c, parameters(paths.raw_dir, StorageFilters(status="complete", origin="remote")), after
    ):
        raw.extend(page)
        if len(raw) > 200000:
            raise MarketForgeError("Historical raw metadata inventory exceeds 200000 records.")
        after = int(str(page[-1]["id"]))
    start = min((row["start_at"] for row in raw if row.get("start_at") is not None), default=None)
    end = max((row["end_at"] for row in raw if row.get("end_at") is not None), default=None)
    start_text = start.astimezone(UTC).isoformat() if start else None
    end_text = end.astimezone(UTC).isoformat() if end else None
    evidence = await load_evidence(c)
    by_key = {identity_key(row): row for row in catalogue}
    references: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in catalogue:
        if (
            row.get("base_asset") == "BTC"
            and row["instrument_type"] in {"future", "option"}
            and not valid_spec(row)
        ):
            references[
                tuple(
                    row.get(k)
                    for k in (
                        "exchange",
                        "instrument_type",
                        "market_category",
                        "base_asset",
                        "quote_asset",
                        "settlement_asset",
                    )
                )
            ].append(row)

    def refs(row: dict[str, Any]) -> list[dict[str, Any]]:
        return references.get(
            tuple(
                row.get(k)
                for k in (
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "base_asset",
                    "quote_asset",
                    "settlement_asset",
                )
            ),
            [],
        )

    def resolve(row: dict[str, Any]) -> dict[str, Any]:
        explicit = [
            e
            for e in evidence.get(identities.get(identity_key(row), -1), [])
            if e.get("evidence_kind") in {"historically_verified", "historical_declaration"}
            and covers(e, start, end)
        ]
        try:
            selected = select_evidence(explicit)
        except ValueError as error:
            return {
                "inference_method": "unresolved",
                "missing_fields": ["unambiguous_historical_evidence"],
                "reason": str(error),
            }
        return selected["definition"] if selected else infer(row, refs(row), start_text, end_text)

    resolutions = {key: resolve(row) for key, row in by_key.items()}
    families: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in catalogue:
        family = native_family(row)
        if family:
            families[
                (row["exchange"], row["instrument_type"], row["market_category"], family)
            ].append(row)
    family_rows = [
        {
            "namespace": NAMESPACE,
            "exchange": key[0],
            "instrument_type": key[1],
            "market_category": key[2],
            "identifier": key[3],
        }
        for key in sorted(families)
    ]
    # Retain source-declared families even when the current catalogue is incomplete.
    for row in raw:
        if row["scope_kind"] == "family":
            key = (
                row["exchange"],
                row["instrument_type"],
                row["market_category"],
                row["scope_identifier"],
            )
            if key not in families:
                families[key] = []
                family_rows.append(
                    {
                        "namespace": NAMESPACE,
                        "exchange": key[0],
                        "instrument_type": key[1],
                        "market_category": key[2],
                        "identifier": key[3],
                    }
                )
    await bulk(
        c,
        """INSERT INTO historical.family_authority(namespace,exchange,instrument_type,market_category,identifier)
     SELECT namespace,exchange,instrument_type,market_category,identifier FROM jsonb_to_recordset(%s) AS x(namespace text,exchange text,instrument_type text,market_category text,identifier text)
     ON CONFLICT DO NOTHING""",
        family_rows,
    )
    family_ids: dict[tuple[str, ...], int] = {
        (r["exchange"], r["instrument_type"], r["market_category"], r["identifier"]): r["id"]
        for r in await records(
            c, "SELECT * FROM historical.family_authority WHERE namespace=%s", (NAMESPACE,)
        )
    }
    cached = {
        (r["local_archive_id"], r["input_sha256"], r["profile_fingerprint"]): r
        for r in await records(
            c,
            "SELECT * FROM historical.archive_identity_evidence WHERE policy_version=%s",
            (POLICY_VERSION,),
        )
    }
    profile_map: dict[str, Any] = {}
    if discover_archives and any(row["scope_kind"] == "family" for row in raw):
        validated = await disk(validate_catalogue, paths, paths.config_dir / "historical")
        profile_map = {p["id"]: p for p in validated["profiles"]}
    discoveries: dict[int, dict[str, Any]] = {}
    blockers: list[dict[str, Any]] = []
    decoded = 0
    reused = 0
    for row in raw:
        if row.get("start_at") is None or row.get("end_at") is None:
            blockers.append({"local_id": row["id"], "missing_fields": ["declared_raw_interval"]})
            continue
        if row["scope_kind"] != "family":
            continue
        key = (
            row["exchange"],
            row["instrument_type"],
            row["market_category"],
            row["scope_identifier"],
        )
        profile_id = ".".join((*key[:3], "trades" if row["data_type"] == "trades" else "depth"))
        profile = profile_map.get(profile_id)
        if not profile:
            blockers.append(
                {
                    "local_id": row["id"],
                    "missing_fields": ["supported_engine_profile"],
                    "profile_id": profile_id,
                }
            )
            continue
        cache_key = (row["id"], row["sha256"], profile["fingerprint"])
        members = families[key]
        result: dict[str, Any]
        try:
            if cache_key in cached:
                result = cached[cache_key]["definition"]
                reused += 1
            elif row["instrument_type"] == "perpetual":
                # Production adapter validates this exact source identity convention.
                names = [
                    member["symbol"]
                    for member in members
                    if str(row.get("filename", "")).startswith(member["symbol"] + "-")
                ]
                if len(names) != 1:
                    raise MarketForgeError(
                        "Perpetual family source lacks an unambiguous native archive identity."
                    )
                result = {
                    "symbols": names,
                    "method": "archive_derived",
                    "archive_population_complete": False,
                    "historical_family_complete": False,
                    "source": "validated exchange archive identity metadata",
                    "normalization_executed": False,
                }
            elif row["instrument_type"] == "future":
                candidates = [
                    m
                    for m in members
                    if resolutions[identity_key(m)].get("inference_method") != "unresolved"
                ]
                if not candidates:
                    raise MarketForgeError(
                        "No compatible current BTC family reference specification."
                    )
                candidates.sort(
                    key=lambda m: (-int(m.get("observation_order") or 0), identity_key(m))
                )
                ref = {**candidates[0], **resolutions[identity_key(candidates[0])]}
                result = await disk(discover, paths, row, ref, family_ids[key], operation)
                decoded += 1
            else:
                raise MarketForgeError(
                    "Option family metadata is supported, but archive identity discovery requires a compatible installed Rust option profile and contract."
                )
            async with c.transaction():
                await c.execute(
                    """INSERT INTO historical.archive_identity_evidence(local_archive_id,input_sha256,profile_fingerprint,policy_version,definition)
                 VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (*cache_key, POLICY_VERSION, Jsonb(serial(result))),
                )
            discoveries[row["id"]] = result
            for symbol in result["symbols"]:
                target_key = "/".join((*key[:3], symbol))
                if target_key not in by_key:
                    native = reference_family(
                        {"exchange": key[0], "instrument_type": key[1], "symbol": symbol}
                    )
                    asset_fields = ("base_asset", "quote_asset", "settlement_asset")
                    semantics = {
                        tuple(member.get(field) for field in asset_fields)
                        for member in members
                        if native is not None
                        and reference_family(member) == native
                        and not valid_spec(member)
                    }
                    # Identity is observed even when the economic metadata is
                    # ambiguous or lacks compatible reference observations.
                    assets = (
                        dict(zip(asset_fields, next(iter(semantics)), strict=True))
                        if len(semantics) == 1
                        else {}
                    )
                    target: dict[str, Any] = {
                        "id": None,
                        "exchange": key[0],
                        "instrument_type": key[1],
                        "market_category": key[2],
                        "symbol": symbol,
                        **assets,
                        "quantity_type": None,
                        "tick_size": None,
                        "contract_value": None,
                        "contract_value_asset": None,
                        "raw": {},
                        "identity_origin": "archive_derived",
                    }
                    by_key[target_key] = target
                    identities.update(await ensure_authority(c, [target]))
                    resolutions[target_key] = resolve(target)
        except (MarketForgeError, ValueError, OSError) as error:
            blockers.append({"local_id": row["id"], "reason": str(error)})
    spec_rows: list[dict[str, Any]] = []
    for key, definition in resolutions.items():
        method = definition.get("inference_method")
        if method not in {"catalogue_inferred", "reference_contract_inferred"}:
            continue
        spec_rows.append(
            {
                "namespace": NAMESPACE,
                "instrument_id": identities[key],
                "fingerprint": evidence_fingerprint(definition),
                "definition": definition,
                "evidence_kind": method,
                "policy_version": definition["policy_version"],
                "source_instrument_id": definition.get("source_catalogue_instrument_id"),
                "source_observation_id": definition.get("source_api_observation_id"),
                "source_observed_at": definition.get("source_observed_at"),
            }
        )
    await bulk(
        c,
        """INSERT INTO historical.specification_evidence(namespace,instrument_id,fingerprint,definition,evidence_kind,policy_version,source_instrument_id,source_observation_id,source_observed_at)
     SELECT namespace,instrument_id,fingerprint,definition,evidence_kind,policy_version,source_instrument_id,source_observation_id,source_observed_at FROM jsonb_to_recordset(%s) AS x(namespace text,instrument_id bigint,fingerprint text,definition jsonb,evidence_kind text,policy_version text,source_instrument_id bigint,source_observation_id bigint,source_observed_at timestamptz)
     ON CONFLICT(namespace,instrument_id,fingerprint) DO NOTHING""",
        spec_rows,
    )
    membership: list[dict[str, Any]] = []
    for key, members in families.items():
        for member in members:
            definition = {
                "identity_key": identity_key(member),
                "method": "catalogue_inferred",
                "policy_version": POLICY_VERSION,
                "observed_at": member.get("observed_at"),
                "source_api_observation_id": member.get("api_id"),
                "source_snapshot_sha256": fingerprint(member.get("raw")),
                "historically_verified": False,
                "historical_family_complete": False,
                "instrument_facts": option_identity(member),
                "assumed_applicable_from": start_text,
                "assumed_applicable_to": end_text,
            }
            membership.append(
                {
                    "family_id": family_ids[key],
                    "namespace": NAMESPACE,
                    "instrument_id": identities[identity_key(member)],
                    "method": "catalogue_inferred",
                    "fingerprint": fingerprint(
                        {
                            k: v
                            for k, v in definition.items()
                            if k not in {"observed_at", "source_api_observation_id"}
                        }
                    ),
                    "definition": definition,
                }
            )
    raw_by_id = {row["id"]: row for row in raw}
    for local_id, result in discoveries.items():
        row = raw_by_id[local_id]
        key = (
            row["exchange"],
            row["instrument_type"],
            row["market_category"],
            row["scope_identifier"],
        )
        for symbol in result["symbols"]:
            target_key = "/".join((*key[:3], symbol))
            if target_key not in identities:
                continue
            definition = {
                "identity_key": target_key,
                "local_archive_id": local_id,
                "input_sha256": row["sha256"],
                "policy_version": POLICY_VERSION,
                "method": "archive_derived",
                "historically_verified": False,
                "historical_family_complete": False,
                "assumed_applicable_from": row["start_at"],
                "assumed_applicable_to": row["end_at"],
                "instrument_facts": option_identity(by_key[target_key]),
            }
            membership.append(
                {
                    "family_id": family_ids[key],
                    "namespace": NAMESPACE,
                    "instrument_id": identities[target_key],
                    "method": "archive_derived",
                    "fingerprint": fingerprint(definition),
                    "definition": definition,
                }
            )
    await bulk(
        c,
        """INSERT INTO historical.family_membership_evidence(family_id,namespace,instrument_id,method,fingerprint,definition)
     SELECT family_id,namespace,instrument_id,method,fingerprint,definition FROM jsonb_to_recordset(%s) AS x(family_id bigint,namespace text,instrument_id bigint,method text,fingerprint text,definition jsonb) ON CONFLICT DO NOTHING""",
        membership,
    )
    # Evaluate actual persisted evidence for each retained raw interval.
    persisted = await load_evidence(c)
    eligible = 0
    for row in raw:
        if row["scope_kind"] == "family":
            discovery = discoveries.get(row["id"])
            keys = (
                [
                    "/".join((row["exchange"], row["instrument_type"], row["market_category"], s))
                    for s in discovery["symbols"]
                ]
                if discovery
                else []
            )
        else:
            key = "/".join(
                (
                    row["exchange"],
                    row["instrument_type"],
                    row["market_category"],
                    row["scope_identifier"],
                )
            )
            if (
                key not in by_key
                and row["exchange"] == "bitget"
                and row["instrument_type"] == "spot"
            ):
                matches = [
                    k
                    for k, v in by_key.items()
                    if v["exchange"] == "bitget"
                    and v["instrument_type"] == "spot"
                    and f"{v['base_asset']}/{v['quote_asset']}".upper()
                    == row["scope_identifier"].upper()
                ]
                key = matches[0] if len(matches) == 1 else key
            keys = [key]
        ready = bool(keys) and row.get("start_at") is not None and row.get("end_at") is not None
        for key in keys:
            try:
                chosen = select_evidence(
                    [
                        e
                        for e in persisted.get(identities.get(key, -1), [])
                        if covers(e, row["start_at"], row["end_at"])
                    ]
                )
                if not chosen or valid_spec(chosen["definition"]):
                    ready = False
            except ValueError:
                ready = False
        if ready:
            eligible += 1
        elif not any(b.get("local_id") == row["id"] for b in blockers):
            blockers.append(
                {
                    "local_id": row["id"],
                    "reason": "No operationally acceptable historical specification/member evidence.",
                    "identity_keys": keys,
                }
            )
    counts: Counter[str] = Counter(
        {
            key: 0
            for key in (
                "historically_verified",
                "historical_declaration",
                "catalogue_inferred",
                "reference_contract_inferred",
                "unresolved",
            )
        }
    )
    counts.update(
        d.get(
            "inference_method",
            "historically_verified" if d.get("historically_verified") else "historical_declaration",
        )
        for d in resolutions.values()
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "environment": "testing",
        "policy_version": POLICY_VERSION,
        "status": "complete" if eligible == len(raw) else "partial",
        "concrete_instruments": dict(counts),
        "instrument_families": {
            "discovered": len(family_ids),
            "members_resolved": len({(m["family_id"], m["instrument_id"]) for m in membership}),
            "archive_derived_members": len(
                {
                    (m["family_id"], m["instrument_id"])
                    for m in membership
                    if m["method"] == "archive_derived"
                }
            ),
            "incomplete_families": len(family_ids),
        },
        "archive_discovery": {"decoded": decoded, "cache_reused": reused},
        "normalization_readiness": {
            "eligible_archives": eligible,
            "blocked_archives": len(raw) - eligible,
            "registered_archives": len(raw),
        },
        "blockers": blockers,
        "assumptions": "Inferred applicability follows the explicitly approved project policy; no historical completeness or independent verification is claimed.",
    }
    async with c.transaction():
        await c.execute(
            "INSERT INTO historical.metadata_enrichment_runs(id,environment,policy_version,report) VALUES(%s,'testing',%s,%s)",
            (run_id, POLICY_VERSION, Jsonb(serial(report))),
        )
    write_job(operation / "enrichment-report.json", report)
    return report


async def enrich_testing(dsn: str, paths: ProjectPaths) -> dict[str, Any]:
    async with testing_connection(dsn) as c:
        cur = await c.execute("SELECT pg_try_advisory_lock(556823701490)")
        if await cur.fetchone() != (True,):
            raise MarketForgeError("Another historical metadata enrichment operation is active.")
        try:
            return await enrich(c, paths)
        finally:
            await c.execute("SELECT pg_advisory_unlock(556823701490)")
