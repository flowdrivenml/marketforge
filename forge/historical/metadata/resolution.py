"""Read immutable historical authority for a registered input interval."""

from collections import defaultdict
from functools import lru_cache
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.files import disk
from forge.historical.metadata.policy import NAMESPACE, POLICY_VERSION, select_evidence, valid_spec
from forge.historical.metadata.service import covers
from forge.historical.profiles.service import discover, validate_catalogue
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@lru_cache(maxsize=8)
def compiled_profiles(
    paths: ProjectPaths, signature: tuple[tuple[str, str], ...]
) -> dict[str, str]:
    return {
        p["id"]: p["fingerprint"]
        for p in validate_catalogue(paths, paths.config_dir / "historical")["profiles"]
    }


async def specification(
    c: AsyncConnection[TupleRow],
    key: str,
    row: dict[str, Any],
    *,
    declared_only: bool = False,
    required_fingerprint: str | None = None,
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT a.instrument_id,s.id,s.definition,s.evidence_kind,s.fingerprint,s.policy_version
            FROM historical.instrument_authority a JOIN historical.specification_evidence s
            ON s.namespace=a.namespace AND s.instrument_id=a.instrument_id
            WHERE a.namespace=%s AND a.identity_key=%s ORDER BY s.id""",
            (NAMESPACE, key),
        )
        candidates = list(await cursor.fetchall())
    accepted = [
        r
        for r in candidates
        if covers(r, row.get("start_at"), row.get("end_at"))
        and (
            not declared_only
            or r["evidence_kind"] in {"historical_declaration", "historically_verified"}
        )
        and (required_fingerprint is None or r["fingerprint"] == required_fingerprint)
        and (r["evidence_kind"] != "catalogue_inferred" or r["policy_version"] == POLICY_VERSION)
    ]
    try:
        chosen = select_evidence(accepted)
    except ValueError as error:
        raise MarketForgeError(str(error)) from error
    if chosen is None:
        if declared_only:
            suffix = f" with fingerprint {required_fingerprint}" if required_fingerprint else ""
            raise MarketForgeError(
                f"No applicable explicit historical specification for {key}{suffix}. "
                "Register evidence covering the full archive interval with mf catalog specs add."
            )
        raise MarketForgeError(
            f"No applicable immutable specification for {key}; run mf init --testing to enrich retained catalogue evidence, or declare missing historical evidence. Current values require explicit inference provenance."
        )
    missing = valid_spec(chosen["definition"])
    if missing:
        raise MarketForgeError(
            f"Historical specification for {key} lacks valid fields: {', '.join(missing)}."
        )
    return (
        chosen["instrument_id"],
        chosen["definition"],
        {
            "evidence_id": chosen["id"],
            "fingerprint": chosen["fingerprint"],
            "kind": chosen["evidence_kind"],
            "definition": chosen["definition"],
        },
    )


class FamilyResolutionError(MarketForgeError):
    def __init__(self, message: str, population: dict[str, Any]) -> None:
        super().__init__(message)
        self.population = population


async def family(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, row: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT definition,profile_fingerprint FROM historical.archive_identity_evidence
        WHERE local_archive_id=%s AND input_sha256=%s AND policy_version=%s ORDER BY id DESC LIMIT 1""",
            (row["id"], row["sha256"], POLICY_VERSION),
        )
        cache = await cursor.fetchone()
    if cache is None:
        raise MarketForgeError(
            "Historical family archive identities unresolved; run mf init --testing. Current catalogue membership alone does not establish historical completeness."
        )
    signature = tuple((p["id"], p["sha256"]) for p in await disk(discover, paths))
    fingerprints = await disk(compiled_profiles, paths, signature)
    profile_id = ".".join(
        (
            row["exchange"],
            row["instrument_type"],
            row["market_category"],
            "trades" if row["data_type"] == "trades" else "depth",
        )
    )
    if fingerprints.get(profile_id) != cache["profile_fingerprint"]:
        raise MarketForgeError(
            "Family profile interpretation changed; rerun mf init --testing for identity discovery."
        )
    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT DISTINCT a.identity_key FROM historical.family_membership_evidence m
         JOIN historical.family_authority f ON f.id=m.family_id AND f.namespace=m.namespace
         JOIN historical.instrument_authority a ON a.namespace=m.namespace AND a.instrument_id=m.instrument_id
         WHERE f.namespace=%s AND f.exchange=%s AND f.instrument_type=%s AND f.market_category=%s AND f.identifier=%s""",
            (
                NAMESPACE,
                row["exchange"],
                row["instrument_type"],
                row["market_category"],
                row["scope_identifier"],
            ),
        )
        membership = {r["identity_key"] for r in await cursor.fetchall()}
    discovery = cache["definition"]
    instruments: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    observed = sorted(set(discovery["symbols"]))
    member_locations: dict[str, set[str | None]] = defaultdict(set)
    for binding in discovery.get("members", []):
        member_locations[binding["symbol"]].add(binding.get("archive_member"))
    unresolved: dict[str, str] = {}
    supported = 0
    for symbol in observed:
        key = "/".join((row["exchange"], row["instrument_type"], row["market_category"], symbol))
        try:
            if key not in membership:
                raise MarketForgeError(
                    f"Missing persisted canonical membership evidence for {key}."
                )
            canonical, spec, provenance = await specification(c, key, row)
            supported += 1
            if len(member_locations[symbol]) > 1:
                raise MarketForgeError(
                    "Explicit family scope supports only one archive member per instrument."
                )
        except MarketForgeError as error:
            unresolved[symbol] = str(error)
            continue
        instruments.append(
            {
                "instrument_id": canonical,
                "exchange": row["exchange"],
                "instrument_type": row["instrument_type"],
                "market_category": row["market_category"],
                "symbol": symbol,
                "base_asset": spec["base_asset"],
                "quote_asset": spec["quote_asset"],
                "instrument": {
                    "tick_size": str(spec["tick_size"]),
                    "contract_value": str(spec["contract_value"])
                    if spec.get("contract_value") is not None
                    else None,
                    "contract_value_asset": spec.get("contract_value_asset"),
                },
            }
        )
        evidence.append(provenance)
    population = {
        "known_registry_count": len(membership),
        "matched_count": len(observed),
        "observed_symbols": observed,
        "selected_count": len(instruments) if not unresolved else 0,
        "supported_count": supported,
        "excluded_count": 0,
        "unresolved_count": len(unresolved),
        "unresolved_symbols": unresolved,
    }
    if unresolved:
        raise FamilyResolutionError(
            "Family archive blocked by unresolved members: "
            + "; ".join(f"{symbol}: {reason}" for symbol, reason in unresolved.items()),
            population,
        )
    if not instruments:
        raise FamilyResolutionError(
            "Historical family archive has no resolved concrete members.", population
        )
    return instruments, {
        "method": "persisted_archive_family_evidence",
        "profile_id": profile_id,
        "profile_fingerprint": cache["profile_fingerprint"],
        "discovery": discovery,
        "population": population,
        "specifications": evidence,
        "namespace": NAMESPACE,
    }
