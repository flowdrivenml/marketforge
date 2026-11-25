"""Select exact reference inputs. Rust validates and prices the resulting job."""

import json
from collections import Counter, defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from forge.historical.conversion.contract import Identity, Policy
from forge.historical.conversion.planning import references
from forge.historical.conversion.selection import ReferenceSelection
from forge.markets.ranking.historical import turnover_entry_issue, turnover_evidence
from forge.markets.ranking.policy import DEFAULT_TURNOVER_POLICY
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

Stream = dict[str, Any]
Route = tuple[tuple[str, ...], tuple[str, ...]]


def eligible_references(
    source: Stream, external: Stream | None
) -> tuple[dict[str, Stream], list[Stream]]:
    """Enumerate eligible exact bindings and retain reasons for excluded declarations."""
    eligible: dict[str, Stream] = {}
    decisions: list[Stream] = []
    internal = source["streams"]
    for provider in (source, external):
        if provider is None:
            continue
        is_external = provider is external
        counts = Counter(
            Identity.model_validate(stream["identity"])
            for stream in provider["streams"]
            if stream["kind"] == "trade"
        )
        ids = Counter(
            stream["stream_id"] for stream in provider["streams"] if stream["kind"] == "trade"
        )
        for stream in provider["streams"]:
            stream_id = stream["stream_id"]
            identity = Identity.model_validate(stream["identity"])
            reason = None
            if stream["kind"] != "trade":
                reason = "Only trade streams can supply last-trade prices."
            elif not stream["market_observed"]:
                reason = (
                    "The committed publication contains no market observations for this stream."
                )
            elif ids[stream_id] != 1:
                reason = "The publication declares the same trade stream ID more than once."
            elif counts[identity] != 1:
                reason = "The Rust instrument selector matches multiple trade streams."
            elif (
                is_external
                and source["mode"] == "combined"
                and any(
                    item["kind"] == "trade"
                    and (item["identity"] == stream["identity"] or item["stream_id"] == stream_id)
                    for item in internal
                )
            ):
                reason = "A combined publication gives its internal trade precedence."
            elif is_external and (
                stream.get("observed_interval_inclusive", provider["observed_interval_inclusive"])[
                    "start_ns"
                ]
                > source["observed_interval_inclusive"]["end_ns"]
            ):
                reason = "This external trade stream begins after all source events."
            decision = {
                **stream,
                "provider": "external" if is_external else "internal",
                "merge_registry_publication_id": provider["registry_publication_id"],
                "merge_engine_publication_id": provider["engine_publication_id"],
                "merge_root": provider["root"],
            }
            decisions.append({**decision, "eligible": reason is None, "reason": reason})
            if reason is None:
                eligible[stream_id] = decision
    return eligible, decisions


def potential_routes(
    candidates: dict[str, Stream], source_quotes: set[str], policy: Policy
) -> dict[str, list[Route]]:
    edges: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for stream_id, stream in candidates.items():
        base, quote = stream["base_currency"], stream["quote_currency"]
        edges[base].append((quote, stream_id))
        edges[quote].append((base, stream_id))
    for neighbors in edges.values():
        neighbors.sort()

    def discover_from(quote: str) -> list[Route]:
        budget = 100_000

        def walk(currencies: tuple[str, ...], legs: tuple[str, ...]) -> Iterator[Route]:
            nonlocal budget
            current = currencies[-1]
            if current == policy.target_quote:
                yield currencies, legs
            elif len(legs) < policy.max_hops:
                for neighbor, stream_id in edges[current]:
                    budget -= 1
                    if budget < 0:
                        raise MarketForgeError(
                            f"Automatic route search exceeded its budget for {quote}. "
                            "Reduce max_hops or select references manually."
                        )
                    if neighbor not in currencies:
                        yield from walk((*currencies, neighbor), (*legs, stream_id))

        # The Engine accepts up to 32 retained paths per source. Bound discovery separately.
        routes: list[Route] = []
        for route in walk((quote,), ()):
            routes.append(route)
            if len(routes) > 1024:
                raise MarketForgeError(
                    f"Too many potential routes for {quote}. Reduce max_hops or select references manually."
                )
        return routes

    return {quote: discover_from(quote) for quote in sorted(source_quotes)}


def ranking_keys(stream: Stream) -> tuple[str, str]:
    identity = stream["identity"]
    prefix = f"{identity['exchange']}.{stream['market_category']}"
    return (
        f"{prefix}.{identity['symbol']}",
        f"{prefix}.{identity['instrument_kind']}.{identity['symbol']}",
    )


async def rank_candidates(
    paths: ProjectPaths,
    source: Stream,
    policy: ReferenceSelection,
    candidates: dict[str, Stream],
    relevant: set[str],
    needed: bool,
) -> tuple[dict[str, int], Stream]:
    # The established turnover preset is a name, not a guarantee that a snapshot exists.
    name = policy.ranking_policy or DEFAULT_TURNOVER_POLICY
    start_ns = source["observed_interval_inclusive"]["start_ns"]
    cutoff = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=start_ns // 1000)
    ex_post = policy.temporal_policy == "ex_post"
    evidence: Stream = {
        "policy": name,
        "requested_snapshot": str(policy.ranking_snapshot) if policy.ranking_snapshot else None,
        "decision_cutoff": cutoff.isoformat(),
        "temporal_policy": policy.temporal_policy,
        "status": "Ranking not required for route selection.",
    }
    if not needed and policy.ranking_snapshot is None:
        return {}, evidence
    keys = {stream_id: ranking_keys(candidates[stream_id]) for stream_id in sorted(relevant)}
    async with (
        database_connection(database_dsn(paths.root)) as connection,
        connection.transaction(),
    ):
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        header, rows, status = await turnover_evidence(
            connection,
            name,
            policy.ranking_snapshot,
            cutoff,
            sorted({key for aliases in keys.values() for key in aliases}),
            ex_post=ex_post,
        )
    evidence["status"] = status
    evidence["applicability"] = "unavailable"
    if header is None:
        return {}, evidence
    snapshot_bytes = json.dumps(
        {"snapshot": header, "entries": rows}, default=str, sort_keys=True, allow_nan=False
    ).encode()
    evidence.update(json.loads(snapshot_bytes))
    evidence["evidence_sha256"] = sha256(snapshot_bytes).hexdigest()
    if status not in {"historical", "ex_post"}:
        return {}, evidence
    evidence["applicability"] = status
    if ex_post:
        evidence["temporal_note"] = (
            "Explicit analytical override. Ranking observations may postdate the historical events."
        )
    positions: dict[str, int] = {}
    issues: dict[str, str] = {}
    by_key = {row["instrument_reference"]: row for row in rows}
    for stream_id, aliases in keys.items():
        stream = candidates[stream_id]
        expected = {
            "exchange": stream["identity"]["exchange"],
            "symbol": stream["identity"]["symbol"],
            "instrument_type": stream["identity"]["instrument_kind"],
            "market": stream["market_category"],
            "base": stream["base_currency"],
            "quote": stream["quote_currency"],
        }
        matches = [
            row
            for alias in aliases
            if (row := by_key.get(alias)) is not None
            and row["instrument_metadata"]
            and all(row["instrument_metadata"].get(key) == value for key, value in expected.items())
        ]
        if len(matches) != 1:
            issues[stream_id] = (
                "No unique ranking entry matches the historical instrument metadata."
            )
            continue
        row = matches[0]
        issue = turnover_entry_issue(row, header, cutoff, ex_post=ex_post)
        if issue is None:
            positions[stream_id] = row["position"]
        else:
            issues[stream_id] = issue
    evidence.update(
        positions=positions,
        unusable_entries=issues,
        status=(
            f"{'Ex-post' if ex_post else 'Historical'} snapshot supplies {len(positions)} "
            "eligible reference positions."
        ),
    )
    return positions, evidence


def ranked_routes(routes: list[Route], positions: dict[str, int]) -> list[Route] | None:
    """Compare alternatives of equal length using only legs that distinguish them."""
    common = set(routes[0][1]).intersection(*(legs for _, legs in routes[1:]))
    distinguishing = {leg for _, legs in routes for leg in legs} - common
    if not distinguishing <= positions.keys():
        return None

    def score(route: Route) -> tuple[list[int], Route]:
        # Prefer the best weakest leg. Stable identity breaks equal published preferences.
        return sorted(
            (positions[leg] for leg in route[1] if leg not in common), reverse=True
        ), route

    return sorted(routes, key=score)


async def resolve_references(
    paths: ProjectPaths,
    source: Stream,
    external: Stream | None,
    policy: Policy,
    selection: ReferenceSelection,
    manual_ids: list[str],
    report: Stream,
) -> list[Stream]:
    report.update(
        mode=selection.mode,
        source_registry_publication_id=source["registry_publication_id"],
        source_engine_publication_id=source["engine_publication_id"],
        target_quote=policy.target_quote,
        path_policy=policy.path_policy,
    )
    if selection.mode == "manual":
        report["ranking"] = {"status": "Manual preference."}
        selected = references(source, external, manual_ids)
        report.update(selected=selected, rationale="Explicit reference stream IDs in recipe order.")
        return selected

    candidates, decisions = eligible_references(source, external)
    report["candidates"] = decisions
    sources = [
        stream
        for stream in source["streams"]
        if policy.instrument_kinds is None
        or stream["identity"]["instrument_kind"] in policy.instrument_kinds
    ]
    routes = potential_routes(candidates, {stream["quote_currency"] for stream in sources}, policy)
    report["sources"] = [
        {"stream_id": stream["stream_id"], "source_quote": stream["quote_currency"]}
        for stream in sources
    ]
    report["routes"] = {
        quote: {"potential": alternatives} for quote, alternatives in routes.items()
    }
    report["ranking"] = {"status": "Ranking not required to establish eligibility."}
    for quote, alternatives in routes.items():
        if not alternatives:
            affected = ", ".join(
                item["stream_id"] for item in sources if item["quote_currency"] == quote
            )
            raise MarketForgeError(
                f"No eligible automatic reference route from {quote} to {policy.target_quote} "
                f"for {affected}. Review the eligible trades or provide compatible references."
            )

    def competing_shortest(alternatives: list[Route]) -> bool:
        shortest = min(len(legs) for _, legs in alternatives)
        return sum(len(legs) == shortest for _, legs in alternatives) > 1

    relevant = {leg for alternatives in routes.values() for _, legs in alternatives for leg in legs}
    positions, evidence = await rank_candidates(
        paths,
        source,
        selection,
        candidates,
        relevant,
        needed=any(
            len(alternatives) > 1
            if policy.path_policy == "all"
            else competing_shortest(alternatives)
            for alternatives in routes.values()
        ),
    )
    report["ranking"] = evidence
    if selection.ranking_snapshot and evidence.get("applicability") not in {
        "historical",
        "ex_post",
    }:
        raise MarketForgeError(f"Requested ranking snapshot cannot be used. {evidence['status']}")

    chosen: dict[str, Route] = {}
    selected_ids: set[str] = set()
    for quote, alternatives in routes.items():
        ordered: list[Route] = []
        comparable = True
        for count in sorted({len(legs) for _, legs in alternatives}):
            group = [route for route in alternatives if len(route[1]) == count]
            preferred = ranked_routes(group, positions) if len(group) > 1 else group
            if preferred is None:
                comparable = False
                if not ordered and policy.path_policy == "primary":
                    raise MarketForgeError(
                        f"Ambiguous automatic primary reference route from {quote} to "
                        f"{policy.target_quote}. Comparable ranking evidence is unavailable "
                        f"for {', '.join(sorted({leg for _, legs in group for leg in legs}))}. "
                        "Select references manually or pin a suitable ranking snapshot."
                    )
            ordered.extend(preferred or group)
        detail: Stream = report["routes"][quote]
        detail.update(
            preference_order=ordered if comparable else None,
            rationale=(
                "Identity conversion."
                if not ordered[0][1]
                else "Only one eligible route."
                if len(ordered) == 1
                else "All eligible routes retained for Engine evaluation."
                if policy.path_policy == "all"
                else "Fewest hops, then comparable published positions when needed."
            ),
        )
        if policy.path_policy == "primary":
            chosen[quote] = ordered[0]
            detail["selected_primary"] = ordered[0]
            ordered = ordered[:1]
        selected_ids.update(leg for _, legs in ordered for leg in legs)

    selected = {key: candidates[key] for key in sorted(selected_ids)}
    for quote, alternatives in potential_routes(selected, set(routes), policy).items():
        if len(alternatives) > 32:
            raise MarketForgeError(
                f"The Engine allows at most 32 routes for {quote}. Select references manually."
            )
        if quote in chosen and alternatives[0] != chosen[quote]:
            raise MarketForgeError(
                f"Preferred primary routes conflict for {quote} under Engine ordering. "
                "Select a compatible set of references manually."
            )
    if source["mode"] == "depth" and not selected:
        raise MarketForgeError(
            "Depth conversion requires an external trade reference used by a conversion path."
        )
    if external is not None and not any(
        stream["provider"] == "external" for stream in selected.values()
    ):
        raise MarketForgeError(
            "The configured external publication supplies no reference on a selected route."
        )
    if source["mode"] == "depth" and external is None:
        raise MarketForgeError("Depth conversion requires an eligible external trade publication.")
    for candidate in decisions:
        if candidate["eligible"]:
            stream_id = candidate["stream_id"]
            candidate["selection"] = (
                "selected"
                if stream_id in selected
                else "unused_alternative"
                if stream_id in relevant
                else "outside_potential_routes"
            )
    result = list(selected.values())
    report.update(
        selected=result,
        rationale="Retained the trade streams used by the selected potential routes.",
    )
    return result
