"""Prepare explicit reference identities and describe metadata-only currency reachability."""

from collections import defaultdict
from pathlib import Path
from typing import Any

from forge.historical.conversion.contract import Identity, Policy
from forge.shared.errors import MarketForgeError


def references(
    source: dict[str, Any], external: dict[str, Any] | None, stream_ids: list[str]
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for stream_id in stream_ids:
        # The combined engine's contract gives internal reference identities precedence.
        providers = [source, external] if source["mode"] == "combined" else [external or source]
        for provider in providers:
            if provider is None:
                continue
            found = [
                stream
                for stream in provider["streams"]
                if stream["stream_id"] == stream_id
                and stream["kind"] == "trade"
                and stream["market_observed"]
            ]
            if not found:
                continue
            if len(found) != 1:
                raise MarketForgeError(f"Multiple trade streams match reference {stream_id!r}.")
            selected.append(
                {
                    **found[0],
                    "provider": "internal" if provider is source else "external",
                    "merge_registry_publication_id": provider["registry_publication_id"],
                    "merge_engine_publication_id": provider["engine_publication_id"],
                    "merge_root": provider["root"],
                }
            )
            break
        else:
            raise MarketForgeError(f"No observed trade reference has stream ID {stream_id!r}.")
    if len({Identity.model_validate(stream["identity"]) for stream in selected}) != len(selected):
        raise MarketForgeError(
            "Multiple reference streams map to the same Rust instrument selector."
        )
    if external is not None and not any(stream["provider"] == "external" for stream in selected):
        raise MarketForgeError("The external publication supplies no selected trade references.")
    return selected


def reachable(
    source: dict[str, Any], selected: list[dict[str, Any]], policy: Policy
) -> list[dict[str, Any]]:
    edges: dict[str, list[dict[str, str]]] = defaultdict(list)
    for stream in selected:
        base, quote = stream["base_currency"], stream["quote_currency"]
        for left, right, direction in ((base, quote, "direct"), (quote, base, "inverse")):
            edges[left].append(
                {
                    "from": left,
                    "to": right,
                    "direction": direction,
                    "stream_id": stream["stream_id"],
                }
            )

    def collect(
        current: str,
        visited: set[str],
        remaining: int,
        prefix: list[dict[str, str]],
        routes: list[list[dict[str, str]]],
    ) -> None:
        if current == policy.target_quote:
            routes.append(prefix)
        elif remaining:
            for edge in edges[current]:
                if edge["to"] not in visited:
                    collect(
                        edge["to"], visited | {edge["to"]}, remaining - 1, [*prefix, edge], routes
                    )
                    if len(routes) > 32:
                        raise MarketForgeError(
                            "Too many currency routes. Narrow the reference selection."
                        )

    result: list[dict[str, Any]] = []
    for stream in source["streams"]:
        native = stream["quote_currency"]
        paths: list[list[dict[str, str]]] = []
        selected_kind = (
            policy.instrument_kinds is None or stream["instrument_type"] in policy.instrument_kinds
        )
        if selected_kind:
            collect(native, {native}, policy.max_hops, [], paths)
            if not paths:
                raise MarketForgeError(
                    f"No metadata route from {native} to {policy.target_quote} for {stream['stream_id']}; "
                    "select explicit trade references or review max_hops."
                )
        result.append(
            {
                "stream_id": stream["stream_id"],
                "stream_rank": stream["stream_rank"],
                "native_quote": native,
                "target_quote": policy.target_quote,
                "observed": stream["observed"],
                "status": "skipped"
                if not selected_kind
                else "identity"
                if native == policy.target_quote
                else "reachable",
                "routes": paths,
            }
        )
    return result


def static_blockers(
    source: dict[str, Any],
    external: dict[str, Any] | None,
    selected: list[dict[str, Any]],
    routes: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    for publication in [source, *([external] if external else [])]:
        name = publication["engine_publication_id"]
        if (
            publication["physical_schema_version"] != 1
            or publication["timeline_ordering"] != "chronological"
        ):
            blockers.append(
                f"Merge {name} uses physical schema {publication['physical_schema_version']} / "
                f"{publication['timeline_ordering']}. Conversion readers require chronological timestamp/rank ordering."
            )
        mismatches = sorted(
            {
                stream["stream_id"]
                for stream in publication["streams"]
                if not stream["engine_first_task_matches"]
            }
        )
        if mismatches:
            blockers.append(
                f"Merge {name}: Rust uses the first task of each normalized parent; it disagrees with "
                + ", ".join(mismatches)
                + "."
            )
    ranks = [stream["stream_rank"] for stream in source["streams"]]
    if len(ranks) != len(set(ranks)):
        blockers.append("Rust conversion planning rejects repeated source stream ranks.")
    for ref in selected:
        provider = source if ref["provider"] == "internal" else external
        assert provider is not None
        inspected = [source, provider] if source["mode"] == "combined" else [provider]
        if any(
            stream["identity"] == ref["identity"] and stream["kind"] != "trade"
            for publication in inspected
            for stream in publication["streams"]
        ):
            blockers.append(
                f"Rust reference lookup rejects {ref['stream_id']} because the instrument also has a depth stream."
            )
        if (
            ref["provider"] == "external"
            and source["mode"] == "combined"
            and any(
                stream["identity"] == ref["identity"] and stream["kind"] == "trade"
                for stream in source["streams"]
            )
        ):
            blockers.append(
                f"Rust would use an internal trade instead of the requested external stream {ref['stream_id']}."
            )
    if external and source["mode"] == "trades":
        blockers.append(
            "Trade-only conversion plans external references but its execution does not replay them."
        )
    if source["mode"] == "depth" and external is None:
        blockers.append("Depth conversion requires an external trade-only reference merge.")
    if (
        source["mode"] == "depth"
        and external
        and not any(any(route for route in item["routes"]) for item in routes)
    ):
        blockers.append("Depth conversion requires a path that uses at least one reference stream.")
    if any(len(item["routes"]) > 1 for item in routes):
        blockers.append(
            "The converter rejects multi-path plans, including with path_policy=primary."
        )
    if source["mode"] == "trades" and any(item["status"] == "skipped" for item in routes):
        blockers.append(
            "Trade-only materialization does not support preserving skipped instrument kinds."
        )
    return blockers


def output_available(output: Path) -> None:
    for path in (output, output.with_suffix(".staging")):
        if path.exists() or path.is_symlink() or path.resolve() != path:
            raise MarketForgeError(
                f"Conversion output or staging path exists or aliases another path: {path}."
            )
