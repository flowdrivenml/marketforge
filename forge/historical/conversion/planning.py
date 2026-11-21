"""Resolve recipe reference selections against verified publication evidence."""

from pathlib import Path
from typing import Any

from forge.historical.conversion.contract import Identity
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
            if (
                provider is external
                and source["mode"] == "combined"
                and any(
                    stream["kind"] == "trade" and stream["identity"] == found[0]["identity"]
                    for stream in source["streams"]
                )
            ):
                raise MarketForgeError(
                    f"An internal trade takes precedence over external reference {stream_id!r}."
                )
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


def output_available(output: Path) -> None:
    for path in (output, output.with_suffix(".staging")):
        if path.exists() or path.is_symlink() or path.resolve() != path:
            raise MarketForgeError(
                f"Conversion output or staging path exists or aliases another path: {path}."
            )
