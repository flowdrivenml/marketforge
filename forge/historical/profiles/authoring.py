"""Reusable instructional steps for profile authoring."""

from dataclasses import dataclass
from typing import Literal

from forge.historical.profiles.documentation import guide_url

ProfileType = Literal["trades", "depth"]


@dataclass(frozen=True)
class Step:
    title: str
    detail: str
    command: str | None = None


@dataclass(frozen=True)
class Guide:
    profile_type: ProfileType
    documentation: str
    steps: tuple[Step, ...]


def authoring_guide(kind: ProfileType) -> Guide:
    identity = f"example_exchange.equity.equity.{kind}"
    raw = "synthetic-trades.csv --no-header" if kind == "trades" else "synthetic-depth.jsonl"
    source = f"docs/content/examples/catalogue/{raw}"
    draft = f"data/.config/engine/normalize/profile-drafts/{identity}.toml"
    fields = (
        (
            Step(
                "Map trade fields",
                "Declare timestamp, price, size and side. Use the raw "
                "column names or one-based positions for headerless CSV.",
            ),
            Step(
                "Choose side and units",
                "Use casefold for buy/sell words, map for vendor "
                "tokens, or sign_to_side for signed input with explicit positive and negative "
                "sides. Declare whether quantity represents base units, quote units or contracts. "
                "Map a native trade ID only when one exists.",
            ),
        )
        if kind == "trades"
        else (
            Step(
                "Map both sides",
                "Declare the raw bid and ask arrays, then map them to bids "
                "and asks. Describe tuple positions or named members for price and quantity. "
                "Quote dotted JSON paths in TOML. Set quantity_encoding to the documented "
                "source units. Confirm the recorded size against the instrument specification.",
            ),
            Step(
                "Choose book operations",
                "Use l2_snapshot for an observed book. For sources "
                "that also provide updates, add l2_update with disjoint event filters. "
                "Identify the opening snapshot, preserve source order, and check the vendor's "
                "level-replacement, deletion and sequence rules. Map native sequence evidence "
                "only when present.",
            ),
        )
    )
    return Guide(
        kind,
        guide_url(kind),
        (
            Step(
                "Identify the source",
                "Choose the venue and market namespace, and identify the "
                "format code used by the catalogue for this record layout.",
            ),
            Step(
                "Sample the input",
                "Check archive packaging, the inner record format, and the "
                "actual source fields before declaring source layout and order.",
                f"mf normalize profiles sample --file {source}",
            ),
            Step(
                "Create a starting draft",
                "Choose a new profile ID for your own source. The "
                "example ID below follows the synthetic walkthrough. Edit with nvim and resolve "
                "the required format code and source mappings.",
                f"mf normalize profiles new {identity}",
            ),
            *fields,
            Step(
                "Decode timestamps",
                "Identify the source's epoch unit or ISO 8601 encoding. "
                "Set encoding on the raw field referenced by event_timestamp and confirm the "
                "decoded instant. Check the system timestamp separately if supplied.",
                f"mf normalize profiles schema --type {kind} --field fields.timestamp.encoding",
            ),
            Step(
                "Compile with Rust",
                "Compilation checks supported structure and effective rule "
                "relationships. It does not establish that the raw records match your interpretation.",
                f"mf normalize profiles validate --file {draft}",
            ),
            Step(
                "Install in a test workspace",
                "Publish through the guarded installer using a workspace and catalogue reserved "
                "for testing. The selected catalogue must contain the venue. A changed "
                "interpretation under an installed ID is rejected.",
                f"mf normalize profiles add {identity} --from {draft} --testing",
            ),
            Step(
                "Verify a bounded normalization",
                "Follow the linked synthetic walkthrough to register a small sample, run its "
                "normalization recipe, and compare the result with the raw records. Verify "
                "event time, price and quantity, plus trade side or initialized book state.",
            ),
        ),
    )
