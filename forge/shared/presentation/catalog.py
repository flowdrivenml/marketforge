"""Read-only projections of catalogue declarations, records and evidence."""

import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from forge.shared.presentation.runs import timestamp

Row = Mapping[str, Any]
Fields = tuple[tuple[str, str], ...]
IDENTITY = ("exchange", "instrument_type", "market_category", "symbol")
NAMES = {"exchange": "Exchange", "instrument": "Instrument", "specs": "Specification"}
ASSETS = (
    ("base_asset", "Base asset"),
    ("quote_asset", "Quote asset"),
    ("settlement_asset", "Settlement asset"),
)
ECONOMICS = (
    ("quantity_type", "Quantity type"),
    ("contract_value", "Contract value"),
    ("contract_value_asset", "Contract-value asset"),
    ("tick_size", "Tick size"),
    ("qty_step", "Quantity step"),
    ("min_qty", "Minimum quantity"),
    ("max_qty", "Maximum quantity"),
    ("min_notional", "Minimum notional"),
    ("min_notional_asset", "Minimum notional asset"),
    ("max_notional", "Maximum notional"),
    ("max_notional_asset", "Maximum notional asset"),
)
DECIMALS = {
    "contract_value",
    "tick_size",
    "qty_step",
    "min_qty",
    "max_qty",
    "min_notional",
    "max_notional",
}
TIMES = {
    "source_observed_at",
    "fetched_at",
    "as_of",
    "valid_from",
    "valid_to",
    "assumed_applicable_from",
    "assumed_applicable_to",
}


def value(item: object, key: str = "") -> str:
    if item is None:
        return "—"
    if isinstance(item, bool):
        return "Yes" if item else "No"
    if key in TIMES:
        formatted = timestamp(item)
        return str(item) if formatted == "—" else formatted
    if isinstance(item, Decimal) or (key in DECIMALS and isinstance(item, str)):
        try:
            return format(Decimal(item), "f")
        except InvalidOperation:
            return str(item)
    return str(item)


def fields(row: Row, labels: Fields) -> Fields:
    return tuple((label, value(row[key], key)) for key, label in labels if row.get(key) is not None)


def catalog_reference(row: Row) -> str:
    if all(row.get(field) is not None for field in IDENTITY):
        return ".".join(str(row[field]) for field in IDENTITY)
    if row.get("instrument"):
        return str(row["instrument"])
    return authority_reference(row.get("identity_key"))


def authority_reference(item: object) -> str:
    if not isinstance(item, str):
        return value(item)
    parts = item.split("/", 3)
    return ".".join(parts) if len(parts) == 4 else item


@dataclass(frozen=True)
class Section:
    title: str
    details: Fields = ()
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[str, ...], ...] = ()
    note: str = ""


@dataclass(frozen=True)
class CatalogView:
    title: str
    environment: str
    sections: tuple[Section, ...]
    status: str = ""
    source: Path | None = None
    inspect: str = ""


def identity_sections(row: Row) -> tuple[Section, ...]:
    return (
        Section(
            "Identity",
            (("Catalog reference", catalog_reference(row)),)
            + fields(
                row,
                (
                    ("id", "Catalog instrument ID"),
                    ("exchange", "Exchange"),
                    ("symbol", "Native symbol"),
                    ("instrument_type", "Instrument type"),
                    ("market_category", "Market category"),
                    ("status", "Status"),
                ),
            ),
        ),
        Section("Assets", tuple((label, value(row.get(key))) for key, label in ASSETS)),
        Section(
            "Historical Authority",
            fields(
                row,
                (
                    ("historical_instrument_id", "Historical instrument ID"),
                    ("namespace", "Namespace"),
                ),
            ),
        ),
    )


def intervals(row: Row) -> Fields:
    assumed = fields(
        row,
        (
            ("assumed_applicable_from", "Assumed applicable from"),
            ("assumed_applicable_to", "Assumed applicable until"),
        ),
    )
    if assumed or row.get("inference_method") in {
        "catalogue_inferred",
        "reference_contract_inferred",
    }:
        return assumed
    return fields(
        row, (("valid_from", "Declared valid from"), ("valid_to", "Declared valid until"))
    )


def evidence_details(row: Row) -> Section:
    definition: Row = row["definition"]
    observed = row.get("source_observed_at") or definition.get("source_observed_at")
    observed_field = (
        (("Source observed at", value(observed, "source_observed_at")),)
        if observed is not None
        else ()
    )
    metadata = fields(row, (("fingerprint", "Fingerprint"), ("evidence_kind", "Evidence kind")))
    metadata += fields(
        definition,
        (
            ("record_kind", "Record kind"),
            ("historically_verified", "Historically verified"),
            ("as_of", "As of"),
            ("fetched_at", "Retrieved at"),
        ),
    )
    metadata += observed_field + intervals(definition)
    metadata += fields(
        definition,
        (
            ("provenance", "Provenance"),
            ("inference_method", "Inference method"),
            ("policy_version", "Policy version"),
            ("assumption", "Assumption"),
            ("source_catalogue_instrument_id", "Source catalog instrument ID"),
            ("source_api_observation_id", "Source observation ID"),
            ("source_snapshot_sha256", "Source snapshot fingerprint"),
        ),
    )
    if reference := definition.get("reference_instrument"):
        metadata += (("Reference instrument", authority_reference(reference)),)
    if inherited := definition.get("inherited_fields"):
        metadata += (("Inherited fields", ", ".join(inherited)),)
    return Section(
        f"Historical Evidence · ID {row['id']}", metadata + fields(definition, ASSETS + ECONOMICS)
    )


def wrap_reference(reference: str) -> str:
    parts = reference.split(".", 3)
    return ".".join(parts[:3]) + ".\n" + parts[3] if len(parts) == 4 else reference


def listing(entity: str, result: Row, *, compact: bool) -> tuple[Section, ...]:
    if entity == "exchange":
        rows = tuple(
            tuple(value(row[key]) for key in ("id", "code", "name")) for row in result["exchanges"]
        )
        return (
            Section(
                "Exchanges",
                columns=("Exchange ID", "Code", "Name"),
                rows=rows,
                note="No exchanges registered. Use mf catalog exchange add --help.",
            ),
        )
    if entity == "instrument":
        instruments = result["instruments"]
        if compact:
            columns = ("Catalog ID", "Catalog reference", "Status")
            rows = tuple(
                (value(row["id"]), catalog_reference(row), value(row.get("status")))
                for row in instruments
            )
        else:
            keys = (
                "id",
                "exchange",
                "symbol",
                "instrument_type",
                "market_category",
                "base_asset",
                "quote_asset",
                "status",
            )
            columns = (
                "Catalog ID",
                "Exchange",
                "Symbol",
                "Type",
                "Market",
                "Base",
                "Quote",
                "Status",
            )
            rows = tuple(tuple(value(row.get(key)) for key in keys) for row in instruments)
        return (
            Section(
                "Instruments",
                columns=columns,
                rows=rows,
                note="No instruments registered. Use mf catalog instrument add --help.",
            ),
        )
    current = tuple(
        (
            catalog_reference(row),
            value(row.get("quantity_type")),
            value(row.get("tick_size"), "tick_size"),
            value(row.get("qty_step"), "qty_step"),
            " ".join(
                value(row[key], key)
                for key in ("contract_value", "contract_value_asset")
                if row.get(key) is not None
            )
            or "—",
            value(row.get("fetched_at"), "fetched_at"),
        )
        for row in result["current_specifications"]
    )
    evidence = tuple(
        (
            value(row["id"]),
            catalog_reference(row),
            value(row.get("evidence_kind")),
            value(row["definition"].get("record_kind")),
            "\n".join(f"{label}: {bound}" for label, bound in intervals(row["definition"])) or "—",
            value(row["definition"].get("source_observed_at"), "source_observed_at"),
            value(row["definition"].get("historically_verified")),
        )
        for row in result["specifications"]
    )
    if compact:
        current_compact = tuple(
            (
                wrap_reference(instrument),
                f"Quantity type: {quantity}\nTick: {tick}\nStep: {step}\n"
                f"Contract value: {contract}\nRetrieved: {observed}",
            )
            for instrument, quantity, tick, step, contract, observed in current
        )
        evidence_compact = tuple(
            (
                f"ID {identity}\n{wrap_reference(instrument)}",
                f"{kind}\nRecord kind: {record}\nHistorically verified: {verified}\n"
                f"{applicable}\nSource observed at: {observed}",
            )
            for identity, instrument, kind, record, applicable, observed, verified in evidence
        )
        return (
            Section(
                "Current Specifications",
                columns=("Instrument", "Specification"),
                rows=current_compact,
                note="No current specifications returned.",
            ),
            Section(
                "Historical Specification Evidence",
                columns=("Evidence ID / instrument", "Evidence"),
                rows=evidence_compact,
                note="No historical specification evidence returned.",
            ),
        )
    return (
        Section(
            "Current Specifications",
            columns=(
                "Instrument",
                "Quantity type",
                "Tick",
                "Step",
                "Contract value",
                "Retrieved (UTC)",
            ),
            rows=current,
            note="No current specifications returned.",
        ),
        Section(
            "Historical Specification Evidence",
            columns=(
                "Evidence ID",
                "Instrument",
                "Evidence kind",
                "Record kind",
                "Applicability",
                "Source time (UTC)",
                "Verified",
            ),
            rows=evidence,
            note="No historical specification evidence returned.",
        ),
    )


def project(
    entity: str,
    action: str,
    result: Row,
    *,
    environment: str = "",
    source: Path | None = None,
    declaration: Row | None = None,
    explicit_fields: frozenset[str] = frozenset(),
    compact: bool = False,
) -> CatalogView:
    name = NAMES[entity]
    if action == "list":
        return CatalogView(
            {"exchange": "Exchanges", "instrument": "Instruments", "specs": "Specifications"}[
                entity
            ],
            environment,
            listing(entity, result, compact=compact),
        )
    if action == "validate":
        assert declaration is not None
        if entity == "exchange":
            details = fields(declaration, (("code", "Code"), ("name", "Name")))
        elif entity == "instrument":
            details = identity_sections(declaration)[0].details + fields(declaration, ASSETS)
            if "status" not in explicit_fields:
                details = tuple(
                    (label, f"{item} (default)" if label == "Status" else item)
                    for label, item in details
                )
        else:
            details = (("Catalog reference", catalog_reference(declaration)),)
            details += fields(declaration, ECONOMICS + (("provenance", "Provenance"),))
            details += tuple(
                (
                    label,
                    value(declaration[key])
                    if declaration.get(key) is not None
                    else "Unresolved in declaration",
                )
                for key, label in ASSETS
            )
            for key, label in (("as_of", "As of"), ("fetched_at", "Retrieved at")):
                if declaration.get(key) is not None:
                    rendered = value(declaration[key], key)
                    details += (
                        (label, rendered if key in explicit_fields else rendered + " (derived)"),
                    )
            details += intervals(declaration)
        return CatalogView(
            f"{name} Validation",
            "",
            (
                Section(
                    "Declaration",
                    details,
                    note="Local declaration checks passed. Registry identities, conflicts and historical verification require separate evidence.",
                ),
            ),
            "valid",
            source,
        )
    if entity == "exchange":
        sections = (
            Section(
                "Identity",
                fields(result, (("id", "Exchange ID"), ("code", "Code"), ("name", "Name"))),
            ),
        )
        ref = str(result["code"])
    elif entity == "instrument":
        sections = identity_sections(result)
        ref = catalog_reference(result)
    elif action == "add":
        assert declaration is not None
        ref = catalog_reference(declaration)
        sections = (Section("Instrument", (("Catalog reference", ref),)), evidence_details(result))
    else:
        ref = catalog_reference(result["instrument"])
        current = result["current"]
        sections = identity_sections(result["instrument"])
        sections += (
            Section(
                "Current Specification",
                fields(current, ECONOMICS + (("fetched_at", "Retrieved at"),)),
            )
            if current is not None
            else Section("Current Specification", note="No current specification returned."),
        )
        sections += tuple(evidence_details(row) for row in result["evidence"]) or (
            Section(
                "Historical Specification Evidence",
                note="No historical specification evidence returned.",
            ),
        )
    if action == "add":
        return CatalogView(
            f"{name} Registration",
            environment,
            sections,
            "registered",
            source,
            f"mf catalog {entity} show {shlex.quote(ref)} --{environment}",
        )
    return CatalogView(name, environment, sections)
