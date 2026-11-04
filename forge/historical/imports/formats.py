"""Bounded input evidence and drafts following the current Rust TOML catalogue."""

import csv
import gzip
import io
import json
import re
import tomllib
import zipfile
from pathlib import Path
from typing import cast

from forge.historical.imports.models import ImportConfig, ImportError, freeze_inputs
from forge.shared.config.toml import dumps

SAMPLE_BYTES = 65536


def sample(path: Path, member: str | None = None, header: bool | None = None) -> dict[str, object]:
    with path.open("rb") as source:
        magic = source.read(4)
    container = "plain"
    selected_member = None
    try:
        if magic.startswith(b"PK\x03\x04"):
            container = "zip"
            # Bound central-directory allocation before constructing ZipFile.
            with path.open("rb") as source:
                size = source.seek(0, 2)
                source.seek(max(0, size - 65557))
                tail = source.read(65557)
            offset = tail.rfind(b"PK\x05\x06")
            import struct

            if (
                offset < 0
                or len(tail) - offset < 22
                or struct.unpack_from("<I", tail, offset + 12)[0] > 8 * 1024 * 1024
            ):
                return {
                    "container": container,
                    "layout": "unsupported",
                    "reason": "ZIP directory is absent or exceeds 8 MiB sample limit.",
                }
            with zipfile.ZipFile(path) as archive:
                members = [i for i in archive.infolist() if not i.is_dir()]
                if member is None and len(members) != 1:
                    return {
                        "container": container,
                        "layout": "requires_member",
                        "members": [i.filename for i in members[:20]],
                        "reason": "Choose --member explicitly; layouts are not assumed identical.",
                    }
                selected_member = member or members[0].filename
                with archive.open(selected_member) as source:
                    content = source.read(SAMPLE_BYTES)
        elif magic.startswith(b"\x1f\x8b"):
            container = "gzip"
            with gzip.open(path, "rb") as source:
                content = source.read(SAMPLE_BYTES)
            if len(content) > 262 and content[257:262] == b"ustar":
                return {
                    "container": "tar_gzip",
                    "layout": "unsupported",
                    "reason": "Tar member selection is not implemented by the bounded sampler; raw import remains supported.",
                }
        else:
            with path.open("rb") as source:
                content = source.read(SAMPLE_BYTES)
        text = content.decode("utf-8-sig")
    except (UnicodeError, OSError, zipfile.BadZipFile, EOFError):
        return {
            "container": container,
            "layout": "unsupported",
            "reason": "Binary or unreadable text sample; importing raw bytes is still supported.",
        }
    lines = text.splitlines()[:20]
    if not lines:
        return {"container": container, "layout": "unsupported", "reason": "Empty sample."}
    fields: list[str] = []
    layout = "unsupported"
    rows: list[object] = []
    if lines[0].lstrip().startswith("{"):
        try:
            record: object = json.loads(lines[0])

            def walk(value: object, prefix: str = "", depth: int = 0) -> None:

                if isinstance(value, dict) and depth < 8:
                    for key, item in cast(dict[str, object], value).items():
                        walk(item, (prefix + "." if prefix else "") + key, depth + 1)
                elif prefix:
                    fields.append(prefix)

            walk(record)
            layout = "jsonl"
            rows = [record]
        except (ValueError, TypeError):
            pass
    else:
        try:
            dialect = csv.Sniffer().sniff("\n".join(lines), delimiters=",\t;|")
            parsed = list(csv.reader(io.StringIO("\n".join(lines)), dialect))[:5]
            if parsed and len(parsed[0]) > 1:
                layout = "csv"
                rows = [row for row in parsed]
                fields = (
                    parsed[0]
                    if header is True
                    else [f"column_{n + 1}" for n in range(len(parsed[0]))]
                )
        except csv.Error:
            pass
    return {
        "container": container,
        "layout": layout,
        "member": selected_member,
        "fields": fields[:256],
        "header": header,
        "representative_rows": rows[:3],
        "sample_limit_bytes": SAMPLE_BYTES,
        "reason": None
        if layout != "unsupported"
        else "No supported CSV/JSONL text structure identified.",
    }


def draft(
    root: Path,
    config: ImportConfig,
    profile: str,
    input_reference: str,
    *,
    member: str | None = None,
    header: bool | None = None,
    overwrite: bool = False,
) -> dict[str, object]:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,100}", profile):
        raise ImportError("Profile draft name must use lowercase letters, digits and hyphens.")
    selected = next((i for i in freeze_inputs(config) if i.reference == input_reference), None)
    if selected is None:
        raise ImportError("--input must select an exact reference from the frozen import inputs.")
    evidence = sample(Path(selected.path), member, header)
    if evidence["layout"] == "requires_member":
        raise ImportError(
            "Choose --member from the inspected ZIP members before creating a format draft."
        )
    family = "trades" if config.data_type == "trades" else "depth"
    catalogue = root / "data" / ".config" / "historical" / "formats" / family
    templates: list[dict[str, object]] = []
    for path in sorted(catalogue.glob("*.toml")):
        with path.open("rb") as source:
            document: dict[str, object] = tomllib.load(source)
        if (
            document.get("kind") == "physical"
            and document.get("container_format") == evidence["layout"]
        ):
            templates.append(document)
    compatible = bool(templates)
    if not templates:
        for path in sorted(catalogue.glob("*.toml")):
            with path.open("rb") as source:
                candidate: dict[str, object] = tomllib.load(source)
            if candidate.get("kind") == "physical":
                templates.append(candidate)
                break
    if not templates:
        raise ImportError(
            "The current Rust format catalogue is missing; restore its physical templates before generating a draft."
        )
    from forge.markets.models import array, string

    physical = dict(templates[0])
    physical_id = "draft-" + profile
    physical.update(id=physical_id, supported_containers=[evidence["container"]])
    fields = [
        {
            "name": string(value),
            "type": "string",
            **({"position": i + 1} if evidence["layout"] == "csv" else {}),
        }
        for i, value in enumerate(array(evidence.get("fields", [])))
    ]
    schema: dict[str, object] = {"fields": fields}
    if header is not None and evidence["layout"] == "csv":
        schema["header"] = header
    physical["raw_schema"] = schema
    if not compatible:
        physical.pop("container_format", None)
    physical.pop("record_format", None)  # rows/events/snapshot_rows/book_rows are not guessed.
    identity = config.identity
    format_code = profile.upper()
    definition: dict[str, object] = {
        "schema_version": 1,
        "kind": "profile",
        "id": f"{identity.exchange}.{identity.market_category}.{identity.instrument_type}.{profile}.v1",
        "physical": physical_id,
        "format_code": format_code,
        "exchange": identity.exchange,
        "instrument_type": identity.instrument_type,
        "market_category": identity.market_category,
        "unsupported_reason": "DRAFT: unresolved normalization semantics; not approved for execution.",
        "rules": [{"target": "trade", "rules": {}}] if config.data_type == "trades" else [],
    }
    unresolved = (
        ["physical.container_format: sampled layout has no compatible engine template"]
        if not compatible
        else []
    ) + [
        "physical.record_format",
        "profile.source_ordering",
        "rules[].timestamp_encoding",
        "rules[].quantity_encoding",
    ]
    if evidence["layout"] == "csv" and header is None:
        unresolved.append("physical.raw_schema.header (use --header or --no-header when known)")
    unresolved += (
        [
            "rules[].rules.event_timestamp",
            "rules[].rules.price",
            "rules[].rules.quantity",
            "rules[].rules.side",
        ]
        if config.data_type == "trades"
        else [
            "rules[].target (l2_snapshot and/or l2_update)",
            "rules[].rules.bids/asks or compatible depth row mapping",
            "snapshot/update semantics",
            "sequence and zero-quantity rules",
        ]
    )
    if identity.exchange not in {"binance", "bybit", "okx", "gateio", "bitget"}:
        unresolved.append("Rust canonical exchange support for this venue is not established.")
    destination = root / "data" / ".config" / "historical" / "format-drafts" / profile
    if destination.exists() and not overwrite:
        raise ImportError("Draft already exists; use --overwrite explicitly.")
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "physical.toml").write_text(
        "# DRAFT - outside the active format catalogue.\n" + dumps(physical), encoding="utf-8"
    )
    (destination / "profile.toml").write_text(
        "# DRAFT - incomplete and explicitly unsupported.\n" + dumps(definition), encoding="utf-8"
    )
    diagnostics = {
        "status": "draft",
        "engine_validation": "pending: no stable profile-only validation command exists in the inspected CLI",
        "profile": str(destination / "profile.toml"),
        "physical": str(destination / "physical.toml"),
        "evidence": evidence,
        "unresolved": unresolved,
        "import_configuration_updated": False,
        "profile_reference": definition["id"],
    }
    (destination / "diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2), encoding="utf-8"
    )
    # Parse the generated TOML now; this is explicitly not Rust semantic validation.
    for file in ("profile.toml", "physical.toml"):
        with (destination / file).open("rb") as source:
            tomllib.load(source)
    return diagnostics
