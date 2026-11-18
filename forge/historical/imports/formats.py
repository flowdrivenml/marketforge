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


def check_profile_sample(
    path: Path,
    definition: dict[str, object],
    symbol: str,
    native_fields: dict[str, int] | None = None,
) -> None:
    """Check bounded structural evidence against the declared profile."""
    import tarfile

    from forge.markets.models import array, obj

    source = obj(definition["source"])
    normalization = obj(definition["normalization"])
    expected = str(source["container"])
    physical = str(source["format"])
    with path.open("rb") as stream:
        magic = stream.read(4)
    if magic.startswith(b"\x28\xb5\x2f\xfd"):
        raise ImportError(f"{path.name}: Zstandard/DBN is unsupported by the selected profile.")
    try:
        if expected == "zip" and physical == "xlsx" and magic.startswith(b"PK\x03\x04"):
            # Refuse ZIP64 or excessive directory metadata before opening ZIP.
            with path.open("rb") as stream:
                size = stream.seek(0, 2)
                stream.seek(max(0, size - 65557))
                footer = stream.read(65557)
            end = footer.rfind(b"PK\x05\x06")
            if (
                end >= 0
                and len(footer) - end >= 22
                and int.from_bytes(footer[end + 12 : end + 16], "little") <= 8 * 1024 * 1024
            ):
                with zipfile.ZipFile(path) as archive:
                    names = [item for item in archive.infolist() if not item.is_dir()]
                    if len(names) == 1 and names[0].filename.lower().endswith(".xlsx"):
                        with archive.open(names[0]) as stream:
                            if stream.read(4) == b"PK\x03\x04":
                                if native_fields:
                                    raise ImportError(
                                        f"{path.name}: native identity sampling requires CSV or JSONL."
                                    )
                                return
            raise ImportError(f"{path.name}: expected one XLSX workbook in the ZIP source.")
        if expected == "tar_gzip" and magic.startswith(b"\x1f\x8b") and physical == "jsonl":
            with tarfile.open(path, "r|gz") as archive:
                item = next((member for member in archive if member.isfile()), None)
                stream = archive.extractfile(item) if item else None
                if stream is None:
                    raise ImportError(f"{path.name}: no data member in the TAR source.")
                with stream:
                    line = stream.readline(SAMPLE_BYTES)
                record = json.loads(line)
            evidence = {"container": "tar_gzip", "layout": "jsonl", "representative_rows": [record]}
        else:
            evidence = sample(path, header=source.get("header") is True)
        if evidence.get("container") != expected or evidence.get("layout") != physical:
            raise ImportError(
                f"{path.name}: source structure does not match {expected}/{physical}."
            )
        rows = array(evidence["representative_rows"])
        header = physical == "csv" and source.get("header") is True
        if len(rows) <= int(header):
            raise ImportError(f"{path.name}: source has no data rows in the bounded sample.")
        record = rows[int(header)]
        fields = obj(definition["fields"])
        identity = obj(normalization.get("instrument") or {}).get("field")
        timestamp = obj(normalization["event_timestamp"])["field"]
        if physical == "jsonl":

            def value(field: str) -> object:
                node: object = record
                for component in field.split("."):
                    if not isinstance(node, dict):
                        return None
                    node = cast(dict[str, object], node).get(component)
                return node
        else:

            def value(field: str) -> object:
                columns = array(record)
                declared = obj(fields.get(field) or {})
                position = declared.get("position")
                if header:
                    names = array(evidence["fields"])
                    if field not in names or names.count(field) != 1:
                        return None
                    index = names.index(field)
                    return columns[index] if index < len(columns) else None
                if type(position) is int and 0 < position <= len(columns):
                    return columns[position - 1]
                return None

        for field, expected_id in (native_fields or {}).items():
            actual_id = value(field)
            if (type(actual_id) is not int and not isinstance(actual_id, str)) or str(
                actual_id
            ) != str(expected_id):
                raise ImportError(
                    f"{path.name}: sampled native identity {field!r} differs from {expected_id}."
                )

        stamp = value(str(timestamp))
        encoding = obj(fields[str(timestamp)]).get("encoding")
        if encoding == "iso8601_nanoseconds":
            valid_stamp = isinstance(stamp, str) and re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", stamp
            )
        else:
            valid_stamp = stamp is not None and re.fullmatch(r"[0-9]+", str(stamp))
        if not valid_stamp:
            raise ImportError(
                f"{path.name}: sampled timestamp conflicts with the profile's declared encoding."
            )
        for mapping in ("price", "quantity", "bids", "asks"):
            field = normalization.get(mapping)
            if isinstance(field, dict):
                field_name = cast(dict[str, object], field).get("field")
                if field_name is not None and value(str(field_name)) is None:
                    raise ImportError(f"{path.name}: missing sampled profile field {field_name!r}.")
        if identity is not None:
            actual = value(str(identity))
            if actual != symbol:
                raise ImportError(
                    f"{path.name}: sampled instrument field conflicts with {symbol!r}."
                )
    except (OSError, EOFError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise ImportError(
            f"{path.name}: cannot confirm source structure against the profile."
        ) from error
