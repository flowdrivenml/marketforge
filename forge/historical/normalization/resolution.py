"""Read-only registry/catalog resolution and conservative input checks."""

import tarfile
import tomllib
import zipfile
from decimal import Decimal
from pathlib import Path
from typing import cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.files import contained
from forge.historical.normalization.config import InputSelection
from forge.markets.models import obj
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.service import inventory


def require(condition: bool, message: str) -> None:
    if not condition:
        raise MarketForgeError(message)


def catalog_entry(directory: Path, identity: str) -> dict[str, object]:
    matches: list[dict[str, object]] = []
    for path in sorted(directory.rglob("*.toml")):
        with path.open("rb") as source:
            document = cast(dict[str, object], tomllib.load(source))
        if document.get("id") == identity:
            matches.append(document)
    require(len(matches) == 1, f"Expected one catalog entry for {identity}; found {len(matches)}.")
    return matches[0]


def check_member(path: Path, container: str, member: str | None) -> None:
    if container not in {"zip", "tar_gzip"}:
        require(member is None, f"archive_member is not supported for {container}.")
        return
    require(member is not None, "Select an explicit archive_member for ZIP/tar_gzip inputs.")
    assert member is not None
    try:
        if container == "zip":
            with zipfile.ZipFile(path) as archive:
                names = [i.filename for i in archive.infolist() if not i.is_dir()]
        else:
            with tarfile.open(path, "r:gz") as archive:
                names = [i.name for i in archive if i.isfile()]
        require(names.count(member) == 1, "archive_member must identify exactly one regular file.")
    except (zipfile.BadZipFile, tarfile.TarError) as exc:
        raise MarketForgeError(f"Cannot inspect archive members: {path}") from exc


def positive_decimal(value: object, field: str) -> str:
    require(isinstance(value, Decimal), f"Missing authoritative {field} metadata.")
    number = cast(Decimal, value)
    require(number.is_finite() and number > 0, f"Invalid {field} metadata.")
    return str(number)


async def resolve_input(
    connection: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    selection: InputSelection,
    content_type: str,
) -> tuple[dict[str, object], str]:
    rows = await inventory(connection, root=paths.raw_dir, archive_id=selection.archive_id)
    require(
        len(rows) == 1, f"Local archive {selection.archive_id} is not registered in this raw root."
    )
    local = rows[0]
    require(local["status"] == "complete", f"Local archive {selection.archive_id} is not complete.")
    path = contained(paths.raw_dir, str(local["relative_path"])).resolve()
    require(path.is_file(), f"Input file is missing: {path}")
    require(
        path.stat().st_size == local["bytes_written"], f"Input size differs from registry: {path}"
    )
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT i.*, e.code AS exchange, s.tick_size, s.contract_value,
                      s.contract_value_asset, s.quantity_type
               FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
               LEFT JOIN catalog.instrument_specs s ON s.instrument_id=i.id WHERE i.id=%s""",
            (selection.instrument_id,),
        )
        instrument = await cursor.fetchone()
        require(instrument is not None, f"Instrument {selection.instrument_id} is not registered.")
        assert instrument is not None
        if local["remote_archive_id"] is not None:
            await cursor.execute(
                "SELECT r.scope_json,r.data_type,e.code AS exchange FROM historical.remote_archives r "
                "JOIN catalog.exchanges e ON e.id=r.exchange_id WHERE r.id=%s",
                (local["remote_archive_id"],),
            )
            remote = await cursor.fetchone()
            require(remote is not None, "Remote archive identity is missing.")
            assert remote is not None
            scope = obj(remote["scope_json"])
            require(
                scope.get("scope_kind") == "instrument",
                "Family/base-coin archives lack registered member-to-instrument mappings; preparation is unsupported.",
            )
            require(
                scope.get("scope_identifier") == instrument["symbol"],
                "Archive symbol differs from selected instrument.",
            )
            identity = {**scope, "exchange": remote["exchange"]}
            data_type = remote["data_type"]
        else:
            await cursor.execute(
                "SELECT * FROM historical.external_archives WHERE id=%s",
                (local["external_archive_id"],),
            )
            external = await cursor.fetchone()
            require(external is not None, "External archive identity is missing.")
            assert external is not None
            require(
                external["normalization_readiness"] == "pending_engine_validation",
                "External archive still requires a profile or historical specifications.",
            )
            require(
                external["instrument_id"] == selection.instrument_id,
                "External archive instrument differs from selection.",
            )
            require(
                external["format_profile"] == selection.format_profile,
                "External archive profile differs from selection.",
            )
            # Historical external specifications cannot safely be replaced by current specs.
            require(
                external["specifications_json"] is None,
                "External historical specification overrides are not yet supported by preparation.",
            )
            identity = obj(external["identity_json"])
            require(
                identity.get("symbol") == instrument["symbol"],
                "External symbol differs from metadata.",
            )
            data_type = external["data_type"]
    require(
        data_type == ("trades" if content_type == "trades" else "order_book_l2"),
        "Archive content type differs from job.",
    )
    for field, column in (
        ("exchange", "exchange"),
        ("market_category", "market_category"),
        ("instrument_type", "instrument_type"),
    ):
        require(
            identity.get(field) == instrument[column], f"Archive {field} differs from instrument."
        )
    historical = paths.config_dir / "historical"
    profile = catalog_entry(historical / "profiles", selection.format_profile)
    require(not profile.get("unsupported_reason"), "Selected format profile is unsupported.")
    for field in ("exchange", "market_category", "instrument_type"):
        require(profile.get(field) == identity[field], f"Profile {field} differs from archive.")
    rules = profile.get("rules")
    require(
        isinstance(rules, list) and len(cast(list[object], rules)) > 0,
        "Profile has no normalization rules.",
    )
    typed_rules = cast(list[object], rules)
    targets = {"trade"} if content_type == "trades" else {"l2_snapshot", "l2_update", "depth"}
    require(
        all(obj(rule).get("target") in targets for rule in typed_rules),
        "Profile content type differs from job.",
    )
    physical_id = profile.get("physical")
    require(
        isinstance(physical_id, str) and bool(physical_id),
        "Profile is missing physical schema identity.",
    )
    physical = catalog_entry(historical / "raw_schemas", str(physical_id))
    containers = physical.get("supported_containers")
    require(
        isinstance(containers, list) and len(cast(list[object], containers)) > 0,
        "Profile has no supported containers.",
    )
    supported = cast(list[object], containers)
    compression = selection.source_compression
    if compression is None:
        require(len(supported) == 1, "Select source_compression explicitly for this profile.")
        compression = str(supported[0])
    require(
        compression in {"plain", "gzip", "zip", "tar_gzip"}, "Unknown source container in profile."
    )
    require(compression in supported, "Selected compression is unsupported by profile.")
    check_member(path, compression, selection.archive_member)
    tick_size = positive_decimal(instrument["tick_size"], "tick_size")
    contract_value: str | None = None
    contract_asset: str | None = None
    if any(obj(rule).get("quantity_encoding") == "contracts" for rule in typed_rules):
        contract_value = positive_decimal(instrument["contract_value"], "contract_value")
        asset = instrument["contract_value_asset"]
        require(isinstance(asset, str) and bool(asset.strip()), "Missing contract value asset.")
        contract_asset = str(asset)
    market = str(instrument["market_category"])
    stream_id = (
        ".".join(
            str(instrument[k]) for k in ("exchange", "market_category", "instrument_type", "symbol")
        )
        + f".{content_type}"
    )
    return {
        "task_id": selection.task_id,
        "stream_id": stream_id,
        "input_path": str(path),
        "format_profile": selection.format_profile,
        "profile_fingerprint": None,
        "exchange": instrument["exchange"],
        "instrument_id": selection.instrument_id,
        "symbol": instrument["symbol"],
        "instrument": {
            "instrument_kind": instrument["instrument_type"],
            "contract_kind": None if market in {"spot", "option"} else market,
            "tick_size": tick_size,
            "contract_value": contract_value,
            "contract_value_asset": contract_asset,
        },
        "archive_member": selection.archive_member,
        "source_compression": compression,
    }, stream_id
