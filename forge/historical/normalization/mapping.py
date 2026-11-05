"""Conservative recipe-to-manual-input resolution; no format processing interpreter."""

import tarfile
import tomllib
import zipfile
from typing import Literal, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.files import contained
from forge.historical.normalization.config import InputSelection
from forge.historical.normalization.resolution import catalog_entry, require
from forge.markets.models import obj
from forge.shared.paths import ProjectPaths


def profile_for(paths: ProjectPaths, row: dict[str, object], content: str) -> str:
    candidates: list[str] = []
    for source in sorted((paths.config_dir / "historical/profiles").rglob("*.toml")):
        with source.open("rb") as stream:
            profile = tomllib.load(stream)
        if profile.get("unsupported_reason") or any(
            profile.get(key) != row.get(key)
            for key in ("exchange", "market_category", "instrument_type")
        ):
            continue
        rules = profile.get("rules", [])
        targets = {"trade"} if content == "trades" else {"l2_snapshot", "l2_update"}
        if rules and all(obj(rule).get("target") in targets for rule in rules):
            if row.get("format_profile") and row["format_profile"] != profile["id"]:
                continue
            candidates.append(str(profile["id"]))
    require(len(candidates) == 1, f"Expected one compatible profile; found {candidates}.")
    return candidates[0]


def container_member(
    paths: ProjectPaths, row: dict[str, object], profile_id: str
) -> tuple[Literal["plain", "gzip", "zip", "tar_gzip"], str | None]:
    historical = paths.config_dir / "historical"
    profile = catalog_entry(historical / "profiles", profile_id)
    physical = catalog_entry(historical / "raw_schemas", str(profile["physical"]))
    supported = physical.get("supported_containers")
    require(isinstance(supported, list), "Profile lacks supported containers.")
    path = contained(paths.raw_dir, str(row["relative_path"]))
    require(path.is_file(), f"Input file is missing: {path}")
    with path.open("rb") as stream:
        magic = stream.read(4)
    names: list[str] = []
    if zipfile.is_zipfile(path):
        compression = "zip"
        with zipfile.ZipFile(path) as archive:
            names = [i.filename for i in archive.infolist() if not i.is_dir()]
    elif magic.startswith(b"\x1f\x8b"):
        if tarfile.is_tarfile(path):
            compression = "tar_gzip"
            with tarfile.open(path, "r:gz") as archive:
                names = [i.name for i in archive if i.isfile()]
        else:
            compression = "gzip"
    else:
        compression = "plain"
    require(
        compression in cast(list[object], supported),
        f"Container {compression} is incompatible with {profile_id}.",
    )
    if compression in {"zip", "tar_gzip"}:
        require(
            len(names) == 1,
            f"Ambiguous archive member: {names}; explicit manual selection is required.",
        )
    return compression, names[0] if names else None


async def selection_for(
    connection: AsyncConnection[TupleRow], paths: ProjectPaths, row: dict[str, object], content: str
) -> InputSelection:
    require(
        row["scope_kind"] == "instrument",
        "Family/base-coin archives have no registered member-to-instrument mapping; preparation is unsupported.",
    )
    cursor = await connection.execute(
        """SELECT i.id FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
           WHERE e.code=%s AND i.market_category=%s AND i.instrument_type=%s AND i.symbol=%s ORDER BY i.id""",
        (row["exchange"], row["market_category"], row["instrument_type"], row["scope_identifier"]),
    )
    instruments = await cursor.fetchall()
    require(
        len(instruments) == 1,
        f"Expected one concrete catalogue instrument; found {len(instruments)}.",
    )
    profile = profile_for(paths, row, content)
    compression, member = container_member(paths, row, profile)
    return InputSelection(
        task_id=1,
        archive_id=int(str(row["id"])),
        instrument_id=instruments[0][0],
        format_profile=profile,
        archive_member=member,
        source_compression=compression,
    )
