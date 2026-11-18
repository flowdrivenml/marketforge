"""Discovery and conflict-safe publication; Rust owns profile semantics."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import tomllib
from pathlib import Path
from typing import Any
from uuid import uuid4

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.normalization.service import write_job
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths

PATTERN = re.compile(
    r"[a-z][a-z0-9_]*\.(spot|perpetual|future|option|equity)\.(spot|linear|inverse|option|equity)\.(trades|depth)"
)


def catalogue(paths: ProjectPaths) -> Path:
    return paths.config_dir / "historical" / "profiles"


def identity(value: str) -> str:
    if not PATTERN.fullmatch(value):
        raise MarketForgeError(
            "Profile ID must be exchange.instrument_type.market_category.trades|depth."
        )
    return value


def installed(paths: ProjectPaths, profile_id: str) -> Path:
    identity(profile_id)
    return catalogue(paths) / profile_id.rsplit(".", 1)[1] / f"{profile_id}.toml"


def inspect(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise MarketForgeError(f"Profile must be a regular bounded TOML file: {path}")
    value = tomllib.loads(path.read_text())
    profile_id = identity(str(value.get("id", "")))
    if path.name != f"{profile_id}.toml":
        raise MarketForgeError("Profile filename must exactly match its declared ID.")
    tokens = profile_id.split(".")
    for field, token in zip(
        ("exchange", "instrument_type", "market_category"), tokens, strict=False
    ):
        if value.get(field) != token:
            raise MarketForgeError(f"Profile {field} contradicts namespaced ID.")
    return {
        "id": profile_id,
        "location": str(path),
        "sha256": checksum(path),
        "validation": "TOML syntax and namespace only",
        "definition": value,
    }


def discover(paths: ProjectPaths, **filters: str | None) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for kind in ("trades", "depth"):
        for path in sorted((catalogue(paths) / kind).glob("*.toml")):
            entry = inspect(path)
            tokens = entry["id"].split(".")
            fields: dict[str, str] = dict(
                zip(("exchange", "instrument_type", "market_category", "type"), tokens, strict=True)
            )
            if all(value is None or fields[key] == value for key, value in filters.items()):
                result.append(entry)
    return result


def validate_catalogue(paths: ProjectPaths, historical_root: Path) -> dict[str, Any]:
    response = supervised(
        [str(resolve(paths)), "--validate-profiles", str(historical_root)], cwd=paths.root
    )
    if response.returncode:
        raise MarketForgeError(response.stdout)
    return json.loads(response.stdout)


def validate(paths: ProjectPaths, path: Path) -> dict[str, Any]:
    entry = inspect(path)
    # Stage outside active discovery, preserving all installed definitions to test
    # catalogue ambiguity as well as compilation of the candidate itself.
    with tempfile.TemporaryDirectory(prefix="marketforge-profiles-") as temporary:
        root = Path(temporary)
        for item in discover(paths):
            source = Path(item["location"])
            destination = root / "profiles" / item["id"].rsplit(".", 1)[1] / source.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        destination = root / "profiles" / entry["id"].rsplit(".", 1)[1] / path.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
        if checksum(destination) != entry["sha256"]:
            raise MarketForgeError("Profile changed while staging Rust validation.")
        compiled = validate_catalogue(paths, root)
        engine = next(item for item in compiled["profiles"] if item["id"] == entry["id"])
        if engine.get("unsupported_reason"):
            raise MarketForgeError(f"Inventory-only profile: {engine['unsupported_reason']}")
        return {
            **entry,
            "validation": "production Rust semantic compilation and candidate catalogue compatibility",
            "engine": engine,
        }


async def register(paths: ProjectPaths, profile_id: str, source: Path) -> dict[str, Any]:
    """Check the testing venue before installing a standalone profile."""
    from forge.catalog.service import exchange
    from forge.historical.archives.storage.files import disk
    from forge.shared.database.testing import testing_connection, testing_dsn

    code = identity(profile_id).split(".", 1)[0]
    async with testing_connection(testing_dsn(paths.root)) as connection:
        await exchange(connection, code)
    return await disk(_install, paths, profile_id, source)


def _install(paths: ProjectPaths, profile_id: str, source: Path) -> dict[str, Any]:
    identity(profile_id)
    entry = inspect(source)
    if entry["id"] != profile_id:
        raise MarketForgeError("Requested profile ID differs from submitted definition.")
    lock = acquire_file_lock(paths.root / ".marketforge-profiles.lock")
    try:
        destination = installed(paths, profile_id)
        if destination.is_symlink():
            raise MarketForgeError("Unsafe profile destination.")
        if destination.exists():
            current = validate(paths, destination)
            candidate = validate(paths, source)
            if current["engine"]["fingerprint"] != candidate["engine"]["fingerprint"]:
                raise MarketForgeError(
                    "Different interpretation already installed under this profile ID."
                )
            return {**current, "publication": "reused", "submitted_sha256": entry["sha256"]}
        # Validate frozen bytes, then install those same bytes. Never change source.
        with tempfile.TemporaryDirectory(prefix="marketforge-profile-") as temporary:
            frozen = Path(temporary) / source.name
            before = source.stat()
            shutil.copyfile(source, frozen)
            after = source.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_ino) != (
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_ino,
            ) or checksum(frozen) != entry["sha256"]:
                raise MarketForgeError("Submitted profile changed during publication preparation.")
            candidate = validate(paths, frozen)
            operation = paths.runs_dir / f"profile-install-{uuid4().hex}"
            operation.mkdir(parents=True, exist_ok=False)
            shutil.copyfile(frozen, operation / source.name)
            evidence: dict[str, Any] = {
                "schema_version": 1,
                "profile_id": profile_id,
                "submitted_location": str(source.resolve()),
                "submitted_sha256": entry["sha256"],
                "installed_location": str(destination),
                "engine_interpretation": candidate["engine"],
            }
            write_job(operation / "intent.json", evidence)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if any(parent.is_symlink() for parent in (destination.parent, catalogue(paths))):
                raise MarketForgeError("Unsafe catalogue symlink.")
            fd, filename = tempfile.mkstemp(prefix=".profile-", dir=destination.parent)
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(frozen.read_bytes())
                    output.flush()
                    os.fsync(output.fileno())
                # Publication lock serializes Forge; hard-link publication is exclusive
                # and the temporary file is removed immediately. Source is independent.
                os.link(filename, destination)
            finally:
                Path(filename).unlink(missing_ok=True)
            write_job(
                operation / "installation.json",
                {**evidence, "installed_sha256": checksum(destination), "publication": "installed"},
            )
            return {
                **candidate,
                "location": str(destination),
                "publication": "installed",
                "submitted_location": str(source),
                "submitted_sha256": entry["sha256"],
            }
    finally:
        lock.close()
