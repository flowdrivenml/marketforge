"""Discovery and conflict-safe publication; Rust owns profile semantics."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import tomllib
from importlib.resources import files
from pathlib import Path
from typing import Any
from uuid import uuid4

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.normalization.service import write_job
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths, require_migrated
from forge.shared.sorting import NAMES, ordered

PATTERN = re.compile(
    r"[a-z][a-z0-9_]*\.(spot|perpetual|future|option|equity)\.(spot|linear|inverse|option|equity)\.(trades|depth)"
)


def catalogue(paths: ProjectPaths) -> Path:
    return paths.engine_config_dir / "normalize/profiles"


def identity(value: str) -> str:
    if not PATTERN.fullmatch(value):
        raise MarketForgeError(
            "Profile ID must be exchange.instrument_type.market_category.trades|depth."
        )
    return value


def require_safe_path(paths: ProjectPaths, path: Path) -> Path:
    """Reject symlinks between the trusted workspace root and a profile."""
    try:
        relative = path.relative_to(paths.root)
    except ValueError:
        raise MarketForgeError(f"Profile path must be inside the workspace: {path}") from None
    current = paths.root
    for part in relative.parts:
        current /= part
        if part == ".." or current.is_symlink():
            raise MarketForgeError(f"Unsafe profile path: {current}")
    return path


def installed(paths: ProjectPaths, profile_id: str) -> Path:
    identity(profile_id)
    relative = Path(profile_id.rsplit(".", 1)[1]) / f"{profile_id}.toml"
    return require_migrated(
        require_safe_path(paths, catalogue(paths) / relative),
        paths.config_dir / "historical/profiles" / relative,
    )


def inspect(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise MarketForgeError(f"Profile must be a regular bounded TOML file: {path}")
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeError) as error:
        raise MarketForgeError(f"Invalid profile TOML: {path}") from error
    profile_id = identity(str(value.get("id", "")))
    if path.name != f"{profile_id}.toml":
        raise MarketForgeError(f"{path}: profile filename must exactly match its declared ID.")
    tokens = profile_id.split(".")
    for field, token in zip(
        ("exchange", "instrument_type", "market_category"), tokens, strict=False
    ):
        if value.get(field) != token:
            raise MarketForgeError(f"{path}: profile {field} contradicts namespaced ID.")
    return {
        "id": profile_id,
        "location": str(path),
        "sha256": checksum(path),
        "validation": "TOML syntax and namespace only",
        "definition": value,
    }


def discover(
    paths: ProjectPaths, *, sort: str | None = None, order: str | None = None, **filters: str | None
) -> list[dict[str, Any]]:
    root = require_safe_path(paths, catalogue(paths))
    legacy = paths.config_dir / "historical/profiles"
    for kind in ("trades", "depth"):
        for path in sorted((legacy / kind).glob("*.toml")):
            if PATTERN.fullmatch(path.stem):
                require_migrated(root / kind / path.name, path)
    return discover_at(root, sort=sort, order=order, **filters)


def discover_at(
    root: Path, *, sort: str | None = None, order: str | None = None, **filters: str | None
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    if root.is_symlink():
        raise MarketForgeError(f"Profile catalogue symlinks are unsupported: {root}")
    for kind in ("trades", "depth"):
        directory = root / kind
        if directory.is_symlink():
            raise MarketForgeError(f"Profile catalogue symlinks are unsupported: {directory}")
        for path in sorted(directory.glob("*.toml")):
            if not PATTERN.fullmatch(path.stem):
                continue
            entry = inspect(path)
            tokens = entry["id"].split(".")
            if tokens[-1] != kind:
                raise MarketForgeError(f"Profile kind contradicts its directory: {path}")
            fields: dict[str, str] = dict(
                zip(("exchange", "instrument_type", "market_category", "type"), tokens, strict=True)
            )
            if all(value is None or fields[key] == value for key, value in filters.items()):
                result.append(entry)

    def identity(entry: dict[str, Any]) -> str:
        return str(entry["id"])

    return ordered(result, NAMES.resolve(sort, order), identity, identity)


def create_draft(paths: ProjectPaths, profile_id: str) -> Path:
    source = installed(paths, profile_id)
    entry = inspect(source)
    if (
        entry["definition"].get("schema_version") != 2
        or entry["definition"].get("kind") != "profile"
    ):
        raise MarketForgeError("Draft creation requires an installed schema-2 profile.")
    data = source.read_bytes()
    if hashlib.sha256(data).hexdigest() != entry["sha256"]:
        raise MarketForgeError("Installed profile changed during draft creation. Retry.")
    return write_draft(paths, profile_id, data)


def new_draft(paths: ProjectPaths, profile_id: str) -> Path:
    exchange, instrument, market, kind = identity(profile_id).split(".")
    if installed(paths, profile_id).exists():
        raise MarketForgeError("Profile is already installed. Use mf normalize profiles edit.")
    template = files("forge.historical.profiles").joinpath(f"template-{kind}.toml")
    body = template.read_text(encoding="utf-8")
    header = (
        "# Starting example: verify every source field, encoding and mapping against your records.\n"
        "# REQUIRED: uncomment format_code and assign your catalogue format code.\n"
        '# format_code = "REPLACE WITH CATALOGUE FORMAT CODE"\n'
        "schema_version = 2\n"
        'kind = "profile"\n'
        f'id = "{profile_id}"\n'
        f'exchange = "{exchange}"\n'
        f'instrument_type = "{instrument}"\n'
        f'market_category = "{market}"\n\n'
    )
    return write_draft(paths, profile_id, (header + body).encode())


def write_draft(paths: ProjectPaths, profile_id: str, data: bytes) -> Path:
    draft = require_safe_path(
        paths, paths.engine_config_dir / "normalize/profile-drafts" / f"{profile_id}.toml"
    )
    draft.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=draft.parent) as frozen:
        frozen.write(data)
        frozen.flush()
        try:
            os.link(frozen.name, draft)
        except FileExistsError:
            raise MarketForgeError(
                f"Draft already exists: {draft}. Open it directly with nvim to continue editing."
            ) from None
    return draft


def validate_catalogue(
    paths: ProjectPaths, historical_root: Path, executable: Path | None = None
) -> dict[str, Any]:
    response = supervised(
        [str(executable or resolve(paths)), "--validate-profiles", str(historical_root)],
        cwd=paths.root,
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


async def register(
    paths: ProjectPaths, profile_id: str, source: Path, *, expected_sha256: str | None = None
) -> dict[str, Any]:
    """Check the selected registry venue before installing a standalone profile."""
    from forge.catalog.service import exchange
    from forge.historical.archives.storage.files import disk
    from forge.shared.database.connection import database_connection, database_dsn

    code = identity(profile_id).split(".", 1)[0]
    async with database_connection(database_dsn(paths.root)) as connection:
        await exchange(connection, code)
    return await disk(_install, paths, profile_id, source, expected_sha256=expected_sha256)


def _install(
    paths: ProjectPaths, profile_id: str, source: Path, *, expected_sha256: str | None = None
) -> dict[str, Any]:
    identity(profile_id)
    entry = inspect(source)
    if expected_sha256 is not None and entry["sha256"] != expected_sha256:
        raise MarketForgeError("Profile draft changed since review.")
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
