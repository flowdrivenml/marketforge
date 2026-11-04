"""Forge registry/config backup and guarded restore only to new disposable databases."""

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.sql import SQL, Identifier

from forge.shared.database.testing import testing_connection
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

SCHEMAS = ("forge_meta", "catalog", "historical", "ranking")


def tool_environment(dsn: str, database: str) -> dict[str, str]:
    if database != "marketforge_test":
        validate_destination(database)
    values = conninfo_to_dict(dsn)
    if values.get("dbname") != "marketforge_test":
        raise MarketForgeError("Backup/restore source must explicitly select marketforge_test.")
    environment = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    keys = {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "sslrootcert": "PGSSLROOTCERT",
        "connect_timeout": "PGCONNECT_TIMEOUT",
        "options": "PGOPTIONS",
    }
    for key, name in keys.items():
        if key in values:
            environment[name] = str(values[key])
    environment["PGDATABASE"] = database
    return environment


def invoke(program: str, arguments: list[str], environment: dict[str, str]) -> None:
    executable = shutil.which(program)
    if executable is None:
        raise MarketForgeError(program + " is unavailable; install the PostgreSQL client tools.")
    try:
        result = subprocess.run(
            [executable, *arguments],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise MarketForgeError(
            program + " exceeded its 300-second limit; destination retained for inspection."
        ) from None
    if result.returncode:
        raise MarketForgeError(
            program
            + " failed (exit "
            + str(result.returncode)
            + "); check database permissions and client/server compatibility. Credentials and server diagnostics were suppressed."
        )


def validate_destination(name: str) -> None:
    if not re.fullmatch(r"marketforge_restore_[a-f0-9]{8,32}", name):
        raise MarketForgeError(
            "Restore requires a fresh disposable database named marketforge_restore_<8..32 hex digits>; marketforge_test and production are forbidden."
        )


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


async def reference_audit(dsn: str, paths: ProjectPaths) -> dict[str, object]:
    async with testing_connection(dsn) as c:
        cur = await c.execute(
            "SELECT count(*) FROM historical.local_archives WHERE status='complete'"
        )
        row = await cur.fetchone()
        cur = await c.execute(
            "SELECT storage_root_key,relative_path FROM historical.local_archives WHERE status='complete' ORDER BY id LIMIT 1000"
        )
        selected = await cur.fetchall()
        from forge.historical.archives.storage.acquire import root_key
        from forge.historical.archives.storage.files import contained

        missing: list[str] = []
        foreign = 0
        for storage, relative in selected:
            if storage != root_key(paths.raw_dir):
                foreign += 1
            elif not contained(paths.raw_dir, relative).is_file():
                missing.append(relative)
        cur = await c.execute(
            """SELECT id,storage_path,manifest_path,result_report_path FROM historical.datasets ORDER BY id LIMIT 1000"""
        )
        datasets: list[dict[str, object]] = []
        for identity, storage, manifest, report in await cur.fetchall():
            datasets.append(
                {
                    "dataset_id": identity,
                    "storage_path": storage,
                    "manifest_path": manifest,
                    "report_path": report,
                    "verification": "References retained; dataset bytes are not part of this backup.",
                }
            )
        return {
            "complete_local_count": row[0] if row else 0,
            "sample_limit": 1000,
            "missing_sample_paths": missing,
            "other_storage_roots_in_sample": foreign,
            "datasets": datasets,
            "raw_and_dataset_bytes_included": False,
            "runs_included": False,
            "scope": "Reference existence sample, not full integrity certification.",
        }


def backup(dsn: str, paths: ProjectPaths, destination: Path) -> dict[str, object]:
    audit = asyncio.run(reference_audit(dsn, paths))
    destination = destination.expanduser().resolve()
    if (
        destination.is_relative_to(paths.config_dir.resolve())
        or destination.is_relative_to(paths.raw_dir.resolve())
        or destination.is_relative_to(paths.datasets_dir.resolve())
    ):
        raise MarketForgeError(
            "Backup destination must be outside configuration, raw and dataset roots."
        )
    if destination.exists():
        raise MarketForgeError("Backup destination must not already exist.")
    destination.mkdir(parents=True, mode=0o700)
    archive = destination / "registry.dump"
    invoke(
        "pg_dump",
        [
            "--format=custom",
            "--no-owner",
            "--no-acl",
            *[arg for schema in SCHEMAS for arg in ("--schema", schema)],
            "--file",
            str(archive),
        ],
        tool_environment(dsn, "marketforge_test"),
    )
    archive.chmod(0o600)
    copied: list[dict[str, object]] = []
    total = 0
    if paths.config_dir.exists():
        for index, path in enumerate(paths.config_dir.rglob("*.toml")):
            if index >= 10000:
                raise MarketForgeError("Configuration snapshot exceeds 10000 TOML files.")
            if path.is_symlink() or not path.resolve().is_relative_to(paths.config_dir.resolve()):
                raise MarketForgeError(
                    "Configuration symlinks outside the config root are not backed up."
                )
            total += path.stat().st_size
            if total > 64 * 1024 * 1024:
                raise MarketForgeError("Configuration snapshot exceeds 64 MiB.")
            relative = path.relative_to(paths.config_dir)
            target = destination / "configuration" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            copied.append({"path": str(relative), "sha256": digest(target)})
    manifest = {
        "version": 1,
        "status": "complete",
        "schemas": list(SCHEMAS),
        "registry_sha256": digest(archive),
        "configuration": copied,
        "artifact_references": audit,
        "secrets_included": False,
        "restore": "Only a new marketforge_restore_<hex> database; raw/dataset/run bytes must be retained separately.",
    }
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "backup": str(destination),
        "manifest": str(destination / "manifest.json"),
        "artifact_references": audit,
    }


def restore(dsn: str, backup_root: Path, destination: str) -> dict[str, object]:
    validate_destination(destination)
    manifest_path = backup_root / "manifest.json"
    archive = backup_root / "registry.dump"
    from forge.markets.models import array, obj, string

    manifest = obj(json.loads(manifest_path.read_text()))
    if (
        manifest.get("version") != 1
        or manifest.get("status") != "complete"
        or digest(archive) != manifest.get("registry_sha256")
    ):
        raise MarketForgeError("Backup manifest/archive validation failed.")
    for entry in array(manifest.get("configuration")):
        row = obj(entry)
        relative = Path(string(row.get("path")))
        candidate = backup_root / "configuration" / relative
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or not candidate.resolve().is_relative_to((backup_root / "configuration").resolve())
            or digest(candidate) != row.get("sha256")
        ):
            raise MarketForgeError("Configuration snapshot integrity failed.")

    async def create() -> None:
        async with testing_connection(dsn) as c:
            cur = await c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (destination,))
            if await cur.fetchone() is not None:
                raise MarketForgeError(
                    "Restore destination already exists; refusing any overwrite."
                )
            cur = await c.execute("SELECT rolcreatedb FROM pg_roles WHERE rolname=current_user")
            if await cur.fetchone() != (True,):
                raise MarketForgeError(
                    "Testing role lacks CREATEDB; an administrator must grant disposable testing-database creation. No database was replaced."
                )
            await c.execute(
                SQL("CREATE DATABASE {} TEMPLATE template0").format(Identifier(destination))
            )

    asyncio.run(create())
    invoke(
        "pg_restore",
        [
            "--exit-on-error",
            "--single-transaction",
            "--no-owner",
            "--no-acl",
            "--dbname",
            destination,
            str(archive),
        ],
        tool_environment(dsn, destination),
    )
    # Only this explicitly created disposable destination may bypass the normal
    # marketforge_test name guard, and its restored purpose must still be testing.
    try:
        with psycopg.connect(make_conninfo(dsn, dbname=destination), autocommit=True) as c:
            if c.execute("SELECT current_database()").fetchone() != (destination,) or c.execute(
                "SELECT purpose FROM forge_meta.database_identity WHERE singleton"
            ).fetchone() != ("testing",):
                raise MarketForgeError("Restored disposable database failed its identity checks.")
            c.execute(
                "UPDATE forge_meta.database_identity SET database_id=gen_random_uuid() WHERE singleton"
            )
    except psycopg.Error:
        raise MarketForgeError(
            "Disposable restore identity verification failed; destination retained for inspection."
        ) from None
    return {
        "status": "restored",
        "database": destination,
        "configuration_snapshot": str(backup_root / "configuration"),
        "raw_and_dataset_bytes_restored": False,
        "configuration_activation": "Review/copy the verified snapshot explicitly; active workspace configuration was not overwritten.",
    }
