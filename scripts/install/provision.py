"""Local provisioning and preflight for the installer. Never emit connection details."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg

from forge.shared.database.connection import TEST_DATABASE_NAME, expected_database
from forge.shared.database.credentials import (
    PRODUCTION_DATABASE_KEY,
    TEST_DATABASE_KEY,
    MissingDatabaseCredentials,
    resolve_database_dsn,
)
from forge.shared.database.status import DatabaseStatus, inspect_database
from forge.shared.errors import MarketForgeError


def scram_verifier(password: str) -> str:
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("ascii"), salt, 4096)
    stored = hashlib.sha256(hmac.digest(salted, b"Client Key", "sha256")).digest()
    server = hmac.digest(salted, b"Server Key", "sha256")

    def encode(value: bytes) -> str:
        return base64.b64encode(value).decode("ascii")

    return f"SCRAM-SHA-256$4096:{encode(salt)}${encode(stored)}:{encode(server)}"


def preflight(root: Path, testing: bool, *, ready: bool = False) -> DatabaseStatus:
    dsn = resolve_database_dsn(project_root=root, testing=testing)
    state = inspect_database(project_root=root, testing=testing)
    with psycopg.connect(dsn, connect_timeout=10) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        if connection.info.server_version < 140000:
            raise MarketForgeError("PostgreSQL 14 or later is required.")
        if not state.initialized:
            # An explicitly named fresh database can be bootstrapped. Refuse
            # adoption of existing user schemas, objects, or extensions.
            occupied = connection.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_namespace
                    WHERE nspname NOT IN ('public', 'information_schema') AND nspname !~ '^pg_'
                    UNION ALL
                    SELECT FROM pg_class WHERE relnamespace = to_regnamespace('public')
                    UNION ALL
                    SELECT FROM pg_proc WHERE pronamespace = to_regnamespace('public')
                    UNION ALL
                    SELECT FROM pg_type WHERE typnamespace = to_regnamespace('public')
                    UNION ALL
                    SELECT FROM pg_extension WHERE extname <> 'plpgsql'
                )
                """).fetchone()
            if occupied != (False,):
                raise MarketForgeError(
                    "The selected database has existing objects and no Forge migration history. "
                    "Review it with its administrator or supply a dedicated empty database."
                )
            privileges = connection.execute(
                "SELECT has_database_privilege(current_database(), 'CREATE')"
            ).fetchone()
            if privileges != (True,):
                raise MarketForgeError(
                    "The selected role needs CREATE on the empty database."
                )
        else:
            required = {"forge_meta", "catalog", "historical"}
            if 5 in state.applied:
                required.add("ranking")
            if required.intersection(state.missing_schemas) or not state.applied:
                raise MarketForgeError(
                    "The selected registry is missing applied schema state."
                )
    if ready and (not state.initialized or state.pending or state.missing_schemas):
        raise MarketForgeError(
            "The selected registry is not fully initialized. Review database status."
        )

    return state


def registry_identity(state: DatabaseStatus) -> str:
    assert state.database_id is not None and state.applied
    return f"{state.database_id}:{state.applied[-1]}"


def registry_command(
    action: str, root: Path, testing: bool, arguments: list[str]
) -> int:
    key = TEST_DATABASE_KEY if testing else PRODUCTION_DATABASE_KEY
    # All checks and child commands use the same literal credential even if
    # another process edits the dotenv file during the installation.
    os.environ[key] = resolve_database_dsn(testing=testing, project_root=root)
    flag = "--testing" if testing else "--production"
    command = [arguments[0], "--project-root", str(root)]
    if action == "migrate":
        state = preflight(root, testing)
        result = subprocess.run(
            [*command, "database", "migrate", flag], stdout=sys.stderr, check=False
        )
        if result.returncode:
            return result.returncode
        verified = preflight(root, testing, ready=True)
        if state.initialized and verified.database_id != state.database_id:
            raise MarketForgeError("The database identity changed during migration.")
        print(registry_identity(verified))
        return 0

    if registry_identity(preflight(root, testing, ready=True)) != arguments[1]:
        raise MarketForgeError(
            "The selected database changed since installation verification."
        )
    return subprocess.run(
        [*command, "--json", "init", flag, "--silent"], check=False
    ).returncode


def admin_query(admin: list[str], socket: str, port: str, query: str) -> str:
    # The admin connection uses the explicit local socket. Never allow inherited
    # libpq settings to redirect provisioning to another host or service.
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    env["PGCONNECT_TIMEOUT"] = "5"
    try:
        result = subprocess.run(
            [
                *admin,
                "-X",
                "-w",
                "-qAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                socket,
                "-p",
                port,
                "-d",
                "postgres",
            ],
            input=query,
            text=True,
            capture_output=True,
            env=env,
            timeout=90,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise MarketForgeError(
            "Could not run the local PostgreSQL administrator client."
        ) from None
    if result.returncode:
        # PostgreSQL may include submitted SQL in an error. Keep it private.
        raise MarketForgeError(
            "Local PostgreSQL administration failed. Verify access to the selected socket "
            "and CREATE ROLE / CREATE DATABASE privileges. Existing objects were preserved."
        )
    return result.stdout.strip()


def write_private(path: Path, payload: str, *, replace: bool) -> None:
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".marketforge-")
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if replace:
            temporary.replace(path)
        else:
            os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def verify_owned_local(dsn: str, name: str, role: str, port: str) -> None:
    with psycopg.connect(dsn, connect_timeout=10) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        row = connection.execute("""
            SELECT current_database(), current_user::text, inet_server_port(),
                   pg_get_userbyid(datdba)::text
            FROM pg_database WHERE datname = current_database()
            """).fetchone()
        if row != (name, role, int(port), role):
            raise MarketForgeError(
                "The newly provisioned database failed its ownership check."
            )


def provision(
    root: Path, testing: bool, socket: str, port: str, admin: list[str]
) -> None:
    if not port.isascii() or not port.isdecimal() or not 0 < int(port) < 65536:
        raise MarketForgeError("Local PostgreSQL port is invalid.")
    if not Path(socket).is_absolute():
        raise MarketForgeError(
            "An explicit local PostgreSQL socket directory is required."
        )

    name = TEST_DATABASE_NAME if testing else "marketforge"
    purpose = "testing" if testing else "production"
    key = TEST_DATABASE_KEY if testing else PRODUCTION_DATABASE_KEY
    directory = root / ".secrets"
    if directory.is_symlink():
        raise MarketForgeError(
            "Use a real .secrets directory to provision local credentials."
        )
    directory.mkdir(mode=0o700, exist_ok=True)
    with os.fdopen(
        os.open(
            directory / ".install.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        ),
        "r+",
    ) as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            resolve_database_dsn(project_root=root, testing=testing)
        except MissingDatabaseCredentials:
            pass
        else:
            return

        source = directory / "postgres.env"
        if source.is_symlink() or (source.exists() and not source.is_file()):
            raise MarketForgeError(
                ".secrets/postgres.env must be a regular file for local provisioning."
            )
        original = source.read_bytes() if source.exists() else None
        marker = directory / f".pending-{purpose}.json"

        def query(statement: str) -> str:
            return admin_query(admin, socket, port, statement)

        version = query("SHOW server_version_num;")
        if not version.isdecimal() or int(version) < 140000:
            raise MarketForgeError("PostgreSQL 14 or later is required.")

        # Names and password verifiers below are generated or validated before
        # use in SQL. The plaintext password never goes through psql.
        owner = query(
            f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '{name}';"
        )
        if not marker.exists() and not marker.is_symlink():
            if owner:
                raise MarketForgeError(
                    f"Local database {name} already exists. Supply its verified {key} credentials "
                    "or choose a dedicated registry. No roles or databases were changed."
                )
            record = {
                "role": f"mf_{purpose}_{secrets.token_hex(12)}",
                "password": secrets.token_hex(32),
                "port": port,
            }
            write_private(marker, json.dumps(record), replace=False)
        else:
            mode = marker.lstat()
            if (
                not stat.S_ISREG(mode.st_mode)
                or stat.S_IMODE(mode.st_mode) != 0o600
                or mode.st_uid != os.getuid()
            ):
                raise MarketForgeError(
                    "The private provisioning record has unsafe ownership or permissions."
                )
            record = json.loads(marker.read_text(encoding="utf-8"))
        role, password = record["role"], record["password"]
        if (
            re.fullmatch(f"mf_{purpose}_[a-f0-9]{{24}}", role) is None
            or re.fullmatch("[a-f0-9]{64}", password) is None
            or record["port"] != port
            or (owner and owner != role)
        ):
            raise MarketForgeError(
                "Local provisioning state conflicts with the selected server or database."
            )
        role_state = query(f"""
            SELECT CASE WHEN rolcanlogin AND NOT
                (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
                THEN 'restricted' ELSE 'unexpected' END
            FROM pg_roles WHERE rolname = '{role}';
            """)
        if role_state == "":
            verifier = scram_verifier(password)
            query(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{verifier}';")
        elif role_state != "restricted":
            raise MarketForgeError("The pending local role has unexpected privileges.")

        if not owner:
            query(f'CREATE DATABASE "{name}" OWNER "{role}";')
        dsn = f"postgresql://{role}:{password}@127.0.0.1:{port}/{name}"
        expected_database(dsn, testing=testing)
        verify_owned_local(dsn, name, role, port)
        if (
            source.is_symlink()
            or (source.read_bytes() if source.exists() else None) != original
        ):
            raise MarketForgeError(
                "Credential file changed during provisioning. Review it and rerun."
            )
        previous = original.decode("utf-8") if original is not None else ""
        separator = "\n" if previous and not previous.endswith("\n") else ""
        write_private(
            source,
            f'{previous}{separator}{key}="{dsn}"\n',
            replace=original is not None,
        )
        marker.unlink()
        print(f"  Provisioned {name} and stored its credentials privately.")


def main() -> int:
    action, path, purpose, *arguments = sys.argv[1:]
    root = Path(path)
    testing = purpose == "testing"
    try:
        if action == "provision":
            socket, port, *admin = arguments
            provision(root, testing, socket, port, admin)
        elif action in ("migrate", "initialize"):
            return registry_command(action, root, testing, arguments)
        else:
            preflight(root, testing)
    except MissingDatabaseCredentials:
        return 4
    except MarketForgeError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, psycopg.Error, ValueError, KeyError, TypeError):
        print(
            "PostgreSQL setup failed. Check authentication, access to the configured database, "
            "and permissions for .secrets; retry after resolving the issue.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
