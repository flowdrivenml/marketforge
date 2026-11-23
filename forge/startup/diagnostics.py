"""Read-only readiness observations for the selected workspace."""

import os
import shutil
import stat
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from shlex import quote
from typing import Literal, cast

from forge.historical.engine.managed import TARGETS, inventory
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.config.models import PresentationSettings
from forge.shared.database.status import DatabaseInspectionError, inspect_database
from forge.shared.paths import PathConfigurationError, ProjectPaths, resolve_project_root
from forge.startup.context import load_workspace_settings

Status = Literal["passed", "warning", "failed", "unknown"]
Readiness = Literal["ready", "ready_with_warnings", "not_ready", "indeterminate"]
Category = Literal["Environment", "Database", "Rust Engines", "Storage", "PostgreSQL Tools"]
CATEGORIES: tuple[Category, ...] = (
    "Environment",
    "Database",
    "Rust Engines",
    "Storage",
    "PostgreSQL Tools",
)
STATUSES: tuple[Status, ...] = ("passed", "warning", "failed", "unknown")
LOW_CAPACITY = 1024**3


@dataclass(frozen=True, slots=True)
class Check:
    id: str
    category: Category
    label: str
    status: Status
    observed: str
    detail: str | None = None
    action: str | None = None
    required: bool = True


@dataclass(frozen=True, slots=True)
class DiagnosticReport:
    checked_at: datetime
    environment: Literal["production", "testing"]
    project_root: Path | None
    presentation: PresentationSettings
    checks: tuple[Check, ...]
    legacy: dict[str, object]

    @property
    def summary(self) -> dict[Status, int]:
        counts = Counter(check.status for check in self.checks)
        return {status: counts[status] for status in STATUSES}

    @property
    def readiness(self) -> Readiness:
        if any(check.required and check.status == "failed" for check in self.checks):
            return "not_ready"
        if any(check.required and check.status == "unknown" for check in self.checks):
            return "indeterminate"
        if any(check.status != "passed" for check in self.checks):
            return "ready_with_warnings"
        return "ready"

    @property
    def exit_code(self) -> int:
        return int(self.readiness in {"not_ready", "indeterminate"})

    def payload(self) -> dict[str, object]:
        return {
            **self.legacy,
            "checked_at": self.checked_at.isoformat(),
            "environment": self.environment,
            "project_root": str(self.project_root) if self.project_root else None,
            "readiness": self.readiness,
            "summary": self.summary,
            "checks": [asdict(check) for check in self.checks],
        }


def database_checks(
    root: Path | None, *, testing: bool, command: str
) -> tuple[list[Check], dict[str, object]]:
    purpose = "testing" if testing else "production"
    inspect = f"Inspect the selected registry.\n{command} database status --{purpose}"
    migrate = (
        f"{inspect}\nReview and apply approved migrations.\n{command} database migrate --{purpose}"
    )
    labels = {
        "connection": "Connection",
        "identity": "Database identity",
        "registry": "Forge registry",
        "schemas": "Required schemas",
        "history": "Migration history",
        "migrations": "Migrations",
    }
    checks = {
        name: Check(f"database.{name}", "Database", label, "unknown", "Awaiting prerequisite")
        for name, label in labels.items()
    }
    legacy: dict[str, object] = {
        "database": "unavailable",
        "applied_migrations": [],
        "pending_migrations": [],
    }

    def record(
        name: str,
        status: Status,
        observed: str,
        detail: str | None = None,
        action: str | None = None,
    ) -> None:
        checks[name] = Check(
            f"database.{name}", "Database", labels[name], status, observed, detail, action
        )

    if root is None:
        record("connection", "unknown", "Workspace unresolved")
        return list(checks.values()), legacy

    failure: DatabaseInspectionError | None = None
    try:
        result = inspect_database(project_root=root, testing=testing)
    except DatabaseInspectionError as error:
        failure = error
        result = error.status

    if failure is None or failure.connected:
        record("connection", "passed", "Connected to PostgreSQL")
    if result is not None:
        legacy.update(
            database=f"verified {result.database} / {result.purpose}"
            if result.initialized
            else "uninitialized database",
            applied_migrations=list(result.applied),
            pending_migrations=list(result.pending),
        )
        if result.initialized:
            record(
                "identity",
                "passed",
                f"{result.database} · {result.purpose}",
                f"Registry ID {result.database_id}",
            )
            record("registry", "passed", "Initialized")
        else:
            record("identity", "unknown", "Database name matched · Forge identity absent")
            clean = failure is None and all(
                schema in result.missing_schemas
                for schema in ("forge_meta", "catalog", "historical", "ranking")
            )
            record(
                "registry",
                "failed",
                "Not initialized",
                "Existing Forge schemas require provisioning review."
                if failure is None and not clean
                else None,
                migrate if clean else inspect,
            )

        if result.initialized and (failure is None or failure.stage == "history"):
            if result.missing_schemas:
                if result.pending or failure is not None:
                    record(
                        "schemas", "unknown", "Missing schemas", ", ".join(result.missing_schemas)
                    )
                else:
                    record(
                        "schemas",
                        "failed",
                        "Missing schemas",
                        ", ".join(result.missing_schemas),
                        inspect,
                    )
            else:
                record("schemas", "passed", "All present")

        if failure is None:
            if result.initialized:
                record("history", "passed", f"{len(result.applied)} applied · history matches")
                if result.pending:
                    record(
                        "migrations",
                        "failed",
                        f"{len(result.pending)} pending",
                        "Versions " + ", ".join(map(str, result.pending)),
                        migrate,
                    )
                else:
                    record("migrations", "passed", "Migrations current")
            else:
                record("history", "unknown", "Registry metadata absent")
                record(
                    "migrations", "unknown", f"{len(result.pending)} available after initialization"
                )

    if failure is not None:
        name = "connection" if failure.stage == "credentials" else failure.stage
        observed = {
            "credentials": "Credentials missing or invalid",
            "connection": "Connection unavailable",
            "identity": "Could not verify database identity",
            "registry": "Could not verify registry metadata",
            "schemas": "Could not inspect required schemas",
            "history": "Could not validate migration history",
        }[failure.stage]
        record(name, "failed", observed, str(failure), inspect)
    return list(checks.values()), legacy


def engine_checks(paths: ProjectPaths | None, command: str) -> tuple[list[Check], object]:
    if paths is None:
        return [
            Check(
                f"engine.{name}",
                "Rust Engines",
                name.capitalize(),
                "unknown",
                "Workspace unresolved",
            )
            for name in TARGETS
        ], None

    result = inventory(paths)
    checks: list[Check] = []
    for entry in result["targets"]:
        name: str = entry["target"]
        state: str = entry["status"]
        installation = entry.get("installation")
        smoke = (
            cast(dict[str, object], installation).get("smoke")
            if isinstance(installation, dict)
            else None
        )
        evidence = cast(dict[str, object], smoke) if isinstance(smoke, dict) else None
        if (
            state == "verified"
            and evidence is not None
            and (
                evidence.get("exit_code") != 0
                or TARGETS[name].smoke_expected not in str(evidence.get("output", ""))
            )
        ):
            state = "smoke_incompatible"
        observed = {
            "verified": "Checksum verified",
            "missing": "Binary missing",
            "checksum_mismatch": "Installed checksum mismatch",
            "smoke_incompatible": "Recorded smoke check incompatible",
        }.get(state, state.replace("_", " ").capitalize())
        checks.append(
            Check(
                f"engine.{name}",
                "Rust Engines",
                name.capitalize(),
                "passed" if state == "verified" else "failed",
                observed,
                entry["path"],
                None
                if state == "verified"
                else f"Repair the managed installation.\n{command} engine build {name}",
            )
        )
    return checks, result


def capacity_label(size: int) -> str:
    if size < 1024**3:
        return f"{size / 1024**2:,.1f} MiB"
    return f"{size / 1024**3:,.1f} GiB"


def storage_checks(root: Path | None) -> tuple[list[Check], dict[str, object]]:
    legacy: dict[str, object] = {
        "storage_parent": None,
        "storage_writable": None,
        "free_bytes": None,
    }
    configure = "Review storage.data_root in the Forge application TOML."
    access = Check("storage.root", "Storage", "Data root", "unknown", "Configuration unavailable")
    capacity = Check(
        "storage.capacity", "Storage", "Free space", "unknown", "Filesystem unavailable"
    )
    if root is None:
        return [access, capacity], legacy

    base = root
    try:
        while True:
            try:
                metadata = base.stat()
                break
            except FileNotFoundError:
                if base == base.parent:
                    raise
                base = base.parent
        legacy["storage_parent"] = str(base)
        if not stat.S_ISDIR(metadata.st_mode):
            access = Check(
                "storage.root",
                "Storage",
                "Data root",
                "failed",
                "Path is not a directory",
                str(base),
                configure,
            )
            return [access, capacity], legacy

        filesystem = os.statvfs(base)
        free = max(0, filesystem.f_bavail * filesystem.f_frsize)
        legacy["free_bytes"] = free
        writable = not bool(filesystem.f_flag & os.ST_RDONLY) and os.access(
            base, os.W_OK | os.X_OK, effective_ids=os.access in os.supports_effective_ids
        )
        if not writable:
            legacy["storage_writable"] = False
            access = Check(
                "storage.root",
                "Storage",
                "Data root",
                "failed",
                "Write or search access unavailable",
                str(base),
                "Review directory permissions and mount state, or change storage.data_root.",
            )
        elif base == root:
            legacy["storage_writable"] = True
            access = Check(
                "storage.root",
                "Storage",
                "Data root",
                "passed",
                "Write and search access indicated",
                str(root),
            )
        else:
            access = Check(
                "storage.root",
                "Storage",
                "Data root",
                "unknown",
                "Directory missing",
                f"{root} · nearest existing parent {base}",
                "Provision the configured data directory or select an existing storage.data_root.",
            )
        status: Status = "failed" if free == 0 else "warning" if free < LOW_CAPACITY else "passed"
        detail = f"Measured on {base}" if base != root else None
        if status != "passed":
            detail = f"{detail + '. ' if detail else ''}Less than 1 GiB available to this process."
        capacity = Check(
            "storage.capacity",
            "Storage",
            "Free space",
            status,
            capacity_label(free),
            detail,
            "Free space or select another data filesystem before processing."
            if status != "passed"
            else None,
        )
    except OSError as error:
        access = Check(
            "storage.root",
            "Storage",
            "Data root",
            "failed",
            "Could not inspect filesystem access",
            f"{root} · {error.strerror or 'filesystem error'}",
            configure,
        )
    return [access, capacity], legacy


def doctor(
    *, project_root: Path | None = None, config_file: Path | None = None, testing: bool = False
) -> DiagnosticReport:
    command = f"mf --project-root {quote(str(project_root))}" if project_root else "mf"
    config_command = f"{command} --config {quote(str(config_file))}" if config_file else command
    checked_at = datetime.now(UTC)
    python = sys.version.split()[0]
    python_ready = sys.version_info >= (3, 12)
    checks = [
        Check(
            "environment.python",
            "Environment",
            "Python",
            "passed" if python_ready else "failed",
            python,
            None if python_ready else "Forge requires Python 3.12 or later.",
            None if python_ready else "Use a supported Python interpreter.",
        )
    ]
    paths: ProjectPaths | None = None
    data_root: Path | None = None
    presentation = PresentationSettings()
    try:
        root = resolve_project_root(project_root)
    except (OSError, PathConfigurationError) as error:
        root = None
        checks.extend(
            (
                Check(
                    "environment.workspace",
                    "Environment",
                    "Workspace",
                    "failed",
                    "Workspace unresolved",
                    str(error),
                    "Select a MarketForge checkout.\nmf --project-root PATH doctor"
                    + (" --testing" if testing else ""),
                ),
                Check(
                    "environment.config",
                    "Environment",
                    "Configuration",
                    "unknown",
                    "Workspace unresolved",
                ),
            )
        )
    else:
        paths = ProjectPaths(root, root / "data")
        checks.append(
            Check(
                "environment.workspace",
                "Environment",
                "Workspace",
                "passed",
                "Workspace found",
                str(root),
            )
        )
        try:
            settings, selected = load_workspace_settings(paths, config_file)
        except (ConfigurationLoadError, OSError, PathConfigurationError) as error:
            checks.append(
                Check(
                    "environment.config",
                    "Environment",
                    "Configuration",
                    "failed",
                    "Invalid or unreadable settings",
                    str(error),
                    f"Correct the application TOML and recheck it.\n{config_command} config show",
                )
            )
        else:
            presentation = settings.presentation
            checks.append(
                Check(
                    "environment.config",
                    "Environment",
                    "Configuration",
                    "passed",
                    "Built-in defaults" if selected is None else "Valid TOML",
                    str(selected) if selected else None,
                )
            )
            try:
                data_root = ProjectPaths.resolve(
                    root=root, data_root=Path(settings.storage.data_root)
                ).data_root
            except (OSError, PathConfigurationError) as error:
                checks.append(
                    Check(
                        "storage.root",
                        "Storage",
                        "Data root",
                        "failed",
                        "Data root could not be resolved",
                        str(error),
                        "Review storage.data_root in the Forge application TOML.",
                    )
                )

    database, database_fields = database_checks(root, testing=testing, command=command)
    engines, engine_fields = engine_checks(paths, command)
    storage, storage_fields = storage_checks(data_root)
    if any(check.id == "storage.root" for check in checks):
        storage = storage[1:]
    tools = {tool: shutil.which(tool) is not None for tool in ("pg_dump", "pg_restore")}
    checks.extend(database + engines + storage)
    for tool, available in tools.items():
        checks.append(
            Check(
                f"postgres_tools.{tool}",
                "PostgreSQL Tools",
                tool,
                "passed" if available else "warning",
                "Available" if available else "Missing",
                action=None
                if available
                else "Install compatible PostgreSQL client tools for registry backup and restore.",
                required=False,
            )
        )
    return DiagnosticReport(
        checked_at,
        "testing" if testing else "production",
        root,
        presentation,
        tuple(checks),
        {
            "python": python,
            "python_ready": python_ready,
            **database_fields,
            **storage_fields,
            "postgres_tools": tools,
            "engine_readiness": engine_fields,
        },
    )
