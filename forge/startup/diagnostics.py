"""Time-stamped checks for the guarded testing workspace."""

import os
import shutil
import sys
from datetime import UTC, datetime

from forge.historical.engine.managed import inventory
from forge.shared.database.status import inspect_testing_database
from forge.shared.paths import ProjectPaths


def doctor(paths: ProjectPaths) -> dict[str, object]:
    database = inspect_testing_database(project_root=paths.root)
    base = paths.data_root
    while not base.exists():
        base = base.parent
    return {
        "checked_at": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "python_ready": sys.version_info >= (3, 12),
        "testing_database": "verified marketforge_test / testing"
        if database.initialized
        else "uninitialized testing database",
        "applied_migrations": list(database.applied),
        "pending_migrations": list(database.pending),
        "storage_parent": str(base),
        "storage_writable": os.access(base, os.W_OK),
        "free_bytes": shutil.disk_usage(base).free,
        "postgres_tools": {
            tool: shutil.which(tool) is not None for tool in ("pg_dump", "pg_restore")
        },
        "engine_readiness": inventory(paths),
    }
