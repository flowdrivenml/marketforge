"""Locations of durable execution artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Self

from forge.shared.identity import RunId
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True, slots=True)
class RunArtifacts:
    """Paths for one run; constructing these creates no directories."""

    root: Path

    @classmethod
    def for_run(cls, paths: ProjectPaths, run_id: RunId) -> Self:
        """Resolve artifact locations for an allocated or restored identity."""
        return cls(root=paths.runs_dir / str(run_id))

    @property
    def submitted_config(self) -> Path:
        return self.root / "submitted.toml"

    @property
    def resolved_plan(self) -> Path:
        return self.root / "resolved-plan.json"

    @property
    def report(self) -> Path:
        return self.root / "report.json"

    @property
    def events(self) -> Path:
        return self.root / "events.jsonl"

    @property
    def steps_dir(self) -> Path:
        return self.root / "steps"


def retain_bytes(path: Path, data: bytes) -> str:
    """Publish exact evidence without replacing a committed artifact."""
    import hashlib
    import os
    from tempfile import NamedTemporaryFile

    from forge.historical.archives.storage.files import publish
    from forge.shared.errors import MarketForgeError

    digest = hashlib.sha256(data).hexdigest()
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(dir=path.parent, prefix=".retaining-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            publish(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != data:
                raise MarketForgeError(f"Retained evidence differs: {path}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return digest
