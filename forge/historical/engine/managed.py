"""Approved native engine builds and checksum-verified independent installations."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import shutil
import signal
import stat
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from forge.historical.engine.process import ProcessControl
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True)
class Target:
    name: str
    package: str
    binary: str
    installed_filename: str
    features: tuple[str, ...]
    default_features: bool = False
    enabled: bool = True
    default: bool = True
    smoke: tuple[str, ...] = ("--help",)
    smoke_expected: str = "--batch"


TARGETS = {
    "normalize": Target(
        "normalize",
        "marketforge-engine",
        "marketforge-normalize",
        "marketforge-normalize",
        ("historical", "monitor-tui"),
    ),
    "merge": Target(
        "merge",
        "marketforge-engine",
        "marketforge-merge",
        "marketforge-merge",
        ("historical", "monitor-tui"),
        default=False,
        smoke_expected="--validate-job",
    ),
    "convert": Target(
        "convert",
        "marketforge-engine",
        "marketforge-convert",
        "marketforge-convert",
        ("historical", "monitor-tui"),
        default=False,
        smoke_expected="--job",
    ),
}


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def supervised(
    argv: list[str], *, cwd: Path, timeout: float | None = 30
) -> subprocess.CompletedProcess[str]:
    """Bounded noninteractive checks; reap only our own process group."""
    with tempfile.TemporaryFile(mode="w+") as output:
        process = subprocess.Popen(
            argv, cwd=cwd, stdout=output, stderr=output, text=True, start_new_session=True
        )
        try:
            code = process.wait(timeout=timeout)
        except BaseException as error:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            if isinstance(error, subprocess.TimeoutExpired):
                raise MarketForgeError("Bounded engine/toolchain check timed out.") from None
            raise
        output.seek(0, os.SEEK_END)
        size = output.tell()
        output.seek(max(0, size - 65536))
        return subprocess.CompletedProcess(argv, code, output.read())


def select(name: str | None) -> list[Target]:
    if name is None:
        return [target for target in TARGETS.values() if target.enabled and target.default]
    if name not in TARGETS or not TARGETS[name].enabled:
        raise MarketForgeError(f"Unknown or disabled engine target: {name}")
    return [TARGETS[name]]


def plan(
    paths: ProjectPaths, target: Target, *, offline: bool = False, jobs: int | None = None
) -> dict[str, Any]:
    if jobs is not None and jobs <= 0:
        raise MarketForgeError("Compilation jobs must be positive.")
    if platform.system() not in {"Linux", "Darwin"}:
        raise MarketForgeError("Managed native builds support Linux and macOS.")
    argv = [
        "cargo",
        "build",
        "--manifest-path",
        str(paths.root / "engine/Cargo.toml"),
        "--package",
        target.package,
        "--bin",
        target.binary,
        "--release",
        "--locked",
        "--message-format=json-render-diagnostics",
    ]
    if not target.default_features:
        argv.append("--no-default-features")
    if target.features:
        argv.extend(["--features", ",".join(target.features)])
    if offline:
        argv.append("--offline")
    if jobs is not None:
        argv.extend(["--jobs", str(jobs)])
    return {
        "target": asdict(target),
        "command": argv,
        "cwd": str(paths.root / "engine"),
        "destination": str(paths.bin_dir / target.installed_filename),
        "missing_prerequisites": [
            name for name in ("cargo", "rustc") if shutil.which(name) is None
        ],
    }


def _read_manifest(path: Path) -> dict[str, Any]:
    value: object = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("Installation manifest must be an object.")
    return cast(dict[str, Any], value)


def inventory(paths: ProjectPaths) -> dict[str, Any]:
    manifest = paths.bin_dir / "manifest.json"
    document: dict[str, Any]
    try:
        if (
            paths.bin_dir.is_symlink()
            or manifest.is_symlink()
            or (manifest.exists() and manifest.stat().st_size > 1024 * 1024)
        ):
            raise ValueError("symlink")
        document = _read_manifest(manifest)
        if document.get("schema_version") != 1 or not isinstance(document.get("targets"), dict):
            raise ValueError("schema")
    except FileNotFoundError:
        document = {"error": "missing_manifest", "targets": {}}
    except (ValueError, OSError):
        document = {"error": "malformed_manifest", "targets": {}}
    results: list[dict[str, Any]] = []
    for target in TARGETS.values():
        path = paths.bin_dir / target.installed_filename
        entry = document["targets"].get(target.name)
        state = "missing"
        if path.is_symlink():
            state = "unsafe_symlink"
        elif path.exists():
            if not path.is_file():
                state = "not_regular"
            elif not os.access(path, os.X_OK):
                state = "not_executable"
            elif not isinstance(entry, dict) or not isinstance(
                entry["sha256"] if "sha256" in entry else None, str
            ):
                state = document.get("error", "missing_metadata")
            else:
                state = "verified" if checksum(path) == entry["sha256"] else "checksum_mismatch"
        results.append(
            {"target": target.name, "path": str(path), "status": state, "installation": entry}
        )
    return {
        "schema_version": 1,
        "targets": results,
        "manifest_status": document.get("error", "valid"),
    }


def resolve(paths: ProjectPaths, target: str = "normalize") -> Path:
    result = next(item for item in inventory(paths)["targets"] if item["target"] == target)
    if result["status"] != "verified":
        raise MarketForgeError(
            f"Managed engine {target}: {result['status']}; run mf engine build {target}."
        )
    return Path(result["path"])


def build(
    paths: ProjectPaths,
    target: Target,
    *,
    offline: bool = False,
    jobs: int | None = None,
    control: ProcessControl | None = None,
) -> dict[str, Any]:
    from datetime import UTC, datetime

    proposed = plan(paths, target, offline=offline, jobs=jobs)
    if proposed["missing_prerequisites"]:
        raise MarketForgeError(f"Missing build prerequisites: {proposed['missing_prerequisites']}")
    if paths.bin_dir.is_symlink() or (paths.bin_dir.exists() and not paths.bin_dir.is_dir()):
        raise MarketForgeError("Unsafe .bin installation root.")
    lock = acquire_file_lock(paths.root / ".marketforge-engine.lock")
    staged: Path | None = None
    try:
        host_check = supervised(["rustc", "-vV"], cwd=paths.root / "engine")
        if host_check.returncode:
            raise MarketForgeError("Cannot determine Rust host.")
        host = next(
            line.split(": ", 1)[1]
            for line in host_check.stdout.splitlines()
            if line.startswith("host: ")
        )
        # Cargo JSON artifacts expose the effective target. Explicit --target pins native
        # compilation even when environment or Cargo config requests a foreign target.
        configured = os.environ.get("CARGO_BUILD_TARGET")
        for directory in (paths.root / "engine", paths.root, *paths.root.parents, Path.home()):
            for name in ("config", "config.toml"):
                config = directory / ".cargo" / name
                if config.is_file():
                    import tomllib

                    value = tomllib.loads(config.read_text()).get("build", {}).get("target")
                    if value is not None:
                        configured = configured or value
        if configured is not None and configured != host:
            raise MarketForgeError(
                f"Configured Cargo target {configured!r} is incompatible with native host {host}."
            )
        artifacts: set[Path] = set()
        errors: list[str] = []
        process = subprocess.Popen(
            proposed["command"],
            cwd=proposed["cwd"],
            stdout=subprocess.PIPE,
            stderr=None if control is None else subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )
        with control.track(process) if control is not None else contextlib.nullcontext():
            try:
                assert process.stdout is not None
                while True:
                    line = process.stdout.readline(16 * 1024 * 1024)
                    if not line:
                        break
                    if not line.endswith("\n") and len(line) >= 16 * 1024 * 1024:
                        raise MarketForgeError(
                            "Cargo JSON message exceeds the bounded message limit."
                        )
                    try:
                        message = json.loads(line)
                    except ValueError:
                        continue
                    if (
                        message.get("reason") == "compiler-artifact"
                        and message.get("target", {}).get("name") == target.binary
                        and message.get("target", {}).get("kind") == ["bin"]
                        and message.get("executable")
                    ):
                        package = message.get("package_id", "")
                        if f"{target.package}@" in package or package.startswith(
                            target.package + " "
                        ):
                            artifacts.add(Path(message["executable"]))
                    if message.get("reason") == "compiler-message":
                        rendered = message.get("message", {}).get("rendered", "")
                        errors.append(rendered[-4000:])
                        errors = errors[-8:]
                code = process.wait()
            except BaseException:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            finally:
                if process.stdout:
                    process.stdout.close()
        if code:
            raise MarketForgeError(
                "Cargo build failed; previous installation retained.\n" + "".join(errors)
            )
        if len(artifacts) != 1:
            raise MarketForgeError("Cargo did not report one unambiguous approved executable.")
        artifact = artifacts.pop()
        if artifact.is_symlink() or not artifact.is_file() or artifact.stat().st_size == 0:
            raise MarketForgeError("Cargo executable is not a nonempty regular file.")
        paths.bin_dir.mkdir(exist_ok=True)
        destination = paths.bin_dir / target.installed_filename
        metadata = paths.bin_dir / "manifest.json"
        if (
            destination.is_symlink()
            or metadata.is_symlink()
            or (destination.exists() and not destination.is_file())
        ):
            raise MarketForgeError("Unsafe installation destination.")
        descriptor, filename = tempfile.mkstemp(prefix=f".{target.binary}-", dir=paths.bin_dir)
        staged = Path(filename)
        with os.fdopen(descriptor, "wb") as output, artifact.open("rb") as source:
            shutil.copyfileobj(source, output, 1024 * 1024)
            os.fchmod(output.fileno(), 0o755)
            output.flush()
            os.fsync(output.fileno())
        digest = checksum(staged)
        if digest != checksum(artifact) or not stat.S_ISREG(staged.stat().st_mode):
            raise MarketForgeError("Staged executable copy verification failed.")
        smoke = supervised([str(staged), *target.smoke], cwd=paths.root)
        if smoke.returncode or target.smoke_expected not in smoke.stdout:
            raise MarketForgeError("Non-processing engine smoke check failed.")
        git = supervised(["git", "rev-parse", "HEAD"], cwd=paths.root)
        dirty = supervised(
            ["git", "diff", "--quiet", "HEAD", "--", "forge", "engine"], cwd=paths.root
        )
        cargo = supervised(["cargo", "--version"], cwd=paths.root / "engine")
        entry = {
            **asdict(target),
            "release_profile": "release",
            "host": host,
            "effective_target": host,
            "rustc": host_check.stdout.strip(),
            "cargo": cargo.stdout.strip(),
            "installed_at": datetime.now(UTC).isoformat(),
            "size": staged.stat().st_size,
            "sha256": digest,
            "git_revision": git.stdout.strip() if git.returncode == 0 else None,
            "dirty": dirty.returncode != 0,
            "source_provenance": "revision and tracked dirty indication only; not an exact source snapshot",
            "smoke": {
                "arguments": target.smoke,
                "exit_code": smoke.returncode,
                "output": smoke.stdout,
            },
        }
        document: dict[str, Any]
        try:
            document = _read_manifest(metadata)
            if document.get("schema_version") != 1 or not isinstance(document.get("targets"), dict):
                document = {"schema_version": 1, "targets": {}}
        except (FileNotFoundError, ValueError):
            document = {"schema_version": 1, "targets": {}}
        document["targets"][target.name] = entry
        # Two atomic replacements: interrupted metadata publication is detectable by status.
        os.replace(staged, destination)
        staged = None
        fd, filename = tempfile.mkstemp(prefix=".manifest-", dir=paths.bin_dir)
        try:
            with os.fdopen(fd, "w") as output:
                json.dump(document, output, indent=2)
                output.flush()
                os.fsync(output.fileno())
            os.replace(filename, metadata)
        finally:
            Path(filename).unlink(missing_ok=True)
        return entry
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
        lock.close()
