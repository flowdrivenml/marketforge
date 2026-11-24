"""Resolve effective operation settings through the managed Rust validators."""

import hashlib
import json
import tomllib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

import tomlkit

from forge.historical.engine.managed import resolve, supervised
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

PIPELINE_STAGES = ("normalize", "merge")


def inspect_runtime(
    paths: ProjectPaths, operation: str, source: Path, binary: Path | None = None
) -> dict[str, Any]:
    engine = binary or resolve(paths, operation)
    response = supervised([str(engine), "--runtime-settings", str(source)], cwd=paths.root)
    if response.returncode:
        raise MarketForgeError(f"{operation} runtime {source}: {response.stdout.strip()}")
    document: dict[str, Any] = json.loads(response.stdout)
    return document


def read_document(path: Path, data: bytes) -> dict[str, Any]:
    try:
        return tomllib.loads(data.decode())
    except (tomllib.TOMLDecodeError, UnicodeError) as error:
        raise MarketForgeError(f"Invalid runtime TOML: {path}") from error


def overlay(base: dict[str, Any], overrides: dict[str, Any]) -> None:
    for key, value in overrides.items():
        inherited = base.get(key)
        if isinstance(inherited, dict) and isinstance(value, dict):
            overlay(cast(dict[str, Any], inherited), cast(dict[str, Any], value))
        else:
            base[key] = value


def compose_pipeline(
    paths: ProjectPaths, destination: Path, binaries: dict[str, Path], *, source: Path | None = None
) -> dict[str, Any]:
    """Freeze source fingerprints and materialize validated stage inputs."""
    pipeline = source or paths.runtime_config("pipeline")
    data = pipeline.read_bytes()
    overrides = read_document(pipeline, data)
    version = overrides.pop("schema_version", None)
    if type(version) is not int or version != 1:
        raise MarketForgeError(f"{pipeline}: schema_version must be 1")
    for key, value in overrides.items():
        if key not in PIPELINE_STAGES or not isinstance(value, dict) or "schema_version" in value:
            raise MarketForgeError(f"{pipeline}: invalid stage override {key}")

    sources = {str(pipeline): hashlib.sha256(data).hexdigest()}
    effective: dict[str, Any] = {}
    for stage in PIPELINE_STAGES:
        source = paths.runtime_config(stage)
        base = source.read_bytes()
        sources[str(source)] = hashlib.sha256(base).hexdigest()
        target = destination / f"{stage}.toml"
        target.write_bytes(base)
        # Validate the source independently, including fields a pipeline could mask.
        try:
            settings = inspect_runtime(paths, stage, target, binaries[stage])
        except MarketForgeError as error:
            raise MarketForgeError(f"{source}: {error}") from error
        if overrides.get(stage):
            document = read_document(source, base)
            overlay(document, overrides[stage])
            target.write_text(tomlkit.dumps(document), encoding="utf-8")
            try:
                settings = inspect_runtime(paths, stage, target, binaries[stage])
            except MarketForgeError as error:
                raise MarketForgeError(f"{pipeline} [{stage}]: {error}") from error
        effective[stage] = settings
    return {"sources": sources, "effective": effective}


def runtime_report(paths: ProjectPaths, operation: str) -> dict[str, Any]:
    if operation == "pipeline":
        with TemporaryDirectory(prefix="marketforge-pipeline-runtime-") as temporary:
            return compose_pipeline(
                paths, Path(temporary), {stage: resolve(paths, stage) for stage in PIPELINE_STAGES}
            )
    if operation not in {"normalize", "merge", "convert"}:
        raise MarketForgeError(f"Unknown Engine runtime operation: {operation}")
    source = paths.runtime_config(operation)
    return {"source": str(source), "effective": inspect_runtime(paths, operation, source)}
