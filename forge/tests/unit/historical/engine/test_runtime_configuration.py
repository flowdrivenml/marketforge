import hashlib
import tomllib
from pathlib import Path
from typing import Any

import pytest

from forge.historical.engine import runtime
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def test_stage_overrides_and_source_pins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.engine_config_dir
    root.mkdir(parents=True)
    base = b"schema_version = 1\n[resources]\nworkers = 2\nmemory = 256\n"
    for stage in runtime.PIPELINE_STAGES:
        (root / f"{stage}.toml").write_bytes(base)
    pipeline = root / "pipeline.toml"
    pipeline.write_text("schema_version = 1\n[normalize.resources]\nworkers = 1\n")

    def parse(paths: ProjectPaths, operation: str, source: Path, binary: Path) -> dict[str, Any]:
        return tomllib.loads(source.read_text())

    monkeypatch.setattr(runtime, "inspect_runtime", parse)
    destination = tmp_path / "pinned"
    destination.mkdir()
    metadata = runtime.compose_pipeline(
        paths, destination, {name: tmp_path / name for name in runtime.PIPELINE_STAGES}
    )
    assert metadata["effective"]["normalize"]["resources"] == {"workers": 1, "memory": 256}
    assert metadata["effective"]["merge"]["resources"] == {"workers": 2, "memory": 256}
    assert metadata["sources"][str(root / "normalize.toml")] == hashlib.sha256(base).hexdigest()
    pipeline.write_text("schema_version = 1\n[convert.resources]\nworkers = 4\n")
    assert b"workers = 1" in (destination / "normalize.toml").read_bytes()
    with pytest.raises(MarketForgeError, match="invalid stage override convert"):
        runtime.compose_pipeline(paths, destination, {})


def test_legacy_pipeline_uses_retained_runtime_only(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    retained = tmp_path / "retained"
    retained.mkdir()
    legacy = retained / "general.toml"
    legacy.write_text("original")
    assert paths.runtime_config("normalize", retained) == legacy
    assert paths.runtime_config("merge", retained) == legacy


def test_conversion_uses_job_types_for_defaults_and_bounds(tmp_path: Path) -> None:
    from forge.historical.conversion.service import load_runtime

    document = b"""schema_version = 1
[resources]
workers = 2
memory_limit_mb = 512
[parquet]
row_group_target_mb = 16
file_target_mb = 64
"""
    source = tmp_path / "convert.toml"
    settings = load_runtime(source, document)
    assert settings.resources.max_open_files == 64
    assert settings.parquet.compression_level == 3
    with pytest.raises(MarketForgeError, match="row_group_target_mb"):
        load_runtime(source, document.replace(b"= 16", b"= 128"))
    with pytest.raises(MarketForgeError, match="partitioning"):
        load_runtime(source, document + b"\n[partitioning]\npartitions_per_worker = 2\n")
