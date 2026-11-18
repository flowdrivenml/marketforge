"""Managed build failure/publication contracts, isolated from real installations."""

import os
from pathlib import Path
from typing import Any

import pytest

from forge.cli.main import build_parser
from forge.historical.engine.managed import (
    TARGETS,
    build,
    checksum,
    inventory,
    plan,
    resolve,
    select,
)
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    paths = ProjectPaths(tmp_path / "project", tmp_path / "data")
    (paths.root / "engine").mkdir(parents=True)
    tools = tmp_path / "tools"
    tools.mkdir()
    artifact = tmp_path / "custom-target/release/marketforge-normalize"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("#!/bin/sh\nprintf '%s\\n' 'usage --batch --help'\n")
    artifact.chmod(0o755)
    (tools / "rustc").write_text(
        "#!/bin/sh\nprintf '%s\\n' 'rustc test' 'host: x86_64-unknown-linux-gnu'\n"
    )
    cargo = tools / "cargo"
    cargo.write_text("""#!/usr/bin/env python3
import json,os,sys
if '--version' in sys.argv:
 print('cargo fake');sys.exit(0)
mode=os.environ.get('FAKE_MODE','ok')
if mode=='fail': sys.exit(1)
if mode!='missing':
 print(json.dumps({'reason':'compiler-artifact','package_id':'path+file:///engine#marketforge-engine@0.1.0','target':{'name':'marketforge-normalize','kind':['bin']},'executable':os.environ['FAKE_ARTIFACT']}))
 if mode=='ambiguous':
  print(json.dumps({'reason':'compiler-artifact','package_id':'marketforge-engine 0.1.0','target':{'name':'marketforge-normalize','kind':['bin']},'executable':os.environ['FAKE_ARTIFACT']+'-other'}))
""")
    cargo.chmod(0o755)
    (tools / "rustc").chmod(0o755)
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_ARTIFACT", str(artifact))
    monkeypatch.delenv("CARGO_BUILD_TARGET", raising=False)
    return paths, artifact


def test_plan_is_read_only_and_exact(fake: Any) -> None:
    paths, _ = fake
    proposed = plan(paths, TARGETS["normalize"], offline=True, jobs=4)
    argv = proposed["command"]
    assert "--locked" in argv and "--offline" in argv
    assert argv[argv.index("--features") + 1] == "historical,monitor-tui"
    assert argv[argv.index("--jobs") + 1] == "4"
    assert not {"--bins", "--all-targets", "--workspace", "--all-features"} & set(argv)
    assert not paths.bin_dir.exists()
    assert select(None) == select("normalize")
    assert not paths.bin_dir.exists()


def test_merge_installation_dispatch(fake: Any) -> None:
    paths, _ = fake
    args = build_parser().parse_args(["engine", "build", "merge", "--dry-run"])
    (target,) = select(args.target)
    proposed = plan(paths, target)
    argv = proposed["command"]
    assert argv[argv.index("--bin") + 1] == "marketforge-merge"
    assert proposed["destination"] == str(paths.bin_dir / "marketforge-merge")
    assert target.smoke == ("--help",)


@pytest.mark.parametrize("target", ["live", "diagnostic", "unknown"])
def test_rejects_unapproved_target(target: str) -> None:
    with pytest.raises(MarketForgeError):
        select(target)


@pytest.mark.parametrize("jobs", ["0", "-1"])
def test_cli_jobs_positive(jobs: str) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["engine", "build", "normalize", "--jobs", jobs])


def test_install_is_independent_and_verified(fake: Any) -> None:
    paths, artifact = fake
    paths.bin_dir.mkdir()
    (paths.bin_dir / "mf").write_text("unrelated launcher")
    entry = build(paths, TARGETS["normalize"])
    destination = resolve(paths)
    assert destination.stat().st_ino != artifact.stat().st_ino
    assert destination.stat().st_mode & 0o777 == 0o755
    assert checksum(destination) == entry["sha256"]
    assert inventory(paths)["targets"][0]["status"] == "verified"
    assert (paths.bin_dir / "mf").read_text() == "unrelated launcher"
    destination.write_text("modified")
    assert inventory(paths)["targets"][0]["status"] == "checksum_mismatch"


@pytest.mark.parametrize("mode", ["fail", "missing", "ambiguous"])
def test_failed_build_preserves_old_bytes(
    fake: Any, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    paths, _ = fake
    build(paths, TARGETS["normalize"])
    old = resolve(paths).read_bytes()
    monkeypatch.setenv("FAKE_MODE", mode)
    with pytest.raises(MarketForgeError):
        build(paths, TARGETS["normalize"])
    assert resolve(paths).read_bytes() == old


def test_smoke_failure_and_competing_publisher(fake: Any) -> None:
    paths, artifact = fake
    build(paths, TARGETS["normalize"])
    old = resolve(paths).read_bytes()
    artifact.write_text("#!/bin/sh\nexit 1\n")
    with pytest.raises(MarketForgeError, match="smoke"):
        build(paths, TARGETS["normalize"])
    assert resolve(paths).read_bytes() == old
    lock = acquire_file_lock(paths.root / ".marketforge-engine.lock")
    try:
        with pytest.raises(MarketForgeError, match="Another"):
            build(paths, TARGETS["normalize"])
    finally:
        lock.close()


def test_metadata_recovery_and_status_does_not_execute(fake: Any) -> None:
    paths, _ = fake
    build(paths, TARGETS["normalize"])
    manifest = paths.bin_dir / "manifest.json"
    manifest.write_text("{")
    assert inventory(paths)["targets"][0]["status"] == "malformed_manifest"
    build(paths, TARGETS["normalize"])
    assert inventory(paths)["targets"][0]["status"] == "verified"
    resolve(paths).chmod(0o644)
    assert inventory(paths)["targets"][0]["status"] == "not_executable"
    manifest.unlink()
    assert inventory(paths)["manifest_status"] == "missing_manifest"


def test_foreign_target_and_symlink_destinations(
    fake: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths, artifact = fake
    monkeypatch.setenv("CARGO_BUILD_TARGET", "aarch64-apple-darwin")
    with pytest.raises(MarketForgeError, match="incompatible"):
        build(paths, TARGETS["normalize"])
    monkeypatch.delenv("CARGO_BUILD_TARGET")
    paths.bin_dir.mkdir()
    (paths.bin_dir / "marketforge-normalize").symlink_to(artifact)
    with pytest.raises(MarketForgeError, match="Unsafe"):
        build(paths, TARGETS["normalize"])


def test_bounded_supervision_reaps_only_owned_children(tmp_path: Path) -> None:
    import subprocess
    import sys

    from forge.historical.engine.managed import supervised

    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(10)"], start_new_session=True
    )
    try:
        with pytest.raises(MarketForgeError, match="timed out"):
            supervised(
                [sys.executable, "-c", "import time; time.sleep(10)"], cwd=tmp_path, timeout=0.05
            )
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)
