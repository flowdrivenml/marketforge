"""Runtime CLI contracts against a real, disposable managed Engine/workspace."""

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest

from forge.cli.commands import normalization_runtime as cli
from forge.cli.main import main
from forge.historical.engine.runtime_config import RuntimeConfiguration, typed_value
from forge.shared.documentation import NORMALIZATION_RUNTIME
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

ROOT = Path(__file__).resolve().parents[4]
RUNTIME = ROOT / "engine/src/shared/historical/config/fixtures/runtime.toml"


@pytest.fixture(autouse=True)
def readable_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    # The relocated canonical link is wider than an 80-column terminal.
    monkeypatch.setenv("COLUMNS", "100")


@pytest.fixture(scope="module")
def runtime_engine(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    path = Path(
        os.environ.get("MF_RUNTIME_TEST_ENGINE", ROOT / "engine/target/debug/marketforge-normalize")
    )
    if not path.is_file():
        pytest.skip("Supply MF_RUNTIME_TEST_ENGINE pointing at a built normalization Engine.")
    probe = subprocess.run([str(path), "--runtime-schema"], capture_output=True, check=False)
    if probe.returncode:
        pytest.skip("Normalization Engine lacks --runtime-schema.")
    target = tmp_path_factory.mktemp("runtime-engine") / "marketforge-normalize"
    if shutil.which("strip"):
        stripped = subprocess.run(
            ["strip", "-o", str(target), str(path)], capture_output=True, check=False
        )
        if not stripped.returncode:
            target.chmod(0o755)
    if not target.exists():
        shutil.copy2(path, target)
    with target.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return target, digest


@pytest.fixture
def runtime(workspace: Path, runtime_engine: tuple[Path, str]) -> Path:
    binary, digest = runtime_engine
    root = workspace / ".bin"
    root.mkdir()
    target = root / binary.name
    try:
        os.link(binary, target)
    except OSError:
        shutil.copy2(binary, target)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "targets": {"normalize": {"sha256": digest}},
            }
        )
    )
    path = workspace / "data/.config/engine/normalize.toml"
    path.parent.mkdir(parents=True)
    text = RUNTIME.read_text().replace("workers = 1", "workers  =  1  # Keep spacing")
    text = text.replace('action = "fail"', "action = 'fail'  # Keep quotes", 1)
    path.write_text("# Runtime test fixture\n" + text)
    return path


def invoke(workspace: Path, *arguments: str, machine: bool = False) -> int:
    global_args = ["--project-root", str(workspace)]
    if machine:
        global_args.append("--json")
    return main([*global_args, "normalize", "config", *arguments])


def report(capture: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capture.readouterr()
    assert captured.err == ""
    assert "\x1b" not in captured.out and NORMALIZATION_RUNTIME not in captured.out
    return json.loads(captured.out)


def test_help_hierarchy_and_documentation(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_code:
        invoke(workspace, "--help")
    assert exit_code.value.code == 0
    output = capsys.readouterr().out
    assert all(
        name in output for name in ("show", "get", "schema", "validate", "set", "unset", "edit")
    )
    assert output.count(NORMALIZATION_RUNTIME) == 1


@pytest.mark.parametrize(
    "action",
    [
        ("show",),
        ("show", "--all"),
        ("show", "--section", "resources"),
        ("schema",),
        ("schema", "--field", "resources.workers"),
        ("get", "resources.workers"),
        ("validate",),
    ],
)
def test_human_inspection(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str], action: tuple[str, ...]
) -> None:
    assert invoke(workspace, *action) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count(NORMALIZATION_RUNTIME) == 1
    assert str(runtime.relative_to(workspace)) in captured.out
    assert "VALID" in captured.out and "\x1b" not in captured.out


def test_effective_defaults_types_and_inactive_preferences(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with runtime.open("a") as output:
        output.write(
            '\n[reconciliation.depth]\noverlap_policy = "snapshot_recovery"\n'
            '[reconciliation.depth.prefer_source]\nselection = "later"\n'
        )
    assert invoke(workspace, "show", "--section", "reconciliation", machine=True) == 0
    fields = {entry["path"]: entry for entry in report(capsys)["fields"]}
    selection = fields["reconciliation.depth.prefer_source.selection"]
    assert (selection["configured"], selection["effective"], selection["default"]) == (
        "later",
        "later",
        "earlier",
    )
    assert selection["active"] is False and selection["inactive_reasons"]
    assert invoke(workspace, "show", "--section", "reconciliation.depth") == 0
    overview = capsys.readouterr().out
    assert "Inactive" in overview and "overlap_policy must be prefer_source" in overview
    assert invoke(workspace, "get", "resources.memory_budget_bytes", machine=True) == 0
    budget = report(capsys)["fields"][0]
    assert budget["effective"] == 1 << 30 and budget["required"]
    assert (
        invoke(
            workspace, "get", "resources.normalization_limits.reconciliation_bytes", machine=True
        )
        == 0
    )
    limit = report(capsys)["fields"][0]
    assert limit["configured_path"] is None and limit["effective"] == limit["default"] == 64 << 20


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("show", "--section", "ordering"), "Unsupported normalization runtime section"),
        (("get", "resources"), "Unsupported normalization runtime field"),
        (
            ("set", "ordering.primary", "event_timestamp_ns"),
            "Unsupported normalization runtime field",
        ),
        (("set", "schema_version", "2"), "immutable"),
        (("unset", "schema_version"), "immutable"),
        (("unset", "resources.workers"), "required field"),
        (("set", "diagnostics.mode", "Detailed"), "choose"),
        (("set", "resources.memory_budget_bytes", "48GB"), "GiB"),
        (("set", "resources.memory_budget_bytes", "0.1KiB"), "exact number of bytes"),
        (("set", "resources.memory_budget_bytes", "18446744073709551616"), "Engine integer range"),
        (("set", "integrity_policy.enabled", "yes"), "true or false"),
        (("set", "integrity_policy.parse_failure.max_rate", "NaN"), "finite decimal"),
        (("set", "resources.workers", "0", "--apply"), "greater than zero"),
        (("set", "resources.parquet.row_group_target_bytes", "1GiB", "--apply"), "cannot exceed"),
        (("set", "diagnostics.interval_ms", "99", "--apply"), "between 100 and 60000"),
        (
            (
                "set",
                "timestamp_regression.trades.policy",
                "reorder",
                "--apply",
                "--acknowledge-risk",
            ),
            "trade reorder requires",
        ),
        (
            (
                "set",
                "timestamp_regression.depth.require_recovery_snapshot",
                "false",
                "--apply",
                "--acknowledge-risk",
            ),
            "cannot be disabled",
        ),
        (
            ("set", "reconciliation.depth.prefer_source.on_unique_data_loss", "mark_degraded"),
            "positive max_excluded_events",
        ),
    ],
)
def test_invalid_changes_leave_exact_bytes(
    workspace: Path,
    runtime: Path,
    capsys: pytest.CaptureFixture[str],
    args: tuple[str, ...],
    expected: str,
) -> None:
    before = runtime.read_bytes()
    assert invoke(workspace, *args, machine=True) == 1
    assert expected in report(capsys)["error"]["message"]
    assert runtime.read_bytes() == before


@pytest.mark.parametrize(
    ("replacement", "expected"),
    [
        ("schema_version = 2", "unsupported version 2"),
        ("ordering = {}", "unknown field"),
        ("schema_version = {", "expected key"),
    ],
)
def test_validate_uses_rust_errors(
    workspace: Path,
    runtime: Path,
    capsys: pytest.CaptureFixture[str],
    replacement: str,
    expected: str,
) -> None:
    runtime.write_text(runtime.read_text().replace("schema_version = 1", replacement))
    assert invoke(workspace, "validate") == 1
    text = capsys.readouterr().out
    assert expected in text and NORMALIZATION_RUNTIME in text


def test_preview_apply_is_round_trip_and_leaves_retained_runtime_untouched(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    prior = runtime.read_bytes()
    runtime.chmod(0o640)
    original_inode = runtime.stat().st_ino
    history = workspace / "data/runs/fixture"
    history.mkdir(parents=True)
    retained = history / "normalize.toml"
    retained.write_bytes(prior)
    (history / "execution-intent.json").write_text('{"immutable":"fixture"}')
    assert invoke(workspace, "set", "resources.workers", "12", machine=True) == 0
    preview = report(capsys)
    assert preview["status"] == "ready_to_apply"
    assert preview["validation"]["rust"]
    assert preview["changes"][0]["after"]["configured"] == 12
    assert runtime.read_bytes() == prior
    assert invoke(workspace, "set", "resources.workers", "12", "--apply", machine=True) == 0
    applied = report(capsys)
    assert applied["candidate_sha256"] == preview["candidate_sha256"]
    assert applied["status"] == "applied" and runtime.stat().st_ino != original_inode
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o640
    assert runtime.read_bytes() == prior.replace(b"workers  =  1", b"workers  =  12")
    assert retained.read_bytes() == prior
    assert (history / "execution-intent.json").read_text() == '{"immutable":"fixture"}'
    assert (
        invoke(workspace, "set", "resources.memory_budget_bytes", "1.5GiB", "--apply", machine=True)
        == 0
    )
    assert report(capsys)["changes"][0]["after"]["effective"] == 3 * (1 << 29)
    assert not list(runtime.parent.glob(".normalize.toml.*.tmp"))


def test_integrity_acknowledgement_unset_and_complete_regime(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = "integrity_policy.parse_failure.max_count"
    prior = runtime.read_bytes()
    assert invoke(workspace, "unset", path, machine=True) == 0
    preview = report(capsys)
    assert preview["requires_risk_acknowledgement"]
    assert preview["changes"][0]["after"]["effective"] is None
    assert preview["changes"][0]["after"]["active"] is False
    assert invoke(workspace, "unset", path, "--apply", machine=True) == 1
    assert "acknowledge-risk" in report(capsys)["error"]["message"]
    assert runtime.read_bytes() == prior
    assert invoke(workspace, "unset", path, "--apply", "--acknowledge-risk", machine=True) == 0
    assert report(capsys)["status"] == "applied"
    assert (
        invoke(
            workspace,
            "set",
            "integrity_policy.parse_failure.action",
            "degrade",
            "--apply",
            "--acknowledge-risk",
            machine=True,
        )
        == 1
    )
    assert "requires at least one threshold" in report(capsys)["error"]["message"]
    assert invoke(workspace, "set", path, "10", "--apply", "--acknowledge-risk", machine=True) == 0
    report(capsys)
    assert (
        invoke(
            workspace,
            "set",
            "integrity_policy.parse_failure.action",
            "degrade",
            "--apply",
            "--acknowledge-risk",
            machine=True,
        )
        == 0
    )
    assert "action = 'degrade'  # Keep quotes" in runtime.read_text()
    assert report(capsys)["requires_risk_acknowledgement"]


def test_supported_feature_modes_and_optional_defaults(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    for action, args, expected in [
        ("set", ("scratch_format", "typed_events"), "typed_events"),
        ("set", ("scratch_codec", "uncompressed"), "uncompressed"),
        ("unset", ("scratch_format",), "json"),
    ]:
        assert invoke(workspace, action, *args, "--apply", machine=True) == 0
        assert report(capsys)["changes"][0]["after"]["effective"] == expected
    assert "scratch_format" not in tomllib.loads(runtime.read_text())


def test_conflicting_replacement_is_never_applied(workspace: Path, runtime: Path) -> None:
    service = RuntimeConfiguration(ProjectPaths(workspace, workspace / "data"))
    original = service.snapshot()
    pending = service.change_field(original, "resources.workers", "12")
    external = runtime.with_name("other.toml")
    external.write_bytes(original.data + b"# Concurrent edit\n")
    external.replace(runtime)
    with pytest.raises(MarketForgeError, match="changed"):
        service.apply(pending)
    assert runtime.read_bytes() == original.data + b"# Concurrent edit\n"


def test_apply_revalidates_filesystem_dependent_constraints(
    workspace: Path,
    runtime: Path,
) -> None:
    service = RuntimeConfiguration(ProjectPaths(workspace, workspace / "data"))
    original = service.snapshot()
    proposed = service.change_field(original, "resources.scratch_path", "future-scratch")
    (workspace / "future-scratch").touch()
    with pytest.raises(MarketForgeError, match="existing path must be a directory"):
        service.apply(proposed)
    assert runtime.read_bytes() == original.data
    assert not list(runtime.parent.glob(".normalize.toml.*.tmp"))


@pytest.mark.parametrize("target", ["leaf", "parent", "directory"])
def test_refuses_unsafe_paths(
    workspace: Path,
    runtime: Path,
    capsys: pytest.CaptureFixture[str],
    target: str,
) -> None:
    relocated = runtime.with_name("elsewhere.toml")
    if target == "leaf":
        runtime.rename(relocated)
        runtime.symlink_to(relocated)
    elif target == "parent":
        moved = runtime.parent.with_name("elsewhere")
        runtime.parent.rename(moved)
        runtime.parent.symlink_to(moved, target_is_directory=True)
    else:
        runtime.unlink()
        runtime.mkdir()
    assert invoke(workspace, "set", "resources.workers", "12", "--apply", machine=True) == 1
    assert "runtime" in report(capsys)["error"]["message"]


def test_missing_engine_and_noninteractive_edit(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runtime = workspace / "data/.config/engine/normalize.toml"
    runtime.parent.mkdir(parents=True)
    runtime.write_bytes(RUNTIME.read_bytes())
    assert invoke(workspace, "validate", machine=True) == 1
    assert "mf engine build normalize" in report(capsys)["error"]["message"]
    assert invoke(workspace, "edit", machine=True) == 1
    assert "incompatible" in report(capsys)["error"]["message"]
    assert invoke(workspace, "edit") == 1
    assert "interactive terminal" in capsys.readouterr().out


@pytest.mark.parametrize("valid", [False, True])
def test_edit_keeps_invalid_draft_and_only_applies_after_confirmation(
    workspace: Path,
    runtime: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    valid: bool,
) -> None:
    prior = runtime.read_bytes()
    seen: list[Path] = []

    def editor(path: Path, *, editor: str | None = None) -> Path:
        assert editor == "nvim"
        assert path != runtime and runtime.read_bytes() == prior
        path.write_bytes(
            prior.replace(b"workers  =  1", b"workers  =  12" if valid else b"workers  =  0")
        )
        seen.append(path)
        return path

    def confirm() -> str:
        assert runtime.read_bytes() == prior
        return "apply"

    monkeypatch.setattr(cli, "edit_file", editor)

    def interactive(_streams: object) -> bool:
        return True

    monkeypatch.setattr(cli, "all", interactive, raising=False)
    monkeypatch.setattr("builtins.input", confirm)
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("EDITOR", raising=False)
    try:
        assert invoke(workspace, "edit") == (0 if valid else 1)
        output = capsys.readouterr().out
        if valid:
            assert "APPLIED" in output and not seen[0].exists()
            assert b"workers  =  12" in runtime.read_bytes()
        else:
            assert "must be greater than zero" in output
            assert str(seen[0]) in output and seen[0].exists()
            assert runtime.read_bytes() == prior
    finally:
        if seen and seen[0].parent.exists():
            shutil.rmtree(seen[0].parent)


@pytest.mark.parametrize(
    ("raw", "expected"), [("1024", 1024), ("1.5 KiB", 1536), ("48GiB", 48 << 30), ("1TiB", 1 << 40)]
)
def test_exact_byte_value(raw: str, expected: int) -> None:
    assert (
        typed_value(
            {"path": "resources.memory_budget_bytes", "kind": "bytes", "integer_bits": 64}, raw
        )
        == expected
    )


def test_reference_documents_each_rust_field_default_and_enum(
    workspace: Path, runtime: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert invoke(workspace, "schema", machine=True) == 0
    fields = report(capsys)["fields"]
    reference = (ROOT / "docs/content/reference/configuration/normalization-runtime.md").read_text()
    for field in fields:
        path = field["path"]
        anchor = '<a id="' + path.replace(".", "-").replace("_", "-") + '"></a>'
        assert anchor in reference, path
        preceding, following = reference.split(anchor, 1)
        if preceding.rsplit("\n", 1)[-1].startswith("|"):
            evidence = following.split("\n", 1)[0].lower()
        else:
            evidence = following.split("\n##", 1)[0].split("<a id=", 1)[0].lower()
        default = field["default"]
        literals = [f"`{value}`" for value in field["choices"]]
        if default is None:
            if field["required"] and path.startswith("resources.parquet."):
                assert "Both `resources.parquet` fields are required" in reference
                continue
            literals.append("required" if field["required"] else "absent")
        elif type(default) is int:
            literals.append(str(default))
        elif type(default) is bool:
            literals.append(f"`{str(default).lower()}`")
        else:
            literals.append(f"`{default}`")
        assert all(literal in evidence.replace(",", "") for literal in literals), path


@pytest.mark.skipif(os.name != "posix", reason="requires a Unix pseudo-terminal")
def test_interactive_editor_and_clickable_documentation(workspace: Path, runtime: Path) -> None:
    import errno
    import fcntl
    import pty
    import select
    import shlex
    import struct
    import sys
    import termios
    import time

    prior = runtime.read_bytes()
    editor = workspace / "editor.py"
    editor.write_text(
        "from pathlib import Path\nfrom sys import argv\n"
        "draft = Path(argv[1])\n"
        "draft.write_bytes(draft.read_bytes().replace(b'workers  =  1', b'workers  =  23'))\n"
    )
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 35, 120, 0, 0))
    env = {
        **os.environ,
        "TERM": "xterm-256color",
        "PYTHONPATH": str(ROOT),
        "VISUAL": shlex.join([sys.executable, str(editor)]),
    }
    env.pop("NO_COLOR", None)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from forge.cli.main import main; raise SystemExit(main())",
            "--project-root",
            str(workspace),
            "normalize",
            "config",
            "edit",
        ],
        cwd=ROOT,
        env=env,
        stdin=slave,
        stdout=slave,
        stderr=slave,
    )
    os.close(slave)
    output = bytearray()
    confirmed = False
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if not select.select([master], [], [], 0.1)[0]:
                continue
            try:
                data = os.read(master, 65536)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
            if not data:
                break
            output.extend(data)
            if not confirmed and b"Type 'apply' to replace" in output:
                assert runtime.read_bytes() == prior
                os.write(master, b"apply\n")
                confirmed = True
        assert process.wait(timeout=3) == 0, bytes(output)
        assert confirmed and b"APPLIED" in output
        assert NORMALIZATION_RUNTIME.encode() in output and b"\x1b]8;" in output
        assert runtime.read_bytes() == prior.replace(b"workers  =  1", b"workers  =  23")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.close(master)
