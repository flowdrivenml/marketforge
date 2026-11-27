"""Non-privileged workspace and CLI verification for install.sh."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import tomlkit
from tomlkit.exceptions import TOMLKitError

from forge.shared.errors import MarketForgeError
from forge.shared.terminal.editor import editor_command
from forge.startup.context import build_context


def read_live_toml(path: Path, *, description: str) -> bytes:
    if not path.is_file():
        raise MarketForgeError(f"{description} is missing or not a file: {path}")
    content = path.read_bytes()
    try:
        tomlkit.parse(content.decode("utf-8"))
    except (TOMLKitError, UnicodeError) as error:
        raise MarketForgeError(
            f"{description} contains invalid TOML: {path}"
        ) from error
    return content


def initialize_live_general(root: Path) -> None:
    # Live resolves this path from the checkout, independently of Forge storage settings.
    directory = root / "data/.config/live"
    target = directory / "general.toml"
    template = directory / "general.example.toml"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        try:
            target.lstat()
        except FileNotFoundError:
            content = read_live_toml(
                template, description="Live configuration template"
            )
            initialized = False
            with tempfile.NamedTemporaryFile(dir=directory) as temporary:
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
                # Publish complete contents atomically without replacing an operator file.
                try:
                    os.link(temporary.name, target)
                except FileExistsError:
                    pass
                else:
                    initialized = True
            if initialized:
                print(f"  Initialized Live configuration at {target}.")
                return

        read_live_toml(target, description="Existing Live configuration")
        print(f"  Preserved existing Live configuration at {target}.")
    except OSError as error:
        # Print filesystem context without reading or displaying configuration values.
        raise MarketForgeError(
            f"Could not initialize or check Live configuration at {target} "
            f"using {template}: {error.strerror}"
        ) from error


def workspace(root: Path) -> None:
    context = build_context(project_root=root)
    if context.settings_file is None:
        path = context.paths.settings_file
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path.parent, mode="w") as temporary:
            temporary.write('[presentation]\neditor = "nvim"\n')
            temporary.flush()
            try:
                os.link(temporary.name, path)
            except FileExistsError:
                pass
        context = build_context(project_root=root)
    editor = editor_command(context.settings.presentation.editor)
    if shutil.which(editor[0]) is None:
        raise MarketForgeError(
            "Forge's configured editor is unavailable. Fix PATH or the editor setting "
            "in the existing application configuration."
        )
    context.paths.ensure_application_directories()
    initialize_live_general(context.paths.root)
    print("  Workspace ready. Forge's editor command resolves on PATH.")


def launchers(root: Path, mode: str | None) -> None:
    directory = Path.home() / ".local/bin"
    for name in ("mf", "marketforge"):
        link = directory / name
        allowed = [
            root / ".install" / group / "bin" / name for group in ("runtime", "dev")
        ]
        if link.is_symlink() and link.resolve(strict=False) in allowed:
            continue
        if link.is_symlink() or link.exists():
            raise MarketForgeError(
                f"Existing executable {link} belongs to another installation. "
                "Move it yourself or use this checkout's .install environment directly."
            )
        if mode is not None:
            directory.mkdir(parents=True, exist_ok=True)
            # symlink is an exclusive write. Never replace a concurrent creation.
            link.symlink_to(root / ".install" / mode / "bin" / name)


def engine(target: str) -> bool:
    state = json.load(sys.stdin)
    for item in state["targets"]:
        if item["target"] == target and item["status"] == "verified":
            result = subprocess.run(
                [item["path"], "--help"],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            return result.returncode == 0 and "--runtime-settings" in result.stdout
    return False


def verify_engines() -> None:
    state = {item["target"]: item["status"] for item in json.load(sys.stdin)["targets"]}
    for target in ("normalize", "merge", "convert"):
        if state.get(target) != "verified":
            raise MarketForgeError(f"The {target} installation is no longer verified.")
        print(f"  {target}: verified")


def initialization_record(
    root: Path, purpose: str, identity: str, *, write: bool
) -> bool:
    marker = root / ".install" / f"market-init-{purpose}"
    if not write:
        return (
            marker.is_file()
            and not marker.is_symlink()
            and marker.read_text() == identity
        )
    with tempfile.NamedTemporaryFile(
        dir=marker.parent, mode="w", delete=False
    ) as temporary:
        pending = Path(temporary.name)
        try:
            temporary.write(identity)
            temporary.flush()
            os.fsync(temporary.fileno())
            pending.replace(marker)
        finally:
            pending.unlink(missing_ok=True)
    return True


def initialization() -> str:
    try:
        value = json.load(sys.stdin)
    except ValueError:
        return "Failed (no usable initialization report)"
    state = value.get("status") if isinstance(value, dict) else None
    if isinstance(value, dict) and state != "complete":
        for item in value.get("stages", []):
            stage = item.get("stage")
            status = item.get("status")
            if stage in (
                "directories",
                "migrations",
                "catalogue",
                "historical_metadata",
                "rankings",
            ):
                if status in ("failed", "partial", "cancelled"):
                    print(f"  {stage}: {status}", file=sys.stderr)
        for item in value.get("rankings", []):
            if item.get("policy") in ("turnover", "volume") and item.get("status") in (
                "failed",
                "partial",
                "cancelled",
            ):
                print(f"  {item['policy']} ranking: {item['status']}", file=sys.stderr)
    return (
        state if state in ("complete", "partial", "cancelled", "failed") else "Failed"
    )


def main() -> int:
    action, *arguments = sys.argv[1:]
    try:
        if action == "engine":
            return 0 if engine(arguments[0]) else 1
        if action == "initialization":
            print(initialization())
            return 0
        if action == "engines":
            verify_engines()
            return 0
        root = Path(arguments[0])
        if action == "workspace":
            workspace(root)
        elif action in ("already-initialized", "record-initialization"):
            return (
                0
                if initialization_record(
                    root,
                    arguments[1],
                    arguments[2],
                    write=action == "record-initialization",
                )
                else 1
            )
        else:
            launchers(root, arguments[1] if action == "launchers" else None)
    except MarketForgeError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        print(
            "Could not prepare or verify the installer-managed files.", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
