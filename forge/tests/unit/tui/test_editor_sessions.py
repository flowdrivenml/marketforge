"""Pilot coverage for external edit, validation, review, and apply."""

import asyncio
import shutil
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.pilot import Pilot
from textual.worker import Worker

from forge.historical.archives.storage.files import disk
from forge.historical.authoring import create_pipeline
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.editor import EditorError
from forge.startup.context import ApplicationContext
from forge.tui.app import ForgeApp
from forge.tui.configuration_screen import ConfigurationEditor
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.resources import Row
from forge.tui.screens import Confirm

ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ForgeApp:
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    import tempfile

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    app = ForgeApp(ApplicationContext(AppSettings(), paths, None, Console(), Console(), True))
    monkeypatch.setattr(app, "check_registry", lambda: None)

    def no_suspend() -> None:
        pytest.fail("A TUI editor suspended the parent's real terminal")

    monkeypatch.setattr(app, "suspend", no_suspend)
    return app


async def until(pilot: Pilot[str], check: Callable[[], bool]) -> None:
    async with asyncio.timeout(8):
        while not check():
            await pilot.pause(0.03)


async def review(pilot: Pilot[str], app: ForgeApp, target: Path) -> Inspection:
    await pilot.press("f9")
    await until(pilot, lambda: isinstance(app.screen, ActionMenu))
    menu = cast(ActionMenu, app.screen)
    await pilot.click(f"#menu-{menu.actions.index(str(target))}")
    await until(pilot, lambda: isinstance(app.screen, Inspection))
    return cast(Inspection, app.screen)


@pytest.mark.asyncio
@pytest.mark.parametrize("result", ["saved", "invalid", "unchanged", "conflict", "failure"])
async def test_pipeline_edit_keeps_workspace_live(
    workspace: ForgeApp, monkeypatch: pytest.MonkeyPatch, result: str
) -> None:
    app = workspace
    target = create_pipeline(app.context.paths.pipeline_config_dir, "test_pipeline")
    original = target.read_bytes()
    released = asyncio.Event()
    opened = asyncio.Event()
    calls: list[Path] = []

    async def launch(path: Path, *, cwd: Path, **_: object) -> None:
        assert cwd == app.context.paths.root and path != target and path.name == target.name
        calls.append(path)
        opened.set()
        await released.wait()
        if result == "failure":
            raise EditorError("Ghostty unavailable")
        if result == "invalid":
            await disk(path.write_bytes, b"broken = [")
        elif result != "unchanged":
            await disk(path.write_bytes, original + b"\n# editor change\n")

    monkeypatch.setattr("forge.tui.app.edit_in_window", launch)
    app.initial_area, app.initial_view = "Workflows", "Pipeline"

    async with app.run_test(size=(125, 35)) as pilot:
        row = Row(target.stem, target.stem, "pipeline", "", {}, target.stem, target)
        await app.open_row(row).wait()
        await until(pilot, lambda: isinstance(app.screen, Inspection))
        await pilot.press("f2")
        menu = cast(ActionMenu, app.screen)
        await pilot.click(f"#menu-{menu.actions.index('Edit TOML')}")
        await asyncio.wait_for(opened.wait(), 5)
        app.edit_selected(row)
        await pilot.pause()
        assert len(calls) == 1
        await pilot.press("escape")
        await pilot.click("#area-0")
        assert app.area == "Overview"
        if result == "conflict":
            target.write_bytes(original + b"\n# external change\n")
        released.set()
        await until(pilot, lambda: not app.editor_open)
        assert app.screen is app.root_screen
        assert app.area == "Overview"
        if result == "unchanged":
            assert not app.editor_sessions and not calls[0].exists()
            return
        screen = await review(pilot, app, target)
        if result in {"saved", "conflict"}:
            assert "# editor change" in str(screen.sections["Changes"])
            screen.links["Apply reviewed changes"]()
            await until(pilot, lambda: isinstance(app.screen, Confirm))
            await pilot.click("#confirm")
            await until(pilot, lambda: not app.editor_open)
            if result == "conflict":
                assert app.editor_sessions[target].state == "Apply blocked"
                assert b"# external change" in target.read_bytes()
                assert calls[0].exists()
            else:
                assert target.read_bytes().endswith(b"# editor change\n")
                assert target not in app.editor_sessions and not calls[0].exists()
        else:
            assert "Reopen in Neovim" in screen.links
            assert "Apply reviewed changes" not in screen.links
            assert calls[0].exists() and target.read_bytes() == original


@pytest.mark.asyncio
async def test_independent_sessions(workspace: ForgeApp, monkeypatch: pytest.MonkeyPatch) -> None:
    app = workspace
    directory = app.context.paths.pipeline_config_dir
    targets = [create_pipeline(directory, name) for name in ("first", "second")]
    waiting = {target.name: asyncio.Event() for target in targets}
    opened: list[Path] = []

    async def launch(path: Path, **_: object) -> None:
        opened.append(path)
        await waiting[path.name].wait()

    monkeypatch.setattr("forge.tui.app.edit_in_window", launch)
    async with app.run_test() as pilot:
        for target in targets:
            app.edit_selected(
                Row(target.stem, target.stem, "pipeline", "", {}, target.stem, target)
            )
        await until(pilot, lambda: len(opened) == 2)
        assert opened[0].parent != opened[1].parent
        waiting[targets[0].name].set()
        await until(pilot, lambda: targets[0] not in app.editor_sessions)
        assert app.editor_open
        waiting[targets[1].name].set()
        await until(pilot, lambda: not app.editor_sessions)


@pytest.mark.asyncio
async def test_normalize_toml_rust_review_and_apply(
    workspace: ForgeApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = workspace
    paths = app.context.paths
    config = ROOT / "data" / ".config" / "engine"
    if not config.is_dir() or not (ROOT / ".bin" / "marketforge-normalize").is_file():
        pytest.skip("Managed engine fixtures unavailable")
    shutil.copytree(config, paths.engine_config_dir)
    shutil.copytree(ROOT / ".bin", paths.bin_dir)
    target = paths.runtime_config("normalize")
    original = target.read_bytes()

    async def launch(path: Path, **_: object) -> None:
        await asyncio.sleep(0.15)
        before = tomllib.loads(original.decode())["resources"]["workers"]
        await disk(
            path.write_text, original.decode().replace(f"workers = {before}", "workers = 7", 1)
        )
        assert await disk(path.read_bytes) != original

    monkeypatch.setattr("forge.tui.app.edit_in_window", launch)
    app.initial_area, app.initial_view = "Configs", "Normalize"
    async with app.run_test(size=(125, 35)) as pilot:
        editor = app.query_one(ConfigurationEditor)
        await until(pilot, lambda: "normalize" in editor.drafts and not editor.loading)
        await pilot.click("#more")
        menu = cast(ActionMenu, app.screen)
        await pilot.click(f"#menu-{menu.actions.index('Edit TOML draft')}")
        await until(pilot, lambda: target in app.editor_sessions and not app.editor_open)
        assert app.editor_sessions[target].state == "Ready for review"
        assert target.read_bytes() == original
        screen = await review(pilot, app, target)
        accept = cast(Worker[None], screen.links["Apply reviewed changes"]())
        await until(pilot, lambda: isinstance(app.screen, Confirm))
        await pilot.click("#confirm")
        await accept.wait()
        session = app.editor_sessions.get(target)
        assert editor.drafts["normalize"].dirty, session.error if session else "No editor session"
        await pilot.press("escape")
        await pilot.click("#apply")
        await until(pilot, lambda: isinstance(app.screen, Confirm))
        await pilot.click("#confirm")
        await until(pilot, lambda: tomllib.loads(target.read_text())["resources"]["workers"] == 7)
        await until(pilot, lambda: not editor.loading and not editor.editing)
        assert editor.drafts["normalize"].effective_document
        assert editor.drafts["normalize"].effective_document["resources"]["workers"] == 7


@pytest.mark.asyncio
async def test_normalization_missing_receipt_can_validate_saved_draft(
    workspace: ForgeApp, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.archives.templates import ACQUISITION_TEMPLATE
    from forge.shared.terminal.editor_window import EditorCompletionUnconfirmed

    app = workspace
    target = app.context.paths.normalization_recipe_dir / "getting_started.toml"
    target.parent.mkdir(parents=True)
    target.write_text(ACQUISITION_TEMPLATE)

    async def launch(path: Path, **_: object) -> None:
        await disk(path.write_text, ACQUISITION_TEMPLATE + "# Saved in Neovim\n")
        raise EditorCompletionUnconfirmed("Editor exit could not be verified.")

    monkeypatch.setattr("forge.tui.app.edit_in_window", launch)
    async with app.run_test() as pilot:
        app.edit_selected(Row(target.stem, target.stem, "normalize", "", {}, target.stem, target))
        await until(pilot, lambda: target in app.editor_sessions and not app.editor_open)
        session = app.editor_sessions[target]
        assert session.state == "Completion unconfirmed"
        assert session.modified and session.draft and session.draft.exists()
        assert session.original == target.read_bytes()
        assert session.id in (session.draft.parent / ".session.json").read_text()
        screen = await review(pilot, app, target)
        assert "Apply reviewed changes" not in screen.links
        screen.links["Validate saved draft"]()
        await until(pilot, lambda: session.state == "Ready for review")
        assert session.updated == session.draft.read_bytes()
        assert target.read_text() == ACQUISITION_TEMPLATE
