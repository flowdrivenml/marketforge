"""Immutable editor snapshots and Textual inspection handoff."""

import asyncio
import json
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest
from rich.console import Console

from forge.historical.archives.storage.files import disk
from forge.orchestration.runs import RunInspection, safe_report
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.editor import EditorError
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values
from forge.tui.app import ForgeApp
from forge.tui.editor_inspection import OpenInspection, RetainedFile, StructuredValue, stage
from forge.tui.inspection import Inspection
from forge.tui.run_views import project
from forge.tui.workflows import Edit, Progress

EXACT: dict[str, object] = {
    "job_id": 2039158607473567512,
    "observed_ns": 1793059200093200001,
    "tick": Decimal("0.00000001000000000000100"),
    "jobs": [{"name": "現物 🐈", "parts": [None, {}, [], 7]}],
}


@pytest.mark.parametrize("suffix", ["json", "jsonl", "toml", "log"])
def test_stage_copies_exact_original_bytes(tmp_path: Path, suffix: str) -> None:
    root = tmp_path / ("workspace の " + "a" * 120)
    root.mkdir()
    source = root / f"retained file.{suffix}"
    original = '  {"市場":null}\r\n'.encode()
    source.write_bytes(original)
    dest = tmp_path / "scratch"
    dest.mkdir()
    staged, filetype, count = stage(RetainedFile("Evidence", source, root), dest)
    assert (staged.name, filetype, count) == (
        source.name,
        {"jsonl": "json", "log": ""}.get(suffix, suffix),
        len(original),
    )
    assert staged.read_bytes() == original
    staged.write_bytes(b"changed")
    assert source.read_bytes() == original


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "unsupported",
        "directory",
        "relative",
        "traversal",
        "outside",
        "symlink",
        "symlink parent",
    ],
)
def test_stage_rejects_unsafe_files(tmp_path: Path, case: str) -> None:
    root = tmp_path / "runs"
    root.mkdir()
    outside = tmp_path / "credentials.json"
    outside.touch()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    source = root / "report.json"
    if case == "unsupported":
        source = root / "report.parquet"
        source.touch()
    elif case == "directory":
        source.mkdir()
    elif case == "relative":
        source = Path("report.json")
    elif case == "traversal":
        source = root / ".." / outside.name
    elif case == "outside":
        source = outside
    elif case == "symlink":
        source.symlink_to(outside)
    elif case == "symlink parent":
        (root / "escape").symlink_to(tmp_path, target_is_directory=True)
        source = root / "escape" / outside.name
    with pytest.raises((OSError, EditorError)):
        stage(RetainedFile("Evidence", source, root), scratch)
    assert not list(scratch.iterdir())


def test_structured_precision_and_large_values(tmp_path: Path) -> None:
    value = {**EXACT, "rows": list(range(100_000))}
    path, kind, _ = stage(StructuredValue(value), tmp_path)
    assert kind == "json"
    assert "現物 🐈" in path.read_text()
    assert json.loads(path.read_bytes()) == {**value, "tick": str(EXACT["tick"])}


def test_retained_report_preview_preserves_decimal_digits(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text('{"tick": 0.00000001000000000000100, "at": 1793059200093200001}')
    value = safe_report(report, tmp_path, exact=True)
    assert value == {"tick": EXACT["tick"], "at": EXACT["observed_ns"]}
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    exported, _, _ = stage(StructuredValue(value), scratch)
    assert json.loads(exported.read_bytes()) == {
        "tick": str(EXACT["tick"]),
        "at": EXACT["observed_ns"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, FileNotFoundError("nvim"), EditorError("exit code 2")])
async def test_run_click_and_keyboard_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception | None
) -> None:
    ctx = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console(), True
    )
    source = ctx.paths.runs_dir / "normalize-test" / "retained report.json"
    source.parent.mkdir(parents=True)
    original = b'{ "status": "complete" }\r\n'
    source.write_bytes(original)

    async def invoke(
        key: str, values: Values, context: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        assert key == "runs report"
        return project(
            RunInspection(
                "normalize-test",
                {"status": "complete", "result": EXACT},
                None,
                report_files={"Execution report": source},
            ),
            context,
            "report",
        )

    app = ForgeApp(ctx, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    launches: list[tuple[Path, bytes]] = []
    opened = asyncio.Event()
    release = asyncio.Event()

    def suspend() -> None:
        pytest.fail("Editor inspection suspended Textual")

    async def editor(
        path: Path,
        *,
        cwd: Path,
        editor: str,
        readonly: bool,
        filetype: str,
        on_ready: Callable[[], None] | None,
    ) -> None:
        assert (cwd, editor, readonly, filetype, on_ready) == (tmp_path, "nvim", True, "json", None)
        assert app.editor_open
        opened.set()
        await release.wait()
        launches.append((path, await disk(path.read_bytes)))
        await disk(path.write_bytes, b"forced change to scratch")
        if failure is not None:
            raise failure

    monkeypatch.setattr(app, "suspend", suspend)
    monkeypatch.setattr("forge.tui.app.edit_in_window", editor)
    async with app.run_test(size=(120, 35)) as pilot:
        await app.inspect_request("runs report", {"reference": "normalize-test"}, "Run")
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Inspection)
        await pilot.click(f"#section-{tuple(screen.sections).index('Artifacts')}")
        assert await pilot.click("#inspect-file-0")
        await asyncio.wait_for(opened.wait(), timeout=5)
        button = screen.query_one("#inspect-file-0", OpenInspection)
        app.inspect_in_editor(button.target)
        refreshed = asyncio.Event()
        app.set_timer(0.01, refreshed.set)
        await asyncio.wait_for(refreshed.wait(), timeout=5)
        await app.switch_environment().wait()
        assert app.context.testing and app.screen is screen
        await pilot.pause()
        release.set()
        await pilot.pause()
        assert app.screen is screen and screen.focused is button
        assert launches[0][1] == source.read_bytes() == original
        assert not launches[0][0].exists()
        assert await pilot.click(f"#section-{tuple(screen.sections).index('Execution result')}")
        screen.query_one("#inspect-open-json", OpenInspection).focus()
        await pilot.press("enter")
        await pilot.pause()
        assert json.loads(launches[1][1]) == {**EXACT, "tick": str(EXACT["tick"])}
        assert not launches[1][0].exists()
        assert app.screen is screen and not app.editor_open


def test_protected_and_unsupported_values(tmp_path: Path) -> None:
    with pytest.raises(EditorError, match="protected"):
        stage(StructuredValue({"metadata": {"password": "protected"}}), tmp_path)
    with pytest.raises(TypeError, match="string object keys"):
        stage(StructuredValue({42: "unsupported key"}), tmp_path)


def test_permission_denied(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    path = tmp_path / "retained.json"
    path.touch()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    actual_open = os.open

    def denied(name: str, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        if name == path.name:
            raise PermissionError(13, "Permission denied", str(path))
        return actual_open(name, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", denied)
    with pytest.raises(PermissionError, match="Permission denied"):
        stage(RetainedFile("Report", path, tmp_path), scratch)
    assert not list(scratch.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "view", ["recipe", "normalize", "merge", "convert", "exchange", "profile", "effective"]
)
async def test_open_controls_in_related_views(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, view: str
) -> None:
    from forge.historical.archives.templates import ACQUISITION_TEMPLATE
    from forge.tui.configuration_screen import ConfigurationEditor
    from forge.tui.editor_inspection import InspectionTarget
    from forge.tui.resources import Row

    ctx = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console(), True
    )
    manifest = ctx.paths.datasets_dir / view / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"streams": []}\n')
    source = manifest
    targets: list[InspectionTarget] = []

    async def publications(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "publications": [
                {"engine_dataset_id": 17, "operation": view, "manifest_path": str(manifest)}
            ]
        }

    async def converted(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"output": {"manifest_path": str(manifest), "statistics": {"rows": 17}}}

    async def exchange(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"code": "binance", **EXACT}

    app = ForgeApp(ctx)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    monkeypatch.setattr(app, "inspect_in_editor", targets.append)
    monkeypatch.setattr(app, "catalogue_call", exchange)
    monkeypatch.setattr("forge.tui.app.publications", publications)
    monkeypatch.setattr("forge.tui.app.browse", converted)

    async with app.run_test(size=(132, 36)) as pilot:
        if view == "recipe":
            source = ctx.paths.acquisition_config_dir / "saved.toml"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(ACQUISITION_TEMPLATE)
            await app.switch("Workflows", "Acquisition")
            await app.inspect_row(Row("saved", "saved", "acquisition", "", {}, "saved", source))
        elif view in {"normalize", "merge"}:
            await app.inspect_publication(17)
        elif view == "convert":
            await app.open_indexed_output("test-output").wait()
        elif view == "exchange":
            await app.switch("Catalogue", "Exchanges")
            await app.inspect_catalogue(Row("1", "binance", "exchange", "", {}, "binance"))
        elif view == "profile":
            source = ctx.paths.engine_config_dir / "normalize/profiles/fixture.toml"
            source.parent.mkdir(parents=True)
            source.write_text("schema_version = 1\n")
            identity = "binance.spot.spot.trades"
            await app.switch("Catalogue", "Profiles")
            await app.inspect_catalogue(
                Row(identity, identity, "Profile", "", {"definition": EXACT}, identity, source)
            )
        else:
            await app.switch("Configs", "General")
            await app.query_one(ConfigurationEditor).load("application").wait()
            await pilot.pause()
            assert await pilot.click("#configuration-open-effective")
            assert targets == [StructuredValue(ctx.settings.model_dump())]
            return

        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Inspection)
        if view == "exchange":
            assert await pilot.click("#inspect-open-json")
            assert targets == [StructuredValue({"code": "binance", **EXACT})]
        else:
            assert await pilot.click(f"#section-{tuple(screen.sections).index('Artifacts')}")
            assert await pilot.click("#inspect-file-0")
            assert len(targets) == 1
            assert isinstance(target := targets[0], RetainedFile)
            assert target.path == source
            temp = tmp_path / "snapshot"
            temp.mkdir()
            assert stage(target, temp)[0].read_bytes() == source.read_bytes()


@pytest.mark.asyncio
async def test_large_snapshot_cancel_and_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.tui.editor_inspection import InspectionTarget
    from forge.tui.screens import Confirm

    ctx = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console(), True
    )
    path = ctx.paths.runs_dir / "test" / "large.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n" * 40)
    target = RetainedFile("Event log", path, ctx.paths.runs_dir)
    app = ForgeApp(ctx)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    monkeypatch.setattr("forge.tui.app.LARGE_INSPECTION", 32)
    snapshots: list[Path] = []

    def capture(value: InspectionTarget, directory: Path) -> tuple[Path, str, int]:
        staged = stage(value, directory)
        snapshots.append(staged[0])
        return staged

    def unexpected_editor(*_args: object, **_kwargs: object) -> None:
        pytest.fail("The editor must only start after confirmation and successful staging.")

    monkeypatch.setattr("forge.tui.app.stage_inspection", capture)
    monkeypatch.setattr("forge.tui.app.edit_in_window", unexpected_editor)
    async with app.run_test(size=(120, 35)) as pilot:
        screen = Inspection("Run logs", {"status": "complete"}, files=(target,))
        await app.push_screen(screen)
        assert await pilot.click("#inspect-primary-file")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        assert snapshots and snapshots[0].is_file()
        assert await pilot.click("#back")
        await pilot.pause()
        assert app.screen is screen and not app.editor_open
        assert not snapshots[0].exists()
        path.unlink()
        assert await pilot.click("#inspect-primary-file")
        await pilot.pause()
        assert screen.query_one("#inspection-primary-artifact").has_class("artifact-missing")
        assert not app.editor_open and len(snapshots) == 1
