"""Pilot coverage for retained diagnostics, their source, and responsive navigation."""

import asyncio
import json
import os
import shlex
import shutil
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.widgets import Button, Input, Select, Static

from forge.orchestration.runs import RunInspection
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values
from forge.tui.app import ForgeApp
from forge.tui.inspection import Inspection
from forge.tui.run_views import RunView, project
from forge.tui.screens import Detail

ID = "pipeline-11111111-1111-4111-8111-111111111111"


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_errors_tab_filters_exact_messages_and_opens_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: tuple[int, int]
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    context = ApplicationContext(AppSettings(), paths, None, Console(), Console(), True)
    folder = paths.runs_dir / ID
    folder.mkdir(parents=True)
    report = folder / "report.json"
    long = "Family/profile preparation failed\n" + "  Archive member unavailable\n" * 8
    evidence: dict[str, object] = {
        "run_id": ID,
        "status": "failed",
        "state": "failed",
        "stages": {
            "normalize": {
                "state": "failed",
                "result": {
                    "job_id": 9007199254740993,
                    "error": long,
                    "symbol": "SOLUSDT",
                    "warnings": ["Optional metadata could not be resolved."],
                },
            },
            "merge": {"state": "pending"},
        },
    }
    report.write_text(json.dumps(evidence))
    log = folder / "engine-stdout.log"
    log.write_text("execution path: identity_reconciliation\n")
    record: dict[str, object] = {
        **evidence,
        "database_purpose": "testing",
        "operation": "pipeline",
        "artifact_root": str(folder),
        "report_path": str(report),
        "attempts": [],
        "children": [],
        "resources": [],
    }

    async def invoke(_key: str, _values: Values, *_: object) -> RunView:
        return project(
            RunInspection(
                ID,
                evidence,
                record,
                report_files={"Execution report": report},
                log_files={"Engine output": log},
            ),
            context,
            "show",
        )

    app = ForgeApp(context, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    opened: list[bytes] = []

    real_editor = app.terminal_editor
    nvim = shutil.which("nvim") if os.getenv("MF_ERRORS_REAL_EDITOR") else None
    socket = tmp_path / "nvim.sock"
    if nvim:
        shim = tmp_path / "nvim"
        shim.write_text(
            f'#!/bin/sh\nexec {shlex.quote(nvim)} --listen {shlex.quote(str(socket))} "$@"\n'
        )
        shim.chmod(0o700)
        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")

    async def terminal(path: Path, *, readonly: bool, **_: object) -> None:
        assert readonly and path != report
        opened.append(await asyncio.to_thread(path.read_bytes))
        if nvim:
            await real_editor(path, readonly=True, filetype="json")

    monkeypatch.setattr(app, "terminal_editor", terminal)
    async with app.run_test(size=size) as pilot:
        await app.inspect_request("runs show", {"reference": ID}, "Run")
        assert isinstance(app.screen, Inspection)
        screen = app.screen
        await pilot.pause()
        if size[0] == 80:
            await pilot.press("f4")
        else:
            assert await pilot.click("#inspect-errors")
        async with asyncio.timeout(4):
            while str(screen.query_one("#inspect-errors", Button).label) != "Errors (1)":
                await pilot.pause(0.06)
        assert screen.section == "Errors"
        assert screen.query_one("#inspect-open-json").display
        assert "Testing" in str(screen.query_one("#errors-summary", Static).content)
        assert "PENDING" in str(screen.query(".error-stage-label").last(Static).content)
        await pilot.pause()
        if directory := os.getenv("MF_ERRORS_SCREENSHOTS"):
            await asyncio.to_thread(
                Path(directory, f"marketforge-errors-{size[0]}.svg").write_text,
                app.export_screenshot(),
            )
        severity = cast(Select[str], screen.query_one("#errors-severity", Select))
        assert "Root cause not established" in str(
            screen.query(".error-explanation").first(Static).content
        )
        assert not any(
            "identity_reconciliation" in str(item.content)
            for item in screen.query(".error-message").results(Static)
        )
        severity.value = "info"
        async with asyncio.timeout(4):
            while not any(
                "identity_reconciliation" in str(item.content)
                for item in screen.query(".error-message").results(Static)
            ):
                await pilot.pause(0.05)
        severity.value = "warning"
        await pilot.pause(0.35)
        assert "Optional metadata" in str(screen.query_one(".error-message", Static).content)
        severity.value = "error"
        screen.query_one("#errors-search", Input).value = "9007199254740993"
        await pilot.pause(0.35)
        button = screen.query_one("#errors-detail-0", Button)
        button.scroll_visible(animate=False)
        await pilot.pause()
        assert await pilot.click(button)
        assert isinstance(app.screen, Detail)
        assert long in app.screen.value_text
        if directory := os.getenv("MF_ERRORS_SCREENSHOTS"):
            if size[0] == 120:
                await asyncio.to_thread(
                    Path(directory, "marketforge-error-detail.svg").write_text,
                    app.export_screenshot(),
                )
        assert await pilot.click("#detail-open-editor")
        async with asyncio.timeout(3):
            while not opened:
                await pilot.pause(0.06)
        assert opened == [report.read_bytes()]
        if nvim:
            async with asyncio.timeout(10):
                while not socket.exists():
                    await pilot.pause(0.15)
                await pilot.pause(0.35)
                inspect = await asyncio.create_subprocess_exec(
                    nvim,
                    "--headless",
                    "--server",
                    str(socket),
                    "--remote-expr",
                    'json_encode([expand("%:t"), &readonly, &modifiable, getline(1)])',
                    stdout=asyncio.subprocess.PIPE,
                )
                output, _ = await inspect.communicate()
                assert inspect.returncode == 0
                assert json.loads(output) == ["report.json", 1, 0, report.read_text()]
                quit_editor = await asyncio.create_subprocess_exec(
                    nvim, "--headless", "--server", str(socket), "--remote-send", ":q<CR>"
                )
                assert await quit_editor.wait() == 0
                while app.editor_open:
                    await pilot.pause(0.10)
        await pilot.press("escape")
        assert app.screen is screen and screen.section == "Errors"
        assert await pilot.click("#section-0")
        assert screen.section == "Summary"


@pytest.mark.asyncio
async def test_errors_tab_loads_more_and_searches_beyond_first_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    context = ApplicationContext(AppSettings(), paths, None, Console(), Console(), True)
    folder = paths.runs_dir / ID
    folder.mkdir(parents=True)
    report = folder / "report.json"
    evidence = {"state": "failed", "run_id": ID}
    report.write_text(json.dumps(evidence))
    (folder / "events.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "level": "error",
                    "message": "Repeated timeout" if i < 70 else "LAST-ONLY",
                    "stage": "merge",
                }
            )
            + "\n"
            for i in range(71)
        )
    )
    record: dict[str, object] = {
        **evidence,
        "status": "failed",
        "operation": "pipeline",
        "database_purpose": "testing",
        "artifact_root": str(folder),
        "report_path": str(report),
    }

    async def invoke(_key: str, _values: Values, *_: object) -> RunView:
        return project(RunInspection(ID, evidence, record), context, "show")

    app = ForgeApp(context, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    async with app.run_test(size=(100, 30)) as pilot:
        await app.inspect_request("runs show", {"reference": ID}, "Run")
        await pilot.pause()
        assert await pilot.click("#inspect-errors")
        screen = app.screen
        button = screen.query_one("#inspect-errors", Button)
        async with asyncio.timeout(4):
            while "30 errors" not in str(screen.query_one("#errors-summary", Static).content):
                await pilot.pause(0.05)
        assert str(button.label) == "Errors"
        assert len(screen.query(".error-card")) == 20
        assert await pilot.click("#errors-next")
        await pilot.pause(0.2)
        assert "21–30" in str(screen.query_one("#errors-range", Static).content)
        assert await pilot.click("#errors-load")
        async with asyncio.timeout(4):
            while "60 errors" not in str(screen.query_one("#errors-summary", Static).content):
                await pilot.pause(0.05)
        assert await pilot.click("#errors-load")
        async with asyncio.timeout(4):
            while str(button.label) != "Errors (71)":
                await pilot.pause(0.05)
        screen.query_one("#errors-search", Input).value = "last-only"
        async with asyncio.timeout(4):
            while len(screen.query(".error-card")) != 1:
                await pilot.pause(0.05)
        assert "LAST-ONLY" in str(screen.query_one(".error-message", Static).content)
