"""Bounded Overview evidence and interaction checks."""

import json
from pathlib import Path

import pytest
from rich.console import Console
from textual.containers import VerticalScroll
from textual.widgets import Static

from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import overview
from forge.tui.app import ForgeApp
from forge.tui.controller import Operation
from forge.tui.inspection import Inspection

RUN = "11111111-1111-4111-8111-111111111111"
SECOND = "22222222-2222-4222-8222-222222222222"
MANAGED = "merge-execute-1001"
RECOVERY = "merge-register-1002"


def context(path: Path) -> ApplicationContext:
    return ApplicationContext(
        AppSettings(), ProjectPaths(path, path / "data"), None, Console(), Console()
    )


def report(root: Path, run: str, result: dict[str, object], filename: str = "report.json") -> None:
    path = root / run / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result))


def test_requested_results_and_full_publication_scope() -> None:
    for state, label in (
        ("valid", "Validation report"),
        ("prepared", "Prepared jobs"),
        ("stopped", "Prepared jobs"),
        ("materialized", "Materialized · unpublished"),
        ("published", "Committed publication"),
    ):
        assert overview.result_label({"status": state}) == label
    assert overview.result_label({"verification": "verified", "registration": "not_available"}) == (
        "Verified output · registration unavailable"
    )
    completed: dict[str, object] = {
        "status": "complete",
        "registration": "registered",
        "publications": {"one": {"publication_id": 5}, "two": {"publication_id": 7}},
    }
    assert overview.publications(completed) == (5, 7)
    assert overview.result_label(completed) == "2 registered publications"
    assert not overview.registration_problem({"registration": "pending", "status": "prepared"})
    assert not overview.pipeline_recovery({"kind": "historical_pipeline", "state": "interrupted"})


def test_standalone_phase_has_no_pipeline_stages() -> None:
    op = Operation("fixture", "normalize run", reference="fixture", progress="validate")
    label = overview.operation_lines(op, True, 24, op.snapshot()).plain
    assert "fixture" in label and "validate" in label
    assert "Acquire" not in label


def test_activity_is_latest_and_retained_running_is_unconfirmed() -> None:
    from forge.tui import workspace_dashboard as dashboard

    reports: list[dict[str, object]] = [
        {"operation": "merge", "status": "completed", "started_at": "2026-11-20T10:00:00Z"},
        {
            "operation": "merge",
            "status": "completed",
            "registration": "failed",
            "started_at": "2026-11-20T11:00:00Z",
        },
        {"operation": "normalize", "status": "prepared", "started_at": "2026-11-20T12:00:00Z"},
        {"operation": "pipeline", "status": "stopped", "started_at": "2026-11-20T12:00:00Z"},
        {"operation": "convert", "status": "running", "started_at": "2026-11-20T12:00:00Z"},
        {
            "operation": "acquisition",
            "status": "running",
            "owned_active": True,
            "started_at": "2026-11-20T12:00:00Z",
        },
    ]
    states = {op: state for op, (_, state) in dashboard.activity(reports).items()}
    assert states == {
        "merge": "Failed",
        "normalize": "Completed",
        "pipeline": "Completed",
        "convert": "—",
        "acquire": "Pending",
    }
    assert dashboard.result("Failed").style == "#f7768e"
    assert dashboard.result("Completed").style == "#9ece6a"
    assert dashboard.result("Pending").style == "#e0af68"
    assert dashboard.cpu_percent(None, (100, 45)) is None
    assert dashboard.cpu_percent((100, 45), (200, 75)) == 70


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_workspace_layout_and_stage_interaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    size: tuple[int, int],
) -> None:
    from typing import Any

    from forge.datasets import workspace_inventory
    from forge.tui import app as shell
    from forge.tui import workspace_dashboard as dashboard

    ctx = context(tmp_path)
    receipts: dict[str, object] = {
        "kind": "historical_pipeline",
        "environment": "production",
        "status": "stopped",
        "name": "Fixture",
        "started_at": "2026-11-20T12:00:00Z",
        "finished_at": "2026-11-20T12:01:00Z",
    }
    report(ctx.paths.runs_dir, RUN, receipts)
    copied: list[str] = []
    inspected: list[str] = []

    async def summary(_: ProjectPaths) -> workspace_inventory.WorkspaceInventory:
        return workspace_inventory.WorkspaceInventory(
            3,
            2,
            1,
            (("bybit", 1, 1, 0, 0, 3),),
            (("bybit", "spot", "spot", True, False),),
            {"Raw": (1, 0), "Normalized": (30, 0), "Merged": (None, 1), "Converted": (50, 0)},
        )

    async def page(_: ProjectPaths, stage: str, offset: int) -> tuple[list[dict[str, Any]], bool]:
        row: dict[str, Any] = {
            "exchange": "bybit",
            "symbol": "LONG-BTCUSDT",
            "data_type": "trade",
            "file_count": 2,
            "recorded_bytes": 10,
            "archives": ["local:100", "local:101"],
            "publication": 41,
        }
        if stage == "Merged":
            row["publication"] = 9_000_000_000_000_001
        if stage == "Raw":
            row.pop("publication")
        if stage == "Converted":
            row.pop("publication")
            row["output_ref"] = "a" * 64
        return [row], offset == 0

    async def selected(_: Path) -> str:
        return "verified-registry"

    async def indexed(_: ProjectPaths) -> list[dict[str, object]]:
        return []

    def inspect(_: ForgeApp, reference: str) -> None:
        inspected.append(reference)

    def copy(_: ForgeApp, identity: str) -> None:
        copied.append(identity)

    monkeypatch.setattr(workspace_inventory, "summary", summary)
    monkeypatch.setattr(workspace_inventory, "stage_page", page)
    monkeypatch.setattr(shell, "selected_registry_id", selected)
    monkeypatch.setattr(shell, "workspace_runs", indexed)
    monkeypatch.setattr(ForgeApp, "copy_dashboard_id", copy)
    monkeypatch.setattr(ForgeApp, "open_indexed_output", inspect)

    def inspect_publication(_: ForgeApp, identity: int) -> None:
        inspected.append(str(identity))

    monkeypatch.setattr(ForgeApp, "open_overview_publication", inspect_publication)
    monkeypatch.setattr(
        dashboard, "host_sample", lambda: dashboard.HostSample((100, 50), (2**30, 4 * 2**30))
    )

    app = ForgeApp(ctx)
    async with app.run_test(size=size) as pilot:
        await pilot.pause(0.5)
        assert (
            app.query_one("#workspace-header", Static).content == "MarketForge Workspace · Overview"
        )
        monitor = str(app.query_one("#workspace-monitor", Static).content)
        assert "PRODUCTION connected" in monitor and "UTC" in monitor
        assert "Engine" not in monitor and "25%" in monitor
        panels = list(app.query(".workspace-panel"))
        assert [panel.border_title for panel in panels] == [
            "Instrument Universe",
            "Data Storage",
            "Recent Activity",
            "Market Data Availability",
            "Data by Stage",
            "Recent Runs",
        ]
        universe, storage, activity, availability, stage_panel, recent_panel = panels
        assert storage.region.y == universe.region.bottom
        assert activity.region.y == storage.region.bottom
        assert stage_panel.region.y == max(activity.region.bottom, availability.region.bottom)
        assert recent_panel.region.y == stage_panel.region.bottom
        assert stage_panel.region.width == recent_panel.region.width
        if size[0] < 110:
            assert availability.region.y >= storage.region.bottom
        else:
            assert universe.region.y == availability.region.y
            assert storage.region.y != activity.region.y
        table = app.query_one("#workspace-inventory", dashboard.OverviewTable)
        table.focus()
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "Raw archive window"
        app.refresh_monitor()
        assert "PRODUCTION connected" in str(
            app.screen.query_one(".workspace-monitor", Static).content
        )
        await pilot.press("escape")
        for index in range(1, 4):
            tab = app.query_one(f"#workspace-stage-{index}")
            tab.scroll_visible(immediate=True, force=True)
            await pilot.pause()
            await pilot.click(tab)
            await pilot.pause(0.2)
            table.focus()
            await pilot.press("enter")
            await pilot.pause()
        assert inspected == ["41", "9000000000000001", "a" * 64]
        table.scroll_visible(immediate=True, force=True)
        await pilot.pause()
        x = sum(
            column.width + 2
            for column in table.columns.values()
            if column.label.plain in {"Exchanges", "Instruments", "Data", "Target"}
        )
        await pilot.click(table, offset=(x + 1, 1))
        await pilot.pause()
        assert copied == ["a" * 64]
        table.focus()
        await pilot.press("pagedown")
        await pilot.pause()
        assert app.inventory_pages["Converted"] == 20
        recent = app.query_one("#overview-results", dashboard.OverviewTable)
        app.query_one("#overview-scroll", VerticalScroll).scroll_end(animate=False, immediate=True)
        await pilot.pause()
        await pilot.click(recent, offset=(3, 1))
        await pilot.pause()
        assert copied[-1] == RUN
        recent.focus()
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        await pilot.press("escape")
        await app.switch("Configs", "General")
        await pilot.pause()
        assert "PRODUCTION connected" in str(app.query_one("#workspace-monitor", Static).content)
        assert app.workspace_header() == "MarketForge Workspace · Configs"


@pytest.mark.asyncio
async def test_outage_preserves_scoped_data_and_host_monitor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace

    from forge.datasets import workspace_inventory
    from forge.tui import app as shell

    ctx = replace(context(tmp_path), testing=True)
    unavailable = False

    async def selected(_: Path) -> str:
        if unavailable:
            raise OSError("fixture registry mismatch")
        return "testing-id"

    async def summary(_: ProjectPaths) -> workspace_inventory.WorkspaceInventory:
        if unavailable:
            raise OSError("fixture registry unavailable")
        return workspace_inventory.WorkspaceInventory(
            0, 0, 0, (), (), dict.fromkeys(("Raw", "Normalized", "Merged", "Converted"), (0, 0))
        )

    monkeypatch.setattr(workspace_inventory, "summary", summary)
    monkeypatch.setattr(shell, "selected_registry_id", selected)
    report(
        ctx.paths.runs_dir,
        RUN,
        {
            "kind": "historical_pipeline",
            "environment": "testing",
            "status": "completed",
            "started_at": "2026-11-20T12:00:00Z",
        },
    )
    report(
        ctx.paths.runs_dir,
        SECOND,
        {
            "kind": "historical_pipeline",
            "environment": "production",
            "status": "failed",
            "started_at": "2026-11-21T12:00:00Z",
        },
    )
    app = ForgeApp(ctx)
    async with app.run_test(size=(120, 32)) as pilot:
        await pilot.pause(0.4)
        monitor = app.query_one("#workspace-monitor", Static)
        assert "TESTING connected" in str(monitor.content)
        assert app.database_id == "testing-id"
        assert set(app.state.rows) == {RUN}
        assert app.inventory_data is not None and app.inventory_data.registered == 0
        unavailable = True
        await app.check_registry().wait()
        app.action_reload()
        await pilot.pause(0.4)
        assert app.database_id == "testing-id"
        assert "TESTING disconnected" in str(monitor.content)
        assert "UTC" in str(monitor.content) and "Disk" in str(monitor.content)
        assert app.inventory_data is not None and app.inventory_failed
        assert set(app.state.rows) == {RUN}
