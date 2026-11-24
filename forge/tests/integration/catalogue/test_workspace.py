"""Exercise catalogue screens against explicitly supplied disposable registries."""

import asyncio
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.pilot import Pilot
from textual.widgets import Button, Input, Select

from forge.historical.profiles import service as profiles
from forge.shared.config.models import AppSettings
from forge.shared.database.connection import (
    DatabaseError,
    database_connection,
    database_environment,
    pinned_registry,
    registry_id,
)
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.app import VIEWS, ForgeApp
from forge.tui.inspection import ActionMenu, Inspection, RecordTable
from forge.tui.presentation import Results
from forge.tui.screens import Confirm, Detail, Form

pytestmark = [pytest.mark.integration, pytest.mark.database]


async def pause_until(pilot: Pilot[str], predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(10):
        while not predicate():
            await pilot.pause(0.05)


async def form(app: ForgeApp, pilot: Pilot[str], **values: str) -> None:
    await pause_until(pilot, lambda: isinstance(app.screen, Form))
    await pilot.pause()
    for key, value in values.items():
        widget = app.screen.query_one(f"#field-{key}")
        if isinstance(widget, Input):
            widget.value = value
        else:
            cast(Select[str], widget).value = value
    app.screen.query_one("#submit", Button).press()


async def review(app: ForgeApp, pilot: Pilot[str], title: str, approve: bool = True) -> None:
    await pause_until(
        pilot,
        lambda: (
            isinstance(app.screen, Confirm)
            or (isinstance(app.screen, Inspection) and "blocked" in app.screen.heading)
        ),
    )
    screen = app.screen
    assert isinstance(screen, Confirm), getattr(screen, "evidence", None)
    assert screen.title_text == title
    assert ("TESTING" if app.context.testing else "PRODUCTION") in screen.explanation
    assert app.database_id and app.database_id in screen.explanation
    await pilot.pause()
    await pilot.click("#confirm" if approve else "#back")


def context(tmp_path: Path, testing: bool) -> ApplicationContext:
    return ApplicationContext(
        AppSettings(),
        ProjectPaths(tmp_path, tmp_path / "data"),
        None,
        Console(),
        Console(),
        testing=testing,
    )


def use_registries(monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    testing = os.environ.get("MF_DISPOSABLE_CATALOGUE_TEST_DSN")
    production = os.environ.get("MF_DISPOSABLE_CATALOGUE_PROD_DSN")
    if not testing or not production:
        pytest.skip("Supply both disposable catalogue registry DSNs.")
    monkeypatch.setenv("TEST_DATABASE_KEY", testing)
    monkeypatch.setenv("PRODUCTION_DATABASE_KEY", production)
    return testing, production


@pytest.mark.asyncio
@pytest.mark.parametrize("testing", [True, False])
async def test_catalogue_pilot_registration(
    testing: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    test_dsn, prod_dsn = use_registries(monkeypatch)
    app = ForgeApp(context(tmp_path, testing))
    from uuid import uuid4

    code = ("manual_test_" if testing else "manual_prod_") + uuid4().hex[:6]
    symbol = "FIXTURE/USD"
    reference = f"{code}.spot.spot.{symbol}"
    async with app.run_test(size=(120, 34)) as pilot:
        await pause_until(pilot, lambda: bool(app.database_id))
        assert [
            str(button.label) for button in app.query("#navigation Button").results(Button)
        ] == [
            "Overview",
            "Workflows",
            "Data",
            "Markets",
            "Catalogue",
            "Runs",
            "Configs",
        ]
        await pilot.click("#area-4")
        assert [str(button.label) for button in app.query("#views Button").results(Button)] == [
            "Exchanges",
            "Instruments",
            "Specifications",
            "Profiles",
        ]
        assert {
            "Import",
            "Registration",
            "Acquisition",
            "Pipeline",
            "Normalization",
            "Merge",
            "Conversion",
        } <= set(VIEWS["Workflows"])
        assert "Policies" in VIEWS["Markets"]

        await pilot.click("#recipe-new")
        await form(app, pilot, code=code, name="Manual fixture venue")
        await review(app, pilot, "Register Exchange", approve=False)
        await pause_until(pilot, lambda: app.screen == app.root_screen)
        dsn = test_dsn if testing else prod_dsn
        async with database_connection(dsn, testing=testing) as connection:
            assert (
                await (
                    await connection.execute(
                        "SELECT id FROM catalog.exchanges WHERE code=%s", (code,)
                    )
                ).fetchone()
                is None
            )
        await pilot.click("#recipe-new")
        await form(app, pilot, code=code, name="Manual fixture venue")
        await review(app, pilot, "Register Exchange")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection) and app.screen.heading == "Exchange registered"
        await pilot.press("escape")
        await pilot.click("#recipe-new")
        await form(app, pilot, code=code, name="Conflicting fixture venue")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection) and "blocked" in app.screen.heading
        assert "conflicts" in str(app.screen.evidence)
        await pilot.press("escape")
        app.query_one("#filter", Input).value = "Manual fixture venue"
        await pause_until(
            pilot, lambda: any(row.reference == code for row in app.state.rows.values())
        )
        table = app.query_one("#results", Results)
        table.move_cursor(
            row=next(
                index for index, row in enumerate(app.state.rows.values()) if row.reference == code
            )
        )
        table.focus()
        await pilot.press("enter")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection)
        app.screen.links["New Instrument"]()
        await form(
            app,
            pilot,
            exchange=code,
            symbol=symbol,
            instrument_type="spot",
            market_category="spot",
            base_asset="FIXTURE",
            quote_asset="USD",
        )
        await review(app, pilot, "Register Instrument")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection) and app.screen.heading == "Instrument registered"
        await pilot.press("escape")
        assert isinstance(app.screen, Inspection)
        app.screen.links["View instruments"]()
        await pause_until(
            pilot, lambda: any(row.reference == reference for row in app.state.rows.values())
        )
        table.focus()
        await pilot.press("enter")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection)
        instrument = cast(dict[str, object], app.screen.evidence)
        assert isinstance(instrument["historical_instrument_id"], int)
        app.screen.links["Copy canonical instrument ID"]()
        await pause_until(
            pilot, lambda: app.clipboard == str(instrument["historical_instrument_id"])
        )
        app.screen.links["New Specification"]()
        await form(
            app,
            pilot,
            instrument=reference,
            quantity_type="base",
            tick_size="0.000000000000000001",
            qty_step="0.0001",
            provenance="Disposable pilot",
            valid_from="2026-01-01T00:00:00Z",
            valid_to="2026-02-01T00:00:00Z",
        )
        await review(app, pilot, "Register Specification")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert (
            isinstance(app.screen, Inspection) and app.screen.heading == "Specification registered"
        )
        await pilot.press("escape")
        assert isinstance(app.screen, Inspection)
        app.screen.links["View specifications"]()
        await pause_until(
            pilot,
            lambda: (
                app.area == "Catalogue"
                and app.view == "Specifications"
                and any(row.reference == reference for row in app.state.rows.values())
            ),
        )
        await pilot.pause(0.25)
        table = app.query_one("#results", Results)
        table.move_cursor(row=0)
        table.focus()
        await pilot.pause()
        assert app.focused == table
        await pilot.press("enter")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection)
        assert app.screen.sections["Specification"]
        assert "Revisions" in app.screen.sections

        with database_environment(testing=testing):
            async with database_connection(dsn, testing=testing) as connection:
                identity = await registry_id(connection)
        with pinned_registry(identity), pytest.raises(DatabaseError, match="original verified"):
            async with database_connection(prod_dsn if testing else test_dsn, testing=not testing):
                pytest.fail("Cross-registry access bypassed the identity pin")
        async with database_connection(
            prod_dsn if testing else test_dsn, testing=not testing
        ) as other:
            assert (
                await (
                    await other.execute("SELECT id FROM catalog.exchanges WHERE code=%s", (code,))
                ).fetchone()
                is None
            )


@pytest.mark.asyncio
async def test_catalogue_pilot_profile_authoring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    test_dsn, _ = use_registries(monkeypatch)
    from forge.catalog.models import Exchange
    from forge.catalog.service import add

    async with database_connection(test_dsn, testing=True) as connection:
        for code in ("profile_fixture", "profile_clone"):
            await add(connection, Exchange(code=code, name=code))
    root = ProjectPaths.resolve()
    shutil.copytree(root.bin_dir, tmp_path / ".bin")
    app = ForgeApp(context(tmp_path, True))
    async with app.run_test(size=(120, 34)) as pilot:
        await pause_until(pilot, lambda: bool(app.database_id))
        await app.switch("Catalogue", "Profiles")
        await pilot.pause()
        for kind in ("trades", "depth"):
            profile_id = f"profile_fixture.spot.spot.{kind}"
            await pilot.click("#recipe-new")
            await form(app, pilot, reference=profile_id)
            await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
            screen = app.screen
            assert isinstance(screen, Inspection) and "Fields" in screen.sections
            if kind == "trades":
                screen.links["Rust validate and review save"]()
                await pause_until(pilot, lambda previous=screen: app.screen is not previous)
                assert isinstance(app.screen, Inspection) and "blocked" in app.screen.heading
                await pilot.press("escape")
                screen.links["Configure profile fields"]()
                await form(app, pilot, format_code='"PROFILE-FIXTURE-T1"')
            else:
                rows = screen.query_one(RecordTable)
                rows.move_cursor(
                    row=next(
                        index
                        for index, item in enumerate(screen.items)
                        if item["Field"] == "format_code"
                    )
                )
                rows.focus()
                await pilot.press("enter")
                await form(app, pilot, value='"PROFILE-FIXTURE-B1"')
            previous_screen = id(screen)
            await pause_until(
                pilot,
                lambda previous=previous_screen: (
                    isinstance(app.screen, Inspection) and id(app.screen) != previous
                ),
            )
            assert isinstance(app.screen, Inspection)
            app.screen.links["Rust validate and review save"]()
            await review(app, pilot, "Save validated profile")
            await pause_until(
                pilot,
                lambda: (
                    isinstance(app.screen, Inspection)
                    and app.screen.heading == "Profile save result"
                ),
            )
            assert profiles.installed(app.context.paths, profile_id).is_file()
            await pilot.press("escape", "escape")
        app.query_one("#filter", Input).value = "trades csv"
        await pause_until(pilot, lambda: len(app.state.rows) == 1)
        await pilot.press("f2")
        await pause_until(pilot, lambda: isinstance(app.screen, ActionMenu))
        menu = app.screen
        assert isinstance(menu, ActionMenu)
        await pilot.click(f"#menu-{menu.actions.index('Clone Profile')}")
        await form(app, pilot, reference="profile_clone.spot.spot.trades")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection)
        assert "profile_clone.spot.spot.trades" in app.screen.heading
        app.screen.links["Documentation"]()
        await pause_until(pilot, lambda: isinstance(app.screen, Detail))
        assert isinstance(app.screen, Detail) and "author-profile" in app.screen.value_text

        await pilot.press("escape")
        assert isinstance(app.screen, Inspection)
        await exercise_profile_editor(app, pilot, monkeypatch)


async def exercise_profile_editor(
    app: ForgeApp, pilot: Pilot[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.archives.storage.files import disk
    from forge.tui.catalogue import draft_path

    original = profiles.installed(app.context.paths, "profile_fixture.spot.spot.trades")
    original_bytes = original.read_bytes()
    clone = "profile_clone.spot.spot.trades"
    draft = draft_path(app.context.paths, clone)
    launches: list[Path] = []

    async def editor(path: Path, **_: object) -> None:
        assert path != draft and path != original
        launches.append(path)
        data = await disk(path.read_bytes)
        await disk(path.write_bytes, data.replace(b"PROFILE-FIXTURE-T1", b"PROFILE-CLONE-T1"))

    async def editor_review() -> Inspection:
        await pause_until(pilot, lambda: draft in app.editor_sessions and not app.editor_open)
        await pilot.press("f9")
        await pause_until(pilot, lambda: isinstance(app.screen, ActionMenu))
        assert isinstance(app.screen, ActionMenu)
        await pilot.click(f"#menu-{app.screen.actions.index(str(draft))}")
        await pause_until(pilot, lambda: isinstance(app.screen, Inspection))
        assert isinstance(app.screen, Inspection)
        return app.screen

    monkeypatch.setattr("forge.tui.app.edit_in_window", editor)
    assert isinstance(app.screen, Inspection)
    app.screen.links["Edit in Neovim"]()
    screen = await editor_review()
    changed = draft.read_bytes() + b"\n# External update\n"
    draft.write_bytes(changed)
    screen.links["Apply reviewed changes"]()
    await pause_until(pilot, lambda: isinstance(app.screen, Confirm))
    await pilot.click("#confirm")
    await pause_until(pilot, lambda: app.editor_sessions[draft].state == "Apply blocked")
    assert draft.read_bytes() == changed and launches[0].exists()
    await pilot.press("escape")
    screen = await editor_review()
    screen.links["Discard draft"]()
    await pause_until(pilot, lambda: isinstance(app.screen, Confirm))
    await pilot.click("#confirm")
    await pause_until(pilot, lambda: draft not in app.editor_sessions)
    await pilot.press("escape")
    assert isinstance(app.screen, Inspection)
    app.screen.links["Edit in Neovim"]()
    screen = await editor_review()
    screen.links["Apply reviewed changes"]()
    await pause_until(pilot, lambda: isinstance(app.screen, Confirm))
    await pilot.click("#confirm")
    await pause_until(
        pilot,
        lambda: (
            isinstance(app.screen, Inspection)
            and app.screen.heading.startswith("Profile draft")
            and draft not in app.editor_sessions
        ),
    )
    assert isinstance(app.screen, Inspection)
    app.screen.links["Rust validate and review save"]()
    await review(app, pilot, "Save validated profile")
    await pause_until(
        pilot,
        lambda: isinstance(app.screen, Inspection) and app.screen.heading == "Profile save result",
    )
    assert len(launches) == 2 and not any(path.exists() for path in launches)
    assert original.read_bytes() == original_bytes
    assert profiles.installed(app.context.paths, clone).read_bytes() == draft.read_bytes()
