"""Bounded end-to-end Forge pipeline using real Rust and PostgreSQL."""

import asyncio
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest

from forge.catalog.models import Instrument, Specification
from forge.catalog.service import add
from forge.cli.commands import pipeline as pipeline_cli
from forge.cli.main import main
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import CachedRemoteArchive, register_remote_archive
from forge.historical.archives.storage.acquire import acquire
from forge.historical.engine.managed import checksum, resolve
from forge.historical.merge import service as merge
from forge.historical.normalization import execution as normalize
from forge.historical.pipelines.config import load_recipe
from forge.historical.pipelines.terminal import Window
from forge.shared.database.connection import database_environment
from forge.shared.database.testing import testing_connection as registry_connection
from forge.shared.database.testing import testing_dsn as registry_dsn
from forge.shared.http.client import open_http_client
from forge.shared.paths import ProjectPaths
from forge.startup.context import build_context
from forge.tui.actions import BY_KEY

from .test_acquisition_execution import ImmediateLimiter, install_http

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.parametrize("interface", ["cli", "cli-convert", "tui", "tui-publications"])
def test_prepared_stop_resume_and_new_run(
    workspace: Path,
    interface: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if os.getenv("MARKETFORGE_TEST_PIPELINE") != "1":
        pytest.skip("Set MARKETFORGE_TEST_PIPELINE=1 for managed engines and the testing registry.")
    root = ProjectPaths.resolve()
    dsn = registry_dsn(root.root)
    monkeypatch.setenv("TEST_DATABASE_KEY", dsn)
    library = workspace / "data/.config/workflows/data_engineering/pipeline"
    library.mkdir(parents=True)
    engine_config = workspace / "data/.config/engine"
    engine_config.mkdir()
    convert = interface == "cli-convert"
    runtime_files = ["normalize.toml", "merge.toml", "pipeline.toml"]
    targets = ["normalize", "merge"]
    if convert or interface == "tui-publications":
        runtime_files.append("convert.toml")
        targets.append("convert")
    for name in runtime_files:
        shutil.copyfile(root.engine_config_dir / name, engine_config / name)
    shutil.copytree(root.engine_config_dir / "normalize", engine_config / "normalize")
    bin_dir = workspace / ".bin"
    bin_dir.mkdir()
    for target in targets:
        engine = resolve(root, target)
        shutil.copy2(engine, bin_dir / engine.name)
    manifest = json.loads((root.bin_dir / "manifest.json").read_bytes())
    if merge_override := os.environ.get("MARKETFORGE_TEST_MERGE_BINARY"):
        fixture_binary = bin_dir / "marketforge-merge"
        shutil.copy2(merge_override, fixture_binary)
        manifest["targets"]["merge"]["sha256"] = checksum(fixture_binary)
    if convert:
        for target in targets:
            manifest["targets"][target]["features"] = []
    (bin_dir / "manifest.json").write_text(json.dumps(manifest))
    storage = workspace / "output"
    settings = workspace / "data/.config/forge/marketforge.toml"
    settings.parent.mkdir()
    settings.write_text(f"[storage]\ndata_root={json.dumps(str(storage))}\n")
    symbol = f"MFP{uuid4().hex[:12].upper()}USDT"
    start = datetime(2026, 7, 9, tzinfo=UTC)
    end = datetime(2026, 7, 10, tzinfo=UTC)

    async def seed() -> None:
        async with registry_connection(dsn) as connection:
            await add(
                connection,
                Instrument(
                    exchange="bybit",
                    instrument_type="spot",
                    market_category="spot",
                    symbol=symbol,
                    base_asset="BTC",
                    quote_asset="USDT",
                ),
            )
            await add(
                connection,
                Specification(
                    instrument=f"bybit.spot.spot.{symbol}",
                    quantity_type="base",
                    tick_size=Decimal("0.01"),
                    valid_from=start,
                    valid_to=end,
                    provenance="Synthetic bounded pipeline integration fixture",
                ),
            )

    asyncio.run(seed())
    recipe = library / "tiny.toml"
    recipe.write_text(
        """version = 1
[interval]
start = 2026-07-09T00:00:00Z
end = 2026-07-10T00:00:00Z
[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }
""".replace("BTCUSDT", symbol)
    )
    if convert:
        with recipe.open("a") as stream:
            stream.write('\n[convert]\ntarget_quote = "USDT"\nmax_staleness_ns = 60000000000\n')
    name = f"{symbol}-2026-07-09.csv.gz"
    stamp = int(datetime(2026, 7, 9, 12, tzinfo=UTC).timestamp()) * 1000
    body = gzip.compress(
        f"id,timestamp,price,volume,side,rpi\n7,{stamp},200,3,buy,0\n8,{stamp + 1},201,1,sell,0\n".encode()
    )
    calls: list[str] = []

    def http(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/"):
            return httpx.Response(
                200, text=f'<pre><a href=../>../</a><a href="{name}">archive</a></pre>'
            )
        assert request.url.path.endswith(name)
        return httpx.Response(200, stream=httpx.ByteStream(body))

    install_http(monkeypatch, http)
    args = ["--project-root", str(workspace), "--json", "pipeline"]

    fixture_worker = workspace / "fixture-worker.py"
    if convert or interface == "tui":
        (workspace / "archive.csv.gz").write_bytes(body)
        fixture_worker.write_text(
            f"""import sys
from contextlib import asynccontextmanager
from pathlib import Path
import httpx
from forge.historical.archives import operation
from forge.historical.pipelines.stage_worker import main
original_client = operation.open_configured_http_client

def http(request):
    if request.url.path.endswith("/"):
        return httpx.Response(200, text='<pre><a href=../>../</a><a href="{name}">archive</a></pre>')
    assert request.url.path.endswith({name!r})
    data = Path({str(workspace / "archive.csv.gz")!r}).read_bytes()
    return httpx.Response(200, stream=httpx.ByteStream(data))

@asynccontextmanager
async def client(settings, **options):
    async with original_client(settings, transport=httpx.MockTransport(http)) as http_client:
        yield http_client

operation.open_configured_http_client = client
raise SystemExit(main(Path(sys.argv[1])))
"""
        )

    launched: list[str] = []

    class ProcessTerminal:
        name = "Fixture terminal"

        async def launch(self, request: Path, *, cwd: Path, title: str) -> Window:
            stage = title.split(" · ")[1]
            launched.append(stage)
            command = (
                [str(fixture_worker)]
                if stage == "Acquisition"
                else ["-m", "forge.historical.pipelines.stage_worker"]
            )
            with (workspace / "fixture-window.log").open("ab") as logs:
                return await asyncio.create_subprocess_exec(
                    sys.executable,
                    *command,
                    str(request),
                    cwd=cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=logs,
                    stderr=logs,
                )

    def invoke(*command: str) -> tuple[int, dict[str, Any]]:
        if convert and (command[0] == "resume" or "--stop-after" in command):
            with monkeypatch.context() as launcher:
                launcher.setattr(pipeline_cli.Ghostty, "detect", ProcessTerminal)
                exit_code = main([*args, *command, "--stage-terminal", "window"])
        else:
            exit_code = main([*args, *command])
        output = capsys.readouterr()
        assert not output.err, output.err
        return exit_code, json.loads(output.out)

    code, plan = invoke("plan", "tiny", "--testing")
    assert code == 0 and plan["state"] == "planned", json.dumps(plan)
    assert not calls and not storage.exists()

    async def register_unrelated(
        instrument: str = symbol,
        content: bytes = body,
        *,
        exchange_quote: bool = False,
        depth: bool = False,
    ) -> int:
        config = load_recipe(recipe.read_bytes())
        registered_name = name.replace(symbol, instrument)
        if depth:
            registered_name = registered_name.replace(".csv.gz", ".zip")
        scoped = config.requests[0].model_copy(
            update={
                "scope": config.requests[0].scope.model_copy(update={"identifier": instrument}),
                "data_type": "order_book_l2" if depth else "trades",
            }
        )
        archive = DiscoveredArchive(
            request=scoped,
            interval=config.interval,
            filename=registered_name,
            source_object_key=f"unrelated/{registered_name}",
            download_url=f"https://fixture.test/{registered_name}",
        )
        async with registry_connection(dsn) as connection:
            if exchange_quote:
                await add(
                    connection,
                    Instrument(
                        exchange="bybit",
                        instrument_type="spot",
                        market_category="spot",
                        symbol=instrument,
                        base_asset="USDT",
                        quote_asset="USDC",
                    ),
                )
                await add(
                    connection,
                    Specification(
                        instrument=f"bybit.spot.spot.{instrument}",
                        quantity_type="base",
                        tick_size=Decimal("0.01"),
                        valid_from=start,
                        valid_to=end,
                        provenance="Synthetic bounded external reference fixture",
                    ),
                )
            async with connection.transaction():
                remote = await register_remote_archive(connection, archive)
            async with open_http_client(
                limiter=ImmediateLimiter(1),
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, stream=httpx.ByteStream(content))
                ),
            ) as client:
                result = await acquire(
                    connection,
                    client,
                    CachedRemoteArchive(
                        remote.id,
                        archive.source_object_key,
                        registered_name,
                        start,
                        end,
                        archive.download_url,
                        None,
                        None,
                        None,
                    ),
                    storage / "raw",
                )
        return result.local_archive_id

    unrelated = asyncio.run(register_unrelated())
    if interface == "tui-publications":
        reference_symbol = f"MFR{uuid4().hex[:12].upper()}USDC"
        body_reference = gzip.compress(
            f"id,timestamp,price,volume,side,rpi\n21,{stamp - 100},1.01,3,buy,0\n".encode()
        )
        reference_archive = asyncio.run(
            register_unrelated(
                reference_symbol,
                body_reference,
                exchange_quote=True,
            )
        )
        depth_data = io.BytesIO()
        with zipfile.ZipFile(depth_data, "w") as archive:
            archive.writestr(
                "depth.data",
                json.dumps(
                    {
                        "topic": f"orderbook.50.{symbol}",
                        "ts": stamp,
                        "cts": stamp,
                        "type": "snapshot",
                        "data": {
                            "s": symbol,
                            "a": [["202", "1"]],
                            "b": [["198", "1"]],
                            "u": 1,
                            "seq": 1,
                        },
                    }
                )
                + "\n",
            )
        depth_archive = asyncio.run(register_unrelated(content=depth_data.getvalue(), depth=True))
        asyncio.run(
            interact_publications(
                workspace, (unrelated, depth_archive), reference_archive, reference_symbol, symbol
            )
        )
        return
    from textual.widgets import Input, Select

    async def interact(stop: bool) -> dict[str, Any]:
        from forge.historical.pipelines.progress import PipelineSnapshot
        from forge.tui.app import ForgeApp
        from forge.tui.inspection import Inspection

        app = ForgeApp(build_context(project_root=workspace, testing=True))
        async with app.run_test(size=(105, 28)) as pilot:
            for key in ("pipeline validate", "pipeline plan"):
                app.open_action(BY_KEY[key])
                await pilot.pause()
                app.screen.query_one("#field-reference", Input).value = "tiny"
                await pilot.click("#submit")
                async with asyncio.timeout(20):
                    while not isinstance(app.screen, Inspection):
                        await pilot.pause(0.03)
                operation = app.controller.current
                assert operation and operation.title == key and operation.error is None
                await pilot.press("escape")
            app.open_action(BY_KEY["pipeline run"])
            await pilot.pause()
            app.screen.query_one("#field-reference", Input).value = "tiny"
            cast(Select[str], app.screen.query_one("#field-stage_terminal")).value = "window"
            if stop:
                cast(
                    Select[str], app.screen.query_one("#field-stop_after")
                ).value = "normalize.prepare"
            await pilot.click("#submit")
            await pilot.pause()
            await pilot.click("#confirm")
            async with asyncio.timeout(25):
                while not app.controller.current or app.controller.current.title != "pipeline run":
                    await pilot.pause(0.03)
            assert app.controller.task is not None
            from forge.tui.operation_view import OperationView

            await pilot.pause()
            if isinstance(app.screen, OperationView):
                await pilot.click("#op-back")
            await app.switch("Data", "Raw")
            assert app.area == "Data" and app.screen is app.root_screen
            operation = await asyncio.wait_for(asyncio.shield(app.controller.task), timeout=30)
            assert not operation.error, operation.error
            assert isinstance(operation.snapshot(), PipelineSnapshot)
            await pilot.press("ctrl+o")
            await pilot.pause()
            assert app.screen is not app.root_screen
            return cast(dict[str, Any], operation.result)

    if interface == "tui":
        with monkeypatch.context() as launcher:
            launcher.setattr(pipeline_cli.Ghostty, "detect", ProcessTerminal)
            stopped, code = asyncio.run(interact(stop=True)), 0
        assert "Acquisition" in launched
    else:
        code, stopped = invoke("run", "tiny", "--testing", "--stop-after", "normalize.prepare")
    assert code == 0, stopped.get("error")
    assert stopped["state"] == "stopped"
    run_id = str(stopped["run_id"])
    prepared = stopped["stages"]["normalize"]["prepared"]
    selected = json.loads(Path(prepared["report"]).read_text())["requests"][0]
    assert selected["request_index"] == 1
    assert len(selected["selected_archives"]) == 1
    assert selected["selected_archives"][0]["local_id"] != unrelated
    acquire_stage = stopped["stages"]["acquire"]
    child = json.loads(Path(acquire_stage["child_report"]).read_text())
    assert child["requests"][0]["index"] == 0
    pin = child["selection"]["archives"][0]
    assert pin["local_archive_id"] == selected["selected_archives"][0]["local_id"]
    assert stopped["stages"]["merge"]["state"] == "pending"

    original_recipe = recipe.read_bytes()
    recipe.write_text("version = 999")
    (engine_config / "normalize.toml").write_text("deliberately invalid")

    pinned_input = storage / "raw" / pin["relative_path"]
    isolated = pinned_input.with_suffix(".held")
    pinned_input.rename(isolated)
    try:
        code, missing = invoke("resume", run_id, "--testing")
        assert code != 0
        assert "unavailable" in missing["error"]
        assert missing["stages"]["merge"]["state"] == "pending"
    finally:
        isolated.rename(pinned_input)

    async def unavailable(*_: object, **__: object) -> dict[str, str]:
        return {
            "registration": "pending",
            "registration_receipt": "injected",
            "error": "Simulated registration outage",
        }

    with monkeypatch.context() as fixture:
        fixture.setattr(normalize, "register_verified", unavailable)
        code, registration_pending = invoke("resume", run_id, "--testing")
    assert code == 2, registration_pending.get("error")
    assert registration_pending["stages"]["normalize"]["result"]["verification"] == "verified"
    assert registration_pending["stages"]["merge"]["state"] == "pending"
    normalization_intent = registration_pending["stages"]["normalize"]["execution_intent"]

    with monkeypatch.context() as fixture:
        fixture.setattr(merge, "register_verified", unavailable)
        code, merge_pending = invoke("resume", run_id, "--testing")
    assert code == 2, merge_pending.get("error")
    assert merge_pending["stages"]["normalize"]["execution_intent"] == normalization_intent
    assert merge_pending["stages"]["merge"]["result"]["verification"] == "verified"
    merge_intent = merge_pending["stages"]["merge"]["execution_intent"]
    code, finished = invoke("resume", run_id, "--testing")
    assert code == 0, finished.get("error")
    assert finished["state"] == "complete"
    assert finished.get("execution_registry_status") == "finalized"
    stages = cast(dict[str, dict[str, Any]], finished["stages"])
    assert stages["merge"]["execution_intent"] == merge_intent
    assert stages["normalize"]["execution_intent"] == normalization_intent
    assert stages["normalize"]["primary_file_ids"] == stages["merge"]["upstream_file_ids"]
    assert len(stages["merge"]["primary_file_ids"]) == 1
    merged_path = Path(json.loads(Path(merge_intent).read_text())["path"])
    assert json.loads((merged_path / "manifest.json").read_text())["events_written"] == 2
    if convert:
        converted = stages["convert"]["result"]
        assert converted["status"] == "complete"
        assert len(converted["jobs"]) == 1
        job = converted["jobs"][0]
        assert len(job["work_key"]) == 64 and job["registration"] == "not_available"
        manifest = json.loads(Path(job["publication"]["manifest"]).read_text())
        assert manifest["statistics"]["identity"] >= 2
        assert launched == ["Acquisition", "Normalization", "Merge", "Conversion"]
        code, resumed = invoke("resume", run_id, "--testing")
        assert code == 0 and resumed["state"] == "complete"
        assert launched == ["Acquisition", "Normalization", "Merge", "Conversion"]

        async def indexed_output() -> None:
            from forge.orchestration.registry import show_index

            with database_environment(testing=True):
                record = await show_index(ProjectPaths(workspace, storage), run_id)
            assert record is not None and record["status"] == "completed"
            assert len(record["attempts"]) == 5
            assert any(
                item["stage"] == "convert"
                and item["kind"] == "conversion_output_ref"
                and item["reference"] == job["work_key"]
                for item in record["resources"]
            )

        asyncio.run(indexed_output())

        async def parent_identity() -> str:
            async with registry_connection(dsn) as connection:
                cursor = await connection.execute(
                    "SELECT parent_run_id FROM historical.execution_runs WHERE run_id=%s",
                    (acquire_stage["child_run_id"],),
                )
                record = await cursor.fetchone()
                assert record is not None
                return str(record[0])

        assert asyncio.run(parent_identity()) == run_id
        output = (workspace / "fixture-window.log").read_text()
        assert "MarketForge · Normalization" in output and "MarketForge · Conversion" in output
        assert "Forge is checking and recording the output" in output

    recipe.write_bytes(original_recipe)
    shutil.copyfile(root.engine_config_dir / "normalize.toml", engine_config / "normalize.toml")
    with monkeypatch.context() as fixture:

        def unexpected_execution(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("Equivalent pipeline work launched a materializing engine.")

        fixture.setattr(normalize, "supervise", unexpected_execution)
        fixture.setattr(merge, "supervise", unexpected_execution)
        if interface == "tui":
            fixture.setattr(pipeline_cli.Ghostty, "detect", ProcessTerminal)
            second, code = asyncio.run(interact(stop=False)), 0
        else:
            code, second = invoke("run", "tiny", "--testing")
    assert code == 0, second.get("error")
    assert second["run_id"] != run_id
    new_stages = cast(dict[str, dict[str, Any]], second["stages"])
    for stage in ("normalize", "merge"):
        assert stages[stage]["primary_file_ids"] == new_stages[stage]["primary_file_ids"]
        assert new_stages[stage]["result"]["disposition"] == "reused"
    if convert:
        second_conversion = new_stages["convert"]["result"]
        assert second_conversion["disposition"] == "reused"
        assert (
            second_conversion["jobs"][0]["work_key"]
            == stages["convert"]["result"]["jobs"][0]["work_key"]
        )
    assert sum(path.endswith(name) for path in calls) == (0 if convert or interface == "tui" else 1)

    if interface == "cli":
        original = Path(normalization_intent).with_name("submitted-batch.json")
        prepared = json.loads(original.read_bytes())
        first_job = prepared["jobs"][0]
        second_id = (uuid4().int & (2**62 - 1)) + 1
        new_output = storage / "datasets" / "normalized" / str(second_id)
        another_job = {
            **first_job,
            "job_id": second_id,
            "dataset_id": second_id,
            "tasks": [
                {**task, "stream_rank": task["stream_rank"] + 1} for task in first_job["tasks"]
            ],
            "output": {
                **first_job["output"],
                "dataset_path": str(new_output),
                "staging_path": str(new_output.with_suffix(".staging")),
            },
        }
        requested = workspace / "mixed-batch.json"
        prepared["jobs"].append(another_job)
        requested.write_text(json.dumps(prepared))
        supervisor = normalize.supervise
        submitted_counts: list[int] = []

        async def record(argv: list[str], root: Path, directory: Path) -> tuple[int, bool]:
            source = Path(argv[argv.index("--batch") + 1])
            submitted_counts.append(
                len(json.loads(await asyncio.to_thread(source.read_bytes))["jobs"])
            )
            return await supervisor(argv, root, directory)

        with monkeypatch.context() as fixture, database_environment(testing=True):
            fixture.setattr(normalize, "supervise", record)
            mixed = asyncio.run(
                normalize.execute_async(ProjectPaths(workspace, storage), requested)
            )
        assert mixed["status"] == "complete", mixed.get("error")
        assert mixed["disposition"] == "mixed" and submitted_counts == [1]
        assert mixed["counts"]["requested"] == 2
        assert mixed["counts"]["reused"] == 1
        assert mixed["counts"]["executed"] == 1
        assert set(stages["normalize"]["primary_file_ids"]) < set(
            mixed["catalogue_dataset_ids"].values()
        )
        assert new_output.is_dir()


async def interact_publications(
    workspace: Path,
    archive_ids: tuple[int, int],
    reference_archive: int,
    reference_symbol: str,
    market_symbol: str,
) -> None:
    """Drive registered inputs through the actual Textual handlers and Engines."""
    from textual.widgets import Button, Checkbox, DataTable, Input, Select

    from forge.tui.app import ForgeApp
    from forge.tui.inspection import ActionMenu, Inspection
    from forge.tui.presentation import Results
    from forge.tui.screens import Confirm, Detail, Form, StreamOrder

    app = ForgeApp(build_context(project_root=workspace, testing=True))
    app.initial_area, app.initial_view = "Data", "Raw"

    async with app.run_test(size=(145, 40)) as pilot:

        async def until(predicate: Any) -> None:
            async with asyncio.timeout(20):
                while not predicate():
                    if isinstance(app.screen, Detail):
                        pytest.fail(app.screen.value_text)
                    await pilot.pause(0.05)

        async def choose(label: str) -> None:
            await pilot.press("f2")
            await until(lambda: isinstance(app.screen, ActionMenu))
            menu = app.screen
            await pilot.click(f"#menu-{menu.actions.index(label)}")
            await pilot.pause()

        async def submit(*, confirm: bool = False, **fields: str) -> None:
            await until(lambda: isinstance(app.screen, Form))
            for key, value in fields.items():
                app.screen.query_one(f"#field-{key}", Input).value = value
            app.screen.query_one("#submit", Button).focus()
            await pilot.press("enter")
            await pilot.pause()
            if confirm:
                await until(lambda: isinstance(app.screen, (Confirm, StreamOrder)))
                if isinstance(app.screen, StreamOrder):
                    await pilot.click("#order-use")
                    await until(lambda: isinstance(app.screen, Confirm))
                await pilot.click("#confirm")
                await pilot.pause()

        async def result(key: str) -> dict[str, Any]:
            await until(
                lambda: app.controller.current is not None and app.controller.current.title == key
            )
            assert app.controller.task is not None
            operation = await asyncio.wait_for(asyncio.shield(app.controller.task), timeout=45)
            assert not operation.error, operation.error
            value = cast(dict[str, Any], operation.result)
            assert value.get("status") in {"prepared", "complete", "valid", "generated"}, value
            return value

        async def open_inventory(view: str, identity: str) -> None:
            while app.screen != app.root_screen:
                await pilot.press("escape")
                await pilot.pause()
            await app.switch("Data", view)
            app.state.filter = identity
            app.request_query()
            await until(lambda: any(row.reference == identity for row in app.state.rows.values()))
            index = next(
                index
                for index, row in enumerate(app.state.rows.values())
                if row.reference == identity
            )
            table = app.query_one("#results", Results)
            table.move_cursor(row=index)
            table.focus()
            await pilot.press("enter")
            await until(lambda: isinstance(app.screen, Inspection))

        from forge.tui.configuration_screen import ConfigurationEditor, FieldEditor

        await app.switch("Configs", "Pipeline")
        editor = app.query_one(ConfigurationEditor)
        await until(lambda: bool(editor.visible_fields))
        editor.query_one("#configuration-search", Input).value = "normalize.resources.workers"
        await until(lambda: len(editor.visible_fields) == 1)
        editor.query_one("#configuration-fields", DataTable).focus()
        await pilot.press("enter")
        await until(lambda: isinstance(app.screen, FieldEditor))
        app.screen.query_one("#setting-input", Input).value = "1"
        await pilot.click("#setting-update")
        await until(lambda: editor.dirty and not editor.editing)
        await pilot.click("#apply")
        await until(lambda: isinstance(app.screen, Confirm))
        await pilot.click("#confirm")
        await until(
            lambda: (
                not editor.dirty
                and not editor.loading
                and any(
                    field["path"] == "normalize.resources.workers"
                    and field["configured"] == field["effective"] == 1
                    for _, field in editor.visible_fields.values()
                )
            )
        )

        # Market handoff uses a real catalogue instrument. Acquisition Plan is the cached preflight.
        await app.switch("Markets", "Instruments")
        app.state.filter = market_symbol
        app.request_query()
        await until(lambda: any(row.name == market_symbol for row in app.state.rows.values()))
        market = next(row for row in app.state.rows.values() if row.name == market_symbol)
        table = app.query_one("#results", Results)
        table.move_cursor(row=list(app.state.rows).index(market.id))
        table.focus()
        await choose("Add to acquisition…")
        await submit(
            confirm=True,
            reference="market-selected",
            start="2026-07-09T00:00:00Z",
            end="2026-07-10T00:00:00Z",
        )
        await until(lambda: isinstance(app.screen, Inspection))
        await choose("Validate")
        await result("acquisition validate")
        await pilot.press("escape")
        await choose("Plan")
        await submit()
        await until(
            lambda: (
                app.controller.current is not None
                and app.controller.current.title == "acquisition plan"
            )
        )
        assert app.controller.task is not None
        acquisition = await asyncio.shield(app.controller.task)
        assert acquisition.error is None
        await until(lambda: isinstance(app.screen, Inspection))
        await pilot.press("escape")
        await pilot.press("escape")
        await app.switch("Data", "Raw")

        def selected_windows() -> dict[str, int]:
            return {
                reference: index
                for index, row in enumerate(app.state.rows.values())
                for reference in cast(dict[str, Any], row.value)["archives"]
            }

        await until(
            lambda: all(f"local:{archive}" in selected_windows() for archive in archive_ids)
        )
        table = app.query_one("#results", Results)
        for index in dict.fromkeys(
            selected_windows()[f"local:{archive}"] for archive in archive_ids
        ):
            table.move_cursor(row=index)
            table.focus()
            await pilot.press("space")
        await choose("Normalize selected archives…")
        await until(lambda: isinstance(app.screen, Form))
        for field in app.screen.spec.fields:
            if field.kind == "choice":
                cast(
                    Select[str], app.screen.query_one(f"#field-{field.key}")
                ).value = field.choices[1]
        await submit(confirm=True, reference="exact-archive")
        await until(lambda: isinstance(app.screen, Inspection))
        await choose("Validate")
        assert (await result("normalize validate"))["status"] == "valid"
        await pilot.press("escape")
        await choose("Prepare")
        await submit(confirm=True)
        prepared = await result("normalize prepare")
        assert prepared["engine_ready"]
        assert set(pin[0] for pin in prepared["exact_raw_inputs"].values()) == set(archive_ids)
        await pilot.press("escape")
        app.open_action(BY_KEY["normalize run"], None, {"reference": prepared["batch_path"]})
        await submit(confirm=True)
        normalized = await result("normalize run")
        registered_files = [job["catalogue_dataset_id"] for job in normalized["registrations"]]
        assert len(registered_files) == 2
        assert all(job["registration"] == "registered" for job in normalized["registrations"])

        async def publication_for(file_id: Any) -> str:
            async with registry_connection(registry_dsn(workspace)) as connection:
                row = await (
                    await connection.execute(
                        "SELECT publication_id FROM historical.datasets WHERE id=%s", (file_id,)
                    )
                ).fetchone()
            assert row is not None
            return str(row[0])

        normal_ids = [await publication_for(file_id) for file_id in registered_files]
        await open_inventory("Normalized", normal_ids[0])
        await pilot.press("escape")
        table = app.query_one("#results", Results)
        for identity in normal_ids:
            app.query_one("#filter", Input).value = identity
            await until(
                lambda expected=identity: any(
                    row.reference == expected for row in app.state.rows.values()
                )
            )
            table.move_cursor(
                row=next(
                    index
                    for index, row in enumerate(app.state.rows.values())
                    if row.reference == identity
                )
            )
            table.focus()
            await pilot.press("space")
        await choose("Create merge from selection…")
        await submit(confirm=True, reference="exact-publication")
        await until(
            lambda: isinstance(app.screen, Inspection) and app.screen.heading == "Recipe saved"
        )
        await choose("Validate")
        validated = await result("merge validate")
        assert validated["recipe"]["publication_ids"] == [int(identity) for identity in normal_ids]
        assert validated["recipe"]["dataset_ids"] == registered_files
        await pilot.press("escape")
        await choose("Prepare")
        await submit(confirm=True)
        prepared_merge = await result("merge prepare")
        await pilot.press("escape")
        app.open_action(BY_KEY["merge run"], None, {"reference": prepared_merge["job_path"]})
        await submit(confirm=True)
        merged = await result("merge run")
        assert merged["verification"] == "verified" and merged["registration"] == "registered"
        merge_id = await publication_for(next(iter(merged["catalogue_dataset_ids"].values())))

        await open_inventory("Merged", merge_id)
        assert isinstance(app.screen, Inspection)
        await pilot.click(f"#section-{tuple(app.screen.sections).index('Files')}")
        table = app.screen.query_one(DataTable)
        table.focus()
        await pilot.press("enter")
        await until(
            lambda: (
                isinstance(app.screen, Inspection) and app.screen.heading.startswith("Primary file")
            )
        )
        await choose("Copy primary-file ID")
        await until(lambda: isinstance(app.screen, Detail))
        await pilot.press("ctrl+y")
        assert app.clipboard == str(next(iter(merged["catalogue_dataset_ids"].values())))
        await pilot.press("escape")
        await choose("Physical partitions")
        await until(
            lambda: (
                isinstance(app.screen, Inspection) and app.screen.heading == "Physical partitions"
            )
        )
        app.screen.query_one(DataTable).focus()
        await pilot.press("enter")
        await until(
            lambda: isinstance(app.screen, Inspection) and "Query preview…" in app.screen.links
        )
        await choose("Query preview…")
        await submit()
        await until(
            lambda: isinstance(app.screen, Inspection) and app.screen.heading == "datasets head"
        )
        assert app.screen.items or app.screen.evidence.get("rows")
        for _ in range(3):
            await pilot.press("escape")
            await pilot.pause()
        assert isinstance(app.screen, Inspection) and app.screen.heading.startswith("Primary file")
        await choose("File lineage")
        await submit()
        await until(
            lambda: isinstance(app.screen, Inspection) and app.screen.heading == "datasets lineage"
        )
        assert app.screen.evidence.get("parents")
        await pilot.press("escape")

        async def convert_publication(
            name: str, quote: str, external_id: str = ""
        ) -> dict[str, Any]:
            await open_inventory("Merged", merge_id)
            await choose("Convert publication…")
            await until(lambda: isinstance(app.screen, Form))
            await submit(confirm=True, reference=name, quote=quote)
            await until(
                lambda: isinstance(app.screen, Inspection) and app.screen.heading == "Recipe saved"
            )
            if external_id:
                await choose("Browse external trade references…")
                await submit(search=reference_symbol)
                await until(
                    lambda: (
                        isinstance(app.screen, Form)
                        and app.screen.preset.get("search") == reference_symbol
                    )
                )
                form = app.screen
                options = next(
                    field.choices for field in form.spec.fields if field.key == "publication"
                )
                option = next(
                    entry for entry in options if entry.startswith(f"Registry {external_id} ·")
                )
                cast(Select[str], form.query_one("#field-publication")).value = option
                await submit()
                await until(
                    lambda: (
                        isinstance(app.screen, Form)
                        and app.screen.spec.key.startswith("Trade streams")
                    )
                )
                picked = app.screen.spec.fields[0].label
                app.screen.query_one("#field-stream-0", Checkbox).value = True
                await submit()
                await until(
                    lambda: (
                        isinstance(app.screen, Form)
                        and "Edit conversion policy" in app.screen.spec.key
                    )
                )
                assert app.screen.preset["reference_publication_id"] == int(external_id)
                assert app.screen.preset["reference_stream_ids"] == picked
                await submit(confirm=True)
                await until(
                    lambda: (
                        isinstance(app.screen, Inspection)
                        and app.screen.heading == "Conversion recipe updated"
                    )
                )
            await choose("Validate")
            await result("convert validate")
            await pilot.press("escape")
            await choose("Prepare")
            await submit(confirm=True)
            prepared = await result("convert prepare")
            assert prepared["compatibility"] == "validated", prepared.get("compatibility_blockers")
            if external_id:
                reference = prepared["jobs"][0]["references"][0]
                assert reference["provider"] == "external"
                assert reference["stream_id"] == picked
                assert reference["merge_registry_publication_id"] == int(external_id)
            await pilot.press("escape")
            app.open_action(BY_KEY["convert run"], None, {"reference": prepared["job_path"]})
            await submit(confirm=True)
            converted = await result("convert run")
            output = converted["jobs"][0]
            assert output["execution"] == "complete"
            assert output["verification"] == "verified" and output["indexing"] == "complete"
            assert output["registration"] == "not_available"
            return output

        identity = await convert_publication("identity-conversion", "USDT")
        manifest = json.loads(
            await asyncio.to_thread(Path(identity["publication"]["manifest"]).read_bytes)
        )
        assert manifest["statistics"]["identity"] >= 2
        await open_inventory("Converted", identity["work_key"])
        assert isinstance(app.screen, Inspection)
        assert app.screen.sections["Summary"]["indexing"] == "complete"
        await choose("Open producing run")
        await until(lambda: isinstance(app.screen, Inspection))
        assert app.screen.heading.startswith("convert-execute-")
        await choose(f"Open conversion output {identity['work_key']}")
        await until(
            lambda: (
                isinstance(app.screen, Inspection)
                and app.screen.heading == f"Conversion output {identity['work_key']}"
            )
        )
        assert app.screen.sections["Summary"]["verification"] == "verified"

        from forge.historical.normalization.batch import inspect_exact_archives, prepare_batch
        from forge.historical.normalization.config import ExactRecipe

        paths = app.context.paths
        with database_environment(testing=True):
            registry, archives = await inspect_exact_archives(paths, (reference_archive,))
            reference_recipe = ExactRecipe.model_validate(
                {
                    "registry_id": registry,
                    "archive_pins": [
                        {
                            "archive_id": reference_archive,
                            "selection_sha256": archives[0]["selection_sha256"],
                        }
                    ],
                }
            )
            prepared_ref = await prepare_batch(paths, reference_recipe, supervised_validation=True)
            assert prepared_ref["engine_ready"], prepared_ref
            reference_result = await normalize.execute_async(
                paths, Path(str(prepared_ref["batch_path"]))
            )
            reference_file = reference_result["registrations"][0]["catalogue_dataset_id"]
            reference_normal_id = await publication_for(reference_file)
            merge_recipe, _ = await merge.publication_recipe(paths, [int(reference_normal_id)])
            ref_prepared_merge = await merge.prepare_async(
                paths, merge_recipe, "external-reference"
            )
            reference_merged = await merge.execute_async(
                paths, Path(ref_prepared_merge["job_path"])
            )
            assert reference_merged["registration"] == "registered"
        external_id = await publication_for(
            next(iter(reference_merged["catalogue_dataset_ids"].values()))
        )
        external = await convert_publication("external-conversion", "USDC", external_id)
        manifest = json.loads(
            await asyncio.to_thread(Path(external["publication"]["manifest"]).read_bytes)
        )
        assert manifest["statistics"]["converted"] >= 2
        await open_inventory("Converted", external["work_key"])
        assert isinstance(app.screen, Inspection)
        assert app.screen.sections["Summary"]["target_quote"] == "USDC"
        if os.getenv("MARKETFORGE_TEST_CLEANUP_DISPOSABLE") == "1":
            await choose("Preview output deletion…")
            await until(
                lambda: (
                    isinstance(app.screen, Inspection)
                    and app.screen.heading == "Testing publication deletion plan"
                )
            )
            await choose("Apply reviewed deletion…")
            await until(lambda: isinstance(app.screen, Confirm))
            await pilot.press("escape")
            assert await asyncio.to_thread(Path(external["publication"]["manifest"]).is_file)
            await choose("Apply reviewed deletion…")
            await until(lambda: isinstance(app.screen, Confirm))
            await pilot.click("#confirm")
            await until(
                lambda: (
                    app.controller.current is not None
                    and app.controller.current.title == "datasets delete"
                )
            )
            assert app.controller.task is not None
            deletion = await asyncio.shield(app.controller.task)
            assert deletion.error is None
            assert deletion.result["removed_conversion_refs"] == [external["work_key"]]
            assert not await asyncio.to_thread(Path(external["publication"]["manifest"]).exists)
