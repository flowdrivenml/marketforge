"""Bounded end-to-end Forge pipeline using real Rust and PostgreSQL."""

import asyncio
import gzip
import json
import os
import shutil
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest

from forge.catalog.models import Instrument, Specification
from forge.catalog.service import add
from forge.cli.main import main
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import CachedRemoteArchive, register_remote_archive
from forge.historical.archives.storage.acquire import acquire
from forge.historical.engine.managed import resolve
from forge.historical.merge import service as merge
from forge.historical.normalization import execution as normalize
from forge.historical.pipelines.config import load_recipe
from forge.shared.database.testing import testing_connection as registry_connection
from forge.shared.database.testing import testing_dsn as registry_dsn
from forge.shared.http.client import open_http_client
from forge.shared.paths import ProjectPaths

from .test_acquisition_execution import ImmediateLimiter, install_http

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_prepared_stop_resume_and_new_run(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if os.getenv("MARKETFORGE_TEST_PIPELINE") != "1":
        pytest.skip("Set MARKETFORGE_TEST_PIPELINE=1 for managed engines and the testing registry.")
    root = ProjectPaths.resolve()
    dsn = registry_dsn(root.root)
    monkeypatch.setenv("MARKETFORGE_DATABASE_URL_TEST", dsn)
    original = root.config_dir / "historical"
    library = workspace / "data/.config/pipeline"
    library.mkdir(parents=True)
    historical = library.parent / "historical"
    historical.mkdir()
    shutil.copyfile(original / "general.toml", historical / "general.toml")
    shutil.copytree(original / "profiles", historical / "profiles")
    bin_dir = workspace / ".bin"
    bin_dir.mkdir()
    for target in ("normalize", "merge"):
        engine = resolve(root, target)
        shutil.copy2(engine, bin_dir / engine.name)
    shutil.copyfile(root.bin_dir / "manifest.json", bin_dir / "manifest.json")
    # Retain generated archives, run evidence and publications in the established
    # test output tree without writing to the ordinary acquired data root.
    storage = root.data_root / "tests/historical/normalize" / f"pipeline-{uuid4()}"
    settings = library.parent / "marketforge.toml"
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

    def invoke(*command: str) -> tuple[int, dict[str, Any]]:
        exit_code = main([*args, *command])
        output = capsys.readouterr()
        assert not output.err, output.err
        return exit_code, json.loads(output.out)

    code, plan = invoke("plan", "tiny", "--testing")
    assert code == 0 and plan["state"] == "planned"
    assert not calls and not storage.exists()

    async def register_unrelated() -> int:
        config = load_recipe(recipe.read_bytes())
        archive = DiscoveredArchive(
            request=config.requests[0],
            interval=config.interval,
            filename=name,
            source_object_key=f"unrelated/{symbol}",
            download_url=f"https://fixture.test/{name}",
        )
        async with registry_connection(dsn) as connection:
            async with connection.transaction():
                remote = await register_remote_archive(connection, archive)
            async with open_http_client(
                limiter=ImmediateLimiter(1),
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, stream=httpx.ByteStream(body))
                ),
            ) as client:
                result = await acquire(
                    connection,
                    client,
                    CachedRemoteArchive(
                        remote.id,
                        archive.source_object_key,
                        name,
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
    (historical / "general.toml").write_text("deliberately invalid")

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
    stages = cast(dict[str, dict[str, Any]], finished["stages"])
    assert stages["merge"]["execution_intent"] == merge_intent
    assert stages["normalize"]["execution_intent"] == normalization_intent
    assert stages["normalize"]["primary_file_ids"] == stages["merge"]["upstream_file_ids"]
    assert len(stages["merge"]["primary_file_ids"]) == 1
    merged_path = Path(json.loads(Path(merge_intent).read_text())["path"])
    assert json.loads((merged_path / "manifest.json").read_text())["events_written"] == 2
    recipe.write_bytes(original_recipe)
    shutil.copyfile(original / "general.toml", historical / "general.toml")
    code, second = invoke("run", "tiny", "--testing")
    assert code == 0, second.get("error")
    assert second["run_id"] != run_id
    new_stages = cast(dict[str, dict[str, Any]], second["stages"])
    for stage in ("normalize", "merge"):
        assert set(stages[stage]["primary_file_ids"]).isdisjoint(
            new_stages[stage]["primary_file_ids"]
        )
    assert sum(path.endswith(name) for path in calls) == 1
