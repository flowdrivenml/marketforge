"""Identity-only preparation: complete archives, explicit metadata, no format decisions."""

import json
import zipfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from pydantic import ValidationError

from forge.cli.main import main
from forge.historical.normalization.config import PreparationRequest
from forge.historical.normalization.mapping import resolve_archive
from forge.markets.models import array, obj
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.dependencies import prepared_references

FORBIDDEN = {
    "format_profile",
    "profile_fingerprint",
    "archive_member",
    "source_compression",
    "instrument_id",
}


@pytest.fixture(autouse=True)
def pinned_preparation_ranking(monkeypatch: pytest.MonkeyPatch) -> None:
    from uuid import UUID

    from forge.historical.normalization.ranking import RankingSelection
    from forge.shared.config.models import HttpSettings

    @asynccontextmanager
    async def lock(c: AsyncConnection[TupleRow], *, shared: bool = False) -> AsyncGenerator[None]:
        yield

    async def ensure(
        c: AsyncConnection[TupleRow],
        dsn: str,
        directory: Path,
        policy: str,
        settings: HttpSettings,
        *,
        dry_run: bool,
    ) -> RankingSelection:
        return RankingSelection(
            UUID(int=1), {"decision": "reused", "policy": policy, "snapshot": str(UUID(int=1))}
        )

    async def positions(c: AsyncConnection[TupleRow], snapshot: UUID | None) -> dict[int, int]:
        return {}

    monkeypatch.setattr("forge.historical.normalization.batch.ranking_lock", lock)
    monkeypatch.setattr("forge.historical.normalization.batch.ensure_ranking", ensure)
    monkeypatch.setattr("forge.historical.normalization.batch.positions_for", positions)


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.query = ""

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, query: str, parameters: object = None) -> None:
        assert query.lstrip().startswith("SELECT"), query
        self.connection.queries.append(query)
        self.query = query

    async def fetchall(self) -> list[dict[str, object]]:
        if "lower(i.base_asset)" in self.query:
            return self.connection.fallback_instruments
        return self.connection.instruments


class Connection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.fallback_instruments: list[dict[str, object]] = []
        self.instruments: list[dict[str, object]] = [
            {
                "id": 3,
                "exchange": "bybit",
                "instrument_type": "spot",
                "market_category": "spot",
                "symbol": "BTC-USDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "tick_size": Decimal("0.0100"),
                "contract_value": None,
                "contract_value_asset": None,
                "quantity_type": "base",
            }
        ]

    def cursor(self, *, row_factory: object) -> Cursor:
        return Cursor(self)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def execute(self, query: str) -> None:
        assert query == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        self.queries.append(query)


def archive(paths: ProjectPaths, *, family: bool = False) -> dict[str, object]:
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    path = paths.raw_dir / "complete.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("first.csv", "one")
        z.writestr("second.csv", "two")
    return {
        "id": 2,
        "status": "complete",
        "relative_path": path.name,
        "bytes_written": path.stat().st_size,
        "origin": "remote",
        "remote_archive_id": 9,
        "exchange": "okx" if family else "bybit",
        "instrument_type": "future" if family else "spot",
        "market_category": "linear" if family else "spot",
        "scope_kind": "family" if family else "instrument",
        "scope_identifier": "BTC-USD_UM" if family else "BTC-USDT",
        "data_type": "trades",
        "feed": None,
    }


@pytest.mark.asyncio
async def test_concrete_identity_without_profiles_or_member_selection(tmp_path: Path) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    c = Connection()
    with patch("zipfile.ZipFile", side_effect=AssertionError("Must not inspect container")):
        resolved = await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )
    task = resolved.task
    assert not FORBIDDEN & task.keys()
    assert task["symbol"] == "BTC-USDT"
    assert task["base_asset"] == "BTC" and task["quote_asset"] == "USDT"
    assert (
        task["exchange"] == "bybit" and task["instrument_type"] == task["market_category"] == "spot"
    )
    assert task["instrument"] == {
        "tick_size": "0.0100",
        "contract_value": None,
        "contract_value_asset": None,
    }
    assert task["input_path"] == str(paths.raw_dir / "complete.zip")
    assert resolved.instrument_id == 3
    assert not paths.config_dir.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem",
    [
        "missing_file",
        "size",
        "state",
        "ambiguous",
        "tick",
        "asset",
        "override",
        "invalid_override",
        "identity_conflict",
    ],
)
async def test_precise_identity_and_local_input_blockers(tmp_path: Path, problem: str) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    c = Connection()
    reason = ""
    if problem == "missing_file":
        (paths.raw_dir / "complete.zip").unlink()
        reason = "Input file is missing"
    elif problem == "size":
        row["bytes_written"] = 0
        reason = "Input size differs"
    elif problem == "state":
        row["status"] = "corrupt"
        reason = "not complete"
    elif problem == "ambiguous":
        c.instruments *= 2
        reason = "found 2"
    elif problem == "tick":
        c.instruments[0]["tick_size"] = None
        reason = "tick_size"
    elif problem == "asset":
        c.instruments[0]["base_asset"] = None
        reason = "base_asset"
    elif problem == "override":
        row["specifications_json"] = {"tick_size": "5"}
        reason = "conflicts"
    elif problem == "invalid_override":
        row["specifications_json"] = {"tick_size": "invalid"}
        reason = "Invalid external historical tick_size"
    elif problem == "identity_conflict":
        row["identity_json"] = {"symbol": "different"}
        reason = "External symbol conflicts"
    with pytest.raises(MarketForgeError, match=reason):
        await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("assets", ["consistent", "conflicting", "missing"])
async def test_family_identity_has_no_member_contract_specs(tmp_path: Path, assets: str) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths, family=True)
    row["feed"] = "snapshot"
    c = Connection()
    c.instruments = [
        {**c.instruments[0], "quote_asset": "USD", "symbol": "BTC-USD-261225"},
        {**c.instruments[0], "quote_asset": "USD", "symbol": "BTC-USD-270326"},
    ]
    if assets == "conflicting":
        c.instruments[1]["quote_asset"] = "USDT"
    elif assets == "missing":
        c.instruments = []
    result = await resolve_archive(
        cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
    )
    assert result.instrument_id is None
    assert result.task["scope"] == {"kind": "family", "identifier": "BTC-USD_UM"}
    assert result.task["symbol"] is None and result.task["instrument"] is None
    assert result.task["source_options"] == {"feed": "snapshot"}
    assert result.task["quote_asset"] == ("USD" if assets == "consistent" else None)
    assert bool(result.warnings) is (assets != "consistent")
    assert not FORBIDDEN & result.task.keys()


@pytest.mark.asyncio
@pytest.mark.parametrize("contracts", [True, False])
async def test_contract_specifications_follow_registered_units(
    tmp_path: Path, contracts: bool
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(exchange="binance", instrument_type="perpetual", market_category="inverse")
    c = Connection()
    c.instruments[0].update(
        exchange="binance",
        instrument_type="perpetual",
        market_category="inverse",
        quantity_type="contracts" if contracts else "base",
        contract_value=Decimal("100") if contracts else None,
        contract_value_asset="USD" if contracts else None,
    )
    result = await resolve_archive(
        cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
    )
    assert obj(result.task["instrument"])["contract_value"] == ("100" if contracts else None)
    assert not {"instrument_kind", "contract_kind"} & obj(result.task["instrument"]).keys()


def test_legacy_request_still_parses() -> None:
    payload: dict[str, object] = {
        "job_id": 7,
        "dataset_id": 8,
        "content_type": "trades",
        "inputs": [
            {
                "task_id": 1,
                "archive_id": 2,
                "instrument_id": 3,
                "format_profile": "old-profile",
                "archive_member": "first.csv",
            }
        ],
    }
    assert PreparationRequest.model_validate(payload).job_id == 7
    with pytest.raises(ValidationError):
        PreparationRequest.model_validate({**payload, "job_id": 0})


def test_manual_prepare_entrypoint_uses_current_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    row = archive(paths)
    paths.normalization_config_dir.mkdir(parents=True)
    (paths.normalization_config_dir / "legacy.toml").write_text("""job_id=7
dataset_id=8
content_type="trades"
[[inputs]]
task_id=1
archive_id=2
instrument_id=3
format_profile="missing-and-ignored"
archive_member="first.csv"
[[inputs]]
task_id=2
archive_id=2
instrument_id=3
format_profile="also-ignored"
archive_member="second.csv"
""")
    c = Connection()

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], cast(object, c))

    async def ranking(connection: AsyncConnection[TupleRow]) -> tuple[None, dict[int, int]]:
        return None, {}

    async def occupied(connection: AsyncConnection[TupleRow], project: ProjectPaths) -> set[int]:
        return set()

    async def scan(
        connection: AsyncConnection[TupleRow], params: dict[str, object], after: int
    ) -> list[dict[str, object]]:
        return [row]

    monkeypatch.setattr("forge.historical.normalization.batch.testing_connection", connect)

    def dsn(root: Path) -> str:
        return "fixture"

    monkeypatch.setattr("forge.historical.normalization.batch.testing_dsn", dsn)
    monkeypatch.setattr("forge.historical.normalization.batch.ranking_positions", ranking)
    monkeypatch.setattr("forge.historical.normalization.batch.used_ids", occupied)
    monkeypatch.setattr("forge.historical.normalization.batch.scan", scan)
    args = [
        "--project-root",
        str(tmp_path),
        "--json",
        "normalize",
        "prepare",
        "legacy",
        "--testing",
    ]
    with patch("subprocess.Popen", side_effect=AssertionError("Engine must not execute")):
        assert main([*args, "--dry-run"]) == 0
        assert not paths.runs_dir.exists()
        assert main(args) == 0
    result = obj(json.loads(capsys.readouterr().out.splitlines()[-1]))
    batch = obj(json.loads(Path(str(result["batch_path"])).read_text()))
    job = obj(array(batch["jobs"])[0])
    assert batch["schema_version"] == 3 and job["job_id"] == 7 and job["dataset_id"] == 8
    tasks = [obj(t) for t in array(job["tasks"])]
    assert len(tasks) == 1 and tasks[0]["stream_rank"] == 1
    assert not FORBIDDEN & tasks[0].keys()
    assert len(prepared_references(paths).paths) == 1
    assert not (paths.data_root / "test/normalized").exists()
    assert Path(str(result["report_path"])).is_file()
    assert result["engine_ready"] is False
    assert c.queries.count("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY") == 2


@pytest.mark.parametrize("family", [False, True])
def test_recipe_prepare_entrypoint_preserves_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    family: bool,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    row = archive(paths, family=family)
    if not family:
        row.update(exchange="gateio", data_type="order_book_l2", feed="snapshot")
    row.update(
        start_at=datetime(2026, 10, 27, tzinfo=UTC),
        end_at=datetime(2026, 10, 28, tzinfo=UTC),
        base_asset="BTC",
        quote_asset="USD" if family else "USDT",
    )
    c = Connection()
    if family:
        # There need not be a registered concrete instrument to select this source.
        c.instruments = []
    else:
        c.instruments[0]["exchange"] = "gateio"
    request = f'''[[requests]]
exchange="{row["exchange"]}"
instrument_type="{row["instrument_type"]}"
market_category="{row["market_category"]}"
data_type="{row["data_type"]}"
scope={{kind="{row["scope_kind"]}",identifier="{row["scope_identifier"]}"}}
'''
    if not family:
        request += 'source_options={feed="snapshot"}\n'
    paths.normalization_recipe_dir.mkdir(parents=True)
    (paths.normalization_recipe_dir / "recipe.toml").write_text(
        """version=1
[interval]
start=2026-10-27T00:00:00Z
end=2026-10-28T00:00:00Z
"""
        + request
        + request
    )
    rankings: list[str] = []

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], cast(object, c))

    def dsn(root: Path) -> str:
        return "fixture"

    from uuid import UUID

    from forge.historical.normalization.ranking import RankingSelection
    from forge.shared.config.models import HttpSettings

    async def ranking(
        connection: AsyncConnection[TupleRow],
        dsn: str,
        directory: Path,
        policy: str,
        settings: HttpSettings,
        *,
        dry_run: bool,
    ) -> RankingSelection:
        rankings.append("resolved")
        return RankingSelection(
            UUID(int=1), {"decision": "reused", "policy": policy, "snapshot": str(UUID(int=1))}
        )

    async def occupied(connection: AsyncConnection[TupleRow], project: ProjectPaths) -> set[int]:
        return set()

    async def scan(
        connection: AsyncConnection[TupleRow], params: dict[str, object], after: int
    ) -> list[dict[str, object]]:
        return [row] if after == 0 else []

    monkeypatch.setattr("forge.historical.normalization.batch.testing_connection", connect)
    monkeypatch.setattr("forge.historical.normalization.batch.testing_dsn", dsn)
    monkeypatch.setattr("forge.historical.normalization.batch.ensure_ranking", ranking)
    monkeypatch.setattr("forge.historical.normalization.batch.used_ids", occupied)
    monkeypatch.setattr("forge.historical.normalization.batch.scan", scan)
    args = [
        "--project-root",
        str(tmp_path),
        "--json",
        "normalize",
        "prepare",
        "recipe",
        "--testing",
    ]
    with patch("subprocess.Popen", side_effect=AssertionError("Engine must not execute")):
        assert main([*args, "--dry-run"]) == 0
        dry = obj(json.loads(capsys.readouterr().out))
        assert not paths.runs_dir.exists()
        assert main(args) == 0
    result = obj(json.loads(capsys.readouterr().out))
    assert {**dry, "dry_run": False} == result
    assert rankings == ["resolved", "resolved"]  # Once per preparation, not per request.
    batch = obj(json.loads(Path(str(result["batch_path"])).read_text()))
    tasks = array(obj(array(batch["jobs"])[0])["tasks"])
    assert len(tasks) == 1  # Repeated requests and multiple members don't expand tasks.
    task = obj(tasks[0])
    assert not FORBIDDEN & task.keys()
    assert task["stream_rank"] == 1
    if family:
        assert task["symbol"] is None and task["instrument"] is None
        assert task["scope"] == {"kind": "family", "identifier": "BTC-USD_UM"}
        assert task["base_asset"] is None and task["quote_asset"] is None
        assert any(
            obj(s)["metadata_warnings"]
            for q in array(result["requests"])
            for s in array(obj(q)["selected_archives"])
        )
    else:
        assert task["source_options"] == {"feed": "snapshot"}
        assert task["symbol"] == "BTC-USDT" and task["base_asset"] == "BTC"
    assert not {"requests", "job_summaries", "archive_content_resolution"} & batch.keys()
    assert "Delegated to engine" in str(result["archive_content_resolution"])
    assert result["engine_ready"] is False
    assert not (paths.data_root / "test/normalized").exists()
    assert set(prepared_references(paths).paths) == {str(paths.raw_dir / "complete.zip")}


def test_output_destinations_remain_execution_fields_without_creation(tmp_path: Path) -> None:
    from forge.historical.normalization.service import output_paths

    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    payload: dict[str, object] = {
        "job_id": 7,
        "dataset_id": 8,
        "content_type": "trades",
        "inputs": [{"task_id": 1, "archive_id": 2, "instrument_id": 3}],
        "output": {"staging_path": "custom/.8.staging.7", "dataset_path": "custom/8"},
    }
    request = PreparationRequest.model_validate(payload)
    staging, dataset = output_paths(paths, request)
    assert staging == tmp_path / "custom/.8.staging.7" and dataset == tmp_path / "custom/8"
    assert not staging.parent.exists()
    dataset.mkdir(parents=True)
    with pytest.raises(MarketForgeError, match="Output already exists"):
        output_paths(paths, request)


def test_checked_in_forge_example_uses_current_task_shape() -> None:
    example = obj(
        json.loads(
            (
                Path(__file__).resolve().parents[5]
                / "docs/historical/normalize-batch-v3.example.json"
            ).read_text()
        )
    )
    assert example["schema_version"] == 3
    jobs = [obj(j) for j in array(example["jobs"])]
    for job in jobs:
        assert not {"streams", "protocol_version"} & job.keys()
        assert job["output"] and job["ordering"]
        for value in array(job["tasks"]):
            task = obj(value)
            assert not FORBIDDEN & task.keys()
            assert {
                "exchange",
                "instrument_type",
                "market_category",
                "symbol",
                "base_asset",
                "quote_asset",
                "instrument",
            } <= task.keys()
            if task["symbol"] is None:
                assert task["scope"] and task["instrument"] is None
            else:
                assert set(obj(task["instrument"])) == {
                    "tick_size",
                    "contract_value",
                    "contract_value_asset",
                }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identifier,symbol,base,quote",
    [
        ("BTC/USDT", "BTCUSDT", "BTC", "USDT"),
        ("ETH/USDT", "ETHUSDT", "ETH", "USDT"),
        ("eth/btc", "ETHBTC", "ETH", "BTC"),
    ],
)
@pytest.mark.parametrize("content", ["trades", "depth"])
async def test_bitget_spot_asset_pair_resolution(
    tmp_path: Path,
    identifier: str,
    symbol: str,
    base: str,
    quote: str,
    content: str,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(
        exchange="bitget",
        scope_identifier=identifier,
        data_type="trades" if content == "trades" else "order_book_l2",
        feed="snapshot",
        identity_json={"symbol": identifier},
    )
    c = Connection()
    c.fallback_instruments = [
        {
            **c.instruments[0],
            "id": 99,
            "exchange": "bitget",
            "symbol": symbol,
            "base_asset": base,
            "quote_asset": quote,
        }
    ]
    c.instruments = []
    resolved = await resolve_archive(
        cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, content
    )
    assert resolved.instrument_id == 99
    assert resolved.task["symbol"] == symbol
    assert resolved.task["base_asset"] == base and resolved.task["quote_asset"] == quote
    assert resolved.task["stream_id"] == f"bitget.spot.spot.{symbol}.{content}.snapshot"
    assert resolved.task["source_options"] == {"feed": "snapshot"}
    assert obj(resolved.task["instrument"])["tick_size"] == "0.0100"
    assert resolved.task["input_path"] == str(paths.raw_dir / "complete.zip")
    assert resolved.identity_resolution == {
        "archive_identifier": identifier,
        "method": "bitget_spot_asset_pair",
        "resolved_symbol": symbol,
        "parsed_assets": {
            "base_asset": identifier.split("/")[0],
            "quote_asset": identifier.split("/")[1],
        },
    }
    assert len(c.queries) == 2
    assert not FORBIDDEN & resolved.task.keys()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 2])
async def test_bitget_exact_lookup_precedes_pair_fallback(tmp_path: Path, count: int) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(exchange="bitget", scope_identifier="BTC/USDT")
    c = Connection()
    c.instruments[0].update(exchange="bitget", symbol="BTC/USDT")
    c.fallback_instruments = [{**c.instruments[0], "id": 99, "symbol": "BTCUSDT"}]
    c.instruments *= count
    if count == 2:
        with pytest.raises(MarketForgeError, match="found 2"):
            await resolve_archive(
                cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
            )
    else:
        resolved = await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )
        assert resolved.task["symbol"] == "BTC/USDT" and resolved.instrument_id == 3
        assert resolved.identity_resolution["method"] == "exact_symbol"
    assert len(c.queries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_bitget_pair_failure_explains_identifier_assets_and_candidates(
    tmp_path: Path,
    ambiguous: bool,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(exchange="bitget", scope_identifier="BTC/USDT")
    c = Connection()
    if ambiguous:
        c.fallback_instruments = [
            {**c.instruments[0], "id": index + 99, "symbol": symbol, "exchange": "bitget"}
            for index, symbol in enumerate(("BTCUSDT", "BTC_USDT"))
        ]
    c.instruments = []
    with pytest.raises(MarketForgeError) as failure:
        await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )
    reason = str(failure.value)
    assert "BTC/USDT" in reason and "base_asset='BTC'" in reason and "quote_asset='USDT'" in reason
    if ambiguous:
        assert "ambiguous" in reason and "BTCUSDT" in reason and "BTC_USDT" in reason
    else:
        assert "no catalogue instrument matches" in reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identifier", ["BTCUSDT", "BTC/", "/USDT", "BTC//USDT", "BTC/USDT/OTHER", "BTC/   "]
)
async def test_malformed_bitget_pair_is_diagnosed_after_exact_miss(
    tmp_path: Path, identifier: str
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(exchange="bitget", scope_identifier=identifier)
    c = Connection()
    c.instruments = []
    with pytest.raises(MarketForgeError, match="Malformed Bitget spot archive identifier"):
        await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )
    assert len(c.queries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exchange,kind,market,scope",
    [
        ("bybit", "spot", "spot", "instrument"),
        ("bitget", "perpetual", "linear", "instrument"),
        ("bitget", "future", "inverse", "instrument"),
        ("bitget", "spot", "spot", "family"),
        ("bitget", "spot", "spot", "base_coin"),
    ],
)
async def test_pair_fallback_does_not_change_other_identities(
    tmp_path: Path,
    exchange: str,
    kind: str,
    market: str,
    scope: str,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    row = archive(paths)
    row.update(
        exchange=exchange,
        instrument_type=kind,
        market_category=market,
        scope_kind=scope,
        scope_identifier="BTC/USDT",
    )
    c = Connection()
    c.fallback_instruments = c.instruments
    c.instruments = []
    if scope == "instrument":
        with pytest.raises(MarketForgeError, match="found 0"):
            await resolve_archive(
                cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
            )
    else:
        resolved = await resolve_archive(
            cast(AsyncConnection[TupleRow], cast(object, c)), paths, row, "trades"
        )
        assert resolved.task["scope"] == {"kind": scope, "identifier": "BTC/USDT"}
        assert resolved.task["symbol"] is None and resolved.instrument_id is None
    assert len(c.queries) == 1
