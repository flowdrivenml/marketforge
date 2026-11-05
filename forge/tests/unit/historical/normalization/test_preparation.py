"""Preparation boundary tests: deterministic v3 output, SQL reads only, no engine."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from pydantic import ValidationError

from forge.historical.normalization.config import PreparationRequest
from forge.historical.normalization.service import prepare
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

REPOSITORY_ROOT = Path(__file__).resolve().parents[5]


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.query = ""

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, query: str, parameters: object = None) -> None:
        self.connection.queries.append(query)
        assert query.lstrip().startswith("SELECT"), query
        self.query = query

    async def fetchone(self) -> dict[str, object] | None:
        if "FROM catalog.instruments" in self.query:
            return self.connection.instrument
        return {"exchange": "bybit", "data_type": "trades", "scope_json": self.connection.scope}

    async def fetchall(self) -> list[dict[str, object]]:
        return [self.connection.local]


class Connection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.instrument: dict[str, object] | None = {
            "exchange": "bybit",
            "market_category": "spot",
            "instrument_type": "spot",
            "symbol": "BTC-USDT",
            "tick_size": Decimal("0.0100"),
            "contract_value": None,
            "contract_value_asset": None,
        }
        self.scope: dict[str, object] = {
            "market_category": "spot",
            "instrument_type": "spot",
            "scope_kind": "instrument",
            "scope_identifier": "BTC-USDT",
        }
        self.local: dict[str, object] = {
            "status": "complete",
            "relative_path": "input.gz",
            "bytes_written": 4,
            "remote_archive_id": 9,
            "external_archive_id": None,
        }

    def cursor(self, *, row_factory: object) -> Cursor:
        return Cursor(self)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def execute(self, query: str) -> None:
        self.queries.append(query)
        assert query == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


@pytest.fixture
def setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[ProjectPaths, Connection]:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    paths.raw_dir.mkdir(parents=True)
    (paths.raw_dir / "input.gz").write_bytes(b"test")
    historical = paths.config_dir / "historical"
    for directory in ("profiles/trades", "raw_schemas/trades"):
        (historical / directory).mkdir(parents=True)
    (historical / "profiles/trades/profile.toml").write_text("""
id = "explicit-profile"
physical = "physical-profile"
exchange = "bybit"
market_category = "spot"
instrument_type = "spot"
[[rules]]
target = "trade"
quantity_encoding = "base"
""")
    (historical / "raw_schemas/trades/schema.toml").write_text("""
id = "physical-profile"
supported_containers = ["gzip"]
""")
    connection = Connection()

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        assert dsn == "fixture"
        yield cast(AsyncConnection[TupleRow], cast(object, connection))

    monkeypatch.setattr("forge.historical.normalization.service.testing_connection", connect)

    def dsn(root: Path) -> str:
        return "fixture"

    monkeypatch.setattr("forge.historical.normalization.service.testing_dsn", dsn)
    return paths, connection


def request(*, multiple: bool = False) -> PreparationRequest:
    inputs: list[dict[str, object]] = [
        {"task_id": 1, "archive_id": 2, "instrument_id": 3, "format_profile": "explicit-profile"}
    ]
    if multiple:
        inputs.append({**inputs[0], "task_id": 2, "archive_id": 4})
    return PreparationRequest.model_validate(
        {"job_id": 7, "dataset_id": 8, "content_type": "trades", "inputs": inputs}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("multiple", [False, True])
async def test_deterministic_jobs_without_engine_or_writes(
    setup: tuple[ProjectPaths, Connection],
    multiple: bool,
) -> None:
    paths, connection = setup
    with patch("subprocess.Popen", side_effect=AssertionError("engine launched")):
        result = await prepare(paths, request(multiple=multiple))
    assert result["engine_ready"] is False
    assert result["task_count"] == (2 if multiple else 1)
    job = paths.normalization_jobs_dir / "7.json"
    original = job.read_bytes()
    document = json.loads(original)
    assert document["protocol_version"] == 3
    assert "resources" not in document and "integrity_policy" not in document
    assert document["tasks"][0]["instrument"]["tick_size"] == "0.0100"
    assert document["streams"] == [
        {"stream_id": "bybit.spot.spot.BTC-USDT.trades", "stream_rank": 0}
    ]
    assert "--job" in str(result["engine_invocation"])
    assert not (paths.data_root / "test/normalized").exists()
    assert connection.queries[0].endswith("READ ONLY")
    job.unlink()
    await prepare(paths, request(multiple=multiple))
    assert job.read_bytes() == original
    with pytest.raises(MarketForgeError, match="Job already exists"):
        await prepare(paths, request())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem",
    ["missing_file", "state", "metadata", "tick", "family", "profile", "content", "output"],
)
async def test_rejects_unresolved_inputs(
    setup: tuple[ProjectPaths, Connection], problem: str
) -> None:
    paths, connection = setup
    if problem == "missing_file":
        (paths.raw_dir / "input.gz").unlink()
    elif problem == "state":
        connection.local["status"] = "corrupt"
    elif problem == "metadata":
        connection.instrument = None
    elif problem == "tick":
        assert connection.instrument is not None
        connection.instrument["tick_size"] = None
    elif problem == "family":
        connection.scope["scope_kind"] = "family"
    elif problem == "profile":
        (paths.config_dir / "historical/profiles/trades/profile.toml").unlink()
    elif problem == "content":
        connection.scope["market_category"] = "inverse"
    elif problem == "output":
        (paths.data_root / "test/normalized/8").mkdir(parents=True)
    with pytest.raises(MarketForgeError):
        await prepare(paths, request())
    assert not (paths.normalization_jobs_dir / "7.json").exists()


def test_strict_request() -> None:
    payload = request().model_dump()
    for field, value in (
        ("job_id", 0),
        ("dataset_id", True),
        ("content_type", "combined"),
        ("resources", dict[str, object]()),
    ):
        with pytest.raises(ValidationError):
            PreparationRequest.model_validate({**payload, field: value})


@pytest.mark.asyncio
@pytest.mark.parametrize("ranks", [(0, 1), (None, None), (0, 0)])
async def test_multiple_stream_priorities(
    setup: tuple[ProjectPaths, Connection],
    monkeypatch: pytest.MonkeyPatch,
    ranks: tuple[int | None, int | None],
) -> None:
    from forge.historical.normalization.config import InputSelection
    from forge.historical.normalization.resolution import resolve_input

    paths, _ = setup

    async def resolve(
        connection: AsyncConnection[TupleRow],
        paths: ProjectPaths,
        selection: InputSelection,
        content_type: str,
    ) -> tuple[dict[str, object], str]:
        task, stream = await resolve_input(connection, paths, selection, content_type)
        if selection.task_id == 2:
            stream = stream.replace("BTC-USDT", "ETH-USDT")
            task = {**task, "stream_id": stream, "symbol": "ETH-USDT"}
        return task, stream

    monkeypatch.setattr("forge.historical.normalization.service.resolve_input", resolve)
    payload = request(multiple=True).model_dump()
    payload["inputs"][0]["stream_rank"] = ranks[0]
    payload["inputs"][1]["stream_rank"] = ranks[1]
    configured = PreparationRequest.model_validate(payload)
    if ranks != (0, 1):
        with pytest.raises(MarketForgeError, match="stream_rank|ranks"):
            await prepare(paths, configured)
        assert not (paths.normalization_jobs_dir / "7.json").exists()
    else:
        result = await prepare(paths, configured)
        assert result["stream_count"] == 2


def test_member_selection(tmp_path: Path) -> None:
    import zipfile

    from forge.historical.normalization.resolution import check_member

    path = tmp_path / "input.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("BTC.csv", "price,quantity\n1,2\n")
        archive.writestr("ETH.csv", "price,quantity\n3,4\n")
    check_member(path, "zip", "BTC.csv")
    for member in (None, "missing.csv"):
        with pytest.raises(MarketForgeError):
            check_member(path, "zip", member)
    with pytest.raises(MarketForgeError):
        check_member(path, "gzip", "BTC.csv")


@pytest.mark.asyncio
async def test_ambiguous_profile_and_output_rejection(
    setup: tuple[ProjectPaths, Connection],
) -> None:
    paths, _ = setup
    profile = paths.config_dir / "historical/profiles/trades/profile.toml"
    profile.with_name("duplicate.toml").write_bytes(profile.read_bytes())
    with pytest.raises(MarketForgeError, match="found 2"):
        await prepare(paths, request())
    profile.with_name("duplicate.toml").unlink()
    payload = request().model_dump()
    payload["output"] = {"staging_path": "same", "dataset_path": "same"}
    with pytest.raises(MarketForgeError, match="distinct"):
        await prepare(paths, PreparationRequest.model_validate(payload))


@pytest.mark.asyncio
async def test_generated_wire_matches_checked_in_v3_shape(
    setup: tuple[ProjectPaths, Connection],
) -> None:
    paths, _ = setup
    await prepare(paths, request())
    document = json.loads((paths.normalization_jobs_dir / "7.json").read_text())
    example = json.loads(
        (REPOSITORY_ROOT / "docs/historical/normalize-job-v3.example.json").read_text()
    )
    assert set(document) == set(example)
    for key in ("ordering", "output"):
        assert set(document[key]) == set(example[key])
    for key in ("tasks", "streams"):
        assert set(document[key][0]) == set(example[key][0])
    assert set(document["tasks"][0]["instrument"]) == set(example["tasks"][0]["instrument"])


@pytest.mark.asyncio
async def test_staging_collision_and_container_selection(
    setup: tuple[ProjectPaths, Connection],
) -> None:
    paths, _ = setup
    stage = paths.data_root / "test/normalized/.8.staging.7"
    stage.mkdir(parents=True)
    with pytest.raises(MarketForgeError, match="Output already exists"):
        await prepare(paths, request())
    stage.rmdir()
    physical = paths.config_dir / "historical/raw_schemas/trades/schema.toml"
    physical.write_text('id = "physical-profile"\nsupported_containers = ["plain", "gzip"]\n')
    with pytest.raises(MarketForgeError, match="source_compression explicitly"):
        await prepare(paths, request())
    payload = request().model_dump()
    payload["inputs"][0]["source_compression"] = "zip"
    with pytest.raises(MarketForgeError, match="unsupported by profile"):
        await prepare(paths, PreparationRequest.model_validate(payload))
