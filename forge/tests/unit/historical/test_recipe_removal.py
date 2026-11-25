"""Configured input removal preserves identity, order and unrelated selections."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.normalization.config import ExactRecipe
from forge.historical.pipelines.config import PipelineRecipe
from forge.historical.recipe_removal import configured_removals
from forge.orchestration.artifacts import retain_bytes
from forge.shared.errors import MarketForgeError

INTERVAL = {"start": datetime(2026, 9, 1, tzinfo=UTC), "end": datetime(2026, 9, 2, tzinfo=UTC)}


def request(symbol: str, data_type: str, exchange: str = "bybit") -> dict[str, object]:
    return {
        "exchange": exchange,
        "instrument_type": "spot",
        "market_category": "spot",
        "data_type": data_type,
        "scope": {"kind": "instrument", "identifier": symbol},
    }


@pytest.mark.parametrize("family", ["acquisition", "pipeline"])
def test_remove_feed_or_canonical_instrument(family: str) -> None:
    entries = [
        request("BTCUSDT", "trades"),
        request("BTCUSDT", "order_book_l2"),
        request("BTCUSDT", "trades", "binance"),
        request("ETHUSDT", "trades"),
    ]
    data: dict[str, Any] = {"version": 1, "interval": INTERVAL, "requests": entries}
    if family == "pipeline":
        data.update(
            normalize={},
            merge={"timeline_ordering": "causal"},
            convert={
                "target_quote": "USD",
                "max_staleness_ns": 100,
                "reference_stream_ids": ["spot"],
            },
        )
    actions = configured_removals(family, data)
    assert actions[1].apply(data) == {**data, "requests": entries[:1] + entries[2:]}
    draft = actions[4].apply(data)
    assert draft == {**data, "requests": entries[2:]}
    model = PipelineRecipe if family == "pipeline" else AcquisitionConfig
    model.model_validate(draft)
    if family == "pipeline":
        assert all(action.path not in {("normalize",), ("merge",)} for action in actions)


def test_remove_archive_preserves_exact_profile_and_other_pins() -> None:
    pins = [
        {"archive_id": 20, "selection_sha256": "a" * 64},
        {
            "archive_id": 24,
            "selection_sha256": "b" * 64,
            "profile_id": "proof",
            "profile_sha256": "c" * 64,
        },
    ]
    data: dict[str, object] = {
        "registry_id": "00000000-0000-4000-8000-000000000001",
        "ranking_policy": "experiment",
        "archive_pins": pins,
    }
    edited = configured_removals("normalize", data)[0].apply(data)
    assert edited == {**data, "archive_pins": pins[1:]}
    assert ExactRecipe.model_validate(edited).archive_pins[0].profile_id == "proof"
    with pytest.raises(ValidationError):
        ExactRecipe.model_validate(configured_removals("normalize", edited)[0].apply(edited))


def test_merge_removal_preserves_identity_domain() -> None:
    direct: dict[str, object] = {"dataset_ids": [20, 22, 21], "stream_ranks": {"trades": 2}}
    derived = {**direct, "selection": {"publication_ids": [105, 107]}}
    assert configured_removals("merge", direct)[1].apply(direct) == {
        **direct,
        "dataset_ids": [20, 21],
    }
    assert configured_removals("merge", derived)[1].apply(derived) == {
        **direct,
        "selection": {"publication_ids": [105]},
    }


def test_conversion_remove_reference_preserves_policy_and_required_source() -> None:
    data: dict[str, object] = {
        "version": 1,
        "source": {
            "kind": "merge",
            "reference": "merged",
            "path": "/merged.toml",
            "sha256": "a" * 64,
        },
        "interval": {"start_ns": 0, "end_ns": 1},
        "conversion": {"target_quote": "USD", "max_staleness_ns": 100},
        "reference_stream_ids": ["external-trades", "internal-trades"],
        "reference_publication_id": 14,
    }
    actions = configured_removals("convert", data)
    assert all(action.path != ("source",) for action in actions)
    draft = actions[1].apply(data)
    assert draft == {**data, "reference_stream_ids": ["external-trades"]}
    ConversionRecipe.model_validate(draft)
    with pytest.raises(ValidationError, match="requires explicit"):
        ConversionRecipe.model_validate(configured_removals("convert", draft)[0].apply(draft))


def test_configuration_artifact_is_never_replaced(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    destination = tmp_path / "recipe.toml"

    def store(data: bytes) -> bytes | None:
        try:
            retain_bytes(destination, data)
        except MarketForgeError:
            return None
        return data

    with ThreadPoolExecutor(2) as workers:
        retained = [result for result in workers.map(store, (b"# first", b"# second")) if result]
    assert retained == [destination.read_bytes()]
    assert sorted(tmp_path.iterdir()) == [destination]


@pytest.mark.asyncio
async def test_pinned_merge_removes_one_publication_and_keeps_file_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.historical.merge import service
    from forge.historical.recipe_removal import remove_configured_input
    from forge.shared.paths import ProjectPaths

    registry = "00000000-0000-4000-8000-000000000001"
    data: dict[str, object] = {
        "registry_id": registry,
        "dataset_ids": [12, 17, 19],
        "publication_ids": [800, 801],
        "selection_sha256": "a" * 64,
        "start_ns": 0,
        "end_ns": 100,
        "stream_ranks": {"spot": 9, "depth": 4},
    }
    bindings = [
        {
            "database_id": 12,
            "registry_publication_id": 800,
            "stream_id": "spot",
            "content_type": "trade",
        },
        {
            "database_id": 17,
            "registry_publication_id": 801,
            "stream_id": "depth",
            "content_type": "depth",
        },
        {
            "database_id": 19,
            "registry_publication_id": 801,
            "stream_id": "spot",
            "content_type": "trade",
        },
    ]

    async def inputs(
        _paths: ProjectPaths,
        _recipe: service.Recipe,
        ids: list[int],
        identity: str,
        *,
        include_publication_ids: bool,
    ) -> list[dict[str, Any]]:
        assert (ids, identity, include_publication_ids) == ([12, 17, 19], registry, True)
        return bindings

    def fingerprint(inputs: list[dict[str, Any]]) -> str:
        assert [item["database_id"] for item in inputs] == [17, 19]
        return "b" * 64

    monkeypatch.setattr(service, "reviewed_inputs", inputs)
    monkeypatch.setattr(service, "publication_selection_sha256", fingerprint)
    draft = await remove_configured_input(
        data,
        "merge",
        configured_removals("merge", data)[0],
        paths=ProjectPaths(tmp_path, tmp_path / "data"),
        registry=registry,
    )
    assert draft == {
        **data,
        "publication_ids": [801],
        "dataset_ids": [17, 19],
        "selection_sha256": "b" * 64,
    }
    service.Recipe.model_validate(draft)


def test_prepared_recipe_lineage_survives_loss_of_managed_file(tmp_path: Path) -> None:
    from uuid import uuid4

    from forge.orchestration.registry import (
        Execution,
        active,
        inherit_recipe,
        recipe_evidence,
        retain_recipe,
    )
    from forge.shared.database.connection import database_environment
    from forge.shared.identity import RunOperation
    from forge.shared.paths import ProjectPaths

    paths = ProjectPaths(tmp_path, tmp_path / "data")

    def execution(name: str) -> Execution:
        return Execution(
            paths,
            name,
            RunOperation.MERGE,
            "prepare",
            uuid4(),
            1,
            datetime.now(UTC),
            "00000000-0000-4000-8000-000000000001",
            "testing",
            paths.runs_dir / name,
            "",
        )

    managed = tmp_path / "example.toml"
    original = b"# exact comments\r\nversion = 1\n"
    managed.write_bytes(original)
    prepare, submit = execution("prepare"), execution("submit")
    for attempt in (prepare, submit):
        attempt.outcome.parent.mkdir(parents=True)
    with database_environment(testing=True), active(prepare):
        metadata = retain_recipe(RunOperation.MERGE, managed, original, effective={"version": 1})
    managed.unlink()
    with database_environment(testing=True), active(submit):
        inherit_recipe(RunOperation.MERGE, metadata)
        retained = recipe_evidence()
        snapshot = Path(retained["snapshot"])
        assert snapshot.parent == submit.outcome.parent
        assert snapshot.read_bytes() == original
        assert retained["sha256"] == metadata["sha256"]
        snapshot.write_bytes(b"changed")
        with pytest.raises(MarketForgeError, match="fingerprint differs"):
            inherit_recipe(RunOperation.MERGE, metadata)
