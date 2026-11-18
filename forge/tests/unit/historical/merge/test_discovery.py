"""Selection tests using stored-manifest and receipt fixtures only."""

import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from forge.historical.merge import discovery
from forge.historical.merge.config import DerivedRecipe, unix_ns
from forge.historical.normalization.config import NormalizationRecipe
from forge.shared.paths import ProjectPaths

START = datetime(2026, 10, 27, tzinfo=UTC)
END = START + timedelta(days=1)
LO, HI = unix_ns(START), unix_ns(END)
MARKET = {"exchange": "okx", "instrument_type": "future", "market_category": "linear"}
SYMBOL = "BTC-USD_UM-261225"


def recipe(**filters: Any) -> DerivedRecipe:
    return DerivedRecipe.model_validate(
        {
            "version": 1,
            "interval": {"start": START, "end": END},
            "selection": {"source": "source", **filters},
        }
    )


def normalization() -> NormalizationRecipe:
    return NormalizationRecipe.model_validate(
        {
            "interval": {"start": START, "end": END},
            "requests": [
                {
                    **MARKET,
                    "data_type": "trades",
                    "scope": {"kind": "family", "identifier": "BTC-USD_UM"},
                },
                {
                    **MARKET,
                    "data_type": "order_book_l2",
                    "scope": {"kind": "instrument", "identifier": SYMBOL},
                },
            ],
        }
    )


def publication(
    pub: int,
    content: str,
    *,
    digest: str = "submission",
    family: bool = True,
    symbols: tuple[str, ...] = (SYMBOL,),
    part_count: int = 1,
) -> tuple[list[tuple[Any, ...]], tuple[Any, ...]]:
    evidence: dict[str, Any] = {
        "manifest_sha256": "manifest-sha",
        "resolved_job": {"tasks": []},
        "manifest": {
            "protocol_version": 5,
            "content_type": content,
            "integrity_status": "clean",
            "processing_quality": {"timestamp_ordering": "event_timestamp_ns"},
            "streams": [],
            "files": [],
        },
    }
    rows: list[tuple[Any, ...]] = []
    engine = str(pub + 1000)
    root = f"/publications/{engine}"
    for task, symbol in enumerate(symbols, 1):
        stream = f"{symbol}.{content}"
        directory = f"streams/{symbol}/{content}"
        member = {**MARKET, "symbol": symbol, "instrument_id": task, "stream_id": stream}
        evidence["manifest"]["streams"].append(
            {
                "stream_id": stream,
                "directory": directory,
                "sources": [{"task_id": task, "start_row": 0, "end_row": part_count}],
            }
        )
        evidence["resolved_job"]["tasks"].append(
            {
                **member,
                "task_id": task,
                "input_path": f"/raw/{task}",
                "raw_schema": {
                    "marketforge_submission_sha256": digest,
                    "marketforge_source": {
                        "scope": {"kind": "family", "identifier": "BTC-USD_UM"} if family else None,
                        "instrument": member,
                    },
                },
            }
        )
        for part in range(part_count):
            relative = f"{directory}/part-{part}.parquet"
            db_id = pub * 100 + len(rows)
            start = LO + part
            end = LO + part
            evidence["manifest"]["files"].append({"path": relative, "rows": 1})
            rows.append(
                (
                    db_id,
                    pub,
                    relative,
                    {"memberships": [member]},
                    start,
                    end,
                    "complete",
                    f"{root}/{relative}",
                    "file-sha",
                )
            )
    return rows, (engine, "ns", root, evidence)


async def discover(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    selection: DerivedRecipe,
    publications: dict[int, tuple[list[tuple[Any, ...]], tuple[Any, ...]]],
    retained: set[int],
) -> dict[str, Any]:
    files = [row for rows, _ in publications.values() for row in rows]
    evidence = {pub: entry for pub, (_, entry) in publications.items()}
    links = {
        (
            namespace,
            engine,
            root,
            doc["resolved_job"]["tasks"][0]["raw_schema"]["marketforge_submission_sha256"],
        ): f"intent-{pub}"
        for pub, (engine, namespace, root, doc) in evidence.items()
        if pub in retained
    }
    monkeypatch.setattr(discovery, "registry", AsyncMock(return_value=(files[::-1], evidence, {})))

    def linked(*_args: object) -> dict[tuple[str, str, str, str], str]:
        return links

    monkeypatch.setattr(discovery, "linked_submissions", linked)
    return await discovery.discover(
        ProjectPaths(tmp_path, tmp_path / "data"),
        selection,
        tmp_path / "source.toml",
        normalization(),
    )


def test_time_conversion_is_integer_and_offset_aware() -> None:
    assert LO == 1793059200000000000
    assert (
        unix_ns(START.astimezone(timezone(timedelta(hours=-4))).replace(microsecond=1)) == LO + 1000
    )
    assert unix_ns(datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)) == -1000


@pytest.mark.asyncio
async def test_single_proven_run_and_deterministic_parts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    result = await discover(
        monkeypatch,
        tmp_path,
        recipe(data_types=["trades"]),
        {
            1: publication(1, "trades", part_count=2),
            2: publication(2, "trades", digest="unrelated"),
            3: publication(3, "depth"),
        },
        {1, 3},
    )
    assert result["requested_count"] == 1
    assert result["matched_count"] == 3
    assert result["selected_dataset_ids"] == [100, 101]
    assert result["blockers"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [set[int](), {1, 2}])
async def test_ambiguous_or_unlinked_requires_explicit_publication(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    retained: set[int],
) -> None:
    pubs = {1: publication(1, "trades"), 2: publication(2, "trades", digest="other")}
    result = await discover(monkeypatch, tmp_path, recipe(data_types=["trades"]), pubs, retained)
    assert result["matched_count"] == 2
    assert result["selected_dataset_ids"] == []
    assert result["blockers"][0]["publication_ids"] == [1, 2]
    result = await discover(
        monkeypatch, tmp_path, recipe(data_types=["trades"], publication_ids=[2]), pubs, retained
    )
    assert result["selected_dataset_ids"] == [200]
    assert result["blockers"] == []


@pytest.mark.asyncio
async def test_mixed_and_depth_only_report_whole_incompatible_selection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pubs = {
        1: publication(1, "trades", symbols=(SYMBOL, "BTC-USD_UM-261030")),
        2: publication(2, "depth", family=False),
    }
    result = await discover(monkeypatch, tmp_path, recipe(), pubs, {1, 2})
    assert set(result["selected_dataset_ids"]) == {100, 101, 200}
    assert any("2 streams" in issue["reason"] for issue in result["blockers"])
    assert any("v5 depth" in issue["reason"] for issue in result["blockers"])
    result = await discover(
        monkeypatch, tmp_path, recipe(data_types=["order_book_l2"]), pubs, {1, 2}
    )
    assert result["selected_dataset_ids"] == [200]
    assert result["blockers"][0]["dataset_ids"] == [200]


@pytest.mark.asyncio
async def test_family_requires_evidence_and_file_memberships_are_indivisible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = await discover(
        monkeypatch,
        tmp_path,
        recipe(data_types=["trades"]),
        {
            1: publication(1, "trades", family=False),
        },
        {1},
    )
    assert result["selected_dataset_ids"] == []
    assert "historical family" in result["missing"][0]["reason"]
    rows, evidence = publication(1, "trades", symbols=(SYMBOL, "BTC-USD_UM-261030"))
    rows[0][3]["memberships"].extend(rows[1][3]["memberships"])
    result = await discover(
        monkeypatch,
        tmp_path,
        recipe(data_types=["trades"], instruments=[f"okx.future.linear.{SYMBOL}"]),
        {1: (rows, evidence)},
        {1},
    )
    assert result["selected_dataset_ids"] == [100]
    assert len(result["candidates"][0]["memberships"]) == 2
    assert any("outside the approved selection" in issue["reason"] for issue in result["blockers"])


def test_retained_receipt_matches_submitted_bytes_and_current_source(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    config = normalization()
    source = paths.normalization_recipe_dir / "source.toml"
    source.parent.mkdir(parents=True)
    source.touch()
    run = paths.runs_dir / "normalize-execute-good"
    run.mkdir(parents=True)
    prepared = tmp_path / "batch.json"
    body = json.dumps(
        {
            "run_id": "batch",
            "instrument_namespace": "ns",
            "jobs": [{"dataset_id": 41, "output": {"dataset_path": "/committed/41"}}],
        }
    )
    digest = hashlib.sha256(body.encode()).hexdigest()
    (run / "submitted-batch.json").write_text(body)
    (run / "execution-intent.json").write_text(
        json.dumps(
            {
                "operation": "normalize",
                "environment": "testing",
                "recipe": "source",
                "execution_id": run.name,
                "run_id": "batch",
                "submitted_source": str(prepared),
                "submitted_sha256": digest,
            }
        )
    )
    report = {
        "submitted_sha256": digest,
        "ranking_policy": "turnover",
        "requested_interval": {"start": "2026-10-27T00:00:00Z", "end": "2026-10-28T00:00:00Z"},
        "requests": [{"request": r.model_dump(mode="json")} for r in config.requests],
    }
    prepared.with_name("prepare-report.json").write_text(json.dumps(report))
    assert discovery.linked_submissions(paths, source, config) == {
        ("ns", "41", "/committed/41", digest): run.name,
    }
    (run / "submitted-batch.json").write_text(body + "\n")
    assert not discovery.linked_submissions(paths, source, config)


def test_native_scope_filters_use_contributing_task_evidence() -> None:
    from forge.historical.normalization.config import NormalizationSource

    gate = {
        "exchange": "gateio",
        "market_category": "spot",
        "instrument_type": "spot",
        "symbol": "BTC_USDT",
    }
    request = NormalizationSource.model_validate(
        {
            **{field: gate[field] for field in ("exchange", "market_category", "instrument_type")},
            "data_type": "order_book_l2",
            "scope": {"kind": "instrument", "identifier": "BTC_USDT"},
            "source_options": {"feed": "snapshot"},
            "source_key": "registered-feed",
        }
    )
    task: dict[str, Any] = {
        "input_path": "/raw/snapshot",
        "raw_schema": {"marketforge_source": {"source_options": {"feed": "snapshot"}}},
    }
    assert discovery.matches(request, gate, [task], {"/raw/snapshot": "registered-feed"})
    assert not discovery.matches(request, gate, [task], {"/raw/snapshot": "other"})
    task["raw_schema"]["marketforge_source"]["source_options"]["feed"] = "incremental"
    assert not discovery.matches(request, gate, [task], {"/raw/snapshot": "registered-feed"})


@pytest.mark.asyncio
async def test_degraded_and_missing_remain_visible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    rows, evidence = publication(5, "trades")
    evidence[-1]["manifest"]["integrity_status"] = "degraded"
    result = await discover(monkeypatch, tmp_path, recipe(), {5: (rows, evidence)}, {5})
    assert result["selected_dataset_ids"] == [500]
    assert result["missing"][0]["request_index"] == 2
    assert any("degraded" in issue["reason"] for issue in result["blockers"])
