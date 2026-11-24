"""Exercise the new projections against rollback-only registry evidence."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from rich.console import Console

from forge.datasets import converted as converted_index
from forge.datasets import parquet, query, views, workspace_inventory
from forge.shared.database.connection import registry_id
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.dataset_browse import next_page, render_browse, render_detail
from forge.shared.terminal.theme import build_theme

pytestmark = [pytest.mark.integration, pytest.mark.database]
START = 1793059200123456789
END = START + 8_000_000_000


@pytest.mark.asyncio
async def test_stage_views(
    database: AsyncConnection[TupleRow],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = uuid4().hex
    root = tmp_path
    registry = await registry_id(database)
    first = {
        "symbol": "BTCUSDT",
        "exchange": "bybit",
        "instrument_id": 11,
        "stream_id": f"{token}.btc",
        "kind": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "start_timestamp_ns": START,
        "end_timestamp_ns": START + 1,
    }
    last = {
        **first,
        "symbol": "ETHUSDT",
        "exchange": "okx",
        "instrument_id": 12,
        "stream_id": f"{token}.eth",
        "kind": "depth",
        "start_timestamp_ns": END - 1,
        "end_timestamp_ns": END,
    }

    async def publish(
        operation: str,
        kinds: list[list[dict[str, Any]]],
        *,
        suffix: str = "",
        sizes: list[int] | None = None,
    ) -> tuple[int, list[int]]:
        location = root / (operation + suffix)
        location.mkdir()
        manifest = location / "manifest.json"
        manifest.write_text("{}")
        selected = await (
            await database.execute(
                """INSERT INTO historical.dataset_publications
            (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    token,
                    token + operation + suffix,
                    operation,
                    str(location),
                    str(manifest),
                    Jsonb({"manifest_sha256": sha256(b"{}").hexdigest()}),
                ),
            )
        ).fetchone()
        assert selected
        publication = int(selected[0])
        ids: list[int] = []
        for idx, members in enumerate(kinds):
            filename = f"part-{idx}.parquet"
            path = location / filename
            path.write_bytes(b"fixture")
            saved = await (
                await database.execute(
                    """INSERT INTO historical.datasets
                (data_type,start_timestamp_ns,end_timestamp_ns,storage_format,storage_path,status,
                completed_at,operation,storage_root_key,manifest_path,operation_details,
                publication_id,primary_file,file_sha256)
                VALUES (%s,%s,%s,'parquet',%s,'complete',now(),%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (
                        "trade_l2"
                        if len(members) > 1
                        else "l2"
                        if members[0]["kind"] == "depth"
                        else "trade",
                        min(int(member["start_timestamp_ns"]) for member in members),
                        max(int(member["end_timestamp_ns"]) for member in members),
                        str(path),
                        operation,
                        str(location),
                        str(manifest),
                        Jsonb(
                            {
                                "memberships": members,
                                "size_bytes": sizes[idx] if sizes else 7,
                                "rows": 2,
                                "integrity": "clean",
                            }
                        ),
                        publication,
                        filename,
                        sha256(b"fixture").hexdigest(),
                    ),
                )
            ).fetchone()
            assert saved
            ids.append(int(saved[0]))
        return publication, ids

    normalized, file_ids = await publish(
        "normalize",
        [[first]] * 3 + [[{**first, "kind": "depth", "stream_id": f"{token}.btc.depth"}]],
    )
    merged, _ = await publish("merge", [[first, last]])

    def indexed(quote: str, variant: str) -> tuple[str, dict[str, Any]]:
        import json

        key = sha256((token + quote + variant).encode()).hexdigest()
        folder = root / key
        folder.mkdir()
        file = folder / "part.parquet"
        pa, pq = parquet.arrow_modules()
        source_price = Decimal("123.456789012")
        target_price = Decimal("246.913578024")
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "event_timestamp_ns": point,
                        "target_quote": quote,
                        "trade": {"price": source_price},
                        "trade_target": {"price_target": target_price},
                    }
                    for point in (START, END)
                ]
            ).append_column("event_kind", pa.array([0, 1], type=pa.uint8())),
            file,
            row_group_size=1,
        )
        manifest = folder / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "job_id": 9876,
                    "protocol_version": 1,
                    "conversion_schema_version": 1,
                    "input": {"merge_job_id": 222},
                    "conversion": {"target_quote": quote},
                    "integrity": {"status": "pass"},
                    "output": {
                        "content_type": "combined",
                        "first_timestamp_ns": START,
                        "last_timestamp_ns": END,
                        "files_written": 1,
                        "files": [
                            {
                                "path": file.name,
                                "rows": 2,
                                "size_bytes": file.stat().st_size,
                                "row_groups": 2,
                            }
                        ],
                    },
                }
            )
        )
        files = [manifest, file, folder / "execution.json", folder / "conversion-runtime.json"]
        files[2].write_text("{}")
        files[3].write_text(
            json.dumps({"job": {"job_id": 9876, "source": {"merge_job_id": 222, "streams": []}}})
        )
        return key, {
            "status": "complete",
            "registration": "not_available",
            "input": {"registry_publication_id": merged, "source_instrument_namespace": token},
            "publication": {
                "manifest": str(manifest),
                "fingerprints": {p.name: sha256(p.read_bytes()).hexdigest() for p in files},
            },
        }

    indexed_records = [indexed("USDT", "a"), indexed("USDT", "b"), indexed("USDC", "a")]
    for key, receipt in indexed_records:
        await database.execute(
            """INSERT INTO historical.resolved_work
            (registry_id,operation,work_key,owner,evidence,result)
            VALUES (%s,'convert',%s,'fixture','{}'::jsonb,%s)""",
            (registry, key, Jsonb(receipt)),
        )
    request = query.DatasetRequest("list", view="publications", search=token, limit=1)
    result = await views.page(database, request)
    assert result["total"] == 5
    assert result["stages"] == {
        "normalize": {"publications": 1, "files": 4},
        "merge": {"publications": 1, "files": 1},
        "convert": {"publications": 3, "files": 3},
    }
    pub = result["publications"][0]
    assert pub["publication_id"] == normalized
    assert pub["file_count"] == 4 and pub["size_bytes"] == 28
    assert result["has_more"]
    combined = await views.page(database, replace(request, data_type="combined"))
    assert combined["publications"][0]["publication_id"] == normalized
    assert combined["publications"][0]["data_type"] == "trade_l2"
    assert combined["publications"][0]["matching_file_count"] == 4
    merged_view = await views.page(database, replace(request, operation="merge", data_type="depth"))
    item = merged_view["publications"][0]
    assert item["data_type"] == "trade_l2" and item["size_bytes"] == 7
    assert {m["symbol"] for m in item["memberships"]} == {first["symbol"], last["symbol"]}
    assert [m["symbol"] for m in item["matching_memberships"]] == [last["symbol"]]
    time_view = await views.page(
        database,
        replace(request, view="coverage", start=END - 1, limit=50),
    )
    assert time_view["total"] == 4
    assert {item["symbol"] for item in time_view["coverage"]} == {last["symbol"]}
    assert {item["operation"] for item in time_view["coverage"]} == {"merge", "convert"}
    time_only = await views.page(
        database,
        query.DatasetRequest("coverage", view="coverage", publication_id=merged, start=END - 1),
    )
    assert [item["symbol"] for item in time_only["coverage"]] == [last["symbol"]]
    converted_view = await views.page(
        database,
        replace(request, view="coverage", operation="convert", target_quote="USDT", limit=50),
    )
    assert len(converted_view["coverage"]) == 4
    assert len({item["variant"] for item in converted_view["coverage"]}) == 2
    assert all(
        item["supporting_outputs"] == 1 and item["supporting_files"] == 0
        for item in converted_view["coverage"]
    )
    timeline_rows = await views.page(database, replace(request, view="timelines", limit=50))
    converted_timeline = [
        row for row in timeline_rows["timelines"] if row["operation"] == "convert"
    ]
    assert len(converted_timeline) == 6
    assert {row["target_quote"] for row in converted_timeline} == {"USDT", "USDC"}
    assert len({row["variant"] for row in converted_timeline}) == 3
    assert all(
        row["coverage"] == "source_observed_extent"
        and row["publications"][0]["registry_publication_id"] is None
        for row in converted_timeline
    )
    assert {(row["operation"], row["data_type"]) for row in timeline_rows["timelines"]} >= {
        ("normalize", "trade"),
        ("merge", "trade"),
        ("merge", "l2"),
        ("convert", "trade"),
    }
    grouped = await views.page(
        database,
        replace(request, view="coverage", operation="normalize", data_type="trades"),
    )
    assert grouped["coverage"][0]["supporting_files"] == 3
    file_request = replace(request, view="files", operation="normalize")
    files = await views.page(database, file_request)
    assert files["total"] == 4 and files["datasets"][0]["id"] == file_ids[0]

    @asynccontextmanager
    async def connection(_: str | None) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(query, "database_connection", connection)

    def fixture_dsn(_: Path | None) -> str:
        return "fixture"

    monkeypatch.setattr(query, "database_dsn", fixture_dsn)
    paths = ProjectPaths(root, root / "data")
    legacy = await query.browse(
        paths, query.DatasetRequest("list", publication=token + "normalize"), detailed=False
    )
    assert set(legacy) == {"datasets", "limit", "offset", "interval_semantics"}
    assert [item["id"] for item in legacy["datasets"]] == file_ids
    show_request = query.DatasetRequest("show", publication_id=merged)
    show = await query.browse(paths, show_request)
    assert show["publication"]["publication_id"] == merged
    assert len(show["publication"]["memberships"]) == 2
    ref = indexed_records[0][0]
    converted_request = query.DatasetRequest("show", output_ref=ref)
    converted = await query.browse(paths, converted_request)
    assert converted["output"]["target_quote"] == "USDT"
    assert converted["output"]["files"][0]["size_bytes"] > 9
    assert converted["output"]["engine_job_id"] == 9876
    converted_schema = await query.browse(
        paths, replace(converted_request, dataset_action="schema", part=1)
    )
    assert {field["name"] for field in converted_schema["columns"]} >= {"trade", "trade_target"}
    converted_head = await query.browse(
        paths,
        replace(converted_request, dataset_action="head", part=1, limit=1, precise=True),
    )
    assert converted_head["rows"][0]["event_timestamp_ns"] == START
    assert converted_head["rows"][0]["trade"]["price"] == Decimal("123.456789012")
    assert converted_head["rows"][0]["trade_target"]["price_target"] == Decimal("246.913578024")
    assert "publication_id" not in converted["output"]

    @asynccontextmanager
    async def workspace_connection(_: ProjectPaths) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(workspace_inventory, "readonly", workspace_connection)
    merged_rows, more = await workspace_inventory.stage_page(paths, "Merged", 0, str(merged))
    assert len(merged_rows) == 1 and not more
    merged_row = merged_rows[0]
    assert merged_row["publication"] == merged
    assert set(merged_row["exchanges"]) == {"bybit", "okx"}
    assert set(merged_row["kinds"]) == {"trade", "depth"}
    assert (merged_row["instruments"], merged_row["file_count"], merged_row["state"]) == (
        2,
        1,
        "Registered",
    )

    outputs, more = await workspace_inventory.stage_page(paths, "Converted", 0, ref)
    assert len(outputs) == 1 and not more
    indexed_output = outputs[0]
    assert indexed_output["output_ref"] == ref and "publication" not in indexed_output
    assert set(indexed_output["exchanges"]) == {"bybit", "okx"}
    assert (indexed_output["instruments"], indexed_output["data_type"]) == (2, "combined")
    assert (indexed_output["target_quote"], indexed_output["state"]) == ("USDT", "Indexed")
    assert (indexed_output["start"], indexed_output["finish"]) == (START, END)
    assert indexed_output["file_count"] == 1
    partition_request = replace(converted_request, dataset_action="partitions")
    inventory = await query.browse(paths, partition_request)
    assert inventory["output_ref"] == ref and inventory["publication_id"] is None
    assert inventory["partitions"][0]["reference"] == "file:1"
    selected = await query.browse(
        paths, replace(partition_request, partition="file:1", row_groups=True)
    )
    assert [row["rows"] for row in selected["row_groups"]] == [1, 1]
    selected_head = await query.browse(
        paths,
        replace(
            partition_request,
            dataset_action="head",
            partition="file:1",
            data_type="trades",
            limit=1,
            column=["event_timestamp_ns", "trade"],
        ),
    )
    assert selected_head["rows"] == [
        {"event_timestamp_ns": START, "trade": {"price": Decimal("123.456789012")}}
    ]

    from forge.cli.commands.datasets import request_from_args
    from forge.cli.main import build_parser
    from forge.shared.config.models import AppSettings
    from forge.startup.context import ApplicationContext
    from forge.tui.services import datasets as tui_datasets

    cli_args = build_parser().parse_args(
        ["datasets", "partitions", "--output-ref", ref, "--testing"]
    )
    assert await query.browse(paths, request_from_args(cli_args)) == inventory
    console = Console(file=StringIO())
    context = ApplicationContext(AppSettings(), paths, None, console, console, testing=True)
    assert await tui_datasets("partitions", {"output_ref": ref}, context) == inventory

    converted_timeline_row = next(row for row in converted_timeline if row["variant"] == ref)
    converted_detail = await query.browse(
        paths,
        query.DatasetRequest(
            "timeline",
            timeline_reference=converted_timeline_row["timeline_reference"],
        ),
    )
    assert converted_detail["timeline"]["publications"][0]["output_ref"] == ref
    verify_request = query.DatasetRequest("verify", id=file_ids[0])
    verify = await query.browse(paths, verify_request)
    assert verify["entire_publication_verified"] is False
    output_check = await query.browse(paths, query.DatasetRequest("verify", output_ref=ref))
    assert output_check["artifacts_checked"] == 4

    for width in (80, 120, 160):
        stream = StringIO()
        console = Console(file=stream, width=width, color_system=None, theme=build_theme())
        render_browse(console, result, request)
        render_browse(console, files, file_request)
        render_browse(console, merged_view, replace(request, operation="merge", data_type="depth"))
        render_browse(console, time_view, replace(request, dataset_action="coverage"))
        render_detail(console, show_request, show, environment="testing")
        render_detail(console, converted_request, converted, environment="testing")
        render_detail(console, verify_request, verify, environment="testing")
        rendered = stream.getvalue()
        assert "1 publication shown of 5 matching" in rendered
        assert "1 file shown of 4 matching" in rendered
        assert "4 coverage groups shown of 4 matching" in rendered
        assert ref in rendered and "USDT" in rendered
        assert "primary file and shared manifest checksum" in rendered
        assert "\x1b" not in rendered
        assert "Trades + Depth" in rendered

    await database.execute(
        """INSERT INTO historical.publication_supersessions
        (publication_id,canonical_id,state,cleanup_id,evidence)
        VALUES (%s,%s,'retained',%s,'{}'::jsonb)""",
        (merged, normalized, uuid4()),
    )
    ordinary = await views.page(database, replace(request, operation="merge"))
    assert ordinary["total"] == 0
    timeline_ordinary = await views.page(
        database, replace(request, view="timelines", operation="merge")
    )
    assert timeline_ordinary["total"] == 0
    historic = await query.browse(paths, show_request)
    assert historic["publication"]["canonical_publication_id"] == normalized
    assert historic["publication"]["supersession_state"] == "retained"
    assert historic["publication"]["status"] == "complete"

    # These later-created outputs have older and tied observed starts.
    older, old_files = await publish(
        "merge", [[{**first, "start_timestamp_ns": START - 1}]], suffix="-one", sizes=[10]
    )
    tied, tied_files = await publish(
        "merge", [[{**first, "start_timestamp_ns": START - 1}]], suffix="-two", sizes=[2]
    )
    newest, newest_files = await publish(
        "merge", [[{**first, "start_timestamp_ns": START + 1}]], suffix="-three", sizes=[100]
    )
    selection = replace(request, operation="merge", sort="time", order="asc")
    oldest_page = await query.browse(paths, selection, detailed=False)
    next_result = await query.browse(paths, replace(selection, offset=1), detailed=True)
    assert oldest_page["publications"][0]["publication_id"] == older
    assert next_result["publications"][0]["publication_id"] == tied
    assert oldest_page["total"] == 3
    assert next_page(selection, oldest_page).endswith(
        f"--search {token} --sort time --order asc --limit 1 --offset 1"
    )
    assert oldest_page == await query.browse(paths, selection, detailed=True)
    assert (
        newest
        == (await views.page(database, replace(selection, sort=None, order=None)))["publications"][
            0
        ]["publication_id"]
    )
    assert (
        older
        == (await views.page(database, replace(selection, sort=None)))["publications"][0][
            "publication_id"
        ]
    )
    assert [
        row["publication_id"]
        for row in (
            await views.page(database, replace(selection, sort="size", order=None, limit=10))
        )["publications"]
    ] == [newest, older, tied]
    for view, key in (
        ("publications", "publications"),
        ("files", "datasets"),
        ("timelines", "timelines"),
    ):
        chosen = replace(selection, view=view, order="desc", limit=10)
        ordered = (await views.page(database, chosen))[key]
        assert ordered == (await views.page(database, replace(chosen, sort="start")))[key]
        bounds = [row["start_timestamp_ns"] for row in ordered]
        assert bounds == sorted(bounds, reverse=True)
    physical = await views.page(database, replace(selection, view="files", limit=10))
    assert [r["id"] for r in physical["datasets"]] == [*old_files, *tied_files, *newest_files]
    extent, _ = await publish("merge", [[first], [last]], suffix="-extent")
    mixed = await views.page(
        database,
        replace(selection, publication_id=extent, instrument="ETHUSDT", sort="end", order="desc"),
    )
    assert (
        mixed["publications"][0]["start_timestamp_ns"],
        mixed["publications"][0]["end_timestamp_ns"],
    ) == (START, END)
    assert (
        mixed["publications"][0]["file_count"] == 2
        and mixed["publications"][0]["matching_file_count"] == 1
    )
    newest_end = await views.page(database, replace(selection, sort="end", order="desc"))
    assert newest_end["publications"][0]["publication_id"] == extent
    for direction in ("asc", "desc"):
        files_with_unknowns = await views.page(
            database, replace(request, view="files", order=direction, limit=100)
        )
        converted_files = files_with_unknowns["datasets"][-3:]
        assert [item["operation"] for item in converted_files] == ["convert"] * 3
        assert all(item["start_timestamp_ns"] is None for item in converted_files)


def test_reference_options() -> None:
    from argparse import ArgumentParser

    from forge.cli.commands.datasets import configure, request_from_args

    parser = ArgumentParser()
    configure(parser)
    assert request_from_args(parser.parse_args(["show", "42"])).id == 42
    assert (
        request_from_args(parser.parse_args(["show", "--publication-id", "42"])).publication_id
        == 42
    )
    assert (
        request_from_args(parser.parse_args(["show", "--output-ref", "a" * 64])).output_ref
        == "a" * 64
    )
    for view in ("publications", "timelines", "files"):
        selected = request_from_args(
            parser.parse_args(
                [
                    "list",
                    "--view",
                    view,
                    "--operation",
                    "merge",
                    "--sort",
                    "time",
                    "--order",
                    "desc",
                ]
            )
        )
        assert selected.sort == "time" and selected.order == "desc"
    with pytest.raises(SystemExit):
        parser.parse_args(["list", "--sort", "unknown"])
    with pytest.raises(SystemExit):
        parser.parse_args(["list", "--order", "sideways"])
    with pytest.raises(SystemExit):
        parser.parse_args(["show", "42", "--publication-id", "42"])
    with pytest.raises(SystemExit):
        parser.parse_args(["find", "--publication", "42", "--engine-publication-id", "42"])


@pytest.mark.asyncio
async def test_conversion_capability_distinguishes_missing_index_from_empty() -> None:
    connection = AsyncMock(spec=AsyncConnection)
    cursor = connection.execute.return_value
    cursor.fetchone.return_value = (None,)
    assert await converted_index.indexed_outputs(connection) == ([], "unavailable", [])
    assert connection.execute.await_count == 1

    index_cursor = AsyncMock()
    index_cursor.fetchone.return_value = ("historical.resolved_work",)
    identity_cursor = AsyncMock()
    identity_cursor.fetchone.return_value = (uuid4(), "testing")
    rows_cursor = AsyncMock()
    rows_cursor.fetchall.return_value = []
    connection.execute.side_effect = [index_cursor, identity_cursor, rows_cursor]
    assert await converted_index.indexed_outputs(connection) == ([], "available", [])


@pytest.mark.asyncio
async def test_workspace_registry_aggregation_and_half_open_windows(
    database: AsyncConnection[TupleRow], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.datasets import workspace_inventory
    from forge.historical.archives.storage.acquire import root_key

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    token = uuid4().hex

    @asynccontextmanager
    async def fixture(_: ProjectPaths) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(workspace_inventory, "readonly", fixture)
    baseline = await workspace_inventory.summary(paths)
    exchange = await (
        await database.execute(
            "INSERT INTO catalog.exchanges(code,name) VALUES (%s,%s) RETURNING id",
            (token, "Disposable workspace fixture"),
        )
    ).fetchone()
    assert exchange
    await database.execute(
        """INSERT INTO catalog.instruments(exchange_id,symbol,instrument_type,market_category)
           VALUES (%s,'ABCTEST','spot','spot'),(%s,'OTHERTEST','option','linear')""",
        (exchange[0], exchange[0]),
    )
    ids: list[str] = []
    for number, day in enumerate((1, 2, 4)):
        archived = await (
            await database.execute(
                """INSERT INTO historical.remote_archives
              (exchange_id,scope_key,scope_json,data_type,source_object_key,filename,start_at,end_at)
              VALUES (%s,%s,%s,'trades',%s,%s,%s,%s) RETURNING id""",
                (
                    exchange[0],
                    token,
                    Jsonb(
                        {
                            "scope_kind": "instrument",
                            "scope_identifier": "ABCTEST",
                            "instrument_type": "spot",
                            "market_category": "spot",
                        }
                    ),
                    str(number),
                    f"a{number}.gz",
                    f"2026-01-0{day}T00:00:00Z",
                    f"2026-01-0{day + 1}T00:00:00Z",
                ),
            )
        ).fetchone()
        assert archived
        local = await (
            await database.execute(
                """INSERT INTO historical.local_archives
             (remote_archive_id,storage_root_key,relative_path,status,bytes_written,sha256,downloaded_at)
             VALUES (%s,%s,%s,'complete',10,%s,now()) RETURNING id""",
                (archived[0], root_key(paths.raw_dir), f"a{number}.gz", "f" * 64),
            )
        ).fetchone()
        assert local
        ids.append(f"local:{local[0]}")
    data = await workspace_inventory.summary(paths)
    assert data.registered == baseline.registered + 2
    assert data.exchanges == baseline.exchanges + 1
    assert data.with_history == baseline.with_history + 1
    assert (token, 1, 0, 0, 0, 2) in data.universe
    assert (token, "spot", "spot", True, False) in data.availability
    assert data.storage["Raw"] == (30, 0)
    rows, more = await workspace_inventory.stage_page(paths, "Raw", 0)
    assert not more
    assert [row["archives"] for row in rows] == [[ids[2]], ids[:2]]
    assert [row["recorded_bytes"] for row in rows] == [10, 20]
