"""Tiny local fixtures using the Forge v5 normalized and v1 physical-merge layouts."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from forge.datasets.parquet import arrow_modules

START = 1793059200000000001
PRICE = Decimal("3456.1234567890123456789012345678")
NAMESPACE = "marketforge:testing:historical:v1"
ID_BASE = 2**53 + 10


def build(data_root: Path) -> None:
    pa, pq = arrow_modules()
    decimal = pa.decimal256(76, 28)
    identity = [
        pa.field("event_timestamp_ns", pa.int64(), False),
        pa.field("system_timestamp_ns", pa.int64()),
        pa.field("exchange", pa.string(), False),
        pa.field("instrument_id", pa.int64(), False),
        pa.field("symbol", pa.string(), False),
        pa.field("stream_id", pa.string(), False),
    ]
    trade = [
        pa.field("trade_id", pa.string()),
        pa.field("sequence", pa.uint64()),
        pa.field("side", pa.string(), False),
        pa.field("price", decimal, False),
        *[
            pa.field(name, decimal)
            for name in ("quantity_base", "quantity_quote", "quantity_contracts")
        ],
        pa.field("is_rpi", pa.bool_()),
        *[
            pa.field(name, decimal)
            for name in ("trade_iv", "mark_iv", "index_price", "mark_price")
        ],
    ]
    level = pa.struct(
        [
            pa.field("side", pa.string(), False),
            pa.field("price", decimal, False),
            *[
                pa.field(name, decimal)
                for name in ("quantity_base", "quantity_quote", "quantity_contracts")
            ],
            pa.field("order_count", pa.uint64()),
        ]
    )
    depth = pa.struct(
        [
            pa.field("event_boundary", pa.uint8(), False),
            pa.field("source_operation", pa.uint8(), False),
            pa.field("sequence_start", pa.uint64()),
            pa.field("sequence_end", pa.uint64()),
            pa.field("changes", pa.list_(pa.field("item", level, False)), False),
        ]
    )
    provenance = [
        pa.field("stream_rank", pa.uint32(), False),
        *[
            pa.field(name, pa.uint64(), False)
            for name in (
                "dataset_id",
                "task_id",
                "event_ordinal",
                "canonical_row_offset",
                "canonical_row_count",
            )
        ],
    ]
    merged_schema = pa.schema(
        [
            *identity,
            pa.field("event_kind", pa.uint8(), False),
            *provenance,
            pa.field("trade", pa.struct(trade)),
            pa.field("depth", depth),
        ]
    )

    def write(
        root: Path,
        relative: str,
        rows: list[dict[str, Any]],
        schema: Any,
        *,
        group_size: int = 2,
    ) -> dict[str, Any]:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist(rows, schema=schema), path, row_group_size=group_size
        )
        times = [row["event_timestamp_ns"] for row in rows]
        return {
            "path": relative,
            "rows": len(rows),
            "size_bytes": path.stat().st_size,
            "start_timestamp_ns": min(times),
            "end_timestamp_ns": max(times),
        }

    def manifest(
        root: Path, payload: dict[str, Any], files: list[dict[str, Any]]
    ) -> None:
        (root / "manifest.json").write_text(
            json.dumps(
                {
                    **payload,
                    "files": files,
                    "files_written": len(files),
                    "start_timestamp_ns": min(
                        file["start_timestamp_ns"] for file in files
                    ),
                    "end_timestamp_ns": max(file["end_timestamp_ns"] for file in files),
                }
            ),
            encoding="utf-8",
        )

    trade_root = data_root / "datasets/normalized/101"
    depth_root = data_root / "datasets/normalized/102"
    merge_root = data_root / "datasets/merged/501"
    eth = {
        "exchange": "binance",
        "symbol": "ETHUSDT",
        "instrument_id": ID_BASE,
        "instrument_type": "perpetual",
        "market_category": "linear",
        "base_asset": "ETH",
        "quote_asset": "USDT",
    }
    btc = {
        "exchange": "okx",
        "symbol": "BTC-USDT-SWAP",
        "instrument_id": ID_BASE + 1,
        "instrument_type": "perpetual",
        "market_category": "linear",
        "base_asset": "BTC",
        "quote_asset": "USDT",
    }
    eth_stream = "binance.linear.perpetual.ETHUSDT.trades"
    btc_stream = "okx.linear.perpetual.BTC-USDT-SWAP.depth"
    payload = {
        "side": "buy",
        "price": PRICE,
        "quantity_base": Decimal("0.125"),
        "sequence": ID_BASE,
    }
    level_payload = {
        "side": "bid",
        "price": Decimal("95000.125"),
        "quantity_base": Decimal(1),
    }
    eth_record = {
        "event_timestamp_ns": START,
        "system_timestamp_ns": START + 100,
        **eth,
        "stream_id": eth_stream,
    }
    btc_record = {"event_timestamp_ns": START + 1, **btc, "stream_id": btc_stream}
    common = {
        "protocol_version": 5,
        "instrument_namespace": NAMESPACE,
        "integrity_status": "clean",
        "auxiliary_files": [],
        "source_tasks": [1],
    }
    trade_dir = f"streams/{'a' * 64}/trades"
    trade_file = write(
        trade_root,
        f"{trade_dir}/part-000000.parquet",
        [
            eth_record | payload,
            eth_record | payload | {"event_timestamp_ns": START + 2},
        ],
        pa.schema([*identity, *trade]),
    )
    manifest(
        trade_root,
        {
            **common,
            "dataset_id": 101,
            "job_id": 11,
            "content_type": "trades",
            "events_written": 2,
            "resolved_instruments": [{"instrument": eth}],
            "streams": [
                {
                    "stream_id": eth_stream,
                    "directory": trade_dir,
                    "sources": [{"task_id": 1, "start_row": 0, "end_row": 2}],
                }
            ],
        },
        [trade_file],
    )
    depth_dir = f"streams/{'b' * 64}/depth"
    depth_file = write(
        depth_root,
        f"{depth_dir}/part-000000.parquet",
        [btc_record | level_payload],
        pa.schema([*identity, *list(level)]),
    )
    manifest(
        depth_root,
        {
            **common,
            "dataset_id": 102,
            "job_id": 12,
            "content_type": "depth",
            "events_written": 1,
            "resolved_instruments": [{"instrument": btc}],
            "streams": [
                {
                    "stream_id": btc_stream,
                    "directory": depth_dir,
                    "sources": [{"task_id": 1, "start_row": 0, "end_row": 1}],
                }
            ],
        },
        [depth_file],
    )

    def evidence(rank: int, engine_id: int, offset: int) -> dict[str, Any]:
        return {
            "stream_rank": rank,
            "dataset_id": engine_id,
            "task_id": 1,
            "event_ordinal": offset + 1,
            "canonical_row_offset": offset,
            "canonical_row_count": 1,
        }

    records = [
        eth_record | evidence(0, 101, 0) | {"trade": payload, "event_kind": 0},
        btc_record
        | evidence(1, 102, 0)
        | {
            "depth": {
                "event_boundary": 1,
                "source_operation": 1,
                "changes": [level_payload],
            },
            "event_kind": 1,
        },
        eth_record
        | evidence(0, 101, 1)
        | {"trade": payload, "event_kind": 0, "event_timestamp_ns": START + 2},
    ]
    physical = write(merge_root, "part-000000.parquet", records, merged_schema)
    streams = [
        {
            **{k: market[k] for k in ("exchange", "symbol", "instrument_id")},
            "stream_id": stream,
            "stream_rank": rank,
            "kind": kind,
            "dataset_id": engine_id,
            "base_currency": market["base_asset"],
            "quote_currency": market["quote_asset"],
        }
        for market, stream, rank, kind, engine_id in (
            (eth, eth_stream, 0, "trade", 101),
            (btc, btc_stream, 1, "depth", 102),
        )
    ]
    manifest(
        merge_root,
        {
            "physical_schema_version": 1,
            "protocol_version": 1,
            "job_id": 501,
            "mode": "combined",
            "input_datasets": [
                {
                    "dataset_path": f"../../normalized/{i}",
                    "dataset_id": i,
                    "manifest_version": 5,
                }
                for i in (101, 102)
            ],
            "time_range": {"start_ns": START, "end_ns": START + 3},
            "streams": streams,
            "events_written": 3,
            "trade_events": 2,
            "depth_events": 1,
            "canonical_rows": 3,
            "row_groups": 2,
            "boundary_links": [],
            "skipped_initializations": [],
            "checkpoints": [],
            "initial_reconstruction": [],
        },
        [physical],
    )
    pq.write_table(pa.table({"value": [42]}), data_root / "datasets/unknown.parquet")
