from __future__ import annotations

TRADE_SCHEMA = {
    "header": True,
    "granularity": "daily",
    "archive_day_boundary": "UTC+8",
    "fields": [
        {
            "name": "instrument_name",
            "type": "string",
        },
        {
            "name": "trade_id",
            "type": "integer/string",
        },
        {
            "name": "side",
            "type": "string",
        },
        {
            "name": "price",
            "type": "decimal",
        },
        {
            "name": "size",
            "type": "decimal",
        },
        {
            "name": "created_time",
            "type": "integer",
            "representation": "unix_epoch",
            "unit": "milliseconds",
            "precision": "millisecond",
        },
        {
            "name": "source",
            "type": "integer",
        },
    ],
}


STANDARD_L2_SCHEMA = {
    "granularity": "daily",
    "archive_day_boundary": "UTC",
    "ordering": "chronological",
    "maximum_depth_per_side": 5000,
    "fields": [
        {
            "name": "instId",
            "type": "string",
        },
        {
            "name": "action",
            "type": "string",
            "values": [
                "snapshot",
                "update",
            ],
        },
        {
            "name": "ts",
            "type": "string/integer",
            "representation": "unix_epoch",
            "unit": "milliseconds",
            "precision": "millisecond",
        },
        {
            "name": "asks",
            "type": "array",
            "level_schema": [
                "price",
                "quantity",
                "order_count",
            ],
        },
        {
            "name": "bids",
            "type": "array",
            "level_schema": [
                "price",
                "quantity",
                "order_count",
            ],
        },
    ],
    "order_book": {
        "event_model": "periodic_snapshot_and_incremental_updates",
        "maximum_depth_per_side": 5000,
        "snapshot_frequency": "approximately_15_minutes",
        "update_frequency": "approximately_1_second",
        "sequence_id": False,
        "update_id": False,
        "delta_delete_quantity": "0",
        "reconstruction_required": True,
        "snapshot_resets_book": True,
    },
}


RAW_FORMATS = [
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            **TRADE_SCHEMA,
            "ordering": "chronological",
        },
        "notes": (
            "Daily UTC+8 ZIP containing headered CSV. "
            "Physical parser is shared by all inspected OKX trade "
            "datasets. For Spot, size represents base-asset quantity."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            **TRADE_SCHEMA,
            "ordering": "chronological",
        },
        "notes": (
            "Daily UTC+8 ZIP containing headered CSV. "
            "Physical parser is shared by all inspected OKX trade "
            "datasets. For Linear Perpetual, size represents contracts."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            **TRADE_SCHEMA,
            "ordering": "chronological",
        },
        "notes": (
            "Daily UTC+8 ZIP containing headered CSV. "
            "Physical parser is shared by all inspected OKX trade "
            "datasets. For Inverse Perpetual, size represents contracts."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "option",
        "market_category": "option",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            **TRADE_SCHEMA,
            "ordering": "not_globally_chronological",
            "archive_members": 1,
            "instrument_scope": "multiple_option_instruments",
            "trade_id_scope": "per_instrument",
            "sorting_required": True,
        },
        "notes": (
            "Daily UTC+8 option-chain ZIP containing one CSV with trades "
            "from many option instruments. Rows are not globally "
            "chronological. trade_id is scoped to instrument_name, so "
            "raw trade identity is at least (instrument_name, trade_id)."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "jsonl",
        "compression": "tar.gz",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Daily UTC TAR.GZ containing one JSON Lines instrument "
            "stream. Periodic full snapshots are followed by incremental "
            "updates. No sequence or update identifier is present."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "jsonl",
        "compression": "tar.gz",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Same physical L2 parser and reconstruction model as OKX "
            "Spot. Level quantity represents derivative contract "
            "quantity rather than Spot base quantity."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "jsonl",
        "compression": "tar.gz",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Same physical L2 parser and reconstruction model as OKX "
            "Spot and Linear Perpetual. Level quantity represents "
            "derivative contract quantity."
        ),
    },
    {
        "format_code": "OKX-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "container_format": "jsonl",
        "compression": "tar.gz",
        "record_format": "events",
        "schema": {
            **STANDARD_L2_SCHEMA,
            "archive_members": "multiple_option_contracts",
            "member_coverage": "depends_on_option_lifetime",
            "order_book": {
                **STANDARD_L2_SCHEMA["order_book"],
                "reconstruction_scope": "per_option_contract",
                "actual_depth": "variable_sparse",
            },
        },
        "notes": (
            "Daily option-chain TAR.GZ containing separate members for "
            "many option instruments. Event representation matches "
            "OKX-B1, but each option contract must be reconstructed "
            "independently. Actual populated depth may be far below "
            "the 5000-level maximum."
        ),
    },
]
