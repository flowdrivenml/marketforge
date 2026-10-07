`marketforge-process` is the offline processing engine used to transform downloaded raw market data into canonical MarketForge datasets and to merge existing canonical datasets.

Users normally do **not** create processing JSON files manually.

Processing and merging are exposed through simple MarketForge CLI commands:

```bash
marketforge process ...
```

and:

```bash
marketforge merge ...
```

The Python CLI resolves the required metadata, builds the appropriate immutable job specification, and invokes the Rust `marketforge-process` engine automatically.

Conceptually:

```text
User
    ↓
marketforge process / marketforge merge
    ↓
Python control plane
    ↓
generated job JSON
    ↓
marketforge-process
    ↓
canonical Parquet dataset(s)
```

The JSON structures documented below describe the internal Python ↔ Rust protocol. They are useful for understanding MarketForge's architecture, debugging processing jobs, reproducibility, and advanced direct engine usage.

## Python ↔ Rust Processing Boundary

Python owns the offline **control plane**:

```text
discovery
metadata resolution
input selection
planning
job construction
Rust invocation
result handling
PostgreSQL state updates
```

Rust owns the offline **execution/data plane**:

```text
source reading
decompression
parsing
normalization
validation
worker execution
temporary runs
deterministic merging
BookStore
Arrow / Parquet
manifest creation
dataset publication
```

Rust must never query PostgreSQL or require additional metadata after execution begins.

Python resolves everything required by Rust into a **self-contained, immutable job JSON**.

Two job types exist:

```text
ProcessJob
MergeJob
```

They are intentionally separate because they operate on fundamentally different inputs.

---

## Process Job

A `ProcessJob` accepts one or more independent raw `WorkTask`s.

From the user's perspective, processing is requested through the CLI:

```bash
marketforge process ...
```

MarketForge then automatically resolves the downloaded files and catalog metadata required to construct the job.

Internally:

```text
ProcessJob
    ↓
WorkTask[]
    ├── raw input A
    ├── raw input B
    ├── raw input C
    └── ...
    ↓
parallel Rust workers
    ↓
one canonical dataset per WorkTask
```

Each `WorkTask` is self-contained. Python resolves PostgreSQL catalog metadata before creating it:

```text
catalog.raw_formats
        +
catalog.normalization_rules
        +
catalog.instruments
        +
catalog.instrument_specs
        +
raw-file discovery
        ↓
Python
        ↓
WorkTask
```

This means the Rust engine does not need to know how to query MarketForge's catalog or discover exchange metadata.

### Process Job Structure

A generated processing job conceptually looks like:

```json
{
  "protocol_version": 1,
  "job_id": 42,

  "operation": {
    "type": "process",

    "inputs": [
      {
        "task_id": 1,
        "dataset_id": 1001,

        "stream_id": "bybit:BTCUSDT:trade",

        "input_path": "data/raw/bybit/spot/BTCUSDT/BTCUSDT_2026-09-01.csv.gz",
        "source_container": "gzip",
        "archive_member": null,

        "format_code": "BYBIT-T1",

        "exchange": "bybit",
        "instrument_id": 1,
        "symbol": "BTCUSDT",

        "raw_format": {
          "container_format": "csv",
          "compression": "gzip",
          "record_format": "rows",
          "header": true,
          "ordering": "chronological",

          "fields": [
            {
              "name": "id",
              "type": "integer"
            },
            {
              "name": "timestamp",
              "type": "integer",
              "unit": "milliseconds"
            },
            {
              "name": "price",
              "type": "decimal"
            },
            {
              "name": "volume",
              "type": "decimal"
            },
            {
              "name": "side",
              "type": "string"
            },
            {
              "name": "rpi",
              "type": "integer/bool"
            }
          ]
        },

        "normalization": {
          "event_timestamp": {
            "source": "timestamp"
          },

          "trade_id": {
            "source": "id"
          },

          "price": {
            "source": "price"
          },

          "quantity": {
            "source": "volume"
          },

          "side": {
            "source": "side",
            "transform": "casefold"
          },

          "optional": {
            "is_rpi": {
              "source": "rpi",
              "transform": "bool"
            }
          }
        },

        "instrument": {
          "instrument_kind": "spot",
          "quantity_type": "base",

          "tick_size": "0.1",
          "qty_step": "0.000001",

          "contract_kind": null,
          "contract_value": null,
          "contract_value_asset": null
        },

        "source_ordering": "source_ordered"
      }
    ]
  },

  "resources": {
    "workers": 4,
    "memory_budget_bytes": 4294967296,

    "scratch_path": "data/.work/42",
    "scratch_budget_bytes": 21474836480,

    "parquet": {
      "row_group_target_bytes": 134217728,
      "file_target_bytes": 536870912
    }
  },

  "integrity_policy": {
    "profile": "standard"
  }
}
```

### Processing Semantics

Processing does **not implicitly merge inputs**.

```text
10 WorkTasks
    ↓
parallel processing
    ↓
10 canonical datasets
```

This allows independent archives to be processed concurrently without coupling their output.

Each worker remains sequential internally:

```text
WorkTask
    ↓
Open Source
    ↓
Stream Decompress / Unpack
    ↓
Parse
    ↓
Normalize
    ↓
Validate
    ↓
Bounded RAM Batch
    ↓
Temporary Runs
    ↓
Parquet
    ↓
Canonical Dataset
```

The number of workers affects execution performance, not dataset semantics.

---

## Merge Job

A `MergeJob` operates only on already-completed canonical MarketForge datasets.

From the user's perspective:

```bash
marketforge merge ...
```

The user selects the datasets to merge. Python resolves their locations and metadata and generates the merge job automatically.

Internally:

```text
MergeJob
    ↓
canonical dataset inputs[]
    ↓
deterministic chronological merge
    ↓
ONE canonical output dataset
```

There are no separate:

```text
trade merge
depth merge
trade + depth merge
same-exchange merge
cross-exchange merge
```

modes.

The rule is simply:

> **Merge the selected canonical datasets into one chronologically ordered canonical dataset.**

If the selected inputs contain trades, the output contains trades.

If they contain L2 events, the output contains L2 events.

If they contain both, the output contains both.

Datasets from different exchanges can be selected in exactly the same way as datasets from the same exchange.

### Merge Job Structure

A generated merge job conceptually looks like:

```json
{
  "protocol_version": 1,
  "job_id": 43,
  "dataset_id": 2001,

  "operation": {
    "type": "merge",

    "inputs": [
      {
        "dataset_id": 1001,
        "dataset_path": "data/datasets/1001",
        "stream_id": "dataset:1001"
      },
      {
        "dataset_id": 1002,
        "dataset_path": "data/datasets/1002",
        "stream_id": "dataset:1002"
      },
      {
        "dataset_id": 1003,
        "dataset_path": "data/datasets/1003",
        "stream_id": "dataset:1003"
      }
    ]
  },

  "streams": [
    {
      "stream_id": "dataset:1001",
      "stream_rank": 0
    },
    {
      "stream_id": "dataset:1002",
      "stream_rank": 1
    },
    {
      "stream_id": "dataset:1003",
      "stream_rank": 2
    }
  ],

  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  },

  "time_range": null,

  "resources": {
    "memory_budget_bytes": 4294967296,

    "scratch_path": "data/.work/43",
    "scratch_budget_bytes": 21474836480,

    "parquet": {
      "row_group_target_bytes": 134217728,
      "file_target_bytes": 536870912
    }
  },

  "output": {
    "staging_path": "data/.work/43/output",
    "dataset_path": "data/datasets/2001"
  }
}
```

### Merge Inputs

Merge inputs are already canonical MarketForge datasets.

They therefore do not require:

```text
raw-format definitions
exchange archive information
compression configuration
normalization rules
instrument normalization rules
raw parsers
```

Python only needs to resolve which completed datasets were selected and provide their canonical dataset locations.

### Merge Ordering

Canonical events are merged deterministically using:

```text
event_timestamp_ns
    ↓
stream_rank
    ↓
preserved source-event order
```

`stream_rank` is assigned during planning and must not depend on worker scheduling, filesystem enumeration order, or runtime completion order.

### Merge Time Range

`time_range` is optional.

```json
"time_range": null
```

means:

```text
merge all available events
```

A bounded merge may instead specify:

```json
{
  "time_range": {
    "start_timestamp_ns": 1788476400000000000,
    "end_timestamp_ns": 1788649200000000000
  }
}
```

which means:

```text
read selected datasets
        ↓
retain events inside requested interval
        ↓
chronological merge
        ↓
one output dataset
```

---

## Process vs Merge

The distinction is intentionally simple:

```text
PROCESS
────────────────────────────────────

raw exchange inputs
        ↓
WorkTask[]
        ↓
parallel normalization
        ↓
N independent canonical datasets


MERGE
────────────────────────────────────

existing canonical datasets
        ↓
DatasetInput[]
        ↓
deterministic chronological merge
        ↓
ONE canonical dataset
```

Processing converts **raw exchange representations into MarketForge's canonical representation**.

Merging operates only on **already canonical MarketForge data**.

This keeps raw-format complexity completely outside the merge engine.

---

## Overall Execution Boundary

```text
                         PYTHON

User CLI
    ↓
marketforge process / marketforge merge
    ↓
discovery
    ↓
PostgreSQL metadata resolution
    ↓
input selection
    ↓
planning
    ↓
┌────────────────────┐
│ ProcessJob         │
│        OR          │
│ MergeJob           │
└────────────────────┘
    ↓
immutable JSON
    ↓
────────────────────────────────────────
             Python ↔ Rust boundary
────────────────────────────────────────
    ↓
                         RUST

load job
    ↓
validate job
    ↓
execute
    ↓
process / merge
    ↓
Parquet
    ↓
manifest
    ↓
atomic publication
    ↓
ProcessingResult
    ↓
────────────────────────────────────────
    ↓
                         PYTHON

read ProcessingResult
    ↓
update PostgreSQL
    ↓
report result to CLI
```

The generated JSON is an internal protocol and normally does not need to be edited manually. Keeping it explicit nevertheless provides a reproducible description of exactly what the Rust engine was instructed to execute.

> **Boundary rule:** Python decides **what must be processed or merged and fully resolves its configuration**. Rust decides **how that immutable job is executed efficiently, deterministically, and correctly**.

Rust must not query PostgreSQL or depend on Python after execution begins.