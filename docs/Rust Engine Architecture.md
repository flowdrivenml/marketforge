
MarketForge uses **one Rust crate with two feature-gated binaries**:

```text
engine
├── marketforge-process
└── marketforge-live
```

Shared logic is compiled once and reused by both paths.

### Shared Core

```text
canonical/
normalization/
book/
validation/
```

Responsibilities:

```text
canonical
    Trade
    L2Snapshot
    L2Update
    L2Level
    sequence metadata

normalization
    timestamps
    sides/actions
    instrument-aware quantities
    shared economic transformations

book
    snapshot replacement
    L2 update application
    in-memory order-book state

validation
    canonical records
    quantities
    book state
    structural invariants
```

Historical and live **physical parsers are not shared**. They converge only after parsing:

```text
Historical archives ─→ process parser ─┐
                                       ├→ normalize → canonical → book/validation
Live WS / REST ──────→ live parser ────┘
```

### `marketforge-process`

Feature:

```text
process
```

Offline pipeline:

```text
Raw Archive
    ↓
Decompress
    ↓
Parse
    ↓
Normalize
    ↓
Validate
    ↓
Reconstruct / Sort
    ↓
Merge / Synchronize
    ↓
Partitioned Parquet
```

Process-only modules:

```text
process/
├── job
├── pipeline
├── archive
├── parser
├── sort
├── merge
└── parquet
```

Supports:

```text
Trades
Depth
Trades + Depth
Cross-exchange merged data
```

Offline merging has complete input available and can therefore perform deterministic global ordering without live reorder windows.

Dataset/catalog orchestration remains primarily in Python. Rust returns processing results; Python records `datasets` and `dataset_inputs`.

### `marketforge-live`

Feature:

```text
live
```

Implemented later:

```text
live/
├── transport
├── exchanges
├── connection
├── heartbeat
├── bootstrap
├── sequence
├── recovery
├── ordering
└── sink
```

Live-only dependencies include:

```text
Tokio
WebSockets
async HTTP
async channels
```

### Cargo Boundary

```text
always compiled
    canonical
    normalization
    book
    validation

feature = process
    archive parsing
    CSV / JSONL / XLSX
    Arrow / Parquet
    offline sorting / merging

feature = live
    Tokio
    WebSockets
    HTTP
    connection / recovery
```

Build independently:

```bash
cargo build --release \
    --bin marketforge-process \
    --features process
```

```bash
cargo build --release \
    --bin marketforge-live \
    --features live
```

Core rule:

> Share canonical semantics, normalization, book state, and validation. Keep historical file handling and live transport/protocol logic separate.