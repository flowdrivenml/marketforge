MarketForge uses one Rust crate with separate offline and live binaries. Shared market semantics remain lightweight, while heavy dependencies are isolated behind Cargo features.

## Quick Navigation

- [Crate Structure](#crate-structure)
- [Shared Core](#shared-core)
- [Offline and Live Binaries](#offline-and-live-binaries)
- [Libraries](#libraries)
- [Processing Architecture](#processing-architecture)
- [Parameterized Formats](#parameterized-formats)
- [Coding Style](#coding-style)
- [Error Handling](#error-handling)
- [Performance](#performance)
- [Cargo Features](#cargo-features)
- [Core Rules](#core-rules)

## Crate Structure

Initial structure:

```text
engine/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── bin/
    │   ├── marketforge-process.rs
    │   └── marketforge-live.rs
    │
    ├── canonical/
    ├── job/
    ├── source/
    ├── decode/
    ├── formats/
    ├── normalize/
    ├── validate/
    ├── book/
    │
    ├── process/
    │   ├── worker/
    │   ├── runs/
    │   ├── merge/
    │   ├── parquet/
    │   ├── manifest/
    │   └── metrics/
    │
    ├── live/
    │   ├── feeds/
    │   ├── websocket/
    │   ├── recovery/
    │   └── transport/
    │
    └── error/
```

The binaries should remain thin.

```text
binary
    ↓
load configuration/job
    ↓
call library
    ↓
serialize result
```

Processing logic belongs in the library rather than `main.rs`.

## Shared Core

Shared modules contain market semantics required by both offline and live processing:

```text
canonical
book
normalize
validate
error
```

They should remain dependency-light.

For example:

```rust
enum CanonicalEvent {
    Trade(Trade),
    L2Snapshot(L2Snapshot),
    L2Update(L2Update),
}
```

Shared canonical types must not depend on:

```text
Arrow
Parquet
Rayon
Tokio
WebSocket libraries
```

Storage and transport representations remain outside the canonical model.

## Offline and Live Binaries

MarketForge builds two binaries:

```text
marketforge-process
    → historical/offline processing

marketforge-live
    → live market-data streaming
```

Their dependency trees are intentionally separated:

```text
                    Shared Core
                        │
               ┌────────┴────────┐
               ▼                 ▼
            process             live
               │                 │
        Arrow / Parquet      Tokio / WebSocket
        Rayon                live transport
        archives             recovery
```

Offline processing does not require an async runtime.

`Tokio` belongs to the live feature rather than `marketforge-process`.

## Libraries

Initial candidates:

```text
serde
serde_json
    → ProcessingJob / manifest / configuration

clap
    → thin Rust binary CLI

thiserror
    → typed engine errors

anyhow
    → top-level binary/application errors

csv
    → CSV decoding

flate2
zip
tar
    → compression/archive handling

rust_decimal
    → exact canonical decimal values

rayon
    → offline worker parallelism

arrow
    → canonical batches / Arrow IPC runs

parquet
    → final Parquet output

ratatui + crossterm
    → optional processing monitor

tokio
WebSocket/network libraries
    → live binary only
```

Exact dependencies and feature flags should be added only when their implementation is introduced.

## Processing Architecture

The initial offline execution strategy remains:

```text
WorkTask
    ↓
SequentialTaskRunner
    ↓
Open Source
    ↓
Decompress / Unpack
    ↓
Parse
    ↓
Normalize
    ↓
Validate
    ↓
Bounded Batch
    ↓
Sort if Required
    ↓
Temporary Arrow Run
    ↓
TaskResult
```

Parallelism exists between tasks:

```text
Task A → Worker 1 → sequential pipeline
Task B → Worker 2 → sequential pipeline
Task C → Worker 3 → sequential pipeline
Task D → Worker 4 → sequential pipeline
```

A fixed Rayon thread pool is sufficient for the initial implementation.

The external contract remains:

```text
WorkTask
    ↓
TaskRunner
    ↓
TaskResult
```

This allows a future `ParallelTaskRunner` without changing market-data semantics.

## Parameterized Formats

Generic mechanisms should be reused aggressively:

```text
SourceReader
Compression
CSV decoder
JSONL decoder
normalization helpers
canonical validators
```

Common formats use typed specifications:

```text
GenericCsvTradeProcessor
    +
CsvTradeSpec

GenericJsonlDepthProcessor
    +
JsonlDepthSpec
```

For CSV, field names are resolved once:

```text
"timestamp" → 0
"price"     → 4
"quantity"  → 3
```

The hot loop then uses resolved integer indexes.

Conceptually:

```rust
struct ResolvedCsvTradeSpec {
    timestamp_idx: usize,
    price_idx: usize,
    quantity_idx: usize,
}
```

Formats that cannot be expressed cleanly through parameterized processors receive custom implementations.

```text
common format
    → parameterized processor

exceptional format
    → custom processor
```

Do not create an increasingly complicated configuration language merely to avoid writing explicit Rust.

## Coding Style

Prefer explicit Rust:

```text
struct
enum
impl
fn
```

over unnecessary abstraction.

Use traits only around genuinely replaceable boundaries such as:

```text
FormatProcessor
TaskRunner
RunReader
DatasetWriter
```

Do not create traits for every helper.

### Use Typed Enums

Avoid internal string states:

```rust
enum TradeSide {
    Buy,
    Sell,
}

enum BookSide {
    Bid,
    Ask,
}

enum L2Action {
    Set,
    Delete,
}
```

Strings belong primarily at serialization/storage boundaries.

### Separate Raw and Canonical Types

Keep exchange-native representations separate:

```text
RawBybitTrade
    ↓
normalize()
    ↓
Trade
```

Do not gradually mutate raw records into canonical records.

This keeps the normalization boundary explicit and independently testable.

### Keep Workers Small

Worker code should primarily orchestrate:

```text
SourceReader
Decoder
FormatProcessor
Normalizer
Validator
BatchBuffer
RunWriter
```

If worker modules begin accumulating exchange semantics or thousands of lines of processing logic, responsibilities should be moved back into their appropriate components.

## Error Handling

Use typed errors inside the engine:

```rust
#[derive(Debug, thiserror::Error)]
enum ProcessError {
    // ...
}
```

Errors should distinguish conditions such as:

```text
invalid job
missing source
corrupt archive
decompression failure
normalization failure
scratch exhaustion
writer failure
```

The binary boundary may use `anyhow` for top-level propagation.

Do not reduce meaningful engine failures to arbitrary error strings because Python eventually consumes structured `ProcessingResult` errors.

## Performance

Optimize data ownership before introducing complicated concurrency.

Prefer:

```text
raw record
    ↓ move
canonical event
    ↓ move
batch
```

Avoid unnecessary cloning of:

```text
L2Snapshot
L2Update.changes[]
large Arrow structures
```

Do not use `Arc` everywhere unless shared ownership is actually required.

For CSV hot paths, prefer resolved indexes and benchmark `ByteRecord` against higher-level deserialization.

Avoid repeated:

```text
HashMap<String, Value>
string field lookup
dynamic JSON traversal
```

when typed or resolved representations are available.

Performance optimization follows profiling:

```text
correct implementation
    ↓
processing monitor / benchmarks
    ↓
identify bottleneck
    ↓
optimize isolated component
```

## Cargo Features

Heavy offline and live dependencies should be optional.

Conceptually:

```toml
[features]
default = []

process = [
    "dep:rayon",
    "dep:arrow",
    "dep:parquet",
    "dep:csv",
    "dep:flate2",
    "dep:zip",
    "dep:tar",
]

live = [
    "dep:tokio",
    "dep:futures-util",
    "dep:tokio-tungstenite",
]

monitor-tui = [
    "dep:ratatui",
    "dep:crossterm",
]
```

Binaries require their corresponding features:

```toml
[[bin]]
name = "marketforge-process"
path = "src/bin/marketforge-process.rs"
required-features = ["process"]

[[bin]]
name = "marketforge-live"
path = "src/bin/marketforge-live.rs"
required-features = ["live"]
```

Modules are gated similarly:

```rust
pub mod canonical;
pub mod book;
pub mod normalize;
pub mod validate;

#[cfg(feature = "process")]
pub mod process;

#[cfg(feature = "live")]
pub mod live;
```

Build offline only:

```bash
cargo build --release \
    --no-default-features \
    --features process \
    --bin marketforge-process
```

Build live only:

```bash
cargo build --release \
    --no-default-features \
    --features live \
    --bin marketforge-live
```

Independent CI checks should verify both configurations:

```bash
cargo check \
    --no-default-features \
    --features process \
    --bin marketforge-process

cargo check \
    --no-default-features \
    --features live \
    --bin marketforge-live
```

If compile times eventually justify it, the existing boundaries allow migration to:

```text
marketforge-core
marketforge-process
marketforge-live
```

as separate workspace crates.

This split is unnecessary initially.

## Core Rules

- one Rust crate initially
- separate `marketforge-process` and `marketforge-live` binaries
- shared market semantics remain dependency-light
- heavy dependency families are optional and feature-gated
- offline processing does not compile the live async/network stack
- live processing should not require offline Arrow/Parquet machinery unless explicitly needed
- use concrete types and explicit code by default
- use traits only at meaningful replacement boundaries
- use enums instead of internal string states
- keep raw exchange types separate from canonical types
- resolve parameterized format configuration before entering hot loops
- reuse common formats but allow explicit custom processors
- avoid unnecessary cloning and shared ownership
- keep binaries thin and processing logic inside the library
- profile before introducing specialized performance implementations
- maintain independent offline/live build checks

> Keep the shared core small and stable, isolate heavy dependency trees, and make optimization possible without obscuring the market-data semantics.