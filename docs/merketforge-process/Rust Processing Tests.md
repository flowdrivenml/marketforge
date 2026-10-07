
`marketforge-process` should be tested for both **correct market semantics** and **execution invariance**.

## Quick Navigation

- [Unit Tests](#unit-tests)
- [Raw Format Fixtures](#raw-format-fixtures)
- [BookStore Tests](#bookstore-tests)
- [Integrity Tests](#integrity-tests)
- [Merge Tests](#merge-tests)
- [Execution Equivalence](#execution-equivalence)
- [Failure Tests](#failure-tests)
- [Parquet Tests](#parquet-tests)
- [End-to-End Tests](#end-to-end-tests)

## Unit Tests

Test small reusable components independently:

```text
compression / archive readers
CSV / JSONL decoders
timestamp conversion
quantity normalization
side / action normalization
ordering keys
BookStore operations
run descriptors
Arrow conversion
```

Tests should be deterministic and fast.

## Raw Format Fixtures

Every supported raw format requires a small real fixture:

```text
fixtures/raw/
├── bybit/
├── binance/
├── okx/
├── bitget/
└── gateio/
```

Test the complete format path:

```text
Raw Fixture
    ↓
Source Reader
    ↓
Parser
    ↓
Normalizer
    ↓
Expected Canonical Events
```

This verifies:

```text
field interpretation
timestamp units
quantity semantics
side/action mapping
sequence mapping
snapshot assembly
atomic L2Update grouping
```

No raw format should be considered supported without fixture coverage.

## BookStore Tests

Replay known canonical sequences:

```text
L2Snapshot
    ↓
L2Update
    ↓
L2Update
    ↓
Expected BookState
```

Verify:

```text
bids / asks
set / delete
best bid
best ask
mid
snapshot replacement
atomic changes[]
book validity
```

## Integrity Tests

Format-specific validators should test:

```text
valid sequence continuity
sequence gaps
timestamp regressions
missing snapshots
invalid records
book inconsistencies
```

Example:

```text
Valid
    ↓
sequence gap
    ↓
Invalid
    ↓
authoritative snapshot
    ↓
Valid
```

Sequence semantics remain format-specific.

MarketForge performs no generic duplicate-detection tests.

## Merge Tests

Test deterministic k-way merging:

```text
A: 1 4 7
B: 2 5 8
C: 3 6 9

↓

1 2 3 4 5 6 7 8 9
```

Also test:

```text
equal timestamps
stream-rank tie breaking
empty streams
single stream
many streams
overlapping temporary runs
atomic L2Update events
```

## Execution Equivalence

The same logical job must produce the same canonical output under different execution configurations.

```text
workers = 1
workers = 8

↓

same canonical events
same ordering
same BookStore result
same integrity result
```

Likewise:

```text
large memory
    → few temporary runs

tiny memory
    → many temporary runs
```

must satisfy:

\[
Output_{\text{large}}
=
Output_{\text{small}}
\]

Concurrency and resource configuration may change performance, never semantics.

## Failure Tests

Explicitly test:

```text
missing archive
corrupt ZIP
truncated GZIP
invalid JSONL
invalid ProcessingJob
unsupported protocol version
scratch-budget exhaustion
fatal worker failure
```

Verify:

```text
non-zero exit
structured error result
no published dataset
correct scratch behavior
correct lifecycle state
```

## Parquet Tests

Canonical events must survive complete round trips:

```text
Canonical Events
    ↓
Arrow
    ↓
Parquet
    ↓
Arrow
    ↓
Canonical Events
```

Verify especially:

```text
Trade payload
L2Snapshot.bids[]
L2Snapshot.asks[]
L2Update.changes[]
nullable fields
event ordering
```

Expected invariant:

\[
Events_{\text{before}}
=
Events_{\text{after}}
\]

## End-to-End Tests

Maintain small complete fixture jobs:

```text
Raw Fixtures
    +
ProcessingJob JSON
    ↓
marketforge-process
    ↓
Workers
    ↓
Temporary Runs
    ↓
Merge
    ↓
BookStore
    ↓
Parquet
    ↓
Manifest
```

Verify:

```text
expected events
expected ordering
expected integrity status
expected Parquet files
valid manifest
correct publication
scratch cleanup
```

Suggested layout:

```text
engine/
├── tests/
│   ├── formats/
│   ├── worker.rs
│   ├── merge.rs
│   ├── book.rs
│   ├── parquet.rs
│   ├── resources.rs
│   ├── failures.rs
│   └── end_to_end.rs
│
└── fixtures/
    ├── raw/
    │   ├── bybit/
    │   ├── binance/
    │   ├── okx/
    │   ├── bitget/
    │   └── gateio/
    ├── jobs/
    │   ├── trades.json
    │   ├── depth.json
    │   ├── combined.json
    │   └── merge.json
    └── expected/
```

> Core rule: the same inputs must produce the same canonical market-data semantics regardless of worker count, memory budget, temporary-run count, or future execution optimizations.