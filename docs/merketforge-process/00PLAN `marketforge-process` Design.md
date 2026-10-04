The core offline-processing architecture is now mostly defined.

Before implementation, several smaller contracts still need to be frozen.

## Quick Navigation

- [Combined Parquet Schema](#combined-parquet-schema)
- [Deterministic Ordering](#deterministic-ordering)
- [Error Policy](#error-policy)
- [BookStore](#bookstore)
- [Temporary Runs](#temporary-runs)
- [Resource Configuration](#resource-configuration)
- [Python Rust Protocol](#python-rust-protocol)
- [Dataset Lifecycle](#dataset-lifecycle)
- [Testing](#testing)
- [Next Step](#next-step)

## Combined Parquet Schema

Define how the canonical event types coexist in one chronological Parquet dataset:

```text
Trade
L2Snapshot
L2Update
```

Decisions include:

```text
event envelope
event_type representation
nullable/shared columns
snapshot bids/asks representation
source-event grouping
Arrow schema
```

This schema will also define the primary input contract for `mlfindgen`.

## Deterministic Ordering

Freeze one ordering policy for merged events:

```text
event_timestamp_ns
    ↓
comparable native sequence where available
    ↓
stable source/stream rank
    ↓
original source-event order
```

Source-event groups must remain atomic.

Thread scheduling and worker completion order must never affect canonical ordering.

## Error Policy

Define how processing reacts to:

```text
malformed records
corrupt archives
missing fields
invalid quantities
sequence gaps
timestamp regressions
missing snapshots
book inconsistencies
```

Failures should map explicitly to states such as:

```text
clean
degraded
failed
```

Integrity problems are surfaced through the monitor and final manifest.

## BookStore

Because:

> If depth is present, always maintain book state.

Define the shared Rust `BookStore` implementation:

```text
snapshot replacement
set/delete updates
bid/ask storage
BBO
mid price
validation
memory accounting
```

The same core book implementation can later be reused by `marketforge-live`.

## Temporary Runs

Freeze the internal temporary-run contract:

```text
Arrow IPC
canonical schema
source-event grouping
ordering metadata
timestamp range
record count
```

Temporary runs are implementation artifacts and never part of the final dataset interface.

## Resource Configuration

Keep user-facing tuning small:

```text
workers
memory_budget
scratch_budget
scratch_path
Arrow batch target
Parquet row-group target
Parquet file target
```

Everything practical should be derived automatically from these values.

## Python Rust Protocol

Define stable JSON contracts:

```text
Python
    ↓
ProcessingJob
    ↓
marketforge-process
    ↓
ProcessingResult
    ↓
Python
```

Python owns:

```text
catalog
job planning
dataset registration
lineage
CLI
```

Rust owns:

```text
processing
temporary runs
merging
book state
Parquet
manifest
integrity results
```

## Dataset Lifecycle

Define:

```text
pending
    ↓
processing
    ↓
complete
```

or:

```text
processing
    ↓
degraded / failed
```

Also define:

```text
temporary-file cleanup
partial Parquet handling
retry behavior
scratch retention after failure
atomic manifest publication
```

A final `manifest.json` marks physical dataset completion.

## Testing

The Rust processor should cover:

```text
raw fixture → expected canonical events

workers=1
vs
workers=N
→ identical logical output

merge ordering
source-event atomicity
BookStore reconstruction
sequence gaps
corrupt inputs
small memory budgets
temporary spilling
Parquet round trips
manifest correctness
```

Concurrency must change performance, never semantics.

## Next Step

The next major design decision is the **combined canonical Parquet schema**:

```text
CanonicalEvent
    ↓
Trade / L2Snapshot / L2Update
    ↓
Arrow representation
    ↓
Parquet
    ↓
mlfindgen
```

Once this is frozen, the temporary-run schema, Arrow builders, Parquet writer, merger output, and downstream reader can all use the same physical contract.