MarketForge should avoid implementing a completely separate processor for every exchange/market combination when the underlying physical format and semantics can be expressed cleanly through reusable components.

The processing layer uses generic mechanics, parameterized processors, shared validation, and custom implementations where required.

## Quick Navigation

- [Generic Mechanics](#generic-mechanics)
- [Parameterized Processors](#parameterized-processors)
- [Normalization](#normalization)
- [Validation](#validation)
- [Custom Processors](#custom-processors)
- [Worker Pipeline](#worker-pipeline)
- [Performance](#performance)
- [Core Rule](#core-rule)

## Generic Mechanics

Low-level mechanisms are completely reusable:

```text
file I/O
GZIP
ZIP
TAR.GZ
CSV iteration
JSONL iteration
XLSX reading
Arrow batching
temporary-run writing
```

These components know nothing about exchange semantics.

Example:

```text
Raw Archive
    ↓
GzipReader
    ↓
CsvReader
    ↓
records
```

The same infrastructure may be reused by Bybit, Binance, OKX, Gate.io, Bitget, or future exchanges.

## Parameterized Processors

Common market-data formats use generic processors configured by small typed specifications:

```text
GenericCsvTradeProcessor
GenericJsonlTradeProcessor
GenericCsvDepthProcessor
GenericJsonlDepthProcessor
```

For example:

```text
Bybit Spot Trades
        ↓
GZIP
        ↓
CSV
        ↓
CsvTradeSpec
        ↓
GenericCsvTradeProcessor
        ↓
Canonical Trade
```

A specification may define:

```text
timestamp column/path
timestamp encoding

price column/path
quantity column/path

side encoding
trade ID

sequence fields

bid/ask locations
snapshot/update representation
```

### Resolved Field Access

CSV column names are resolved once during initialization:

```text
"timestamp" → column 0
"price"     → column 4
"size"      → column 3
```

The hot processing loop then uses resolved indexes:

```text
row[0]
row[4]
row[3]
```

Repeated string-based field lookup is avoided.

JSONL processors follow the same principle where practical:

```text
JSONL
    ↓
deserialize record
    ↓
resolved/typed fields
    ↓
processor
```

## Normalization

Common normalization behavior should also be reusable and parameterized where practical.

Examples:

```text
timestamp conversion
side mapping
action mapping
quantity conversion
sequence mapping
optional-field mapping
```

Typed rules may describe common behavior:

```text
TimestampEncoding
├── Seconds
├── Milliseconds
├── Microseconds
└── Nanoseconds
```

Likewise:

```text
SideRule
QuantityRule
SequenceRule
ActionRule
```

Instrument economics come from canonical `instrument_specs` rather than being hardcoded into individual format processors.

If normalization semantics cannot be expressed cleanly through the common rules, the format uses custom normalization logic.

## Validation

Validation is divided into two layers:

```text
Canonical Validation
        +
Format Validation
```

### Canonical Validation

Canonical validation answers:

> Is this a valid MarketForge canonical event?

These rules are shared across all exchanges.

For `Trade`:

```text
valid event_timestamp_ns
side ∈ {buy, sell}
price > 0
required fields present
valid quantity representation
```

For `L2LevelUpdate`:

```text
side ∈ {bid, ask}
action ∈ {set, delete}
price > 0
valid quantity representation
order_count >= 0 when present
```

For `L2Snapshot`:

```text
valid levels
bids descending
asks ascending
required fields present
valid quantities
```

Generic sequence structure may also validate invariants such as:

```text
sequence_first <= sequence_last
```

Shared validators may therefore live under:

```text
validation/
├── canonical.rs
├── trade.rs
└── l2.rs
```

### Format Validation

Format validation answers:

> Is this canonical event consistent with the source exchange's semantics?

Examples include:

```text
sequence continuity
previous-sequence relationships
update-range continuity
snapshot/update transitions
source-specific timestamp behavior
archive-specific invariants
```

These rules cannot always be universal.

For example, one format may require:

```text
next.sequence_previous
    ==
current.sequence_last
```

while another may use update ranges:

```text
next.sequence_first
    <=
current.sequence_last + 1

next.sequence_last
    >=
current.sequence_last + 1
```

Sequence continuity therefore remains format-specific.

### Parameterized Format Validators

Common validation models should be reusable:

```text
SequenceValidator
├── PreviousSequence
├── ContiguousRange
├── Monotonic
└── None
```

A format specification may select the appropriate validator:

```text
sequence_validation = PreviousSequence
```

If the source requires unusual validation semantics, it receives a custom validator.

```text
common rule
    → parameterized validator

unusual rule
    → custom validator
```

Canonical validation and format validation remain separate:

```text
Canonical Event
      ↓
Canonical Validator
      ↓
Format Validator
      ↓
Validated Event
```

A record may therefore be structurally valid as a MarketForge event while still violating source-specific continuity rules.

## Custom Processors

Not every exchange format should be forced into the generic model.

Formats requiring behavior such as:

```text
multi-record assembly
complex nested structures
stateful raw interpretation
unusual sequence semantics
conditional field meaning
special event reconstruction
```

receive an explicit custom processor.

```text
Format
    ↓
can it be expressed cleanly by existing specs?
    │
    ├── yes → parameterized processor
    │
    └── no  → custom processor
```

Custom processors may still reuse:

```text
source readers
decompression
record decoders
normalization helpers
canonical validators
Arrow batching
temporary-run infrastructure
```

Both generic and custom paths produce the same canonical event model.

## Worker Pipeline

The worker remains responsible for the complete sequential task pipeline:

```text
WorkTask
    ↓
Open Source
    ↓
Stream Decompress / Unpack
    ↓
Parse Raw Records
    ↓
Normalize
    ↓
Canonical Validate
    ↓
Format Validate
    ↓
Bounded RAM Batch
    ↓
Sort if Required
    ↓
Temporary Arrow Run
    ↓
Repeat Until EOF
    ↓
TaskResult
```

The implementation underneath each stage may be:

```text
generic
parameterized
custom
```

without changing the worker contract.

Conceptually:

```text
Worker
    │
    ├── SourceReader
    ├── Decoder
    ├── FormatProcessor
    ├── Normalizer
    ├── CanonicalValidator
    ├── FormatValidator
    ├── BatchBuffer
    ├── Sorter
    └── RunWriter
```

## Performance

Parameterization should be resolved before entering the hot processing loop.

Avoid:

```text
HashMap<String, Value>
    ↓
lookup("timestamp")
lookup("price")
lookup("quantity")
```

for every record.

Prefer:

```text
format specification
    ↓
resolve once
    ↓
integer indexes / typed rules
    ↓
fast processing loop
```

Runtime rules should use small typed enums or resolved configuration.

The expected abstraction overhead is small relative to:

```text
decompression
CSV / JSON parsing
decimal parsing
Arrow construction
sorting
disk I/O
```

Further specialization should only be introduced when profiling demonstrates a meaningful bottleneck.

## Core Rule

The processing hierarchy is:

```text
LEVEL 1
Generic Mechanics
    ↓
I/O / compression / archives
CSV / JSONL / XLSX
Arrow / batching / runs


LEVEL 2
Parameterized Market Processing
    ↓
generic trade/depth processors
normalization rules
canonical validators
common format validators
    +
typed format specifications


LEVEL 3
Custom Format Logic
    ↓
exceptional parsing
normalization
assembly
sequence validation
source semantics
```

The design principle is:

> **Reuse mechanisms, parameterize common semantics and validation, and specialize exceptional behavior.**

If representing a format requires increasingly complicated configuration, callbacks, or special cases, stop extending the generic specification and implement a custom processor instead.