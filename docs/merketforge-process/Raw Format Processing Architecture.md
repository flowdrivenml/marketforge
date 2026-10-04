Historical processing uses a **hybrid design**:

> Reuse generic mechanisms while keeping exchange-specific semantics explicit.

## Quick Navigation

- [Architecture](#architecture)
- [Shared IO](#shared-io)
- [Shared Decoders](#shared-decoders)
- [Format Processors](#format-processors)
- [Shared Canonical Helpers](#shared-canonical-helpers)
- [Worker Composition](#worker-composition)
- [Core Rule](#core-rule)

## Architecture

Raw processing is divided into reusable infrastructure and explicit format implementations:

```text
Raw File
    ↓
Shared Source / Decompression
    ↓
Shared Record Decoder
    ↓
Format-Specific Processor
    ↓
Shared Normalization Helpers
    ↓
CanonicalEvent
```

This avoids both excessive duplication and overly generic exchange abstractions.

## Shared IO

Container and compression handling is exchange-independent:

```text
source/
├── plain
├── gzip
├── zip
└── tar_gz
```

Responsibilities include:

```text
file opening
stream decompression
archive traversal
member selection
buffered reading
```

These modules know nothing about exchanges, instruments, or canonical schemas.

## Shared Decoders

Physical record decoding is also reusable:

```text
decode/
├── csv
├── jsonl
└── xlsx
```

Examples:

```text
GZIP → CSV
ZIP  → CSV
ZIP  → JSONL
plain → JSONL
```

Decoders iterate records without assigning exchange-specific meaning.

Typed readers may be used where useful:

```text
CsvReader<BybitTradeRow>
CsvReader<BinanceTradeRow>

JsonlReader<OkxDepthRow>
JsonlReader<BitgetTradeRow>
```

The iteration and decoding machinery is shared while the raw row structures remain format-specific.

## Format Processors

Exchange and market semantics remain explicit:

```text
formats/
├── bybit/
│   ├── spot_trades
│   ├── linear_trades
│   ├── inverse_trades
│   ├── option_trades
│   └── depth
├── binance/
├── okx/
├── bitget/
└── gateio/
```

A format processor defines:

```text
field interpretation
timestamp selection
side interpretation
quantity interpretation
snapshot/update semantics
sequence mapping
format-specific edge cases
```

Different formats may reuse the same source and decoder infrastructure without sharing semantic logic.

## Shared Canonical Helpers

Universal transformations remain shared:

```text
timestamp conversion
decimal parsing
quantity conversion primitives
side/action primitives
canonical validation
sequence structures
batching
temporary-run writing
```

For example:

```text
ms → ns
us → ns

base × price → quote
quote / price → base
contracts → canonical quantities
```

Instrument-specific economics come from `instrument_specs` rather than being hardcoded into format processors.

## Worker Composition

A worker composes the required components for its assigned format.

Example:

```text
Bybit Spot Trades
        ↓
GzipReader              shared
        ↓
CsvReader               shared
        ↓
BybitSpotTradeRow       explicit raw format
        ↓
BybitSpotTradeProcessor explicit semantics
        ↓
Normalization Helpers   shared
        ↓
Trade                    shared canonical type
```

Another format may reuse the same infrastructure:

```text
Binance Trades
        ↓
GzipReader
        ↓
CsvReader
        ↓
BinanceTradeRow
        ↓
BinanceTradeProcessor
        ↓
Normalization Helpers
        ↓
Trade
```

Testing follows the same separation:

```text
source tests
decoder tests
normalization-helper tests
format fixture tests
canonical-output tests
```

Failures can therefore be isolated to:

```text
container / compression
record decoding
raw-format interpretation
canonical normalization
validation
```

## Core Rule

```text
Reuse:
    mechanisms

Keep explicit:
    market-data semantics
```

MarketForge does not attempt to encode arbitrary exchange algorithms into generic metadata.

Shared infrastructure handles repetitive mechanics, while small explicit Rust format processors retain full control over exchange-specific behavior.