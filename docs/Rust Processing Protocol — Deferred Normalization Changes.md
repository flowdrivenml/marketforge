
The current Rust `ProcessingJob` protocol is sufficient for planning and job construction, but the normalization contract must be extended before actual generic raw-data normalization is implemented.

### Current Limitation

`NormalizationConfig` currently contains only:

```rust
pub struct NormalizationConfig {
    pub timestamp_encoding: TimestampEncoding,
    pub quantity_encoding: QuantityEncoding,
}
```

This describes units/encoding but does **not** describe the raw record structure or how raw fields map into canonical MarketForge fields. :chatgpt-content-reference{index="0"}

Python already resolves this information from PostgreSQL:

```text
catalog.raw_formats.schema_json
catalog.normalization_rules.rules_json
```

but the current `WorkTask` has nowhere to carry the complete information to Rust. :chatgpt-content-reference{index="1"}

### Required Change

Extend `WorkTask` to contain the resolved raw schema:

```text
WorkTask
├── input_path
├── format_code
├── raw_schema          ← ADD
├── instrument
├── normalization
├── source_ordering
├── source_compression
└── archive_member
```

Conceptually:

```rust
pub struct WorkTask {
    // existing fields...

    pub raw_schema: RawSchema,

    pub instrument: InstrumentSpec,
    pub normalization: NormalizationConfig,

    // existing fields...
}
```

`raw_schema` describes **what the physical input looks like**, including information resolved from `raw_formats.schema_json`, such as:

```text
fields
field names
field types
header presence
record structure
```

### Extend `NormalizationConfig`

The normalization configuration must also carry the resolved canonical mapping:

```rust
pub struct NormalizationConfig {
    pub timestamp_encoding: TimestampEncoding,
    pub quantity_encoding: QuantityEncoding,

    pub target_schema: ...,
    pub rules: ...,
}
```

The rules originate from:

```text
catalog.normalization_rules.rules_json
```

and describe mappings such as:

```text
raw timestamp      → event_timestamp
raw price          → price
raw size/volume    → quantity
raw trade ID       → trade_id
raw side           → side
optional raw field → optional canonical field
```

They may also define transformations such as:

```text
casefold
bool
equals
map
```

### Intended Boundary

After this change:

```text
PostgreSQL
├── raw_formats.schema_json
├── normalization_rules.rules_json
├── instruments
├── instrument_specs
└── config.processing
        ↓
      Python
        ↓
fully resolved immutable ProcessingJob
        ↓
       Rust
```

Rust must not query PostgreSQL or contain exchange-specific knowledge merely to understand a raw format.

For example, Rust should **not** need logic such as:

```rust
if format_code == "BYBIT-T2" {
    // hardcoded Bybit column mapping
}
```

Instead:

```text
BYBIT-T2
    → provenance / format identity

raw_schema
    → describes the physical records

normalization
    → describes how those records become canonical data
```

### Goal

Adding a new exchange or raw format should normally require:

```text
new raw-format specification
+
new normalization rules
```

rather than:

```text
new raw-format specification
+
new normalization rules
+
new exchange-specific Rust normalization implementation
```

The Rust engine should remain a generic execution/data plane operating on completely resolved processing instructions supplied by Python.

> **Deferred:** Python planning may continue using the current protocol for now. Extend the Rust protocol and corresponding Python `ProcessingJob` models before implementing actual generic raw-record normalization.