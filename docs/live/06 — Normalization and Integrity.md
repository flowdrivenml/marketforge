## 1. Purpose

The Normalization and Integrity subsystem transforms exchange-native market-data messages into validated canonical events while preserving their original semantics, ordering, identity, and provenance.

It is responsible for ensuring that MarketForge Live produces consistent market-data representations across:

- Binance
- Bybit
- OKX
- Bitget
- Gate.io

Each exchange supports three initial market families:

- Spot
- Linear
- Inverse

The initial supported data types are:

- Individual and exchange-aggregated trades.
- L2 order-book snapshots.
- L2 order-book updates.

The subsystem must be **configuration-driven, exchange-agnostic, deterministic, and suitable for high-throughput Rust processing**.

Exchange-specific field mappings, quantity semantics, timestamp representations, and integrity rules are defined in the TOML specifications documented in **04 — Exchange Protocols and Connectivity**.

### 1.1 Primary Objectives

The subsystem must:

- Decode exchange-native market-data messages.
- Extract records using compiled format specifications.
- Resolve canonical instrument identities.
- Normalize prices and quantities.
- Normalize timestamps without avoidable precision loss.
- Normalize aggressor-side semantics.
- Preserve native trade and sequence identifiers.
- Validate individual records.
- Detect duplicate and inconsistent events.
- Validate order-book sequence continuity.
- Coordinate snapshot and delta synchronization.
- Detect data gaps and integrity violations.
- Preserve source provenance.
- Produce canonical events for downstream consumers.
- Report integrity failures to the Recovery Coordinator.

Normalization must not silently invent missing market data or conceal unresolved integrity problems.

### 1.2 Architectural Principle

Normalization and integrity validation are related but distinct responsibilities.

**Normalization** determines what an exchange-native message means in the canonical MarketForge domain.

**Integrity validation** determines whether that message can be trusted and whether it preserves the required continuity guarantees.

A syntactically valid message is not necessarily a valid market-data event.

Similarly, a correctly normalized event does not necessarily establish complete historical coverage.

These distinctions must remain explicit throughout the implementation.

---

## 2. Architectural Boundaries

The subsystem operates downstream of exchange transport and protocol routing.

### 2.1 Processing Pipeline

    Exchange WebSocket
            |
            v
    Connection Manager
            |
            v
    Exchange Protocol Adapter
            |
            v
    Stream Message Router
            |
            v
    Raw Message Decoder
            |
            v
    Record Extraction
            |
            v
    Field Validation
            |
            v
    Canonical Normalization
            |
            v
    Integrity Validation
            |
            v
    Stream State Processing
            |
            v
    Canonical Event Publication

This diagram represents the conceptual processing stages.

The Rust implementation may combine compatible stages to reduce allocations and processing overhead.

However, the semantic boundaries must remain independently testable.

### 2.2 Component Responsibilities

| Component | Responsibility |
|---|---|
| Connection Manager | Physical transport and frame acquisition |
| Exchange Protocol Adapter | Message classification and native routing |
| Configuration Compiler | Compile TOML specifications into executable processing plans |
| Raw Message Decoder | Decode JSON or other supported encodings |
| Record Extractor | Extract exchange-native records |
| Field Normalizer | Convert native values into canonical representations |
| Instrument Resolver | Supply validated instrument metadata |
| Trade Integrity Validator | Validate trade identity and record correctness |
| L2 Sequence Validator | Validate native order-book continuity |
| BookStore | Maintain synchronized order-book state |
| Recovery Coordinator | Coordinate recovery after integrity failures |
| Publisher | Distribute accepted canonical events |
| Recording Manager | Optionally persist canonical market data |
| Metrics Registry | Record processing and integrity measurements |

The Normalization and Integrity subsystem must not own physical connections, subscription admission, or persistent instrument metadata.

### 2.3 Design Constraints

- Rust-only hot-path implementation.
- No exchange-specific normalization pipelines.
- No PostgreSQL queries during event processing.
- No Python calls on the hot path.
- No direct TUI dependencies.
- No floating-point conversion for exact decimal quantities.
- No silent timestamp truncation.
- No silent quantity-unit assumptions.
- No implicit acceptance of unknown instrument metadata.
- No unbounded buffering.
- No silent event dropping.
- No publication of invalid synchronized-book state.
- No fabricated trade recovery.
- No assumption that all exchanges provide equivalent integrity guarantees.

The subsystem must remain deterministic for identical inputs, configuration revisions, and initial processing state.

---

## 3. Configuration-Driven Processing

MarketForge Live uses compiled format specifications rather than hardcoded exchange-specific decoders.

The configuration compiler transforms validated TOML definitions into immutable processing plans.

### 3.1 Configuration Sources

Trade formats:

    data/.config/live/formats/trades/

L2 formats:

    data/.config/live/formats/depth/

Exchange connectivity profiles:

    data/.config/live/exchanges/

Each source stream references one exchange profile and one corresponding format specification.

### 3.2 Compilation Pipeline

    Exchange Profile
           +
    Format Specification
           +
    Instrument Metadata
           |
           v
    Configuration Validation
           |
           v
    Semantic Compatibility Checks
           |
           v
    Field Extraction Compilation
           |
           v
    Normalization Strategy Resolution
           |
           v
    Integrity Strategy Resolution
           |
           v
    Compiled Processing Plan

Compilation occurs during stream initialization or controlled configuration replacement.

The resulting plan is reused for subsequent messages.

### 3.3 CompiledProcessingPlan

Conceptual structure:

    CompiledProcessingPlan {
        format_id
        configuration_revision
        instrument_context
        message_matcher
        record_extractor
        field_extractors
        timestamp_normalizer
        price_normalizer
        quantity_normalizer
        side_normalizer
        identity_strategy
        integrity_strategy
        output_schema
    }

L2 processing plans additionally contain:

    sequence_strategy
    snapshot_strategy
    level_extractor
    book_update_strategy

The exact Rust representation is an implementation detail.

The important requirement is that normalization-critical decisions are resolved before event processing.

### 3.4 Compilation Requirements

The compiler must validate:

- Supported schema version.
- Exchange and market compatibility.
- Supported channel.
- Required field selectors.
- Field type compatibility.
- Timestamp representation.
- Decimal representation.
- Quantity denomination.
- Contract-value requirements.
- Native identity semantics.
- Sequence-validation strategy.
- Snapshot synchronization requirements.
- Canonical output compatibility.

A syntactically valid TOML file may still fail semantic compilation.

For example:

    raw_unit = "requires_verification"

must prevent activation of a normalizer requiring that unresolved quantity unit.

### 3.5 Configuration Revisions

Each processing plan is associated with a configuration revision.

Events produced by that plan must retain sufficient provenance to identify the effective normalization specification.

A configuration change affecting field extraction, quantity semantics, or integrity rules requires controlled processor replacement.

An existing processor must not silently switch normalization rules in the middle of a message or atomic L2 update.

---

## 4. Canonical Data Contracts

The subsystem produces canonical events defined in **02 — Domain Models and Contracts**.

It must reuse those domain types rather than introduce competing canonical schemas.

### 4.1 Canonical Event Types

Initial outputs:

    Trade
    L2Snapshot
    L2Update

The precise field definitions remain authoritative in document 02.

This document specifies how exchange-native messages are transformed into those contracts.

### 4.2 Event Envelope

Every accepted event is associated with the MarketForge Live event envelope.

Relevant metadata includes:

| Field | Purpose |
|---|---|
| stream_id | Logical source stream identity |
| stream_cursor | Source-stream event ordering |
| connection_generation | Physical connection incarnation |
| subscription_generation | Native subscription incarnation |
| configuration_revision | Effective processing configuration |
| instrument_id | Canonical instrument identity |
| event_timestamp | Normalized exchange event time |
| received_timestamp | Local receipt time |
| source_provenance | Native message identity and origin |

L2 events additionally reference the applicable book generation.

The event envelope must not confuse transport ordering with exchange-native market-data ordering.

### 4.3 Native Provenance

Canonical normalization must preserve enough native information to support:

- Debugging.
- Recovery reconciliation.
- Duplicate detection.
- Sequence diagnostics.
- Exchange protocol verification.
- Historical/live consistency testing.

Examples include:

- Native trade IDs.
- Market-specific trade IDs.
- Native update sequence ranges.
- Exchange timestamps.
- Native quantity denomination.
- Exchange-specific execution classifications.

Not every native field needs to become a first-class canonical field.

Optional attributes may be retained through a defined provenance representation.

### 4.4 Canonical Semantic Consistency

Equivalent economic events from different exchanges must produce equivalent canonical semantics.

For example, a buyer-initiated trade must normalize to:

    side = Buy

regardless of whether the exchange represents it through:

    "side": "buy"

or:

    "isBuyerMaker": false

or:

    signed_size > 0

The exchange-native interpretation must be defined explicitly in the compiled format.

The normalizer must not infer aggressor direction from price movement or other unrelated observations.

---

## 5. Raw Message Decoding

The Raw Message Decoder transforms exchange-native transport payloads into structured representations suitable for record extraction.

It operates after transport-level routing and before canonical normalization.

The decoder must remain independent of exchange identity. Exchange-specific message structures are supplied through compiled format specifications.

### 5.1 Responsibilities

The Raw Message Decoder is responsible for:

- Decoding supported message encodings.
- Validating payload structure.
- Identifying expected message envelopes.
- Extracting structured message representations.
- Preserving native numeric representations.
- Reporting malformed payloads.
- Supporting multiple records within one message.
- Avoiding unnecessary allocations and copies.
- Maintaining source-message ordering.

The decoder must not:

- Resolve instrument metadata.
- Calculate economic quantities.
- Determine trade aggressor semantics.
- Validate order-book sequence continuity.
- Apply order-book updates.
- Execute recovery operations.
- Publish canonical events.

### 5.2 Supported Encodings

The initial implementation supports:

| Encoding | Description | Initial support |
|---|---|---|
| JSON | Exchange-native JSON messages | Required |
| Binary | Exchange-specific binary protocols | Deferred |
| SBE | Simple Binary Encoding | Deferred |
| MessagePack | Binary serialization | Deferred |
| Protobuf | Protocol Buffers | Deferred |

All 45 initial configuration specifications target JSON market-data feeds.

The architecture must permit additional decoding implementations without changing canonical normalization contracts.

### 5.3 JSON Decoding

JSON decoding must preserve the semantics of:

- Objects.
- Arrays.
- Strings.
- Booleans.
- Null values.
- Integer numbers.
- Decimal numbers.

The decoder must not convert arbitrary JSON numeric values through `f64` when doing so could lose precision.

This is particularly important for:

- Native trade identifiers.
- Order-book sequence identifiers.
- Timestamps.
- Prices.
- Quantities.
- Contract sizes.

The selected Rust JSON implementation must support exact extraction of the required native representations.

### 5.4 Numeric Representation Preservation

Exchange-native values may appear as:

    "65000.25"

    65000.25

    "1792350000123"

    1792350000123

These representations are not interchangeable at the raw decoding layer.

The decoder must preserve enough information to distinguish native JSON strings from JSON numbers.

The normalization stage determines whether a representation is permitted by the compiled field specification.

For exact decimal parsing, the original numeric token or an equivalent lossless representation must remain available.

### 5.5 DecodedMessage

Conceptual structure:

    DecodedMessage {
        connection_id
        connection_generation
        receive_sequence
        received_timestamp
        payload
        format_id
    }

The payload contains the structured native message representation.

`DecodedMessage` is an internal processing object, not a canonical market-data event.

The decoder must not assign a canonical event timestamp before the relevant native field has been extracted.

### 5.6 Malformed Payloads

Malformed JSON must produce a structured decoding error.

Possible causes include:

- Invalid JSON syntax.
- Invalid UTF-8 where required.
- Unexpected root type.
- Excessive nesting.
- Oversized payload.
- Invalid numeric representation.
- Unsupported encoding.

A malformed message must not produce partially accepted canonical records unless the configured protocol explicitly supports independent record recovery.

The failure is reported to the stream-integrity system.

Repeated malformed messages may indicate a protocol change, exchange malfunction, or incorrect format configuration.

---

## 6. Message Identification

Before extracting market-data records, the processor must verify that the decoded message matches the expected format.

### 6.1 MessageMatcher

The compiled `MessageMatcher` evaluates exchange-native message identity.

Possible selectors include:

| Selector | Purpose |
|---|---|
| Channel | Identify native market-data channel |
| Topic | Identify exchange subscription topic |
| Instrument | Identify native instrument |
| Market category | Identify native product family |
| Event type | Distinguish updates from acknowledgements |
| Action | Distinguish snapshots from deltas |
| Message type | Identify protocol-specific payload variants |

The matcher is compiled from the format specification.

### 6.2 Exchange Examples

**Binance**

A message may be identified through its stream wrapper or native event type.

**Bybit**

Messages commonly expose:

    topic
    type

The `type` field may distinguish:

    snapshot
    delta

**OKX**

Messages commonly expose:

    arg.channel
    arg.instId

Order-book messages additionally expose an action indicating snapshot or update behavior.

**Bitget**

UTA V3 messages use:

    arg.instType
    arg.topic
    arg.symbol

**Gate.io**

Messages use:

    channel
    event

Market-data notifications generally use:

    event = update

These differences belong to compiled message selectors rather than separate exchange-specific normalization pipelines.

### 6.3 Message Matching Outcomes

The matcher returns one of:

    Matched
    NotMatched
    Malformed
    Unsupported

| Outcome | Meaning |
|---|---|
| Matched | Message belongs to the configured format |
| NotMatched | Message belongs to another known route or channel |
| Malformed | Required identification fields are invalid |
| Unsupported | Message uses an unsupported protocol variant |

A `NotMatched` result does not necessarily represent an integrity failure.

For example, a shared WebSocket connection may carry several subscriptions.

The router should direct each message to the appropriate processor before format-specific extraction.

### 6.4 Instrument Identity Validation

The message's native instrument identity must match the subscription assignment.

For example:

    Expected: BTCUSDT
    Received: ETHUSDT

A mismatched instrument must not be normalized using the expected instrument's metadata.

The processor must reject or reroute the message according to the routing contract.

Instrument identity must never be inferred solely from the physical WebSocket endpoint when multiple instruments share that connection.

---

## 7. Record Extraction

A single exchange-native message may contain one or multiple market-data records.

The Record Extractor converts the decoded message into an ordered sequence of native records.

### 7.1 Supported Record Representations

The initial implementation supports:

| Representation | Description |
|---|---|
| Object | One record represented as a JSON object |
| Array | Multiple records represented as an array |
| Nested array | Records located within a nested message structure |

The format specification defines the extraction path.

Examples:

    result

    data

    data.trades

The extractor must not assume that every exchange uses the same envelope structure.

### 7.2 Single-Record Messages

Some channels deliver one trade or order-book update per message.

Conceptually:

    Native Message
          |
          v
    Extract result
          |
          v
    Native Record
          |
          v
    Normalize

Gate Spot's configured trade format is an example of an object-based record representation.

### 7.3 Multi-Record Messages

Other channels deliver arrays of records.

Conceptually:

    Native Message
          |
          v
    Extract result[]
          |
          +-- Record 0
          |
          +-- Record 1
          |
          +-- Record 2

Each record is normalized independently when the protocol defines independent events.

Gate Futures trades and Bitget public-trade messages are examples of array-based extraction.

### 7.4 Record Ordering

Records must preserve their native array order unless the exchange protocol explicitly requires another ordering strategy.

For example:

    result[0]
    result[1]
    result[2]

must not be arbitrarily reordered during extraction.

However, native array order does not automatically establish event-time ordering.

The processor must distinguish:

- Transport receive order.
- Message record order.
- Exchange-native sequence order.
- Event timestamp order.

Cross-stream event-time merging belongs to the separate merge and reorder subsystem.

### 7.5 Record Identity

Each extracted record receives an internal source position.

Conceptually:

    SourceRecordPosition {
        connection_generation
        receive_sequence
        record_index
    }

This identifies a record's position within the received transport stream.

It is not a replacement for exchange-native trade IDs or L2 sequence identifiers.

The source position is useful for:

- Debugging.
- Deterministic replay.
- Error reporting.
- Ordering verification.
- Provenance reconstruction.

### 7.6 Empty Record Collections

An empty record array is not automatically malformed.

Its validity depends on the channel specification.

Examples include:

- Empty L2 updates.
- Exchange keepalive messages.
- Valid protocol messages without market-data records.

The compiled format determines whether an empty collection is:

    ValidNoOp
    InvalidMessage
    UnsupportedVariant

The processor must not fabricate canonical events for empty messages.

---

## 8. Field Extraction

Field extraction maps native record fields into typed intermediate values.

### 8.1 Compiled Field Extractors

Each field extractor contains:

    FieldExtractor {
        field_id
        selector
        expected_type
        required
        conversion_strategy
    }

Selectors are compiled during stream initialization.

The hot path must not repeatedly interpret TOML configuration strings.

### 8.2 Field Sources

Fields may originate from:

- Message envelope.
- Individual record.
- Subscription context.
- Instrument context.
- Transport metadata.

For example:

| Canonical requirement | Possible source |
|---|---|
| Instrument identity | Envelope or record |
| Trade price | Record |
| Trade quantity | Record |
| Aggressor side | Record or quantity sign |
| Event timestamp | Record |
| Native sequence | Record |
| Connection generation | Transport context |
| Instrument ID | Compiled instrument context |

The field extractor must preserve these source distinctions.

### 8.3 Required and Optional Fields

Every extracted field has an explicit presence policy.

Supported policies:

    Required
    Optional
    Defaulted
    Derived

**Required**

The field must exist and satisfy its type contract.

**Optional**

The field may be absent without invalidating the record.

**Defaulted**

A missing field receives a configured value only when the exchange protocol explicitly supports that interpretation.

**Derived**

The field is calculated from other validated values.

Missing required fields must not silently receive arbitrary defaults.

### 8.4 Null Handling

Missing and null fields are distinct conditions.

For example:

    Field absent

is not necessarily equivalent to:

    "field": null

The compiled field specification determines whether either condition is permitted.

Optional fields may preserve three states:

    Present(value)
    Null
    Missing

This is useful for exchange-specific metadata where absence carries semantic meaning.

### 8.5 Field Type Validation

Supported initial field types include:

| Type | Meaning |
|---|---|
| string | JSON string |
| boolean | JSON boolean |
| integer | Exact JSON integer |
| integer_string | String containing an integer |
| decimal_string | String containing an exact decimal |
| signed_decimal_string | Signed decimal string |
| optional_string | Optional JSON string |
| optional_boolean | Optional JSON boolean |
| numeric_string | String containing a numeric identifier |
| integer_or_integer_string | Exact integer in either representation |

Additional aliases may be defined by the Configuration System.

Type validation must be strict by default.

Permissive conversions require explicit configuration.

---

## 9. Intermediate Native Records

Normalization should operate on typed intermediate records rather than repeatedly traversing arbitrary JSON structures.

### 9.1 NativeTradeRecord

Conceptual representation:

    NativeTradeRecord {
        instrument
        trade_id
        price
        quantity
        side
        timestamp
        attributes
        provenance
    }

The record retains exchange-native semantics.

For example, `quantity` may represent:

    BaseAsset
    QuoteAsset
    Contracts

Its denomination is not assumed until the compiled quantity normalizer is applied.

### 9.2 NativeL2Record

Conceptual representation:

    NativeL2Record {
        instrument
        message_kind
        bids
        asks
        timestamp
        sequence
        attributes
        provenance
    }

Native sequence information may include:

    FirstUpdateId
    LastUpdateId
    PreviousSequenceId
    SequenceId

The exact fields depend on the exchange protocol.

### 9.3 NativeBookLevel

Conceptual representation:

    NativeBookLevel {
        price
        quantity
        order_count
    }

`order_count` is optional.

The level retains native quantity semantics until normalization.

A zero quantity may represent deletion rather than an invalid economic quantity.

This distinction is resolved by the compiled L2 update strategy.

### 9.4 Intermediate Record Lifetime

Intermediate records should have short lifetimes.

The implementation should minimize:

- Heap allocations.
- String cloning.
- Repeated numeric parsing.
- Repeated instrument lookup.
- Unnecessary conversion between intermediate types.

Borrowed representations may be used where safe.

However, zero-copy optimization must not compromise ownership safety or deterministic processing.

---

## 10. Field Validation

Field validation occurs before canonical normalization.

Its purpose is to reject structurally invalid records before performing economic transformations.

### 10.1 Validation Categories

| Category | Examples |
|---|---|
| Presence | Missing required field |
| Type | String where integer required |
| Numeric | Invalid decimal or integer |
| Range | Negative price, invalid quantity |
| Identity | Missing native trade identifier |
| Instrument | Unknown or mismatched symbol |
| Timestamp | Invalid or unrepresentable timestamp |
| Structure | Malformed price-level array |
| Protocol | Unsupported message variant |

Validation must be configuration-driven.

### 10.2 Price Validation

Trade prices and order-book level prices must generally satisfy:

    price > 0

The parser must reject:

- Invalid decimal syntax.
- Negative prices.
- Zero prices.
- Unrepresentable values.
- Precision loss under the configured decimal representation.

Exchange-specific exceptions require explicit format support.

### 10.3 Quantity Validation

Quantity validation depends on record type.

**Trades**

Normalized economic quantity must be positive.

    quantity > 0

For signed native trade quantities:

    native_size != 0

The sign may encode aggressor direction.

**L2 updates**

Zero quantity is valid when it represents price-level deletion.

    quantity >= 0

Negative L2 quantities are rejected unless a future explicitly supported protocol defines different semantics.

### 10.4 Timestamp Validation

Timestamp parsing must verify:

- Valid numeric representation.
- Supported source unit.
- Representability in canonical time.
- Absence of unintended precision loss.
- Consistency with secondary timestamps where required.

A timestamp that cannot be represented exactly under the configured policy must not be silently truncated.

### 10.5 Record Validation Outcomes

Validation returns:

    Valid
    Invalid
    Unsupported

An invalid record produces a structured error.

An unsupported record indicates a protocol variant that the current format cannot interpret.

Neither outcome may produce a normal canonical event.

### 10.6 Validation Failure Scope

The response to an invalid record depends on whether the exchange protocol provides independent record boundaries and whether rejecting it would compromise stream integrity.

For example:

- An invalid standalone trade may be quarantined while the stream continues under an explicit degraded-coverage policy.
- An invalid record inside a batch may require rejecting the entire batch if atomicity or completeness cannot be established.
- An invalid L2 delta must not be partially applied to the BookStore.
- An unknown L2 sequence transition may require immediate book invalidation.

The Integrity subsystem determines the appropriate response.

The Raw Message Decoder must not independently decide whether a market-data stream remains valid.

---

## 11. Exact Decimal Arithmetic

MarketForge Live must preserve the numerical precision of exchange-native prices, quantities, and contract specifications throughout normalization.

Financial quantities must not be parsed or normalized through binary floating-point arithmetic when exact decimal semantics are required.

The normalization engine must use the same numerical conventions as the existing MarketForge historical processing system wherever their canonical contracts are compatible.

### 11.1 Numerical Requirements

The decimal subsystem must support:

- Exact parsing of decimal strings.
- Signed and unsigned decimal values.
- Variable decimal scales.
- Exact multiplication.
- Division with explicit precision and rounding policies.
- Scale normalization.
- Decimal comparison.
- Zero detection.
- Overflow detection.
- Conversion to canonical numerical representations.

The implementation must not silently round or truncate values.

### 11.2 Decimal Representation

The preferred conceptual representation is a decimal coefficient and scale:

    Decimal {
        coefficient: i128
        scale: u32
    }

A decimal value is interpreted as:

    value = coefficient × 10^(-scale)

Examples:

| Input | Coefficient | Scale |
|---|---:|---:|
| `65000.25` | 6500025 | 2 |
| `0.000001` | 1 | 6 |
| `-12.5` | -125 | 1 |
| `100` | 100 | 0 |

This representation is illustrative.

The implementation may reuse an existing decimal library or the historical engine's decimal primitives.

The selected representation must satisfy the supported exchange precision requirements.

An `i128` coefficient is not automatically sufficient for every possible intermediate multiplication.

Overflow must be detected explicitly.

### 11.3 Decimal Parsing

Decimal parsing must accept only representations permitted by the compiled format.

Examples:

    "65000.25"
    "0.000001"
    "-12.5"
    "100"

Scientific notation may be supported when explicitly required by an exchange format.

The parser must reject:

- Invalid characters.
- Empty numeric strings.
- Unsupported scientific notation.
- Values exceeding configured precision.
- Values exceeding numerical capacity.
- Non-finite values.
- Unsupported decimal representations.

Parsing must not use `f32` or `f64` as an intermediate representation.

### 11.4 Decimal Scale

Prices and quantities may have different scales.

For example:

    Price:     65000.125
    Quantity:  0.00001234

The normalizer must preserve the original numerical value while converting it into the canonical representation.

Scale reduction is permitted only under an explicit rounding or quantization policy.

The source instrument's tick size and quantity step are validation constraints, not automatic instructions to round received market data.

### 11.5 Arithmetic Operations

Required operations include:

    Add
    Subtract
    Multiply
    Divide
    Compare
    AbsoluteValue
    Rescale

Multiplication should preserve the exact mathematical result when representable.

Division requires an explicit precision policy because the result may have a nonterminating decimal expansion.

For example:

    100 / 60000 = 0.001666666...

No finite decimal representation can preserve this value exactly.

The normalizer must therefore distinguish:

    Exact
    Rounded
    Unrepresentable

The chosen rounding policy must be deterministic and documented.

### 11.6 Canonical Precision Policy

MarketForge should distinguish native precision from derived precision.

**Native values**

Exchange-provided prices and quantities must retain their original numerical values whenever representable.

**Derived values**

Economic quantities calculated through multiplication or division may require controlled rounding.

For example:

    Native quote quantity: 100 USD
    Native price:          60000 USD/BTC

    Derived base quantity: 100 / 60000 BTC

The native values remain exact.

The derived base quantity may require rounding to the configured canonical scale.

This rounding must not modify the preserved native quantity.

### 11.7 Precision Provenance

When a derived value requires rounding, the system should retain sufficient information to identify:

- Source quantity.
- Source price.
- Contract value.
- Contract-value denomination.
- Calculation strategy.
- Applied precision.
- Rounding policy.

This information may be represented through compiled normalization metadata rather than repeated in every event.

The configuration revision identifies the applicable calculation policy.

---

## 12. Timestamp Normalization

MarketForge Live receives timestamps in multiple native representations.

The timestamp normalizer converts these values into the canonical time unit defined in **02 — Domain Models and Contracts**.

The initial normalization target is:

    Unix nanoseconds

### 12.1 Supported Timestamp Units

| Native unit | Conversion to nanoseconds |
|---|---|
| Seconds | Multiply by 1,000,000,000 |
| Milliseconds | Multiply by 1,000,000 |
| Microseconds | Multiply by 1,000 |
| Nanoseconds | Preserve |
| Decimal milliseconds | Exact decimal conversion |

The source unit is defined in the compiled format.

The normalizer must not guess timestamp units from numerical magnitude.

### 12.2 Integer Timestamp Conversion

Integer timestamps use checked arithmetic.

For example:

    Native timestamp:
    1792350000123 milliseconds

    Canonical timestamp:
    1792350000123000000 nanoseconds

The conversion must reject values that exceed the canonical timestamp representation.

### 12.3 Decimal Millisecond Timestamps

Some exchanges expose fractional milliseconds.

Gate.io Spot is an example.

Native value:

    "1606292218213.4578"

Source unit:

    Decimal milliseconds

Canonical conversion:

    1606292218213.4578 × 1,000,000

Result:

    1606292218213457800 nanoseconds

The conversion must not pass through binary floating-point arithmetic.

### 12.4 Sub-Nanosecond Precision

A source timestamp may theoretically contain precision finer than one nanosecond.

For example:

    1000.1234567 microseconds

Converting to nanoseconds produces:

    1000123.4567 nanoseconds

This cannot be represented exactly as an integer nanosecond timestamp.

The normalizer must follow an explicit policy:

    Reject
    Round
    Truncate

The initial recommended policy is:

    Reject

unless the exchange format explicitly defines an acceptable rounding rule.

### 12.5 Timestamp Sources

A market-data message may contain multiple timestamps.

Examples include:

- Trade execution time.
- Exchange event-generation time.
- Exchange message-publication time.
- Local transport receipt time.
- Local processing time.

These timestamps have different semantics.

They must not be substituted for one another without an explicit contract.

### 12.6 Event Timestamp Selection

The compiled format specifies the authoritative event timestamp.

For trades:

    event_timestamp = execution_timestamp

For L2:

    event_timestamp = exchange_book_update_timestamp

The precise native field depends on the exchange.

The normalizer must not automatically select the latest or earliest available timestamp.

### 12.7 Receipt Timestamps

Local receipt timestamps are assigned by the transport layer.

They are not exchange-native event timestamps.

MarketForge should retain:

    event_timestamp_ns
    received_timestamp_ns

Where supported, additional timestamps may be preserved through provenance.

### 12.8 Timestamp Validation

Timestamp validation includes:

- Numeric representation.
- Source unit.
- Canonical representability.
- Precision preservation.
- Optional consistency checks.
- Configured timestamp range constraints.

A timestamp outside an expected operational range may be suspicious without necessarily being structurally invalid.

The Integrity subsystem determines whether such conditions require rejection, warning, or degraded classification.

### 12.9 Timestamp Ordering

Exchange event timestamps may be equal or arrive out of order.

The normalizer must not assume:

    current.event_timestamp >= previous.event_timestamp

unless the specific exchange protocol guarantees it.

Out-of-order timestamps must not automatically be treated as transport loss.

Cross-stream event-time ordering belongs to the separate merge and reorder subsystem.

---

## 13. Instrument Context Provisioning and Quantity Normalization

MarketForge Live separates exchange protocol definitions from instrument-specific economic metadata.

The exchange format determines how native market-data fields must be interpreted.

The instrument catalog determines the economic specifications of the instrument being processed.

Both are required to construct a valid normalization strategy.

The authoritative instrument catalog is maintained by Python orchestration and stored in PostgreSQL.

Rust Live receives validated instrument metadata during stream initialization, maintains immutable runtime contexts, and uses those contexts for high-throughput normalization.

The Rust processing hot path must never depend on Python or PostgreSQL availability.

### 13.1 Metadata Ownership and Responsibilities

Instrument metadata has one authoritative owner: the Python orchestration layer.

Responsibilities are divided as follows:

| Component | Responsibility |
|---|---|
| Python orchestration | Instrument discovery, metadata acquisition, refresh, and distribution |
| PostgreSQL | Authoritative instrument catalog and metadata history |
| Configuration System | Exchange and normalization protocol specifications |
| Rust Instrument Registry | Immutable runtime instrument contexts |
| Configuration Compiler | Combine exchange formats with instrument metadata |
| Stream Registry | Associate logical streams with compiled contexts |
| Stream Processor | Apply compiled normalization strategies |
| Recovery Coordinator | Select appropriate metadata revisions for recovered events |

Rust must not independently maintain a competing authoritative instrument database.

The Rust Instrument Registry is a runtime cache of validated metadata, not a separate source of truth.

### 13.2 Separation of Protocol and Instrument Metadata

MarketForge distinguishes two configuration categories.

**Exchange protocol configuration**

Stored in:

    data/.config/live/exchanges/
    data/.config/live/formats/trades/
    data/.config/live/formats/depth/

These specifications define:

- Native message structure.
- Field extraction.
- Timestamp representations.
- Aggressor-side semantics.
- Raw quantity denominations.
- Trade identity.
- L2 sequence rules.
- Snapshot synchronization.
- Integrity policies.

**Instrument metadata**

Maintained through Python orchestration and PostgreSQL.

This metadata defines:

- Canonical instrument identity.
- Native instrument symbol.
- Base and quote assets.
- Settlement asset, where applicable.
- Instrument quantity classification.
- Contract value.
- Contract-value denomination.
- Price and quantity constraints.

The same protocol format may be reused across thousands of instruments.

Therefore, MarketForge must not create an independent normalization TOML file for every instrument.

Example:

    Binance Spot Format
             |
             +-- BTCUSDT
             |
             +-- ETHUSDT
             |
             +-- SOLUSDT

All three streams use the same exchange format but different instrument contexts.

The final normalization strategy is compiled from both sources.

### 13.3 Instrument Metadata Contract

Python provides a structured instrument metadata contract to Rust.

Conceptually:

    InstrumentMetadata {
        instrument_id
        exchange
        market
        native_symbol
        base_asset
        quote_asset
        settlement_asset
        quantity_type
        contract_value
        contract_value_asset
        tick_size
        qty_step
        min_qty
        max_qty
        min_notional
        metadata_revision
        fetched_at
    }

This representation is illustrative.

The authoritative schema must remain compatible with the existing MarketForge instrument catalog and document 02.

#### Normalization-Critical Fields

| Field | Purpose |
|---|---|
| instrument_id | Canonical instrument identity |
| exchange | Exchange identity |
| market | Spot, Linear, Inverse |
| native_symbol | Exchange-native subscription symbol |
| base_asset | Base-asset identity |
| quote_asset | Quote-asset identity |
| settlement_asset | Settlement denomination, where applicable |
| quantity_type | Instrument quantity classification |
| contract_value | Native contract multiplier or face value |
| contract_value_asset | Denomination of the contract value |

These fields determine whether native quantities can be converted into canonical economic quantities.

#### Trading Constraint Fields

| Field | Purpose |
|---|---|
| tick_size | Permitted price increment |
| qty_step | Permitted order quantity increment |
| min_qty | Minimum accepted order quantity |
| max_qty | Maximum accepted order quantity |
| min_notional | Minimum order notional |

These fields may support validation and operational diagnostics.

However, trading constraints must not automatically be enforced against historical or live execution records.

#### Metadata Provenance

The context must also identify the metadata revision used for normalization.

The existing `fetched_at` timestamp records when metadata was retrieved.

It does not necessarily establish when the exchange's contract specifications became effective.

A separate effective-time model is required when historical specification changes must be represented accurately.

### 13.4 Python-to-Rust Stream Initialization

Instrument metadata is transferred through the existing Python-to-Rust orchestration interface during stream initialization.

The user does not manually supply contract specifications.

For example, the TUI may expose:

    Exchange: Binance
    Market: Spot
    Instrument: BTCUSDT
    Channel: Trades

Internally, Python resolves the instrument and provides the metadata required by Rust.

The conceptual initialization flow is:

    Python Orchestration
             |
             v
    PostgreSQL Instrument Catalog
             |
             v
    Resolve Instrument Metadata
             |
             v
    Construct Stream Request
             |
             v
    Python-to-Rust Control Interface
             |
             v
    Rust Stream Registry
             |
             v
    Instrument Context Validation
             |
             v
    Configuration Compilation
             |
             v
    Stream Activation

The request must contain sufficient information for Rust to activate the stream without additional Python or PostgreSQL lookups.

A conceptual request:

    {
        "operation": "register_stream",
        "stream": {
            "exchange": "binance",
            "market": "spot",
            "symbol": "BTCUSDT",
            "channel": "trades",
            "instrument_id": 15
        },
        "instrument": {
            "instrument_id": 15,
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "quantity_type": "base",
            "contract_value": null,
            "contract_value_asset": null,
            "tick_size": "0.01",
            "qty_step": "0.0001",
            "min_qty": "0.0001",
            "max_qty": "3700",
            "min_notional": "5"
        }
    }

The example is illustrative and does not finalize the IPC wire protocol.

Decimal values must be serialized losslessly.

For JSON-based control messages, decimal strings are preferred over floating-point numbers.

Python may supply metadata as part of the stream registration request or through a preceding instrument-context registration operation.

Both approaches must result in the same validated Rust runtime representation.

### 13.5 Rust Instrument Context Registry

Rust maintains an in-memory Instrument Context Registry.

Its purpose is to reuse validated instrument metadata across source streams.

Conceptually:

    Rust Instrument Registry
              |
              +-- Instrument 15
              |       |
              |       +-- Trades Stream
              |       |
              |       +-- L2 Stream
              |
              +-- Instrument 16
              |
              +-- Instrument 17

Multiple streams referencing the same instrument and metadata revision should share one immutable context.

The registry may use reference-counted ownership such as:

    Arc<InstrumentContext>

The precise Rust implementation is not prescribed here.

#### Registry Identity

An instrument context is identified by:

    InstrumentId
    MetadataRevision

A single instrument may have multiple retained revisions when active processing or recovery requires them.

#### Context Immutability

A published instrument context must not be modified in place.

Metadata changes produce a new immutable revision.

Existing processors continue using their assigned context until an explicitly controlled replacement occurs.

#### Registry Lifetime

The Instrument Context Registry maintains contexts while they are referenced by:

- Active source streams.
- Stream initialization operations.
- Recovery operations.
- Other explicitly registered consumers.

Unused contexts may be removed according to a bounded cache policy.

Context eviction must not invalidate an active processor.

### 13.6 Independent Startup and Local Metadata Snapshots

MarketForge Live must be capable of starting independently of a running Python process.

Therefore, Python orchestration maintains a versioned local snapshot of the instrument metadata required by Rust.

Conceptual location:

    data/.config/live/instruments.snapshot

The exact file format and final path are defined by the Configuration System.

The snapshot is generated programmatically.

It is not manually maintained by users.

#### Snapshot Responsibilities

A metadata snapshot must contain:

- A supported snapshot schema version.
- Instrument identities.
- Native exchange symbols.
- Required economic specifications.
- Metadata revisions.
- Metadata provenance.
- Integrity information sufficient to detect incomplete or corrupted snapshots.

#### Independent Startup

When starting without Python:

    Rust Live Startup
            |
            v
    Load Local Metadata Snapshot
            |
            v
    Validate Snapshot
            |
            v
    Initialize Instrument Registry
            |
            v
    Load Stream Declarations
            |
            v
    Compile Stream Contexts
            |
            v
    Activate Streams

The snapshot is a cache of authoritative metadata previously supplied by Python.

It is not a second authoritative catalog.

#### Snapshot Safety

Snapshots must be written atomically.

A partially written snapshot must never replace the last valid snapshot.

The loader must reject:

- Unsupported schema versions.
- Missing required instrument fields.
- Invalid decimal representations.
- Inconsistent instrument identities.
- Invalid metadata revisions.
- Corrupted or incomplete snapshots.

An invalid snapshot must not result in partially initialized stream contexts.

#### Metadata Freshness

Snapshot age must be visible to operators.

A metadata snapshot may be structurally valid but outdated.

The Configuration System must define freshness policies for instruments whose contract specifications can change.

A stale context must not be silently treated as current when its freshness requirements are violated.

### 13.7 Compiled Normalization Context

The Configuration Compiler combines an exchange format specification with an immutable instrument context to produce an executable normalization plan.

The compiled plan contains all normalization-critical information required during event processing.

The purpose is to eliminate repeated configuration interpretation, instrument resolution, and database access from the processing hot path.

#### Compilation Inputs

The compiler receives:

- Validated exchange protocol configuration.
- Validated trade or L2 format configuration.
- Immutable instrument metadata.
- Instrument metadata revision.
- Effective configuration revision.
- Channel-specific normalization requirements.

Conceptually:

    Exchange Format
           |
           +-----------------------+
                                   |
    Instrument Context             |
           |                       |
           +-----------+-----------+
                       |
                       v
             Configuration Compiler
                       |
                       v
          Compiled Normalization Context
                       |
                       v
                Stream Processor

#### CompiledNormalizationContext

Conceptual representation:

    CompiledNormalizationContext {
        instrument_id
        metadata_revision
        configuration_revision

        base_asset
        quote_asset
        settlement_asset

        native_quantity_unit
        quantity_conversion_strategy
        contract_value
        contract_value_asset

        price_normalizer
        quantity_normalizer
        timestamp_normalizer
        side_normalizer

        validation_policy
    }

This is an illustrative structure.

The actual Rust representation must reuse the canonical domain types established in document 02.

#### Compile-Time Resolution

The compiler resolves:

- Native quantity denomination.
- Contract-value interpretation.
- Required arithmetic operations.
- Output quantity representations.
- Decimal precision policy.
- Timestamp conversion strategy.
- Aggressor-side interpretation.
- Instrument validation requirements.

For example:

    Native unit: Contracts
    Contract value: 100
    Contract value asset: USD
    Price: USD/BTC

The compiled quantity strategy becomes:

    QuoteValuedContracts

The hot path performs the necessary arithmetic without repeatedly determining which conversion strategy applies.

#### Trade and L2 Contexts

Trade and L2 streams may share the same instrument metadata while using different native quantity semantics.

For example:

    InstrumentContext
           |
           +-- Trade Format
           |       |
           |       v
           |   TradeNormalizer
           |
           +-- Depth Format
                   |
                   v
               L2Normalizer

The compiled normalization contexts are separate.

They may reference the same immutable instrument metadata.

The compiler must not assume that trade quantities and depth quantities use identical native denominations merely because they belong to the same instrument.

---

### 13.8 Instrument Metadata Revisions

Instrument metadata changes over time.

MarketForge must support revisioned instrument contexts to preserve normalization correctness across configuration updates, reconnections, and historical recovery.

#### Revision Identity

Each immutable context is identified by:

    InstrumentId
    MetadataRevision

A metadata revision identifies a particular validated set of instrument specifications.

Conceptually:

    Instrument 15
         |
         +-- Revision 1
         |
         +-- Revision 2
         |
         +-- Revision 3

Multiple revisions may coexist while different consumers require them.

#### Revision Requirements

A metadata revision must provide:

- Stable revision identity.
- Instrument identity.
- Relevant economic specifications.
- Metadata provenance.
- Acquisition timestamp.
- Effective-time information when available.

A revision must never be silently modified after publication.

#### Fetched Time vs Effective Time

The existing instrument catalog contains:

    fetched_at

This identifies when Python retrieved the metadata.

It does not necessarily identify when the exchange changed the specification.

Therefore:

    fetched_at != effective_at

For example:

    Exchange changes contract specification at 12:00.

    Python discovers the change at 12:05.

The metadata retrieval time is 12:05.

The economic specification may have become effective at 12:00.

If the exchange does not provide an authoritative effective time, the interval between the last known old specification and the first observed new specification must remain explicitly uncertain.

MarketForge must not invent an exact effective timestamp.

#### Historical Normalization

Recovered or historical market data may require an older metadata revision.

Example:

    Historical execution: 10:00
    Metadata revision change: 12:00
    Recovery performed: 15:00

The execution must be normalized using the specification applicable at 10:00, if that revision can be established.

Using the latest metadata merely because recovery occurs at 15:00 may produce incorrect quantities.

If the applicable revision cannot be established, the system must report unresolved metadata compatibility rather than silently applying current specifications.

#### Revision Retention

The Instrument Context Registry must retain revisions referenced by:

- Active stream processors.
- Pending configuration replacements.
- Recovery operations.
- Other explicitly registered consumers.

Unused revisions may be evicted according to a bounded retention policy.

Persistent metadata history remains owned by Python and PostgreSQL.

---

### 13.9 Runtime Metadata Updates

Python orchestration may refresh instrument metadata while Rust Live is running.

A metadata refresh does not automatically require stream reconnection.

The required action depends on which fields changed.

#### Update Pipeline

    Python Metadata Refresh
              |
              v
    PostgreSQL Catalog Update
              |
              v
    Detect Metadata Changes
              |
              v
    Create Metadata Revision
              |
              v
    Send Update to Rust
              |
              v
    Validate New Revision
              |
              v
    Identify Affected Streams
              |
              v
    Compile Replacement Contexts
              |
              v
    Apply at Safe Boundary

The Rust runtime must validate updates independently before accepting them.

#### Change Classification

| Metadata change | Required behavior |
|---|---|
| `min_qty` | Refresh trading constraints |
| `max_qty` | Refresh trading constraints |
| `min_notional` | Refresh trading constraints |
| `tick_size` | Update validation context at a safe boundary |
| `qty_step` | Update validation context at a safe boundary |
| `contract_value` | Replace normalization context |
| `contract_value_asset` | Replace normalization context |
| `quantity_type` | Recompile and validate normalization strategy |
| Base or quote asset | Revalidate instrument identity and conversion compatibility |
| Native symbol | Evaluate subscription replacement |
| Native market category | Evaluate subscription replacement |

The Configuration System determines change compatibility.

The Stream Registry coordinates affected stream lifecycle operations.

#### Atomic Context Replacement

A processing context must remain immutable during event processing.

A replacement is prepared independently:

    Active Context Revision N
              |
              v
    Compile Revision N+1
              |
              v
    Validate Compatibility
              |
              v
    Establish Safe Boundary
              |
              v
    Activate Revision N+1
              |
              v
    Release Revision N When Unused

A trade record must never use a mixture of two metadata revisions.

An atomic L2 message must never be partially normalized under different contract specifications.

#### Market-Data Boundary

The replacement boundary must be explicit.

At minimum, the runtime must identify:

- Last event processed using the previous context.
- First event processed using the replacement context.
- Previous metadata revision.
- Replacement metadata revision.
- Relevant configuration revision.

When the effective-time boundary is known, processing must respect it.

When the effective-time boundary is unknown, MarketForge must preserve the uncertainty rather than claim exact historical correctness.

#### L2 Contract Specification Changes

Changes to normalization-critical contract metadata require particular care for active order books.

The BookStore may already contain normalized quantities derived from the previous contract specification.

Changing the normalizer without rebuilding or converting existing book state could mix incompatible economic quantities.

Therefore, an L2 metadata update must use one of two explicitly supported strategies:

    RebuildFromAuthoritativeSnapshot

    RecalculateFromPreservedNativeLevels

The second strategy is permitted only if the BookStore retains sufficient exact native quantities and the conversion is valid.

Otherwise, the current book generation must be invalidated and reconstructed.

The system must never continue publishing a book containing levels normalized using incompatible metadata revisions.

#### Update Failure

If the replacement context cannot be compiled or validated:

- Preserve the last valid context when safe.
- Report the failed metadata revision.
- Do not partially apply the update.
- Prevent activation of incompatible streams.
- Escalate when continuing with the old context would violate known current instrument semantics.

A previously valid context must not automatically be considered safe after an authoritative contract specification change.

---

### 13.10 Metadata Validation and Stream Activation

Instrument metadata must be validated before activating any stream that depends on it.

#### Validation Requirements

The compiler verifies:

- Known instrument identity.
- Matching exchange and market.
- Matching native subscription symbol.
- Supported native product category.
- Valid base and quote asset identities.
- Valid settlement denomination where applicable.
- Supported quantity classification.
- Required contract metadata.
- Supported contract-value denomination.
- Representable decimal values.
- Compatible metadata revision.
- Compatible exchange format.

The validator must distinguish missing optional fields from missing normalization-critical fields.

#### Spot Instruments

For ordinary Spot instruments, contract metadata may be absent.

Example:

    quantity_type = base
    contract_value = null
    contract_value_asset = null

This is valid when the exchange format reports base-asset quantities and no contract conversion is required.

Rust must not require a contract multiplier for every instrument.

#### Derivative Instruments

For contract-denominated derivatives, the compiler requires the metadata necessary to resolve the contract's economic value.

Example:

    quantity_type = contracts
    contract_value = 100
    contract_value_asset = USD

The precise strategy depends on the exchange format and canonical instrument definitions.

Missing required contract metadata must cause a compilation failure.

#### Raw Quantity Compatibility

The exchange format determines the meaning of the incoming field.

The instrument metadata determines how to calculate canonical economic quantities.

These must be compatible.

For example:

    Exchange format:
        raw_unit = quote_asset

    Instrument context:
        quote_asset = USD

The normalizer may interpret the native quantity as USD-denominated economic size.

Conversely:

    Exchange format:
        raw_unit = contracts

requires contract conversion metadata.

The compiler must not substitute the instrument's general `quantity_type` for a channel-specific raw quantity denomination.

#### Unresolved Protocol Requirements

A format containing:

    raw_unit = "requires_verification"

is not ready for economic normalization.

The compiler must reject activation unless the unresolved field has been replaced by a validated supported value.

This applies to any normalization-critical uncertainty, not only quantity units.

#### Activation Contract

A stream may enter active processing only when:

    Exchange Format Valid
             AND
    Instrument Context Valid
             AND
    Normalization Strategy Compiled
             AND
    Integrity Strategy Compiled
             AND
    Required Metadata Available

Failure to satisfy these conditions prevents stream activation.

The Stream Registry receives a structured configuration or initialization error.

---

### 13.11 Trading Constraints vs Market-Data Validation

MarketForge must distinguish trading-order constraints from market-data normalization requirements.

The instrument catalog contains both economic specifications and exchange trading constraints.

These fields serve different purposes.

#### Trading Constraints

Examples:

    tick_size
    qty_step
    min_qty
    max_qty
    min_notional

These values describe restrictions imposed on order submission or instrument trading.

They are not universal validity rules for individual exchange-reported executions.

#### Trade Quantity Validation

An exchange may report an individual fill smaller than the minimum quantity accepted for a new order.

Therefore:

    trade_quantity < min_qty

does not automatically imply an invalid trade.

Likewise:

    trade_notional < min_notional

does not automatically imply invalid market data.

Such observations may be diagnostically interesting, but the normalizer must not reject them solely because they violate current order-admission constraints.

#### Price and Quantity Increments

Tick sizes and quantity steps may support additional consistency checks.

However, their use requires care because:

- Exchange specifications change over time.
- Historical executions may follow earlier constraints.
- Special execution types may have different rules.
- Aggregated records may not resemble individual order submissions.
- Contract-size representations may differ between trading and market-data APIs.

Consequently, increment checks must be explicitly configured.

They must not be enforced blindly across all feeds.

#### Recommended Policy

| Constraint | Default market-data behavior |
|---|---|
| `tick_size` | Optional diagnostic validation |
| `qty_step` | Optional diagnostic validation |
| `min_qty` | Do not reject individual trades |
| `max_qty` | Do not reject individual trades |
| `min_notional` | Do not reject individual trades |
| `contract_value` | Required when contract conversion depends on it |
| `contract_value_asset` | Required when contract conversion depends on it |
| `quantity_type` | Required for applicable economic normalization |

The normalizer must prioritize preservation of valid exchange-reported data over enforcing unrelated order-entry restrictions.

---

### 13.12 Architectural Invariants

The following invariants define the instrument metadata architecture.

#### Ownership

    Python owns authoritative instrument metadata.

    PostgreSQL stores the authoritative catalog.

    Rust maintains validated runtime contexts.

#### Configuration

    Exchange TOML defines native protocol semantics.

    Instrument metadata defines instrument economics.

    Compiled normalization combines both.

#### Runtime Independence

    No per-event Python calls.

    No per-event PostgreSQL queries.

    No dependency on a continuously running Python process.

#### Context Immutability

    Instrument contexts are immutable after publication.

    Metadata updates create new revisions.

    Processing-context replacement occurs at explicit boundaries.

#### Precision

    Decimal metadata is transferred losslessly.

    Contract values are never guessed.

    Unsupported conversions are rejected.

#### Integrity

    Unknown quantity units prevent activation.

    Missing contract metadata prevents dependent normalization.

    Historical events must use applicable metadata revisions.

    L2 state must not mix incompatible normalization revisions.

#### Resource Reuse

    Multiple streams may share immutable instrument contexts.

    Trade and L2 streams may use different compiled normalization strategies.

    Runtime metadata storage is bounded and reference-safe.

#### Restart Safety

    Independent startup uses validated local metadata snapshots.

    Snapshots are versioned and written atomically.

    Cached metadata must not silently become authoritative.

#### Final Contract

For every accepted canonical event, MarketForge must be able to identify:

    Source Exchange
    Native Instrument
    Canonical InstrumentId
    Exchange Format Revision
    Instrument Metadata Revision
    Quantity Normalization Strategy
    Timestamp Normalization Strategy

This information must be recoverable through the canonical event provenance and associated revision records.

The system must never publish an economically normalized event when its required interpretation remains unresolved.

---

**End of Section 13 — Instrument Context Provisioning and Quantity Normalization.**

Sections 14–17 continue unchanged.

---

## 14. Base-Asset Quantity Normalization

Base-denominated quantities are common in Spot markets and some Linear Futures protocols.

### 14.1 Conversion

Given:

    price = P
    native_base_quantity = Qb

Canonical quantities:

    base_quantity = Qb
    quote_quantity = Qb × P

Example:

    Price:         65000 USD/BTC
    Base quantity: 0.25 BTC

    Quote quantity = 0.25 × 65000
                   = 16250 USD

The native base quantity is preserved exactly.

The quote quantity is derived using exact decimal multiplication.

### 14.2 Applicable Protocols

Examples from the initial configuration set include:

- Binance Spot.
- Bybit Spot.
- OKX Spot.
- Bitget Spot.
- Bitget Linear public fills.
- Gate.io Spot.

Actual applicability is determined by the compiled format, not by this illustrative list.

### 14.3 Contract Metadata

A derivative instrument may provide contract metadata even when its native trade size is already base-denominated.

The normalizer must not automatically apply the contract multiplier.

For example:

    Native size = 0.25 BTC

must not become:

    0.25 × contract_value

unless the format and instrument semantics explicitly require that transformation.

---

## 15. Quote-Asset Quantity Normalization

Quote-denominated quantities represent economic value in the quote asset.

### 15.1 Conversion

Given:

    price = P
    native_quote_quantity = Qq

Canonical quantities:

    quote_quantity = Qq
    base_quantity = Qq / P

Example:

    Price:          60000 USD/BTC
    Quote quantity: 150 USD

    Base quantity = 150 / 60000
                  = 0.0025 BTC

The quote quantity remains exact.

The derived base quantity follows the configured decimal division policy.

### 15.2 Applicable Protocols

Bitget UTA V3 COIN-Futures public fills are an example of quote-denominated native quantities.

The configuration specifies:

    raw_unit = "quote_asset"

This does not imply that every inverse exchange uses quote-denominated raw quantities.

Other inverse exchanges may report native contract counts.

### 15.3 Quantity Interpretation

The normalizer must distinguish:

    Quote-denominated raw quantity

from:

    Contract count with quote-denominated contract value

These are different representations.

For example:

    100 USD

is not necessarily equivalent to:

    100 contracts

unless the instrument metadata establishes a contract value of 1 USD per contract.

---

## 16. Contract Quantity Normalization

Derivative exchanges frequently report quantities as native contract counts.

The economic meaning of one contract depends on instrument specifications.

### 16.1 Required Metadata

Contract normalization requires:

    contract_value
    contract_value_asset
    base_asset
    quote_asset

Additional metadata may be required for nonstandard instruments.

The normalizer must not infer contract value from the instrument symbol.

### 16.2 Base-Valued Contracts

For a contract whose value is denominated in the base asset:

    contracts = C
    contract_value = Vb

Then:

    base_quantity = C × Vb
    quote_quantity = base_quantity × price

Example:

    Contracts:       5
    Contract value:  0.001 BTC
    Price:           60000 USD/BTC

    Base quantity:   0.005 BTC
    Quote quantity:  300 USD

The contract value must be obtained from the instrument catalog.

### 16.3 Quote-Valued Contracts

For a contract whose value is denominated in the quote asset:

    contracts = C
    contract_value = Vq

Then:

    quote_quantity = C × Vq
    base_quantity = quote_quantity / price

Example:

    Contracts:       5
    Contract value:  100 USD
    Price:           60000 USD/BTC

    Quote quantity:  500 USD
    Base quantity:   0.008333333... BTC

The derived base quantity requires the configured division precision policy.

### 16.4 Unsupported Contract Denominations

If `contract_value_asset` is neither the base nor quote asset, the normalizer must not invent a conversion.

Possible cases include:

- Quanto derivatives.
- Third-asset contract denominations.
- Nonstandard settlement structures.
- Instruments requiring additional conversion rates.

These require an explicitly supported normalization strategy.

Until supported, activation must fail with a structured configuration error.

### 16.5 Contract Normalization Strategy

The compiled normalizer selects one of:

    BaseDenominated
    QuoteDenominated
    BaseValuedContracts
    QuoteValuedContracts
    Unsupported

This selection occurs during stream initialization.

The hot path executes the already-resolved strategy.

---

## 17. Trade and L2 Quantity Differences

Trade quantities and order-book quantities may use the same native denomination but different semantic rules.

### 17.1 Trade Quantities

Trade quantities represent executed economic volume.

A trade quantity must be positive after normalization.

Some exchanges encode aggressor direction through the sign of the native quantity.

Example:

    Gate Futures size = -5.5

Interpretation:

    aggressor = Sell
    contracts = 5.5

The sign is removed only after aggressor-side interpretation.

### 17.2 L2 Quantities

L2 quantities represent resting liquidity at a price level.

Incremental updates generally specify absolute quantities for the affected price level.

Example:

    Price:    65000
    Quantity: 2.5 contracts

A subsequent update:

    Price:    65000
    Quantity: 1.5 contracts

means the new resting quantity is 1.5 contracts.

It does not mean that 1.5 contracts should be added.

### 17.3 Zero-Quantity Deletion

For supported L2 protocols:

    quantity = 0

means:

    Delete price level

The normalizer must preserve this operation.

A zero-quantity L2 update must not be rejected merely because trade quantities require positive values.

### 17.4 Quantity Normalization Ordering

The recommended processing order is:

    Extract Native Quantity
              |
              v
    Validate Native Representation
              |
              v
    Interpret Native Semantics
              |
              v
    Resolve Economic Quantity
              |
              v
    Apply Precision Policy
              |
              v
    Produce Canonical Quantity

For signed trades, aggressor-side extraction occurs before taking the absolute quantity.

For L2, zero-quantity deletion is identified before attempting economic conversion.

---
## 18. Trade Normalization

Trade normalization converts exchange-native execution records into the canonical `Trade` representation defined in **02 — Domain Models and Contracts**.

The subsystem must support different native execution formats without introducing exchange-specific processing pipelines.

The initial implementation supports:

- Individual trade executions.
- Exchange-aggregated trade records.
- Explicit aggressor-side fields.
- Maker-side indicators.
- Signed trade quantities.
- Base-, quote-, and contract-denominated quantities.
- Native execution identifiers.
- Exchange-specific execution attributes.

Trade normalization must preserve the distinction between **an individual execution and an aggregated execution record**.

### 18.1 Trade Normalization Pipeline

    Native Trade Record
             |
             v
    Validate Required Fields
             |
             v
    Resolve Instrument Context
             |
             v
    Normalize Timestamp
             |
             v
    Normalize Price
             |
             v
    Interpret Aggressor Side
             |
             v
    Normalize Quantity
             |
             v
    Resolve Native Identity
             |
             v
    Normalize Trade Attributes
             |
             v
    Validate Canonical Trade
             |
             v
    Trade Integrity Validator
             |
             v
    Canonical Trade

The implementation may combine compatible operations into a compiled processing function.

However, each semantic transformation must remain independently testable.

### 18.2 Canonical Trade Requirements

The resulting canonical trade must provide the fields required by the existing domain contract.

Conceptually:

    Trade {
        instrument_id
        trade_id
        event_timestamp
        price
        base_quantity
        quote_quantity
        aggressor_side
        trade_semantics
        provenance
    }

This representation is illustrative.

The authoritative Rust types and field definitions remain those specified in document 02.

The normalizer must not introduce a competing canonical trade schema.

### 18.3 Normalization Invariants

Every accepted canonical trade must satisfy:

    price > 0

    base_quantity > 0

    quote_quantity > 0

    aggressor_side ∈ {Buy, Sell}

    instrument_id is valid

    event_timestamp is representable

    native identity satisfies the configured strategy

Derived quantities may use explicitly configured decimal precision.

The original native values must remain recoverable through the defined provenance mechanism where required.

---

## 19. Aggressor-Side Normalization

Different exchanges encode trade direction using different conventions.

The normalizer must determine **which side initiated the execution**, not which side provided passive liquidity.

### 19.1 Canonical Aggressor Side

Supported canonical values:

    Buy
    Sell

An unknown side must not be silently assigned.

If a future canonical contract supports an explicit unknown state, its use must be defined separately.

The initial trade normalization policy rejects records whose aggressor side cannot be determined under the compiled strategy.

### 19.2 Explicit Side Fields

Some exchanges provide an explicit taker-side field.

Examples:

    "side": "buy"

    "side": "sell"

The compiled strategy maps native values directly:

| Native value | Canonical aggressor |
|---|---|
| `buy` | Buy |
| `sell` | Sell |

Applicable examples include:

- Bybit public trades.
- OKX public trades.
- Bitget public trades.
- Gate.io Spot trades.

The exact field selector is defined in the exchange-specific TOML.

### 19.3 Maker-Side Indicators

Some exchanges identify whether the buyer was the maker.

Binance Spot is an example.

Native field:

    m

Interpretation:

| `m` | Buyer role | Canonical aggressor |
|---|---|---|
| `true` | Maker | Sell |
| `false` | Taker | Buy |

The compiled strategy must explicitly invert the maker-side indicator.

Conceptually:

    if buyer_is_maker:
        aggressor = Sell
    else:
        aggressor = Buy

The normalizer must not confuse maker direction with aggressor direction.

### 19.4 Signed Quantity

Gate.io Futures encodes aggressor direction through the sign of the executed contract quantity.

Native examples:

    size = 10.5

    size = -10.5

Interpretation:

| Native size | Aggressor | Economic quantity |
|---|---|---|
| Positive | Buy | Absolute size |
| Negative | Sell | Absolute size |
| Zero | Invalid trade | Reject |

The transformation is:

    aggressor_side = sign(native_size)

    native_contract_quantity = abs(native_size)

Quantity normalization then uses the instrument catalog.

### 19.5 Aggressor-Side Strategy

The compiled normalizer selects one of:

    ExplicitSide
    BuyerMakerFlag
    SignedQuantity

Each strategy is configured through the exchange format.

The strategy must be resolved during configuration compilation.

The hot path must not repeatedly inspect exchange identity to determine which interpretation applies.

### 19.6 Aggressor-Side Validation

Reject a trade when:

- The required side field is missing.
- The native value is unsupported.
- A maker indicator has an invalid type.
- Signed quantity is zero.
- The selected strategy conflicts with the configured field representation.

The normalizer must not infer aggressor direction from price changes, bid/ask proximity, or tick-rule classification.

Those are separate analytical estimators and do not replace exchange-reported execution semantics.

---

## 20. Individual and Aggregated Trades

MarketForge must preserve the distinction between individual executions and exchange-aggregated trade records.

This is essential for future market-microstructure research.

### 20.1 Individual Executions

An individual execution represents one exchange-reported trade event.

Conceptually:

    TradeSemantics = Individual

Individual executions may contain:

- Native execution ID.
- Execution price.
- Executed quantity.
- Aggressor side.
- Execution timestamp.
- Exchange-specific attributes.

One individual execution is not necessarily equivalent to one original taker order.

A single aggressive order may match multiple resting orders and generate multiple executions.

### 20.2 Aggregated Executions

Some exchanges expose aggregate trade feeds.

Binance Futures aggregate trades are an example.

An aggregate record may represent multiple underlying executions grouped according to exchange-defined rules.

Conceptually:

    TradeSemantics = Aggregated

The canonical event must preserve its aggregated nature.

### 20.3 Aggregation Metadata

Where provided, preserve:

- Aggregate trade ID.
- First underlying trade ID.
- Last underlying trade ID.
- Aggregation timestamp.
- Aggregation-specific source metadata.

The exact fields depend on the exchange.

An aggregate record must not be silently converted into a fictitious individual execution.

### 20.4 Aggregation Limitations

Aggregated trades may obscure:

- Individual execution counts.
- Individual fill sizes.
- Exact execution timing within the aggregate.
- Multiple underlying counterparties.
- Microstructure details available only in individual feeds.

Therefore:

    One aggregate record != One individual execution

MarketForge must not fabricate underlying executions by dividing aggregate quantity across an inferred number of trades.

### 20.5 Canonical Representation

The canonical trade contract must retain the distinction between individual and aggregated records.

If the existing domain model cannot represent this distinction, the schema must be extended deliberately before enabling aggregated feeds.

The normalizer must not silently discard aggregation metadata to satisfy an incompatible output schema.

---

## 21. Native Trade Identity

Native trade identity is used for deduplication, reconciliation, provenance, and integrity diagnostics.

Different exchanges provide different identifier structures and guarantees.

### 21.1 Identity Requirements

The trade normalizer must:

- Extract the configured native trade identifier.
- Preserve its exact representation.
- Respect its documented scope.
- Construct a stable deduplication identity.
- Avoid unsupported assumptions about consecutiveness.
- Support reconciliation between live and recovered trades.
- Preserve supplementary native identifiers where available.

### 21.2 Native Identifier Types

Supported representations include:

    UnsignedInteger
    IntegerString
    OpaqueString

An identifier that appears numeric must not automatically be converted into a sequential integer.

For example:

    "12345678901234567890"

must not pass through `f64`.

Opaque identifiers must retain their exact native value.

### 21.3 Identity Scope

Native trade IDs may be unique within different scopes.

Examples include:

- Exchange-wide.
- Market-specific.
- Instrument-specific.
- Channel-specific.
- Aggregation-specific.

The configured identity strategy determines the applicable scope.

The normalizer must not assume that an identifier is globally unique merely because it is called `tradeId`.

### 21.4 Deduplication Identity

The initial generic identity structure is:

    TradeIdentity {
        exchange
        market
        instrument_id
        native_trade_id
        trade_semantics
    }

Additional fields may be required when the exchange's documented identity scope is narrower.

The compiled identity strategy must produce a deterministic key.

### 21.5 Gate.io Spot Identity

Gate.io Spot provides two relevant identifiers:

    id

    id_market

The configuration preserves both.

`id_market` is used as the preferred market-specific identity where supported by the compiled format.

The exchange-wide identifier remains available for provenance and reconciliation.

These identifiers must not be conflated.

### 21.6 Aggregated Trade Identity

Aggregated trades require identity semantics appropriate to their native aggregation protocol.

For example:

    aggregate_trade_id

must not automatically be interpreted as an individual execution ID.

An aggregate trade and an individual trade may represent overlapping economic activity.

Their identifiers must not be compared as though they belonged to the same namespace.

---

## 22. Trade Deduplication

Trade deduplication prevents the same native execution record from being published multiple times within the configured identity scope.

Potential duplicates arise from:

- WebSocket reconnection.
- Initial subscription snapshots.
- REST recovery overlap.
- Replayed buffered messages.
- Duplicate exchange notifications.
- Recovery retries.
- Subscription replacement.

### 22.1 Deduplication Strategy

The initial strategy is:

    NativeIdentityDeduplication

Each accepted trade identity is recorded in a bounded deduplication structure.

Conceptually:

    Native Trade
         |
         v
    Construct Identity
         |
         v
    Check Deduplication State
         |
      +--+--+
      |     |
      v     v
    New   Existing
      |     |
      v     v
    Accept Duplicate Handling

The deduplication structure must not grow without bounds.

### 22.2 Duplicate Outcomes

A duplicate identity may produce:

    ExactDuplicate
    ConflictingDuplicate
    UnknownIdentity

**ExactDuplicate**

The identity and relevant canonical values match an already accepted trade.

The record may be suppressed from duplicate publication while retaining a diagnostic counter.

**ConflictingDuplicate**

The identity matches, but one or more economically relevant fields differ.

Examples:

- Different price.
- Different quantity.
- Different aggressor side.
- Different execution timestamp.

This is an integrity conflict, not an ordinary duplicate.

**UnknownIdentity**

The record lacks a valid identity under the configured strategy.

The record must follow the format's explicit identity policy.

### 22.3 Conflicting Duplicates

Conflicting duplicates must not be silently resolved by choosing the newest or oldest record.

The Integrity subsystem must report:

    TradeIdentityConflict

The response may include:

- Quarantining the conflicting record.
- Marking stream coverage as degraded.
- Requesting reconciliation.
- Escalating repeated conflicts.

The exact response depends on the integrity policy.

### 22.4 Deduplication Window

Deduplication state must have bounded retention.

Possible policies include:

    MaximumEntries
    TimeWindow
    NativeIdentityRange

The selected policy must account for the exchange's replay and recovery behavior.

A record outside the retained deduplication window cannot automatically be classified as new solely because its identity is absent from memory.

Recovery reconciliation may require a broader identity index or persisted coverage metadata.

### 22.5 Recovery Deduplication

Live and recovered trades must use compatible identity semantics.

Conceptually:

    Live Trades ---------+
                         |
                         v
                  Identity Resolver
                         ^
                         |
    REST Recovery -------+
                         |
                         v
                  Deduplication
                         |
                         v
                  Canonical Output

REST and WebSocket identity compatibility must be verified for each exchange.

If native identifiers differ between sources, an explicit reconciliation strategy is required.

The system must not assume equivalence from similar field names alone.

### 22.6 Deduplication and Ordering

Deduplication must not arbitrarily reorder accepted trades.

When recovery produces earlier event timestamps than currently received live events, ordering is handled through the configured recovery and downstream event-ordering contracts.

The deduplication component determines identity, not global event-time ordering.

---

## 23. Trade Continuity and Gap Detection

Trade continuity is different from L2 sequence continuity.

Many public trade feeds do not provide enough information to prove that every execution was received.

### 23.1 Continuity Guarantees

The Integrity subsystem must distinguish:

    VerifiedContinuous
    SuspectedGap
    ConfirmedGap
    UnverifiedCoverage

These conditions represent different levels of evidence.

### 23.2 Sequential Trade Identifiers

Some exchanges provide increasing trade identifiers.

However:

    Increasing != Consecutive

A native ID jump does not automatically prove that a specific number of trades was missed.

Exact gap detection is permitted only when the exchange protocol establishes the required sequence semantics.

### 23.3 Timestamp Discontinuities

A large interval between consecutive trade timestamps does not necessarily indicate data loss.

A market may simply have no executions during that interval.

Therefore:

    Large Time Gap != Confirmed Missing Trades

Timestamp gaps may trigger diagnostics or reconciliation, but they must not independently establish missing execution counts.

### 23.4 Transport Disconnections

A transport disconnection creates an interval of uncertain trade coverage.

The Integrity subsystem must record the interruption boundary.

Conceptually:

    Last Accepted Trade
             |
             v
    Connection Lost
             |
             v
    Coverage Unverified
             |
             v
    Connection Restored
             |
             v
    Recovery / Reconciliation
             |
             v
    Coverage Classification

A successful reconnection does not prove that no trades occurred during the outage.

### 23.5 Recovery Evidence

Coverage may be verified using:

- Exchange-supported historical trade APIs.
- Native execution identifiers.
- Documented sequence guarantees.
- Historical archives.
- Overlap between recovered and live records.

The Recovery Coordinator determines whether available evidence is sufficient.

The normalizer must not independently declare an interval complete.

### 23.6 Partial Recovery

A recovery request may return only part of the missing interval.

For example, an exchange may expose only the most recent executions.

In this case:

    RecoverySucceeded != CoverageComplete

The subsystem must preserve unresolved coverage intervals.

Partial recovery must not be reported as complete restoration.

---

## 24. Exchange-Specific Trade Attributes

Some exchanges expose execution metadata beyond the canonical trade fields.

These attributes may be important for future market-microstructure analysis.

### 24.1 Supported Attributes

Examples from the initial configurations:

| Exchange | Attribute | Meaning |
|---|---|---|
| OKX | `source` | Exchange-native execution source |
| Bitget | `execLinkId` | Execution correlation identifier |
| Bitget | `isRPI` | Retail Price Improvement classification |
| Gate.io Spot | `trade_mode` | Unified trading-pair mode |
| Gate.io Spot | `stock` | Actual base asset |
| Gate.io Spot | `money` | Actual quote asset |
| Gate.io Spot | `range` | Native trade range metadata |
| Gate.io Futures | `is_internal` | Internal execution classification |

These fields must be interpreted according to their exchange-specific specifications.

### 24.2 Attribute Preservation

Attributes may be retained through:

- Dedicated canonical optional fields.
- Typed provenance structures.
- Exchange-native attribute containers.

The chosen representation must be compatible with the canonical contracts in document 02.

Arbitrary untyped JSON should not be introduced into the high-throughput canonical hot path without an explicit performance and schema justification.

### 24.3 Missing Attributes

Optional attributes require explicit missing-value semantics.

Possible states include:

    Present(value)
    Missing
    Unknown

For example, an absent RPI indicator must not automatically mean that the trade was not RPI-related unless the exchange protocol explicitly defines that behavior.

### 24.4 Internal and Nonstandard Executions

Some exchange-reported trades may not correspond to ordinary visible order-book matching.

Gate Futures `is_internal` is an example.

Such records should remain identifiable.

The normalizer must not silently remove them or classify them as ordinary executions when the native protocol provides a meaningful distinction.

### 24.5 Unified Trading Pairs

Gate.io Spot may provide actual traded assets that differ from the nominal subscribed pair.

The normalizer must validate:

    currency_pair
    stock
    money
    trade_mode

When the actual economic assets differ from the instrument's nominal base and quote assets, the configured strategy must resolve the correct semantics.

If the required conversion is unsupported, the record must not be normalized using incorrect asset denominations.

---

## 25. Trade Integrity Outcomes

After normalization, the Trade Integrity Validator determines whether the record may be accepted.

### 25.1 Validation Outcomes

    Accepted
    Duplicate
    Rejected
    Quarantined
    RequiresRecovery

| Outcome | Meaning |
|---|---|
| Accepted | Valid trade eligible for canonical publication |
| Duplicate | Previously accepted native execution |
| Rejected | Invalid or unsupported trade record |
| Quarantined | Record retained for investigation without normal publication |
| RequiresRecovery | Integrity condition requires reconciliation |

These outcomes must be distinguishable in metrics and operational diagnostics.

### 25.2 Validation Order

The recommended order is:

    Native Record
         |
         v
    Structural Validation
         |
         v
    Instrument Validation
         |
         v
    Numeric Validation
         |
         v
    Semantic Normalization
         |
         v
    Canonical Validation
         |
         v
    Identity Validation
         |
         v
    Deduplication
         |
         v
    Continuity Evaluation
         |
         v
    Integrity Outcome

The implementation may combine compatible stages but must preserve equivalent behavior.

### 25.3 Publication Eligibility

Only accepted trades are eligible for normal canonical publication.

Rejected or quarantined records must not silently appear as valid canonical trades.

Duplicate records must not be published again unless a separately defined replay contract explicitly requires it.

### 25.4 Integrity Metrics

The Metrics Registry should expose:

| Metric | Description |
|---|---|
| trades_received_total | Native trade records received |
| trades_normalized_total | Successfully normalized trades |
| trades_accepted_total | Accepted canonical trades |
| trades_duplicate_total | Exact duplicates |
| trades_conflicting_duplicate_total | Conflicting identities |
| trades_rejected_total | Invalid records |
| trades_quarantined_total | Quarantined records |
| trade_identity_errors_total | Native identity failures |
| trade_quantity_errors_total | Quantity normalization failures |
| trade_timestamp_errors_total | Timestamp normalization failures |
| trade_coverage_gaps_total | Confirmed or suspected coverage gaps |
| trade_recovery_requests_total | Recovery requests triggered by integrity conditions |

Metrics must distinguish malformed records from confirmed missing executions.

---

## 26. L2 Order Book Normalization

L2 normalization transforms exchange-native order-book snapshots and incremental updates into canonical MarketForge L2 events.

Unlike trade normalization, L2 processing is stateful.

A valid L2 update cannot always be interpreted independently because its correctness may depend on previously accepted updates, native sequence identifiers, and an authoritative snapshot.

The subsystem must distinguish between:

- A structurally valid L2 message.
- A semantically valid L2 update.
- A synchronized order book.
- A complete history of order-book changes.

These conditions are not equivalent.

### 26.1 Primary Responsibilities

The L2 normalization and integrity subsystem must:

- Extract native bid and ask levels.
- Normalize prices and quantities.
- Interpret snapshot and delta semantics.
- Preserve native sequence identifiers.
- Validate sequence continuity.
- Establish authoritative snapshot baselines.
- Apply incremental updates atomically.
- Detect stale, duplicate, and missing updates.
- Maintain synchronized book generations.
- Track observable depth coverage.
- Detect invalid order-book states.
- Coordinate resynchronization after integrity failures.
- Produce canonical L2 snapshots and updates.
- Preserve source provenance.

The subsystem must not fabricate missing order-book updates.

### 26.2 L2 Processing Pipeline

    Native L2 Message
           |
           v
    Message Classification
           |
           v
    Structural Validation
           |
           v
    Instrument Resolution
           |
           v
    Native Sequence Extraction
           |
           v
    Price-Level Extraction
           |
           v
    Price and Quantity Normalization
           |
           v
    Sequence Integrity Validation
           |
           v
    Snapshot / Delta Synchronization
           |
           v
    Atomic Book State Transition
           |
           v
    Canonical L2 Event
           |
           v
    Publisher / Recording Manager

The pipeline represents logical processing stages.

Implementations may combine compatible stages to reduce allocations and processing overhead.

However, validation must occur before an update modifies authoritative book state.

### 26.3 Canonical L2 Contracts

The canonical L2 contracts are defined in **02 — Domain Models and Contracts**.

The normalization subsystem must reuse those definitions.

Conceptually:

    L2Snapshot {
        instrument_id
        event_timestamp
        bids
        asks
        sequence
        book_generation
        provenance
    }

    L2Update {
        instrument_id
        event_timestamp
        bid_changes
        ask_changes
        sequence
        book_generation
        provenance
    }

These representations are illustrative.

The authoritative Rust field definitions remain those specified in document 02.

### 26.4 L2 Event Semantics

Canonical L2 events must distinguish:

    Snapshot
    Delta

**Snapshot**

An authoritative representation of the order book within a defined coverage scope.

**Delta**

A set of changes to an existing synchronized order book.

A delta must not be interpreted as a complete book.

A snapshot must not be interpreted as an incremental modification.

---

## 27. Native Price-Level Extraction

Exchange-native L2 messages use different price-level representations.

MarketForge must support these representations through compiled format specifications.

### 27.1 Supported Level Representations

The initial implementation supports:

    ArrayLevel
    ObjectLevel

**Array-based levels**

Example:

    ["65000.50", "1.25"]

The format specifies:

    price_index = 0
    quantity_index = 1

**Object-based levels**

Example:

    {
        "p": "65000.50",
        "s": "1.25"
    }

The format specifies:

    price_field = "p"
    quantity_field = "s"

The generic extractor must support both without exchange-specific processing pipelines.

### 27.2 Exchange Representations

| Exchange | Typical L2 representation |
|---|---|
| Binance | Array-based levels |
| Bybit | Array-based levels |
| OKX | Array-based levels |
| Bitget | Array-based levels |
| Gate.io Spot | Array-based levels |
| Gate.io Futures | Object-based levels |

The compiled TOML remains authoritative for each channel.

### 27.3 NativeBookLevel

Conceptual intermediate representation:

    NativeBookLevel {
        price
        quantity
        order_count
    }

The values retain native representations until normalization.

The extractor must preserve:

- Original price.
- Original quantity.
- Optional order count.
- Native record position.
- Source-message provenance.

### 27.4 Optional Level Metadata

Some exchanges expose additional price-level information.

OKX is an example.

A native level may contain:

    [
        price,
        quantity,
        deprecated_field,
        order_count
    ]

The format specification identifies which elements are meaningful.

Deprecated or unsupported fields must not be assigned invented semantics.

Optional order counts may be preserved when the canonical BookLevel contract supports them.

### 27.5 Level Validation

Every extracted level must satisfy:

    price > 0

    quantity >= 0

Additional requirements:

- Price must be representable exactly.
- Quantity must be representable exactly.
- Required fields must exist.
- Field types must match the compiled format.
- Negative quantities are rejected unless explicitly supported.
- Optional metadata must satisfy its declared type.

Zero quantity is permitted for deletion semantics.

### 27.6 Duplicate Price Levels

A single native message may contain repeated updates for the same price.

The processor must not arbitrarily sum them.

The configured protocol determines whether repeated prices are:

    ValidSequentialUpdates
    InvalidDuplicateLevels
    LastValueWins

For snapshots, duplicate prices should be rejected unless the exchange protocol explicitly defines a valid interpretation.

For deltas, repeated prices may be processed in native order only when the protocol guarantees that interpretation.

Unknown duplicate-level semantics must produce an integrity error.

---

## 28. Bid and Ask Normalization

Bid and ask levels must preserve their economic side.

### 28.1 Canonical Sides

Supported sides:

    Bid
    Ask

The native message structure determines the side.

For example:

    bids -> Bid

    asks -> Ask

The normalizer must not infer side from price movement or position relative to the current midprice.

### 28.2 Price Normalization

Native prices are converted using exact decimal arithmetic.

The normalizer must reject:

- Zero prices.
- Negative prices.
- Invalid decimal representations.
- Unrepresentable values.
- Unsupported precision.

Tick-size validation may be applied when the instrument catalog provides a compatible tick-size specification.

However, the normalizer must not silently round prices to the nearest tick.

### 28.3 Quantity Normalization

L2 quantity normalization uses the same compiled instrument context described in Part 3.

Supported native denominations:

    BaseAsset
    QuoteAsset
    Contracts

The quantity normalizer must preserve the native economic meaning.

For example:

    Native quantity = 100 contracts

does not automatically mean:

    Base quantity = 100

or:

    Quote quantity = 100

Contract conversion requires authoritative instrument metadata.

### 28.4 Absolute Quantity Updates

The initial supported L2 protocols use absolute quantity updates.

Given an existing level:

    Price:    65000
    Quantity: 5.0

A delta containing:

    Price:    65000
    Quantity: 3.0

means:

    New quantity = 3.0

It does not mean:

    New quantity = 8.0

The compiled strategy is:

    update_mode = absolute

### 28.5 Zero-Quantity Deletion

A delta containing:

    Price:    65000
    Quantity: 0

means:

    Remove price level 65000

The canonical operation must preserve this deletion semantic.

A zero quantity must not be converted into an ordinary resting-liquidity level.

### 28.6 Snapshot Quantity Rules

Snapshot levels represent existing resting liquidity.

Therefore, accepted snapshot levels should normally satisfy:

    quantity > 0

Zero-quantity entries in snapshots require explicit protocol support.

The initial conservative policy is to reject malformed snapshot levels rather than silently remove them.

---

## 29. Snapshot and Delta Classification

Before applying an L2 message, MarketForge must determine whether it represents an authoritative snapshot or an incremental update.

### 29.1 Classification Strategies

Supported strategies include:

    ExplicitMessageType
    ExplicitActionField
    FullSnapshotFlag
    ExternalRestSnapshot
    ProtocolSpecificClassification

The selected strategy is compiled from the format specification.

### 29.2 Explicit Message Types

Some exchanges expose:

    type = snapshot

or:

    type = delta

Bybit is an example.

The classifier maps the native type into the canonical message kind.

### 29.3 Explicit Action Fields

Other exchanges expose:

    action = snapshot

or:

    action = update

OKX and Bitget are examples.

The classifier must validate the expected action values.

### 29.4 Full Snapshot Flags

Gate.io may expose:

    full = true

This indicates an authoritative snapshot within the channel's documented coverage.

The absence of the flag may indicate an incremental update when the protocol explicitly defines that behavior.

### 29.5 External REST Snapshots

Some protocols initialize L2 state using a REST snapshot.

Examples include:

- Binance Spot.
- Binance Futures.
- Gate.io Spot.
- Gate.io Futures.

A REST snapshot is not automatically compatible with arbitrary WebSocket updates.

The synchronization strategy must establish a valid sequence bridge.

### 29.6 Classification Failure

An L2 message with an unknown or ambiguous classification must not modify the authoritative book.

Possible outcomes:

    RejectMessage
    QuarantineMessage
    InvalidateBook
    RequestResynchronization

The selected response depends on the compiled integrity policy.

---

## 30. Snapshot Processing

A snapshot establishes an authoritative order-book baseline.

Its authority is limited to the depth and market-data scope provided by the exchange.

### 30.1 Snapshot Requirements

A valid snapshot must provide:

- Correct instrument identity.
- Valid price levels.
- Valid quantity semantics.
- Required native sequence metadata.
- Supported snapshot classification.
- Compatible configuration revision.
- Valid connection or recovery generation.
- Defined depth coverage.

### 30.2 Snapshot Processing Pipeline

    Native Snapshot
          |
          v
    Validate Instrument
          |
          v
    Validate Snapshot Metadata
          |
          v
    Extract Bid / Ask Levels
          |
          v
    Normalize Prices / Quantities
          |
          v
    Validate Book Structure
          |
          v
    Establish Sequence Baseline
          |
          v
    Create New Book Generation
          |
          v
    Atomically Replace Book
          |
          v
    Publish Canonical Snapshot

The snapshot must be fully validated before replacing authoritative book state.

### 30.3 Atomic Snapshot Replacement

Snapshot replacement must be atomic.

The BookStore must never expose a partially initialized book.

The implementation should construct the replacement state independently and publish it only after successful validation.

Conceptually:

    Existing Book
         |
         v
    Build Replacement
         |
         v
    Validate Replacement
         |
       +---+---+
       |       |
       v       v
     Valid   Invalid
       |       |
       v       v
    Commit   Reject

An invalid replacement must not partially modify the existing book.

### 30.4 Snapshot Sequence Baseline

The snapshot establishes the native sequence position from which incremental processing may continue.

Depending on the exchange, the baseline may use:

    lastUpdateId
    seqId
    u
    update_id

The exact field and semantics are defined by the compiled synchronization strategy.

### 30.5 Snapshot Depth

Snapshots may contain a limited number of price levels.

Examples:

    Top 20
    Top 50
    Top 100
    Top 400
    Top 1000

A synchronized 100-level snapshot does not establish knowledge of deeper liquidity.

The BookStore must preserve the coverage scope.

### 30.6 Snapshot Replacement During Recovery

A new authoritative snapshot may arrive after:

- Reconnection.
- Sequence violation.
- Exchange maintenance.
- Explicit resynchronization.
- Subscription replacement.

The new snapshot establishes a new book generation.

Old-generation deltas must not be applied afterward.

---

## 31. Incremental L2 Processing

Incremental messages modify an existing synchronized order book.

They must satisfy the configured native sequence rules before becoming authoritative.

### 31.1 Delta Requirements

A delta must provide:

- Correct instrument identity.
- Valid message classification.
- Valid price-level changes.
- Required native sequence metadata.
- Compatible subscription generation.
- Compatible book generation.
- Valid sequence continuity.

A delta received before synchronization must not initialize an order book unless the protocol explicitly supports that behavior.

### 31.2 Delta Processing Pipeline

    Native Delta
         |
         v
    Validate Message
         |
         v
    Normalize Level Changes
         |
         v
    Validate Sequence
         |
         v
    Validate Book Generation
         |
         v
    Prepare Atomic Mutation
         |
         v
    Apply Bid / Ask Changes
         |
         v
    Validate Resulting Book
         |
         v
    Commit Book State
         |
         v
    Publish Canonical L2 Update

The update must not be published as accepted before its required integrity checks succeed.

### 31.3 Atomic Delta Application

All changes within one exchange-native L2 message form one logical atomic update when the protocol defines message-level atomicity.

For example:

    Bid changes:
        65000 -> 2.5
        64999 -> 0

    Ask changes:
        65001 -> 1.2
        65002 -> 3.0

The processor must apply the entire update consistently.

A failure while processing one level must not leave only part of the message applied.

### 31.4 Update Ordering

Deltas must be applied in the order required by the exchange protocol.

Transport order alone may be sufficient for some channels.

Other channels require native sequence validation.

The processor must not reorder L2 updates solely by exchange timestamps.

Native sequence semantics take precedence over timestamp ordering.

### 31.5 Stale Updates

An update may be stale when its native sequence position has already been incorporated into the current book.

The configured strategy determines whether the update may be safely ignored.

A stale update must not overwrite newer book state.

### 31.6 Sequence Gaps

A sequence gap occurs when the next required native update is not covered by the received message.

The precise definition depends on the exchange's synchronization strategy.

For a range-based protocol:

    last_applied_id = 1500

    next_required_id = 1501

    received update:
        U = 1505
        u = 1510

The update does not cover the next required sequence.

Therefore:

    SequenceGapDetected

The processor must not apply the update to the authoritative book.

Required behavior:

- Mark the current book generation invalid.
- Record the sequence violation.
- Prevent publication of further synchronized-book updates.
- Notify the Recovery Coordinator.
- Begin the configured resynchronization procedure.
- Preserve the failure boundary for diagnostics.

The processor must not attempt to repair the book by assuming that missing updates contained no meaningful changes.

### 31.7 Empty Incremental Updates

Some exchange protocols may emit incremental messages containing no price-level changes.

Such messages may still carry meaningful sequence information.

For example:

    bids = []
    asks = []

An empty update may be valid if the exchange protocol explicitly permits it.

The sequence validator must still evaluate the message.

If accepted, the message may advance the native sequence baseline without changing price levels.

Whether it produces a canonical L2 event depends on the canonical event contract and configured publication policy.

The processor must not invent a price-level modification merely to represent sequence advancement.

### 31.8 Delta Validation Failure

A delta must not partially modify authoritative book state when validation fails.

Possible failures include:

- Malformed price levels.
- Invalid quantity representation.
- Unsupported sequence transition.
- Incorrect instrument identity.
- Obsolete connection generation.
- Obsolete subscription generation.
- Invalid book generation.
- Unsupported message classification.

The processor must classify the failure and determine whether the existing book remains trustworthy.

A malformed delta that may contain unobserved book changes generally invalidates continuity unless the exchange protocol provides a safe recovery rule.

---

## 32. Native Sequence Validation

Order-book sequence validation is responsible for determining whether incremental updates can be applied to the current authoritative book.

The validator must support multiple exchange protocols through compiled synchronization strategies.

### 32.1 Sequence Representation

Different exchanges expose different sequence structures.

The initial implementation supports:

    SingleSequence
    PreviousSequence
    UpdateRange
    SnapshotSequence
    ProtocolSpecificSequence

Conceptually:

    NativeSequence {
        first_update_id
        last_update_id
        previous_sequence_id
        sequence_id
    }

Not every protocol provides every field.

The compiled strategy determines which fields are required.

### 32.2 Sequence Strategy Interface

The sequence validator exposes conceptual operations:

    Initialize(snapshot_sequence)

    Validate(delta_sequence)

    Commit(delta_sequence)

    Reset()

Validation must not mutate authoritative sequence state.

State advances only after the corresponding book update has been successfully committed.

This separation is essential for atomicity.

### 32.3 Sequence Validation Outcomes

Supported outcomes:

    Accept
    Stale
    Gap
    Reset
    Invalid
    RequiresSnapshot

| Outcome | Meaning |
|---|---|
| Accept | Update satisfies continuity requirements |
| Stale | Update is already covered by accepted state |
| Gap | Required native sequence coverage is missing |
| Reset | Exchange-defined sequence reset detected |
| Invalid | Sequence representation or transition is invalid |
| RequiresSnapshot | Existing state cannot establish continuity |

The validator must distinguish a documented reset from an ordinary sequence gap.

### 32.4 Sequence State

Conceptual structure:

    SequenceState {
        strategy
        initialized
        last_sequence
        previous_sequence
        snapshot_sequence
        connection_generation
        subscription_generation
        book_generation
    }

The exact representation depends on the selected strategy.

For example, a range-based validator may only require the last applied update ID.

A previous-sequence validator requires the preceding accepted sequence identifier.

---

## 33. Range-Based Sequence Validation

Range-based protocols provide the first and last native update identifiers represented by a message.

Examples include Binance and Gate.io.

### 33.1 Update Range

A native update contains:

    U = first_update_id
    u = last_update_id

The range must satisfy:

    U <= u

Invalid ranges must be rejected.

### 33.2 Snapshot Bridge

Given an authoritative snapshot:

    snapshot_id = S

The next required update is:

    next_required = S + 1

For protocols using an inclusive range bridge, the first applicable update must satisfy:

    U <= next_required <= u

Example:

    Snapshot ID: 1000

    First update:
        U = 998
        u = 1005

The update covers:

    1001

and may be accepted under the configured protocol.

The validator must not require:

    U == 1001

when overlapping ranges are explicitly supported.

### 33.3 Stale Updates

Given:

    last_applied_id = 1005

An update:

    U = 998
    u = 1005

is already covered.

The validator returns:

    Stale

The update must not modify the book.

### 33.4 Continuity

Given:

    last_applied_id = 1005

The next required update is:

    1006

An update:

    U = 1004
    u = 1010

covers the required sequence.

The validator may accept it when overlapping ranges are permitted by the specific exchange protocol.

An update:

    U = 1008
    u = 1010

does not cover the required sequence.

The validator returns:

    Gap

### 33.5 Exchange-Specific Constraints

Not all range-based protocols have identical continuity requirements.

For example:

- Binance Spot uses its documented snapshot and update-range synchronization rules.
- Binance Futures may additionally use `pu` to verify previous-update continuity.
- Gate.io uses its documented REST snapshot bridge and native update ranges.

The generic range validator must support configurable constraints.

It must not apply one exchange's synchronization rules universally.

### 33.6 Range Validation Invariants

For an accepted incremental update:

    U <= u

The update must cover the next required native sequence position under the selected protocol.

After successful book mutation:

    last_applied_id = u

Sequence state must not advance before the book mutation commits.

---

## 34. Previous-Sequence Validation

Some exchanges explicitly provide the previous sequence identifier associated with an update.

Examples include OKX and Bitget.

### 34.1 Previous-Sequence Model

A message may provide:

    previous_sequence_id = P

    sequence_id = S

Normal continuity may require:

    P == last_accepted_sequence

The exact interpretation depends on the exchange protocol.

### 34.2 Continuity Example

Current book:

    last_accepted_sequence = 1500

Incoming update:

    previous_sequence_id = 1500
    sequence_id = 1510

The previous sequence matches.

The update may be accepted if all other protocol requirements are satisfied.

### 34.3 Sequence Mismatch

Current book:

    last_accepted_sequence = 1500

Incoming update:

    previous_sequence_id = 1490
    sequence_id = 1510

The previous sequence does not match.

Unless a documented protocol exception applies, the update must be rejected and the book invalidated.

### 34.4 Snapshot Bridging

Some previous-sequence protocols use a different rule for the first update after a snapshot.

Bitget V3 is an example.

Its configured initial bridge is:

    update.pseq <= snapshot.seq <= update.seq

Subsequent updates use:

    current.pseq == previous.seq

The validator must distinguish initial synchronization from normal incremental continuity.

### 34.5 Sequence Reset Handling

Some exchanges document special reset behavior.

Examples may include:

- Sequence values returning to zero.
- Previous sequence becoming zero.
- Sequence decreases during maintenance.
- New authoritative snapshots.
- Exchange service restarts.

The validator must not assume:

    sequence_id > previous_sequence_id

for every protocol and every message.

Documented exceptions must be implemented explicitly.

### 34.6 Unsupported Sequence Transitions

If the incoming sequence transition does not match any supported rule:

    InvalidSequenceTransition

The processor must not guess whether the update is safe.

The book must be invalidated when continuity cannot be established.

---
