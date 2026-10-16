# MarketForge Live — Configuration System

## 1. Purpose

Define the configuration architecture of MarketForge Live, including declarative exchange specifications, runtime settings, per-stream policies, configuration inheritance, validation, persistence, and dynamic reconfiguration.

The system must support approximately 15 exchange/API families across Binance, Bybit, OKX, Bitget, and Gate.io without requiring independently implemented streaming pipelines.

**Primary objective:** Separate exchange-specific protocol declarations from generic Rust execution logic while allowing safe runtime configuration changes without interrupting unrelated streams.

The configuration system must support:

- Declarative TOML specifications.
- Global, exchange, source-stream, and derived-stream configuration.
- Reuse of existing MarketForge instrument metadata and normalization specifications.
- Typed configuration compilation and validation.
- Explicit inheritance and per-resource overrides.
- Immutable configuration revisions.
- Runtime reconfiguration through the control API and Ratatui.
- Persistent desired state and restart recovery.
- Deterministic configuration testing.
- Minimal configuration access in performance-critical processing paths.

Configuration must describe supported behavior rather than implement arbitrary execution logic.

---

## 2. Configuration Architecture

Configuration is divided into four primary domains.

| Domain | Responsibility | Runtime editing |
|---|---|---|
| Global | Service-wide policies, resource limits, publication, metrics, recording defaults | Selected fields |
| Exchange/API family | REST/WebSocket protocols, endpoints, subscriptions, heartbeats, rate limits | Read-only initially |
| Source stream | Instrument/channel acquisition, recording, recovery, diagnostics | Supported fields |
| Derived stream | Merging, ordering, conversion, dependencies | Supported fields |

A fifth category contains **format and normalization specifications**, which are compiled into source-stream processing plans but remain separate from operational configuration.

### 2.1 Configuration Resolution

```text
                  STATIC SPECIFICATIONS
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
         Exchange       Raw Format    Instrument
         Profiles       Definitions    Metadata
             |             |             |
             +-------------+-------------+
                           |
                           v
                   Configuration Compiler
                           ^
                           |
                   DESIRED RUNTIME STATE
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
           Global        Sources       Derived
           Settings      Streams       Streams
             |             |             |
             +-------------+-------------+
                           |
                           v
                    Validation
                           |
                           v
                  Resolved LivePlan
                           |
                           v
                    Rust Runtime
```

Static specifications define supported exchange behavior.

Desired runtime state defines which resources should exist and which operational policies should apply.

The compiler resolves both into an immutable, validated execution plan.

### 2.2 Configuration Principles

- TOML is the primary human-editable configuration format.
- Runtime components consume typed, resolved configuration rather than raw TOML.
- Configuration resolution occurs outside the per-event processing path.
- Exchange protocol constraints cannot be overridden by incompatible stream settings.
- Source identity remains independent of recording, subscriber, and diagnostic settings.
- Derived-stream configuration remains independent of underlying source acquisition.
- Configuration changes are validated before acceptance.
- Only affected runtime resources are reconciled.
- Invalid updates must not modify the active configuration.
- Configuration persistence and runtime activation are separate operations.

---

## 3. Configuration File Organization

Proposed structure:

```text
data/.config/
└── live/
    ├── live.toml
    │
    ├── exchanges/
    │   ├── binance_spot.toml
    │   ├── binance_linear.toml
    │   ├── binance_inverse.toml
    │   ├── bybit_spot.toml
    │   ├── bybit_linear.toml
    │   ├── bybit_inverse.toml
    │   ├── okx_spot.toml
    │   ├── okx_linear.toml
    │   ├── okx_inverse.toml
    │   ├── bitget_spot.toml
    │   ├── bitget_linear.toml
    │   ├── bitget_inverse.toml
    │   ├── gateio_spot.toml
    │   ├── gateio_linear.toml
    │   └── gateio_inverse.toml
    │
    ├── formats/
    │   ├── trades/
    │   ├── depth/
    │   └── liquidations/
    │
    └── runtime/
        ├── sources.toml
        └── derived.toml
```

The directory layout is conceptual.

Exchange profiles may share protocol definitions where their APIs are equivalent.

The system must not require 15 independent implementations merely because 15 configuration profiles exist.

### 3.1 Static and Dynamic Configuration

**Static specifications:**

- Exchange endpoints and protocol capabilities.
- Subscription and unsubscription message formats.
- Heartbeat strategies.
- Native message classification.
- Raw JSON extraction.
- Normalization rules.
- Sequence and snapshot synchronization strategies.
- Exchange rate-limit definitions.

**Dynamic desired state:**

- Enabled source streams.
- Recording requirements.
- Supported stream-level policies.
- Combined-stream definitions.
- Currency-conversion definitions.
- Hot-editable global settings.

Static specifications may change between deployments but are not hot-reloaded initially.

Dynamic state is managed through the versioned configuration system.

### 3.2 Configuration Source of Truth

The system must avoid conflicting configuration authorities.

Initial proposed precedence:

```text
Static TOML Specifications
          +
Global Runtime Defaults
          +
Persisted Desired State
          +
Explicit Resource Overrides
          |
          v
Resolved Effective Configuration
```

The Ratatui interface modifies persisted desired state through the control API.

It must not independently edit exchange protocol files.

Manual configuration imports must pass through the same validation and revision mechanism as TUI changes.

---

## 4. Global Configuration

Global configuration defines service-wide operational policies and defaults.

Proposed file:

`config/live/live.toml`

```toml
schema_version = 1

[runtime]
shutdown_timeout_seconds = 15

[publication]
enabled = true
transport = "unix_socket"
socket_path = "data/.live/marketforge-data.sock"
max_subscribers = 64
subscriber_queue_bytes = 16777216

[control]
socket_path = "data/.live/marketforge-control.sock"

[recording]
enabled = false
batch_max_records = 10000
batch_max_delay_ms = 100

[retention]
enabled = false
memory_limit_bytes = 536870912
spill_threshold_ratio = 0.70
spill_directory = "data/.live/retention"
max_disk_bytes = 107374182400

[metrics]
enabled = true
detailed_timing = false
snapshot_interval_ms = 1000

[logging]
level = "info"

[defaults.recovery]
enabled = true
initial_backoff_ms = 500
max_backoff_seconds = 60
jitter = true
```

Values are proposed initial defaults and require validation against actual workload characteristics.

### 4.1 Global Configuration Ownership

| Setting            | Owner                  |
| ------------------ | ---------------------- |
| Runtime shutdown   | Runtime Supervisor     |
| Publication limits | Unix Publisher         |
| Recording batching | PostgreSQL Recorder    |
| Retention budgets  | Retention Manager      |
| Metrics collection | Metrics Registry       |
| Logging            | Logging Infrastructure |
| Recovery defaults  | Recovery Coordinator   |

Each subsystem receives only the configuration relevant to its responsibilities.

The global configuration object must not become a shared mutable structure accessed by every event processor.

### 4.2 Global Resource Budgets

Memory and disk limits must be interpreted as service-wide budgets unless explicitly declared otherwise.

For example:

```toml
[retention]
memory_limit_bytes = 536870912
```

This represents the total configured retention-memory budget, not an automatic allocation of 512 MiB to every stream.

Per-stream limits may constrain individual consumption but must not bypass global resource accounting.

### 4.3 Global Hot-Editable Settings

Initially supported:

| Setting                   | Application semantics                 |
| ------------------------- | ------------------------------------- |
| Logging level             | Immediate                             |
| Metrics enabled           | Immediate where supported             |
| Detailed timing           | Apply to subsequent measurements      |
| Metrics snapshot interval | Apply to subsequent collection cycles |
| Recording batch size      | Apply at the next batch boundary      |
| Recording flush interval  | Apply at the next batch boundary      |
| Maximum subscribers       | Apply to future subscriber admission  |

Changing a global setting must not restart exchange connections unless explicitly required by its reconfiguration policy.

Existing subscribers must not be disconnected merely because the configured maximum subscriber count is reduced below the current count.

---

## 5. Exchange and API-Family Configuration

Exchange profiles describe protocol behavior independently of individual instruments and active subscriptions.

Each profile represents a specific exchange/API family.

Examples:

```text
binance.spot
binance.linear
binance.inverse

bybit.spot
bybit.linear
bybit.inverse
```

Profiles may share transport connections, rate-limit pools, and protocol strategies when supported by the exchange.

### 5.1 ExchangeProfile

Conceptual Rust representation:

```rust
pub struct ExchangeProfile {
    pub id: ExchangeProfileId,

    pub exchange: ExchangeId,
    pub market: MarketKind,

    pub rest: RestProfile,
    pub websocket: WebSocketProfile,

    pub channels: HashMap<ChannelId, ChannelProfile>,
    pub capabilities: ExchangeCapabilities,
}
```

The configuration compiler must resolve references and validate the complete profile before activation.

### 5.2 REST Configuration

REST configuration must support:

- Base URLs.
- Endpoint paths and HTTP methods.
- Request parameters.
- Parameter-dependent request weights.
- Shared rate-limit pools.
- Response usage headers.
- Request timeouts.
- Retry and cooldown policies.
- Endpoint capabilities.

Example:

```toml
[rest]
base_url = "https://fapi.binance.com"
api_pool = "binance_linear"
request_timeout_seconds = 10

[rest.endpoints.depth]
path = "/fapi/v1/depth"
method = "GET"
params = { symbol = "{symbol}", limit = 1000 }
weight_policy = "binance_linear_depth"

[rest.endpoints.trades]
path = "/fapi/v1/historicalTrades"
method = "GET"
params = { symbol = "{symbol}", limit = 1000 }
```

The example is illustrative. Endpoint availability, authentication requirements, pagination, retention limits, and rate-limit weights must be verified against the selected exchange API version.

REST acquisition modes must be explicit:

```rust
pub enum RestAcquisitionMode {
    OnDemand,
    Periodic,
    Disabled,
}
```

Do not represent disabled or on-demand requests using arbitrarily large polling intervals.

### 5.3 Shared Rate-Limit Pools

Rate limits belong to shared API pools rather than individual REST clients.

```toml
[[rest.rate_limits]]
id = "request_weight_1m"
window_seconds = 60
max_weight = 1000
response_header = "x-mbx-used-weight-1m"
```

The values above illustrate configuration syntax rather than authoritative exchange limits.

Each endpoint declares its applicable rate-limit policies.

The request scheduler must coordinate all requests sharing the same quota, including recovery, snapshots, metadata requests, and periodic acquisition.

Rate-limit policies may include multiple simultaneous constraints.

### 5.4 WebSocket Configuration

WebSocket profiles describe:

- Connection routes.
- Connection lifetime.
- Subscription capacity.
- Connection admission limits.
- Heartbeat strategy.
- Subscription protocol.
- Message classification.
- Reconnection policies.
- Connection sharding constraints.

Example:

```toml
[ws.routes.public]
base_url = "wss://fstream.binance.com/public"

[ws.routes.market]
base_url = "wss://fstream.binance.com/market"

[ws.connection]
max_streams = 200
max_age_seconds = 86400
rotate_before_seconds = 300

[ws.heartbeat]
strategy = "websocket_control_frames"
respond_to_ping = true

[ws.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
unsubscribe_method = "UNSUBSCRIBE"
request_id_type = "unsigned_integer"
```

Routes and limits must be validated against the applicable exchange documentation before deployment.

The generic connection manager must not contain exchange-name branching for behavior already represented by supported strategies.

### 5.5 Heartbeat Strategies

Heartbeat behavior must be selected through reusable protocol strategies.

```rust
pub enum HeartbeatStrategy {
    WebSocketControlFrames,
    TextPingPong,
    JsonPingPong,
    ServerHeartbeat,
    None,
}
```

Each strategy may require additional typed parameters.

Examples:

```toml
[ws.heartbeat]
strategy = "json_ping_pong"
interval_seconds = 20
timeout_seconds = 10
```

The exact message templates and acknowledgement rules belong to the strategy configuration.

Heartbeat settings must distinguish transport-level control frames from application-level heartbeat messages.

### 5.6 Subscription Protocol

Subscription configuration must support:

- Subscribe and unsubscribe templates.
- Request identifier generation.
- Subscription acknowledgements.
- Subscription rejection detection.
- Topic construction.
- Multiple subscriptions per connection.
- Reconnection restoration.

Example:

```toml
[ws.subscription]
strategy = "json_rpc"

subscribe_method = "SUBSCRIBE"
unsubscribe_method = "UNSUBSCRIBE"

request_id_type = "unsigned_integer"
```

Exchange-specific subscription formats should be represented declaratively whenever possible.

Unsupported protocol behavior may require a specialized Rust strategy.

### 5.7 Channel Definitions

Channels describe available exchange feeds and their processing requirements.

```toml
[channels.depth]
route = "public"
topic = "{symbol_lower}@depth@100ms"

format = "binance.linear.depth.v1"

bootstrap = "rest_snapshot_with_buffer"
snapshot_endpoint = "depth"

[channels.trades]
route = "market"
topic = "{symbol_lower}@trade"

format = "binance.linear.trade.v1"

bootstrap = "none"
```

Channel identifiers must preserve native semantics.

Individual trades and aggregated trades must not be treated as equivalent when their exchange representations differ.

Channel definitions may reference:

- Raw format specifications.
- Normalization plans.
- Sequence strategies.
- Snapshot synchronization strategies.
- REST recovery capabilities.
- Supported subscription parameters.

### 5.8 Exchange Capabilities

The configuration compiler must expose a typed capability model.

```rust
pub struct ExchangeCapabilities {
    pub supported_channels: Vec<ChannelCapability>,

    pub supports_dynamic_subscriptions: bool,
    pub supports_rest_trade_recovery: bool,
    pub supports_rest_depth_snapshot: bool,
}
```

Capabilities may vary by market family and channel.

The final model should therefore support channel-specific capabilities rather than assume that every capability applies globally to an exchange.

The Stream Registry, Recovery Coordinator, and Ratatui configuration forms consume these capabilities.

Unsupported requests must be rejected before runtime activation.

---

## 6. Raw Message Formats and Normalization Specifications

Raw exchange message formats must remain separate from connection management.

The generic WebSocket transport delivers raw messages.

The protocol dispatcher identifies their purpose.

The decoder and normalizer interpret market-data payloads according to compiled format specifications.

### 6.1 Processing Boundary

```text
WebSocket Frame
       |
       v
Message Classification
       |
       +---- Heartbeat
       |
       +---- Subscription Acknowledgement
       |
       +---- Protocol Error
       |
       +---- Market Data
                 |
                 v
          Format Selection
                 |
                 v
          Field Extraction
                 |
                 v
          Normalization
                 |
                 v
          Canonical Validation
```

Transport-control messages must never enter canonical market-data processing.

### 6.2 LiveFormatSpec

Conceptual representation:

```rust
pub struct LiveFormatSpec {
    pub id: FormatId,
    pub version: u32,

    pub message_matcher: MessageMatcher,
    pub extraction: ExtractionSpec,
    pub normalization: NormalizationSpec,

    pub target_schema: CanonicalSchemaId,
}
```

Format specifications define:

- Message identification.
- Record location.
- Field selectors.
- Array/object layouts.
- Timestamp representation.
- Numeric conversions.
- Trade-side mappings.
- Quantity semantics.
- Native sequence extraction.
- Canonical output schema.

### 6.3 Declarative Extraction

Example:

```toml
schema_version = 1

id = "binance.linear.depth.v1"
target_schema = "l2_update"

[message]
event_type_path = "e"
event_type_value = "depthUpdate"

[fields]
event_timestamp = "T"
symbol = "s"

sequence_first = "U"
sequence_last = "u"
sequence_previous = "pu"

[fields.bids]
path = "b"
level_type = "array"
price_index = 0
quantity_index = 1

[fields.asks]
path = "a"
level_type = "array"
price_index = 0
quantity_index = 1

[normalization]
timestamp_unit = "milliseconds"
quantity_source = "instrument_specs"
level_update_type = "absolute"
zero_quantity = "delete"
```

The same generic extraction infrastructure must support object-based and array-based exchange payloads.

### 6.4 Compiled Extraction Plans

TOML specifications must not be interpreted repeatedly for every event.

```text
TOML Format
     |
     v
Schema Validation
     |
     v
Field Selector Compilation
     |
     v
Typed Extraction Plan
     |
     v
Reusable Runtime Processor
```

Compilation should resolve:

- Field paths.
- Expected JSON types.
- Required and optional fields.
- Numeric transformations.
- Timestamp units.
- Enum mappings.
- Instrument-dependent normalization rules.

The runtime should use the compiled plan without repeatedly resolving configuration strings.

### 6.5 Normalization Reuse

Live normalization must reuse existing MarketForge primitives:

```text
normalize/decimal.rs
normalize/timestamp.rs
normalize/quantity.rs
normalize/side.rs
```

Existing canonical schemas and instrument specifications remain authoritative.

Live-specific format handling may differ from historical archive decoding, but equivalent source values must produce equivalent canonical results.

### 6.6 Specialized Protocol Strategies

Not all exchange behavior can be represented safely through declarative field mappings.

Specialized Rust strategies are permitted for:

- Snapshot/update synchronization.
- Exchange-specific sequence continuity.
- Checksum algorithms.
- Nonstandard subscription protocols.
- Exceptional heartbeat behavior.
- Specialized REST pagination or recovery.

Strategies must be registered by capability or protocol identifier rather than scattered exchange-name conditionals.

---

## 7. Source-Stream Configuration

Source-stream configuration defines which exchange feeds MarketForge Live should acquire and which operational policies apply to each logical stream.

Source configuration must remain independent of physical WebSocket connection assignment.

### 7.1 SourceStreamConfig

Conceptual Rust representation:

```rust
pub struct SourceStreamConfig {
    pub id: StreamId,
    pub key: SourceStreamKey,

    pub enabled: bool,
    pub recording: RecordingOverride,
    pub recovery: RecoveryOverride,
    pub metrics: MetricsOverride,
}
```

The configuration compiler resolves this representation into an immutable `EffectiveStreamConfig`.

The source identity must not include recording, metrics, or recovery settings.

### 7.2 Source Definitions

Proposed file:

`config/live/runtime/sources.toml`

```toml
schema_version = 1

[[sources]]
id = "binance_linear_btc_trades"

exchange = "binance"
market = "linear"
instrument = "BTCUSDT"
channel = "trades"

enabled = true
record = true

[[sources]]
id = "binance_linear_btc_depth"

exchange = "binance"
market = "linear"
instrument = "BTCUSDT"
channel = "depth"

enabled = true
record = true

[sources.parameters]
depth = 1000
interval_ms = 100

[[sources]]
id = "bybit_linear_eth_trades"

exchange = "bybit"
market = "linear"
instrument = "ETHUSDT"
channel = "trades"

enabled = true
record = false
```

Each source definition references an exchange profile and channel capability.

Instrument symbols must be resolved through the existing MarketForge instrument catalog.

### 7.3 Source Identity Validation

The compiler must reject:

- Unknown exchanges.
- Unsupported market families.
- Unknown instruments.
- Unsupported channels.
- Invalid channel parameters.
- Conflicting stream identifiers.
- Incompatible protocol capabilities.

Equivalent source specifications must resolve to the same canonical acquisition key.

The registry must not establish duplicate exchange subscriptions merely because two configuration entries reference equivalent source streams.

### 7.4 Source-Level Recording

Recording is independently configurable per source stream.

```toml
[[sources]]
id = "binance_btc_trades"

exchange = "binance"
market = "spot"
instrument = "BTCUSDT"
channel = "trades"

record = true
```

Recording requirements contribute to source demand ownership.

Disabling recording must not stop acquisition when subscribers or derived streams still require the source.

Enabling recording must define the event boundary from which persistence begins.

### 7.5 Source-Level Recovery

Recovery policies may override applicable global defaults.

```toml
[[sources]]
id = "binance_btc_trades"

exchange = "binance"
market = "spot"
instrument = "BTCUSDT"
channel = "trades"

[sources.recovery]
enabled = true
trade_overlap_seconds = 30
```

Recovery settings must be capability-aware.

For example:

- Trade recovery settings apply only to supported trade channels.
- L2 snapshot synchronization follows the exchange protocol strategy.
- Unsupported REST recovery capabilities cannot be enabled through configuration.
- Exchange-required integrity checks cannot be disabled through ordinary stream overrides.

### 7.6 Source-Level Metrics

Per-stream metrics are collected independently.

Optional overrides may enable additional diagnostics for selected streams.

```toml
[sources.metrics]
detailed_timing = true
```

Disabling detailed timing must not disable integrity counters or sequence validation.

### 7.7 Source Lifecycle Configuration

Source definitions describe persistent acquisition requirements.

Runtime subscriber demand is managed separately by the Stream Registry.

```text
Persistent Source Configuration
              |
              v
         Stream Registry
              |
      +-------+-------+
      |               |
      v               v
Existing Source    New Source
      |               |
      v               v
Attach Demand     Start Acquisition
```

Removing a persistent source definition releases its corresponding demand.

The physical acquisition stops only when no remaining demand requires it.

---

## 8. Derived-Stream Configuration

Derived streams consume existing source or derived streams and produce new logical event sequences.

Initially supported transformations:

- Multi-source merging.
- Adaptive event-time ordering.
- Currency conversion.

Derived-stream configuration must not contain exchange connection details.

### 8.1 DerivedStreamConfig

```rust
pub struct DerivedStreamConfig {
    pub id: DerivedStreamId,
    pub enabled: bool,
    pub transformation: DerivedTransformation,
    pub recording: RecordingOverride,
}
```

```rust
pub enum DerivedTransformation {
    Merge(MergeConfig),
    Conversion(ConversionConfig),
}
```

Derived streams acquire their dependencies through the registry.

### 8.2 Combined Streams

Proposed file:

`config/live/runtime/derived.toml`

```toml
schema_version = 1

[[derived]]
id = "btc_cross_exchange_trades"

type = "merge"
enabled = true
record = false

sources = [
    "binance_spot_btc_trades",
    "bybit_linear_btc_trades",
    "okx_linear_btc_trades",
]

[derived.merge]
ordering = "adaptive_event_time"

initial_window_ms = 10
max_window_ms = 500

late_event_policy = "emit_late"
source_failure_policy = "continue_degraded"
```

The merger consumes existing canonical streams.

It must not create independent exchange subscriptions when equivalent source acquisitions already exist.

### 8.3 Merge Ordering Policies

Supported initial strategies:

```rust
pub enum MergeOrdering {
    Arrival,
    AdaptiveEventTime,
}
```

**Arrival ordering:** Publish according to the order events are admitted by the merger.

**Adaptive event-time ordering:** Use bounded buffering and timestamp ordering, increasing the reorder window when disorder is observed.

Adaptive ordering configuration includes:

- Initial buffering window.
- Maximum buffering window.
- Late-event handling.
- Source failure behavior.
- Buffer resource limits.

Strict chronological ordering cannot be guaranteed when source lateness is unbounded.

### 8.4 Currency Conversion

Example:

```toml
[[derived]]
id = "binance_ethbtc_usdt"

type = "conversion"
enabled = true
record = false

source = "binance_spot_ethbtc_trades"

[derived.conversion]
target_currency = "USDT"

reference_policy = "event_time"
max_reference_age_ms = 5000

missing_reference_policy = "publish_unavailable"
path_resolution = "automatic"
```

Conversion paths must be resolved using existing MarketForge instrument and currency metadata.

Reference streams must reuse existing source acquisitions.

The conversion processor maintains bounded reference-price state.

### 8.5 Conversion Policies

Initially supported reference policies:

```rust
pub enum ReferencePolicy {
    EventTime,
    LatestObserved,
}
```

**EventTime:** Select the latest eligible reference whose timestamp does not exceed the target event timestamp.

**LatestObserved:** Use the most recently observed reference available at processing time.

Reference freshness must be enforced according to the selected policy.

Missing or stale references must not silently produce fabricated converted values.

### 8.6 Derived Dependencies

Derived-stream dependencies form a directed acyclic graph.

```text
BTCUSDT Trades ------+
                     |
ETHBTC Trades -------+----> Currency Conversion
                               |
                               v
                         Converted ETH Trades
                               |
                               v
                         Combined Stream
                               ^
                               |
Other Source Streams ----------+
```

The configuration compiler must:

- Resolve all dependencies.
- Reject missing references.
- Reject dependency cycles.
- Validate input/output schema compatibility.
- Determine required source acquisitions.
- Validate transformation capabilities.
- Compute the effective dependency graph.

### 8.7 Derived-Stream Reconfiguration

Supported runtime changes include:

- Creating derived streams.
- Removing derived streams.
- Enabling/disabling persistent derived-stream requirements.
- Changing supported merge parameters.
- Changing conversion-reference policies.
- Enabling/disabling recording.

Changes affecting output semantics must establish explicit derived-stream generation boundaries.

Underlying source streams remain unaffected unless dependency requirements change.

---

## 9. Configuration Inheritance

Configuration inheritance must be explicit, shallow, and predictable.

Not every field is inheritable.

### 9.1 Resolution Hierarchy

```text
Global Defaults
       |
       v
Exchange/API Constraints
       |
       v
Resource Overrides
       |
       v
Effective Configuration
```

Exchange constraints determine which settings are valid.

Resource overrides customize permitted operational behavior.

### 9.2 Override Representation

Overrides must distinguish an absent value from an explicitly configured value.

Conceptually:

```rust
pub enum ConfigValue<T> {
    Inherit,
    Explicit(T),
}
```

An absent TOML field generally represents inheritance when inheritance is supported.

Explicit overrides remain independent of future default changes.

### 9.3 Effective Configuration

Example:

```toml
[defaults.recovery]
enabled = true
trade_overlap_seconds = 30
```

Stream override:

```toml
[sources.recovery]
trade_overlap_seconds = 60
```

Effective configuration:

```text
Recovery enabled:       true   (global default)
Trade overlap:          60s    (stream override)
```

The runtime consumes only resolved effective values.

The configuration manager retains origin metadata for diagnostics and the TUI.

### 9.4 Default Changes

Changing a global default affects only resources inheriting that setting.

Example:

```text
Global recovery overlap:
30s -> 45s

Affected streams:
38 inherited

Unaffected streams:
4 explicit overrides
```

The configuration planner should report affected resources before applying changes.

### 9.5 Reset to Default

The TUI must support removing explicit overrides.

Resetting an override means restoring inheritance, not copying the current global value into the resource configuration.

### 9.6 Non-Inheritable Settings

The following must not be inherited ambiguously:

- Source identity.
- Exchange protocol identity.
- Instrument identity.
- Channel semantics.
- Derived-stream dependencies.
- Canonical schema identity.
- Native sequence strategy.
- Exchange-required synchronization rules.

These values must be explicitly resolved from authoritative specifications.

---
## 10. Configuration Compilation

Configuration compilation converts human-readable TOML specifications and desired runtime state into immutable, validated execution plans.

Runtime workers must never interpret TOML, resolve instrument identities, or compile JSON selectors while processing market-data events.

### 10.1 Compilation Pipeline

```text
Configuration Files
        |
        v
TOML Parsing
        |
        v
Structural Validation
        |
        v
Reference Resolution
        |
        +---- Exchange Profiles
        +---- Instrument Catalog
        +---- Format Specifications
        +---- Source Stream Definitions
        +---- Derived Dependencies
        |
        v
Semantic Validation
        |
        v
Inheritance Resolution
        |
        v
Protocol / Format Compilation
        |
        v
Immutable LivePlan
        |
        v
Runtime Reconciliation
```

Compilation must complete before the affected runtime resources are activated.

### 10.2 Configuration Compiler

Conceptual interface:

```rust
pub struct LiveConfigCompiler {
    exchange_profiles: ExchangeProfileRegistry,
    format_registry: FormatRegistry,
    instrument_catalog: InstrumentCatalog,
}
```

```rust
impl LiveConfigCompiler {
    pub fn compile(
        &self,
        desired: &DesiredLiveConfig,
    ) -> Result<CompiledLivePlan, ConfigError>;
}
```

The compiler must not establish network connections, create database transactions, or modify active runtime state.

It is a deterministic transformation from validated inputs to a resolved plan.

### 10.3 CompiledLivePlan

```rust
pub struct CompiledLivePlan {
    pub revision: ConfigRevisionId,

    pub global: EffectiveGlobalConfig,

    pub connections: Vec<ConnectionPoolPlan>,
    pub sources: Vec<CompiledSourcePlan>,
    pub derived: Vec<CompiledDerivedPlan>,
}
```

The compiled plan contains resolved configuration sufficient for runtime construction.

It must not contain unresolved TOML references or raw configuration expressions requiring repeated interpretation.

### 10.4 CompiledSourcePlan

```rust
pub struct CompiledSourcePlan {
    pub stream_id: StreamId,
    pub key: SourceStreamKey,

    pub protocol: CompiledProtocol,
    pub normalization: CompiledNormalizationPlan,

    pub recovery: EffectiveRecoveryPolicy,
    pub recording: EffectiveRecordingPolicy,
    pub metrics: EffectiveMetricsPolicy,
}
```

Source plans must include all information required for runtime processing without database lookups or repeated configuration resolution.

### 10.5 CompiledDerivedPlan

```rust
pub struct CompiledDerivedPlan {
    pub id: DerivedStreamId,

    pub dependencies: Vec<StreamId>,
    pub transformation: CompiledTransformation,

    pub recording: EffectiveRecordingPolicy,
}
```

Derived plans must reference resolved stream identities and validated transformation specifications.

Dependency resolution must produce an acyclic graph.

### 10.6 Compiled Plan Immutability

Compiled plans are immutable after creation.

Runtime components receive immutable plan references or extracted subsystem-specific configurations.

Configuration changes produce a new compiled plan.

The active configuration must never be modified partially through direct mutation of shared configuration objects.

### 10.7 Compilation Performance

Configuration compilation is outside the high-frequency processing path.

Compilation may perform relatively expensive validation and preprocessing when required.

Runtime optimization priorities include:

- Precompiled JSON field selectors.
- Resolved instrument metadata.
- Precomputed quantity conversions.
- Validated channel parameters.
- Resolved protocol strategies.
- Preconstructed subscription templates.
- Precomputed dependency relationships.

The compiler should optimize repeated per-event work rather than minimize one-time compilation cost.

---

## 11. Configuration Validation

Validation must distinguish structural correctness, semantic correctness, capability compatibility, and runtime applicability.

### 11.1 Validation Layers

| Layer | Responsibility |
|---|---|
| Structural | Required fields, types, ranges, TOML schema |
| Referential | Exchange profiles, instruments, formats, dependencies |
| Semantic | Internally consistent acquisition and transformation definitions |
| Capability | Exchange/channel support and required recovery strategies |
| Resource | Memory, connection, subscription, and retention limits |
| Runtime | Whether a proposed change is supported without restart |

A configuration may be structurally valid but semantically invalid.

For example:

```toml
channel = "depth"

[recovery]
trade_overlap_seconds = 30
```

A trade-specific recovery parameter is not applicable to an L2-only stream.

The compiler must reject or explicitly ignore unsupported fields according to the schema contract; silent acceptance of ineffective settings should be avoided.

### 11.2 ConfigError

Conceptual representation:

```rust
pub struct ConfigError {
    pub path: ConfigPath,
    pub category: ConfigErrorCategory,
    pub message: String,
}
```

```rust
pub enum ConfigErrorCategory {
    Parse,
    MissingField,
    InvalidValue,
    UnknownReference,
    UnsupportedCapability,
    DependencyCycle,
    ResourceLimit,
    IncompatibleOverride,
    UnsupportedRuntimeChange,
    RevisionConflict,
}
```

Validation errors should identify the relevant configuration path.

Example:

```text
sources[3].recovery.trade_overlap_seconds

Error:
Trade recovery is not supported for this source channel.
```

This allows the Ratatui interface to highlight the affected field.

### 11.3 Cross-Resource Validation

Validation must evaluate the complete desired configuration rather than only individual resource entries.

Required checks include:

- Duplicate stream identifiers.
- Conflicting source definitions.
- Missing derived dependencies.
- Cyclic derived dependencies.
- Incompatible derived input schemas.
- Unsupported source-channel combinations.
- Resource-budget violations.
- Inconsistent recording and retention requirements.
- Unsupported runtime reconfiguration operations.

### 11.4 Validation Side Effects

Validation and compilation must not mutate runtime state.

Invalid proposals must leave:

- Active connections unchanged.
- Active source streams unchanged.
- Derived streams unchanged.
- Recording state unchanged.
- Persisted accepted configuration unchanged.

Validation must be deterministic for identical configuration inputs and identical referenced metadata/specification versions.

---

## 12. Configuration Revisions

Runtime configuration is managed through immutable revisions.

Revisions identify accepted desired-state configurations, not individual runtime generations.

### 12.1 ConfigRevision

```rust
pub struct ConfigRevision {
    pub id: ConfigRevisionId,

    pub parent: Option<ConfigRevisionId>,

    pub created_at_ns: i64,

    pub desired: DesiredLiveConfig,
}
```

Additional metadata may record the source of the change and a human-readable description.

Revision identifiers must remain stable across process restarts.

### 12.2 Revision-Based Updates

Configuration updates must use optimistic concurrency control.

```rust
pub struct ConfigUpdate {
    pub expected_revision: ConfigRevisionId,
    pub changes: Vec<ConfigChange>,
}
```

The configuration manager accepts the update only if the expected revision matches the current revision.

Example:

```text
Current revision: 42

Client A:
    expected = 42
    change = recording batch size

Client B:
    expected = 42
    change = add source stream
```

After one update is accepted, the other must detect a revision conflict and revalidate against the new state.

### 12.3 ConfigChange

Conceptual representation:

```rust
pub enum ConfigChange {
    UpdateGlobal(GlobalConfigPatch),

    AddSource(SourceStreamConfig),
    UpdateSource(StreamId, SourceConfigPatch),
    RemoveSource(StreamId),

    AddDerived(DerivedStreamConfig),
    UpdateDerived(DerivedStreamId, DerivedConfigPatch),
    RemoveDerived(DerivedStreamId),
}
```

Patches must preserve explicit inheritance semantics.

Removing an override must be distinguishable from setting a value to its current default.

### 12.4 Revision History

The configuration manager should maintain bounded revision history or durable revision metadata.

Useful information includes:

```text
Revision
Timestamp
Change category
Affected resources
Previous values
New values
Application outcome
```

Configuration history is operational metadata and must remain separate from canonical market-data history.

### 12.5 Revision Persistence

Accepted configuration revisions must be persisted atomically.

Conceptual filesystem procedure:

```text
Serialize Candidate
        |
        v
Write Temporary File
        |
        v
Flush / Synchronize
        |
        v
Atomic Rename
        |
        v
Synchronize Directory
        |
        v
Revision Published
```

Atomic replacement prevents readers from observing partially written configuration files.

Crash-durable persistence requires appropriate filesystem synchronization, not merely successful serialization.

The exact on-disk revision layout belongs to the implementation design.

---

## 13. Runtime Reconciliation

Runtime reconciliation converts differences between desired and actual state into targeted resource operations.

The reconciler must not blindly restart the entire service whenever configuration changes.

### 13.1 Desired vs. Actual State

```text
Desired State
    |
    | Configuration revision 43
    |
    v
Runtime Reconciler
    ^
    |
    | Connection and stream snapshots
    |
Actual State
```

Example:

```text
Desired:
    Binance BTCUSDT Trades = Enabled

Actual:
    Binance BTCUSDT Trades = Recovering
```

The configuration is valid even though the stream has not yet reached its desired operational state.

### 13.2 Reconciliation Plan

```rust
pub struct ReconciliationPlan {
    pub revision: ConfigRevisionId,
    pub operations: Vec<ReconciliationOperation>,
}
```

```rust
pub enum ReconciliationOperation {
    StartSource(StreamId),
    StopSource(StreamId),

    UpdateSourcePolicy(StreamId),

    StartDerived(DerivedStreamId),
    StopDerived(DerivedStreamId),
    ReplaceDerived(DerivedStreamId),

    UpdateRecordingPolicy,
    UpdateMetricsPolicy,
    UpdatePublicationLimits,
}
```

The operation set should be extended only when required by supported runtime transitions.

### 13.3 Reconciliation Rules

The reconciler must:

- Compare compiled desired plans against applied configuration.
- Identify semantic differences rather than textual TOML differences.
- Avoid unnecessary stream restarts.
- Preserve unaffected connection and stream generations.
- Apply changes in dependency-safe order.
- Report partial convergence.
- Retry recoverable application failures.
- Distinguish configuration errors from temporary runtime failures.

### 13.4 Reconciliation Ordering

Dependency-aware ordering is required.

For source activation:

```text
Validate Source
      |
      v
Acquire Source Demand
      |
      v
Establish Connection / Subscription
      |
      v
Reach Required Operational State
```

For derived activation:

```text
Validate Dependencies
        |
        v
Acquire Dependency Handles
        |
        v
Initialize Derived Processor
        |
        v
Activate Derived Publication
```

For derived removal:

```text
Stop Derived Publication
        |
        v
Release Derived Dependencies
        |
        v
Remove Derived Runtime State
```

Removal must not terminate source streams still required by other demand owners.

### 13.5 Runtime Application Outcomes

```rust
pub enum ReconciliationState {
    Pending,
    Applying,
    Converged,
    Degraded,
    Failed,
}
```

A revision may be accepted and persisted while its runtime application remains degraded because an exchange is unavailable.

The reconciler must continue attempting permitted recovery without losing the accepted desired state.

### 13.6 Partial Application

Runtime reconfiguration is not automatically transactional across independent exchange connections, storage writers, and derived processors.

A reconciliation plan may partially succeed.

Therefore, the system must track:

- Accepted desired revision.
- Applied configuration per resource.
- Pending operations.
- Failed operations.
- Recovery/retry status.
- Actual runtime state.

The configuration manager must not report complete convergence until every required transition has reached its declared completion condition.

### 13.7 Restart Recovery

At process startup:

```text
Load Persisted Desired State
          |
          v
Validate Static Specifications
          |
          v
Compile Effective LivePlan
          |
          v
Construct Runtime
          |
          v
Reconcile Resources
          |
          v
Resume Supervision
```

Persistent acquisition and derived-stream requirements must be restored.

Transient subscriber demand must not be restored automatically.

Previous connection and book generations must not be reused in ways that permit stale state to be treated as current.

### 13.8 Reconciliation Invariants

- Invalid configuration updates never reach reconciliation.
- Configuration acceptance and runtime convergence remain distinct.
- Unaffected streams retain their existing runtime resources.
- Resource transitions are idempotent where possible.
- Repeated reconciliation must not duplicate subscriptions.
- Dependencies are activated before dependent derived processors.
- Removed dependencies are released only when no longer required.
- Failed transitions remain observable.
- Runtime recovery must not require Ratatui to remain connected.

---

## 14. Runtime Reconfiguration Policies

Every configurable field must declare its runtime reconfiguration policy.

The configuration compiler and reconciler must enforce these policies consistently, regardless of whether changes originate from Ratatui, another control client, or configuration import.

### 14.1 Reconfiguration Categories

```rust
pub enum ReconfigurationPolicy {
    Hot,
    StreamTransition,
    ConnectionReplacement,
    ServiceRestart,
    Immutable,
}
```

| Category | Semantics | Initial support |
|---|---|---|
| `Hot` | Update an existing component without restarting it | Yes |
| `StreamTransition` | Reconfigure, create, replace, or remove an affected stream | Yes |
| `ConnectionReplacement` | Replace affected physical connections | Deferred |
| `ServiceRestart` | Requires restarting the daemon | Configuration files only |
| `Immutable` | Cannot change within the current deployment/schema | No runtime changes |

Policies apply to individual configuration fields, not entire configuration files.

### 14.2 Hot Reconfiguration

Initially supported:

| Setting | Scope | Application boundary |
|---|---|---|
| Logging level | Global | Immediate |
| Metrics enabled | Global | Subsequent collection |
| Detailed latency metrics | Global / supported stream override | Subsequent measurements |
| Metrics snapshot interval | Global | Next collection cycle |
| Recording batch size | Recording writer | Next batch |
| Recording flush interval | Recording writer | Next batch |
| Maximum subscribers | Global publisher | Future admission |

Hot configuration changes must not require exchange reconnection.

Changes to batching parameters must not discard or reorder records already pending in the recorder.

Changes to subscriber limits must not automatically terminate existing subscribers unless an explicit policy permits it.

### 14.3 Stream-Level Reconfiguration

Initially supported:

- Add or remove persistent source-stream requirements.
- Enable or disable recording for a stream.
- Update supported source recovery policies.
- Update supported per-stream diagnostic settings.
- Create or remove combined streams.
- Modify supported derived-stream ordering parameters.
- Create or remove currency-conversion streams.
- Modify supported currency-reference policies.

Stream-level changes must be reconciled through the relevant registry.

Removing persistent acquisition demand must not terminate a stream still required by subscribers, recording, retention, or derived dependencies.

### 14.4 Semantic Configuration Changes

Changes affecting canonical output or derived transformation semantics require explicit lifecycle boundaries.

Examples:

```text
Merge ordering policy changed
    -> New derived generation

Conversion reference policy changed
    -> New derived generation

Source channel depth changed
    -> New source acquisition identity

Source recording enabled
    -> Recording begins at an explicit cursor boundary
```

A new derived generation does not imply that underlying source streams must reconnect.

### 14.5 Deferred Runtime Changes

Initially excluded from hot reconfiguration:

- WebSocket endpoint changes.
- REST endpoint changes.
- Heartbeat protocol replacement.
- Subscription protocol replacement.
- Native message-format changes.
- Normalization-rule changes.
- Canonical schema changes.
- Exchange sequencing-strategy replacement.
- Structural changes to connection-pool topology.

These changes may be supported through validated configuration files followed by controlled service restart.

The architecture should permit future targeted connection replacement without requiring redesign of the configuration system.

---

## 15. Control API Integration

The control API is the exclusive runtime mutation interface for external management clients.

Ratatui must not directly modify TOML files or access internal runtime configuration objects.

### 15.1 Configuration Operations

Initial operations:

| Operation | Responsibility |
|---|---|
| `config.get` | Retrieve current desired configuration |
| `config.effective` | Retrieve resolved effective values |
| `config.validate` | Validate proposed changes |
| `config.diff` | Calculate affected resources |
| `config.apply` | Submit a revision-checked configuration update |
| `config.status` | Inspect runtime convergence |
| `config.history` | Retrieve accepted revision metadata |

The final protocol names and serialization format belong to the Control API specification.

### 15.2 Configuration Update Workflow

```text
Ratatui
   |
   v
Fetch Current Revision
   |
   v
Edit Draft Configuration
   |
   v
Validate Draft
   |
   v
Inspect Effective Diff
   |
   v
Submit Expected Revision
   |
   v
Configuration Manager
   |
   v
Persist Accepted Desired State
   |
   v
Runtime Reconciler
   |
   v
Return Operation Identifier
   |
   v
Ratatui Tracks Convergence
```

The client must distinguish:

- Validation success.
- Revision acceptance.
- Persistence success.
- Runtime application progress.
- Runtime convergence.

### 15.3 Draft Configuration

Drafts are client-side proposed changes.

They must not affect running components until submitted through the control API.

Drafts should support:

- Editing multiple settings together.
- Resetting fields to inherited defaults.
- Comparing against current effective configuration.
- Canceling without side effects.
- Detecting concurrent revision changes.

The server remains the authoritative validator.

### 15.4 Impact Reporting

Before application, the control API should report:

```text
Affected source streams
Affected derived streams
Connection changes required
Stream generations affected
Recording changes
Resource-budget changes
Restart requirements
```

The reconciler must calculate impact from effective semantic changes rather than textual differences between TOML documents.

### 15.5 Control API Safety

- Mutating requests require revision validation.
- Unsupported runtime changes are rejected.
- Control requests must be bounded.
- Expensive validation must not block market-data processing.
- Disconnected clients must not cancel accepted configuration revisions.
- Retrying requests must not duplicate stream acquisitions.
- Unix socket permissions must restrict unauthorized modifications.
- Runtime changes must remain possible without the TUI.

---

## 16. Configuration Persistence and Recovery

Configuration persistence must survive ordinary daemon restarts and support recovery from interrupted updates.

### 16.1 Persistent State

Conceptual layout:

```text
data/.live/
├── config/
│   ├── desired.toml
│   ├── revision.json
│   └── history/
│       ├── revision-000041.json
│       ├── revision-000042.json
│       └── revision-000043.json
│
└── state/
    └── reconciliation.json
```

The exact physical layout remains an implementation decision.

A single transactionally published configuration snapshot may replace separate desired-state and revision files.

The essential requirement is that configuration contents and revision identity cannot become inconsistent after a crash.

### 16.2 Atomic Persistence

Configuration persistence must provide:

- Complete-file publication.
- Revision consistency.
- Crash recovery.
- Detection of corrupted or incomplete files.
- Explicit reporting of persistence failures.

A valid accepted revision must not be silently replaced by an unvalidated candidate.

### 16.3 Startup Recovery

```text
Daemon Startup
      |
      v
Load Last Committed Configuration
      |
      v
Verify Schema and Revision
      |
      v
Load Exchange Specifications
      |
      v
Resolve Instrument Metadata
      |
      v
Compile Effective LivePlan
      |
      v
Initialize Runtime
      |
      v
Reconcile Desired Resources
```

If the persisted desired configuration is invalid under the installed static specifications, startup must fail safely or enter an explicitly defined degraded management mode.

It must not silently modify acquisition semantics to achieve startup.

### 16.4 Runtime Reconciliation Persistence

Persisted desired state and observed runtime state are independent.

The daemon must reconstruct runtime state from authoritative resources after restart.

Persisted reconciliation metadata may assist diagnostics but must not be treated as proof that an exchange connection or subscription remains active.

### 16.5 Rollback

Rollback creates a new configuration revision based on a previous validated desired state.

```text
Revision 41
    |
    v
Revision 42
    |
    v
Revision 43
    |
    v
Rollback to contents of 41
    |
    v
Revision 44
```

Rollback must not reuse previous revision identifiers.

Rollback does not reverse canonical events, database commits, or retention operations already performed.

---

## 17. Rust Module Design

The configuration subsystem must remain independent of transport execution, stream processing, persistence implementations, and Ratatui.

### 17.1 Proposed Structure

```text
engine/src/live/config/
├── mod.rs
├── schema.rs
├── loader.rs
├── compiler.rs
├── validation.rs
├── effective.rs
├── inheritance.rs
├── revision.rs
├── diff.rs
├── store.rs
└── manager.rs
```

### 17.2 Module Responsibilities

| Module | Responsibility |
|---|---|
| `schema.rs` | Serializable configuration structures and supported schema versions |
| `loader.rs` | TOML loading, parsing, and static specification discovery |
| `compiler.rs` | Resolve configuration into immutable execution plans |
| `validation.rs` | Structural, semantic, capability, and resource validation |
| `effective.rs` | Resolved runtime configuration models |
| `inheritance.rs` | Default resolution and explicit overrides |
| `revision.rs` | Immutable configuration revisions and metadata |
| `diff.rs` | Semantic comparison and affected-resource identification |
| `store.rs` | Atomic configuration persistence and recovery |
| `manager.rs` | Configuration ownership, revision coordination, and public operations |

The runtime reconciler remains outside the configuration subsystem:

```text
engine/src/live/runtime/reconciler.rs
```

This preserves the distinction between configuration management and runtime execution.

### 17.3 ConfigurationManager

The configuration manager is the authoritative owner of desired state.

Conceptual structure:

```rust
pub struct ConfigurationManager {
    current: Arc<DesiredLiveConfig>,
    compiler: LiveConfigCompiler,
    store: ConfigStore,
    revision: ConfigRevisionId,
}
```

The implementation must protect concurrent updates without introducing configuration locks into the market-data processing path.

### 17.4 ConfigurationStore

Conceptual interface:

```rust
pub trait ConfigStore {
    async fn load(
        &self,
    ) -> Result<ConfigRevision, ConfigError>;

    async fn commit(
        &self,
        revision: &ConfigRevision,
    ) -> Result<(), ConfigError>;
}
```

The concrete implementation must guarantee consistent revision publication and crash recovery.

An in-memory implementation must be available for deterministic tests.

### 17.5 ConfigurationDiff

Semantic differences must be represented explicitly.

```rust
pub struct ConfigurationDiff {
    pub previous_revision: ConfigRevisionId,
    pub proposed_revision: ConfigRevisionId,

    pub changes: Vec<EffectiveConfigChange>,
    pub affected_resources: Vec<ResourceId>,
}
```

The diff engine must distinguish:

- Source acquisition identity changes.
- Source operational-policy changes.
- Derived transformation changes.
- Global hot-setting changes.
- Unsupported runtime changes.

Textual differences that do not change effective behavior must not trigger unnecessary reconciliation.

### 17.6 Configuration Dependencies

```text
ConfigurationManager
        |
        +---- ConfigStore
        |
        +---- LiveConfigCompiler
        |          |
        |          +---- Exchange Profiles
        |          +---- Format Registry
        |          +---- Instrument Catalog
        |
        +---- ConfigurationDiff
        |
        v
Runtime Reconciler
        |
        v
Subsystem Owners
```

The configuration subsystem must not directly establish WebSocket connections, write PostgreSQL records, or modify order-book state.

---

## 18. Testing and Validation Strategy

Configuration testing must be deterministic and independent of network connections, PostgreSQL, and terminal rendering.

Tests must cover parsing, compilation, inheritance, revisions, persistence, and runtime reconciliation.

### 18.1 Structural Validation Tests

Verify:

- Valid TOML parsing.
- Required fields.
- Optional fields and defaults.
- Unsupported schema versions.
- Unknown or misspelled fields.
- Invalid numeric ranges.
- Invalid enum values.
- Duplicate identifiers.
- Malformed protocol templates.

Where practical, configuration deserialization should reject unknown fields rather than silently ignore them.

### 18.2 Exchange Configuration Tests

Each supported exchange/API family must have fixtures covering:

- REST endpoint resolution.
- WebSocket route selection.
- Subscription template compilation.
- Subscription parameters.
- Heartbeat strategy selection.
- Rate-limit specifications.
- Channel capabilities.
- Raw-format references.
- Sequence/snapshot strategy resolution.

Every configured channel must resolve to an executable processing strategy.

### 18.3 Normalization Compilation Tests

Verify:

- JSON selector compilation.
- Required and optional field semantics.
- Timestamp-unit resolution.
- Numeric conversion rules.
- Quantity semantics.
- Native sequence extraction.
- Unsupported field transformations.
- Canonical schema compatibility.

Compiled plans must produce deterministic results for identical inputs.

### 18.4 Inheritance Tests

Verify:

- Global defaults are inherited correctly.
- Explicit source overrides take precedence.
- Removing an override restores inheritance.
- Global default changes affect only inheriting resources.
- Exchange constraints reject incompatible overrides.
- Non-inheritable fields remain explicit.
- Effective configuration origin metadata is correct.

### 18.5 Derived Configuration Tests

Verify:

- Valid merge dependencies.
- Missing dependencies.
- Dependency cycles.
- Duplicate references.
- Incompatible event schemas.
- Adaptive ordering parameter constraints.
- Currency-conversion path resolution.
- Missing reference instruments.
- Unsupported conversion targets.
- Semantic generation-change detection.

### 18.6 Configuration Revision Tests

Verify:

- Monotonic revision identifiers.
- Revision conflict detection.
- Immutable accepted revisions.
- Idempotent update handling.
- Rejected invalid proposals.
- Concurrent configuration submissions.
- Persistent revision recovery.
- Rollback as a new revision.

### 18.7 Reconciliation Tests

Use synthetic runtime state and mock subsystem controllers.

Example:

```text
Initial Desired State:
    Binance BTCUSDT Trades = Enabled

Proposed Change:
    Recording = Enabled

Expected Reconciliation:
    Update recording demand/policy

Forbidden Side Effects:
    Restart WebSocket connection
    Recreate source processor
    Reset stream cursor
```

Additional scenarios:

```text
Add source stream
    -> One new acquisition

Add equivalent source requirement
    -> Reuse existing acquisition

Remove persistent demand
    -> Preserve source if other demand exists

Change merge ordering
    -> Replace affected derived generation

Change logging level
    -> No stream transition

Change exchange endpoint
    -> Reject as unsupported hot update
```

### 18.8 Persistence Failure Tests

Inject failures during:

- Temporary-file creation.
- Serialization.
- File write.
- File synchronization.
- Atomic rename.
- Directory synchronization.
- Revision-history publication.
- Configuration reload.

The system must recover the last fully committed configuration or explicitly report an unrecoverable configuration-store error.

It must never silently combine fields from multiple partially committed revisions.

### 18.9 Historical Regression Tests

Changes to shared instrument metadata, normalization specifications, or canonical interpretation must preserve existing historical regression behavior.

Exchange-format compiler reuse must not alter historical processing semantics without explicit verification.

---

## 19. Configuration Invariants

The following invariants are mandatory.

### 19.1 Configuration Identity

- Source identity is independent of recording and monitoring settings.
- Equivalent source definitions resolve to equivalent acquisition keys.
- Derived-stream identity and transformation semantics remain explicit.
- Configuration revisions are independent of runtime generations.
- Exchange protocol profiles remain independent of active subscriptions.

### 19.2 Compilation

- All runtime configuration is structurally and semantically validated.
- Runtime workers never parse TOML on the event hot path.
- Raw field selectors are compiled before activation.
- Instrument metadata is resolved before source processing.
- Derived dependencies form an acyclic graph.
- Unsupported exchange/channel combinations are rejected.

### 19.3 Runtime Reconfiguration

- Invalid proposals do not mutate runtime state.
- Unaffected resources are not restarted.
- Configuration acceptance and runtime convergence remain distinct.
- Stream-level changes preserve unrelated source acquisitions.
- Semantic derived-stream changes establish explicit generation boundaries.
- Static protocol changes cannot bypass restart restrictions.
- Runtime changes are possible without an active TUI.

### 19.4 Persistence

- Accepted revisions are immutable.
- Configuration snapshots and revision identifiers remain consistent.
- Interrupted writes cannot expose partial configurations.
- Restart recovery uses authoritative persisted desired state.
- Transient subscriber demand is not restored as persistent configuration.
- Rollback creates a new revision.

### 19.5 Resource Safety

- Configuration updates cannot bypass global resource budgets.
- Per-stream overrides must remain within supported constraints.
- Configuration compilation must not block the market-data hot path.
- Reconciliation must not create duplicate physical acquisitions.
- Unsupported resource allocations must fail explicitly.

---

## 20. Open Implementation Decisions

| Area | Decision Required |
|---|---|
| File layout | Exact static/runtime TOML directory organization |
| Schema versioning | Compatibility and migration rules |
| TOML validation | Unknown-field rejection and diagnostic formatting |
| Compiled selectors | JSON extraction representation and optimization |
| Exchange strategies | Registry and typed strategy identifiers |
| Instrument catalog | Snapshot/version handling during compilation |
| Configuration storage | Snapshot and revision-history physical layout |
| Revision coordination | Atomic persistence and in-memory publication boundary |
| Reconciliation | Partial-application retry and convergence tracking |
| Resource validation | Global admission and reservation calculations |
| Derived configuration | Semantic identity and generation transitions |
| Runtime imports | Interaction between manual file edits and control API |

These decisions must be finalized before implementing their dependent components.

---

## 21. Implementation Checklist

### Configuration Models

- [ ] Define versioned TOML schemas.
- [ ] Define global configuration.
- [ ] Define exchange/API-family profiles.
- [ ] Define source-stream configuration.
- [ ] Define derived-stream configuration.
- [ ] Define explicit override types.
- [ ] Define effective configuration models.

### Configuration Compilation

- [ ] Implement static specification loading.
- [ ] Resolve exchange profiles.
- [ ] Resolve instrument metadata.
- [ ] Compile raw message selectors.
- [ ] Compile normalization plans.
- [ ] Validate channel capabilities.
- [ ] Resolve source acquisition identities.
- [ ] Validate derived dependency graphs.
- [ ] Produce immutable compiled plans.

### Runtime Configuration

- [ ] Implement configuration revisions.
- [ ] Implement semantic configuration diffs.
- [ ] Implement affected-resource calculations.
- [ ] Implement revision conflict detection.
- [ ] Implement atomic persistence.
- [ ] Implement startup recovery.
- [ ] Implement runtime reconciliation.
- [ ] Expose configuration commands and queries.

### Testing

- [ ] Structural and semantic validation tests.
- [ ] Exchange-profile compilation tests.
- [ ] Normalization-plan compilation tests.
- [ ] Inheritance and override tests.
- [ ] Derived dependency validation tests.
- [ ] Concurrent revision tests.
- [ ] Reconciliation idempotency tests.
- [ ] Persistence fault-injection tests.
- [ ] Historical regression verification.

---

## 22. Completion Criteria

The configuration subsystem is complete when:

1. All supported exchange/API-family specifications compile into validated Rust protocol plans.
2. Source and derived-stream definitions resolve to stable identities and executable configurations.
3. Runtime workers consume immutable effective configuration without repeated TOML parsing or dynamic selector compilation.
4. Global defaults, exchange constraints, and supported per-resource overrides resolve deterministically.
5. Invalid configurations are rejected before modifying runtime state.
6. Accepted desired-state revisions are atomically persisted and recoverable after restart.
7. Runtime reconciliation applies supported changes without unnecessarily restarting unaffected streams.
8. Equivalent acquisition requirements do not create duplicate exchange subscriptions.
9. Ratatui and external clients can query, validate, and modify supported settings through the same control API.
10. Configuration tests and existing historical regression tests pass.

**Final architectural rule:** Configuration declares and validates desired behavior; runtime subsystems execute immutable compiled plans. Configuration management must never become an implicit dependency of the high-frequency market-data processing path.

---

