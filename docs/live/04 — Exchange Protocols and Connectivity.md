## Exchange Protocol Configuration Registry

MarketForge Live supports five exchanges and three market families:

- Binance
- Bybit
- OKX
- Bitget
- Gate.io

Each exchange is divided into:

- Spot
- Linear
- Inverse

Each exchange-market combination has three TOML configuration files:

1. **Exchange profile** — connectivity, subscriptions, heartbeats, rate limits, channels, and recovery endpoints.
2. **Trade format** — extraction, normalization, native identities, timestamps, and integrity validation.
3. **L2 depth format** — extraction, quantity normalization, synchronization, sequencing, snapshots, and recovery.

The resulting configuration registry contains:

| Component | Count |
|---|---:|
| Exchanges | 5 |
| Market families per exchange | 3 |
| Exchange-market profiles | 15 |
| Connectivity configurations | 15 |
| Trade normalization configurations | 15 |
| L2 normalization configurations | 15 |
| **Total TOML files** | **45** |

All files use the proposed MarketForge Live configuration schema v1.

The configurations have been drafted but must pass schema compilation,
protocol-fixture verification, and integration testing before production use.

---

## Configuration Directory Structure

```text
data/.config/live/
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
└── formats/
    ├── trades/
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
    └── depth/
        ├── binance_spot.toml
        ├── binance_linear.toml
        ├── binance_inverse.toml
        ├── bybit_spot.toml
        ├── bybit_linear.toml
        ├── bybit_inverse.toml
        ├── okx_spot.toml
        ├── okx_linear.toml
        ├── okx_inverse.toml
        ├── bitget_spot.toml
        ├── bitget_linear.toml
        ├── bitget_inverse.toml
        ├── gateio_spot.toml
        ├── gateio_linear.toml
        └── gateio_inverse.toml
```

These files are maintained within MarketForge's private data configuration
directory, outside the end-user repository.

Python orchestration manages instrument metadata and configuration generation.

Rust Live loads and compiles the selected configurations independently
of the Python process.

---

# Binance

Binance provides separate market-data protocols for Spot, USD-margined
Linear Futures, and COIN-margined Inverse Futures.

The exchange profiles specify the corresponding WebSocket endpoints,
REST endpoints, subscription methods, connection limits, and recovery policies.

## Binance Spot

Protocol configuration:  
`data/.config/live/exchanges/binance_spot.toml`

Trade normalization:  
`data/.config/live/formats/trades/binance_spot.toml`

L2 normalization:  
`data/.config/live/formats/depth/binance_spot.toml`

**Protocol characteristics**

- Binance Spot public WebSocket streams.
- Individual trades from the configured trade channel.
- Base-asset trade quantities.
- Incremental L2 depth.
- REST order-book snapshot initialization.
- Native `U`/`u` update ranges.
- Snapshot bridging using `lastUpdateId`.
- Automatic book invalidation and resynchronization on sequence gaps.

## Binance Linear

Protocol configuration:  
`data/.config/live/exchanges/binance_linear.toml`

Trade normalization:  
`data/.config/live/formats/trades/binance_linear.toml`

L2 normalization:  
`data/.config/live/formats/depth/binance_linear.toml`

**Protocol characteristics**

- USD-margined Futures market data.
- Futures-specific WebSocket and REST endpoints.
- Individual trade normalization.
- Contract-aware instrument metadata.
- Incremental Futures depth.
- Native `U`/`u` sequence ranges.
- Previous-update continuity using `pu`, where applicable.
- REST snapshot initialization and recovery.

## Binance Inverse

Protocol configuration:  
`data/.config/live/exchanges/binance_inverse.toml`

Trade normalization:  
`data/.config/live/formats/trades/binance_inverse.toml`

L2 normalization:  
`data/.config/live/formats/depth/binance_inverse.toml`

**Protocol characteristics**

- COIN-margined Futures market data.
- Dedicated Inverse WebSocket and REST endpoints.
- Contract-denominated trade quantities.
- Instrument-specific contract-value normalization.
- Incremental L2 order books.
- Exchange-native sequencing.
- REST snapshot synchronization.
- Resynchronization following connection loss or sequence violations.

---

# Bybit

Bybit uses its V5 market-data protocol.

Spot, Linear, and Inverse streams share reusable message-processing
strategies but require category-specific subscriptions and instrument contexts.

## Bybit Spot

Protocol configuration:  
`data/.config/live/exchanges/bybit_spot.toml`

Trade normalization:  
`data/.config/live/formats/trades/bybit_spot.toml`

L2 normalization:  
`data/.config/live/formats/depth/bybit_spot.toml`

**Protocol characteristics**

- V5 public Spot WebSocket.
- `publicTrade` channel.
- `orderbook` channel.
- WebSocket-provided book snapshots.
- Incremental book deltas.
- Native update and cross-sequence metadata.
- Base-asset quantity normalization.
- Snapshot replacement on resets or reconnects.

## Bybit Linear

Protocol configuration:  
`data/.config/live/exchanges/bybit_linear.toml`

Trade normalization:  
`data/.config/live/formats/trades/bybit_linear.toml`

L2 normalization:  
`data/.config/live/formats/depth/bybit_linear.toml`

**Protocol characteristics**

- V5 Linear Futures WebSocket.
- `publicTrade` channel.
- `orderbook` channel.
- WebSocket snapshot initialization.
- Incremental L2 updates.
- Native update IDs and cross-sequence metadata.
- Contract-aware quantity normalization.
- Exchange-specific book reset handling.

## Bybit Inverse

Protocol configuration:  
`data/.config/live/exchanges/bybit_inverse.toml`

Trade normalization:  
`data/.config/live/formats/trades/bybit_inverse.toml`

L2 normalization:  
`data/.config/live/formats/depth/bybit_inverse.toml`

**Protocol characteristics**

- V5 Inverse Futures WebSocket.
- `publicTrade` channel.
- `orderbook` channel.
- WebSocket snapshot and delta processing.
- Native sequencing and book-generation tracking.
- Inverse instrument metadata.
- Contract-aware economic quantity conversion.
- Snapshot replacement following invalidation.

---

# OKX

OKX uses public WebSocket channels with instrument identifiers
and exchange-specific order-book sequencing.

The selected profile determines the appropriate Spot, Linear,
or Inverse instrument context.

## OKX Spot

Protocol configuration:  
`data/.config/live/exchanges/okx_spot.toml`

Trade normalization:  
`data/.config/live/formats/trades/okx_spot.toml`

L2 normalization:  
`data/.config/live/formats/depth/okx_spot.toml`

**Protocol characteristics**

- OKX public WebSocket.
- `trades` channel.
- Configured incremental order-book channel.
- WebSocket snapshot initialization.
- Native `seqId` and `prevSeqId`.
- Exchange-defined sequence reset behavior.
- Checksum validation where supported by the selected depth channel.
- Base-asset trade and depth quantities.

## OKX Linear

Protocol configuration:  
`data/.config/live/exchanges/okx_linear.toml`

Trade normalization:  
`data/.config/live/formats/trades/okx_linear.toml`

L2 normalization:  
`data/.config/live/formats/depth/okx_linear.toml`

**Protocol characteristics**

- Linear derivatives classified through instrument metadata.
- Public trade and incremental book channels.
- Contract-denominated quantities.
- Instrument-specific contract values.
- Native order-book sequencing.
- Snapshot and delta processing.
- Checksum validation where applicable.
- Recovery through exchange-supported synchronization mechanisms.

## OKX Inverse

Protocol configuration:  
`data/.config/live/exchanges/okx_inverse.toml`

Trade normalization:  
`data/.config/live/formats/trades/okx_inverse.toml`

L2 normalization:  
`data/.config/live/formats/depth/okx_inverse.toml`

**Protocol characteristics**

- Inverse derivatives classified through instrument metadata.
- Public trade and incremental depth channels.
- Contract-denominated quantities.
- Quote-valued or otherwise catalog-defined contract specifications.
- Native sequence validation.
- WebSocket snapshot initialization.
- Channel-specific checksum validation.
- Explicit recovery after book invalidation.

---

# Bitget

Bitget uses its Unified Trading Account V3 public market-data protocol.

Its Spot, Linear, and Inverse markets share the V3 public WebSocket endpoint,
with market classification expressed through the native `instType`.

## Bitget Spot

Protocol configuration:  
`data/.config/live/exchanges/bitget_spot.toml`

Trade normalization:  
`data/.config/live/formats/trades/bitget_spot.toml`

L2 normalization:  
`data/.config/live/formats/depth/bitget_spot.toml`

**Protocol characteristics**

- UTA V3 public WebSocket.
- Native instrument type `spot`.
- `publicTrade` channel.
- `books` channel.
- Base-asset public trade quantities.
- WebSocket snapshot and incremental updates.
- Native `seq`/`pseq` sequencing.
- No order-book checksum in the current V3 protocol.
- Snapshot replacement following synchronization failure.

## Bitget Linear

Protocol configuration:  
`data/.config/live/exchanges/bitget_linear.toml`

Trade normalization:  
`data/.config/live/formats/trades/bitget_linear.toml`

L2 normalization:  
`data/.config/live/formats/depth/bitget_linear.toml`

**Protocol characteristics**

- UTA V3 public WebSocket.
- Native `usdt-futures` and `usdc-futures` categories.
- Native category resolved through instrument metadata.
- `publicTrade` channel.
- `books` channel.
- Base-denominated public trade quantities.
- Native `seq`/`pseq` book sequencing.
- WebSocket-provided snapshots.
- No current V3 order-book checksum.
- Depth quantity semantics subject to fixture validation.

## Bitget Inverse

Protocol configuration:  
`data/.config/live/exchanges/bitget_inverse.toml`

Trade normalization:  
`data/.config/live/formats/trades/bitget_inverse.toml`

L2 normalization:  
`data/.config/live/formats/depth/bitget_inverse.toml`

**Protocol characteristics**

- UTA V3 public WebSocket.
- Native instrument type `coin-futures`.
- REST category `COIN-FUTURES`.
- `publicTrade` channel.
- `books` channel.
- Quote-denominated public trade quantities.
- Instrument-aware inverse normalization.
- Native `seq`/`pseq` synchronization.
- WebSocket snapshot initialization.
- No current V3 order-book checksum.

**Outstanding verification**

The raw quantity unit of the standard COIN-Futures `books` channel
has not been independently established.

The depth configuration intentionally declares:

`raw_unit = "requires_verification"`

Production activation must fail until this is resolved.

---

# Gate.io

Gate.io uses API V4.

Spot and Futures share the general channel/event/payload subscription
pattern but differ in endpoints, trade representations, depth-level layouts,
and quantity semantics.

## Gate.io Spot

Protocol configuration:  
`data/.config/live/exchanges/gateio_spot.toml`

Trade normalization:  
`data/.config/live/formats/trades/gateio_spot.toml`

L2 normalization:  
`data/.config/live/formats/depth/gateio_spot.toml`

**Protocol characteristics**

- Spot WebSocket V4.
- `spot.trades` channel.
- `spot.order_book_update` channel.
- Individual trade records.
- Exchange-wide and market-specific trade identifiers.
- High-precision trade timestamps.
- Base-asset quantities.
- Unified trading-pair metadata.
- Array-based order-book price levels.
- Native `U`/`u` sequence ranges.
- REST snapshot initialization with `with_id=true`.
- Support for documented WebSocket full-depth snapshots.

## Gate.io Linear

Protocol configuration:  
`data/.config/live/exchanges/gateio_linear.toml`

Trade normalization:  
`data/.config/live/formats/trades/gateio_linear.toml`

L2 normalization:  
`data/.config/live/formats/depth/gateio_linear.toml`

**Protocol characteristics**

- Settlement-specific Futures WebSocket endpoints.
- USDT and supported USD1 settlement routes.
- `futures.trades` channel.
- `futures.order_book_update` channel.
- Signed contract quantities in trades.
- Absolute contract quantities in L2.
- Object-based depth levels containing `p` and `s`.
- Decimal contract-size preservation.
- Native `U`/`u` update ranges.
- REST snapshot initialization using `with_id=true`.
- WebSocket full-snapshot support.

**Mandatory transport requirement**

The physical WebSocket handshake must include:

`X-Gate-Size-Decimal: 1`

This prevents loss of fractional contract-size precision.

**Outstanding verification**

The applicable Futures public REST rate-limit policy must be confirmed
before enabling automatic high-volume recovery.

## Gate.io Inverse

Protocol configuration:  
`data/.config/live/exchanges/gateio_inverse.toml`

Trade normalization:  
`data/.config/live/formats/trades/gateio_inverse.toml`

L2 normalization:  
`data/.config/live/formats/depth/gateio_inverse.toml`

**Protocol characteristics**

- BTC-settled Futures WebSocket.
- `futures.trades` channel.
- `futures.order_book_update` channel.
- Signed trade quantities.
- Absolute L2 contract quantities.
- Instrument-specific inverse contract normalization.
- Object-based price levels.
- Native `U`/`u` sequencing.
- REST snapshot initialization.
- WebSocket full-snapshot support.
- Generation-safe resynchronization.

**Mandatory transport requirement**

The physical WebSocket handshake must include:

`X-Gate-Size-Decimal: 1`

**Outstanding verification**

The applicable Futures public REST rate-limit policy remains unresolved.

---

# Shared Configuration Responsibilities

The 45 TOML files define exchange-specific behavior.

They must not introduce 45 unrelated implementations.

MarketForge Live compiles these files into reusable components.

## Exchange Profiles

Exchange profiles describe:

- WebSocket endpoints and settlement-specific routes.
- Authentication requirements.
- Connection lifecycle and heartbeat configuration.
- Subscription and unsubscription protocols.
- Routing and acknowledgement handling.
- Available market-data channels.
- Native exchange rate limits.
- REST snapshot and trade-history endpoints.
- Exchange-specific recovery procedures.

## Trade Formats

Trade formats describe:

- Message identification.
- Record extraction.
- Native field mappings.
- Decimal and timestamp parsing.
- Trade identity and deduplication.
- Aggressor-side normalization.
- Base, quote, and contract quantity semantics.
- Instrument metadata resolution.
- Integrity validation.
- Canonical trade output.

## Depth Formats

Depth formats describe:

- Message identification.
- Bid and ask extraction.
- Native price-level representation.
- Snapshot and delta classification.
- Quantity update semantics.
- Zero-quantity deletion.
- Native sequence validation.
- REST/WebSocket synchronization.
- Book generation management.
- Recovery and invalidation.
- Canonical L2 output.

---

# Shared Instrument Metadata

Instrument specifications are not hardcoded into exchange formats.

Python orchestration maintains the instrument catalog and provides
normalization-critical metadata to Rust Live.

Relevant fields include:

- `instrument_id`
- Exchange-native symbol
- Exchange and market classification
- Native settlement or product category
- Base and quote assets
- `quantity_type`
- `contract_value`
- `contract_value_asset`
- Tick and quantity constraints

The Rust configuration compiler resolves these values when a
source stream is initialized.

The compiled instrument context is reused on the hot path.

No PostgreSQL query is performed for every trade or L2 update.

---

# Shared Synchronization Strategies

Exchange-specific sequencing is selected through configuration.

The implementation should reuse generic validation primitives
where protocols have equivalent mathematical requirements.

| Protocol family | Synchronization approach |
|---|---|
| Binance Spot | REST snapshot + buffered update ranges |
| Binance Futures | REST snapshot + native update continuity |
| Bybit normal L2 | WebSocket snapshot + delta |
| OKX | WebSocket snapshot + sequence/checksum validation |
| Bitget V3 books | WebSocket snapshot + `seq`/`pseq` |
| Gate Spot | REST bridge + `U`/`u`, with full-snapshot support |
| Gate Futures | REST bridge + `U`/`u`, with full-snapshot support |

Each strategy must enforce:

1. A valid initialization baseline.
2. Native sequence continuity.
3. Atomic application of book updates.
4. Generation fencing.
5. Explicit invalidation on unrecoverable gaps.
6. Resynchronization before synchronized book publication.

REST recovery must not claim to reconstruct historical L2 events
that were permanently lost during a disconnection.

---

# Configuration Validation Requirements

Before a configuration becomes operational, MarketForge must validate:

1. TOML syntax.
2. Supported schema version.
3. Exchange and market identifiers.
4. Connectivity profile references.
5. Trade and depth format references.
6. Required message-field mappings.
7. Supported timestamp and decimal representations.
8. Instrument normalization requirements.
9. Snapshot and sequencing strategies.
10. Transport headers and protocol prerequisites.
11. Rate-limit policies.
12. Recovery capabilities.
13. Unsupported or unresolved configuration values.

A syntactically valid TOML file is not necessarily operationally valid.

The configuration compiler must reject missing normalization-critical
metadata and unresolved protocol requirements.

---

# Protocol Verification Status

The initial configuration-drafting phase is complete.

| Exchange | Spot | Linear | Inverse |
|---|---|---|---|
| Binance | Drafted | Drafted | Drafted |
| Bybit | Drafted | Drafted | Drafted |
| OKX | Drafted | Drafted | Drafted |
| Bitget | Drafted | Drafted | Drafted |
| Gate.io | Drafted | Drafted | Drafted |

**Total: 45 proposed TOML specifications.**

Remaining requirements include:

- Cross-file schema consistency checks.
- Official protocol verification.
- Captured WebSocket JSON fixtures.
- REST response fixtures.
- Decimal and timestamp precision tests.
- Trade identity and deduplication tests.
- L2 synchronization and fault-injection tests.
- Quantity-unit verification.
- Rate-limit policy verification.
- Configuration compiler integration tests.

These configurations are implementation specifications,
not evidence that exchange connectivity has already been tested.