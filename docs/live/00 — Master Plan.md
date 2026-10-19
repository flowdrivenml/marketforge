# MarketForge Live — Master Plan

## 1. Objective

Implement **MarketForge Live**, a high-performance, fault-tolerant Rust service for continuous cryptocurrency market-data acquisition, canonical normalization, integrity validation, distribution, and optional persistence.

The system initially supports:

- **Exchanges:** Binance, Bybit, OKX, Bitget, Gate.io.
- **Markets:** Spot, linear perpetuals, inverse perpetuals.
- **Data:** Trades and L2 order-book snapshots/updates.
- **Transport:** WebSocket acquisition, REST recovery, Unix domain socket publication.
- **Persistence:** Optional PostgreSQL recording and temporary Parquet-backed retention.
- **Management:** Independent Rust/Ratatui TUI and local control API.

The service operates independently of Python orchestration, historical jobs, and terminal interfaces.

### Scope Boundaries

| System | Responsibility |
|---|---|
| Historical Engine | Historical archives, normalization, merging, conversion, Parquet materialization |
| **MarketForge Live** | Continuous acquisition, validation, recovery, derived streams, publication, recording, retention |
| Bootstrap Engine (future) | Historical/live continuity, catch-up, bootstrap Parquet generation, dedicated TUI |
| Downstream Applications | Feature engineering, imbalance bars, ML, strategies, execution |

MarketForge Live must not implement feature generation, trading strategies, or historical bootstrap orchestration.

---

## 2. Architectural Principles

1. **Contract-first modularity.** Define domain models, interfaces, ownership, state machines, and failure semantics before implementations.
2. **Separation of concerns.** Ingestion, normalization, recovery, registry, publication, persistence, metrics, configuration, and UI remain independently testable.
3. **Data/control-plane isolation.** Management operations and observability must not interfere with high-frequency event processing.
4. **Declarative exchange integration.** Reusable Rust components consume compiled TOML protocol specifications and instrument metadata; exchange-specific strategies handle irreducible protocol differences.
5. **Single-source acquisition.** Semantically identical source requests share one exchange subscription.
6. **Explicit integrity guarantees.** Detect, classify, recover, and report discontinuities; never silently treat invalid market state as valid.
7. **Explicit delivery semantics.** Every queue, subscriber, writer, and retention component defines backpressure, failure, and durability behavior.
8. **Historical/live consistency.** Reuse existing canonical schemas, normalization primitives, `BookStore`, validation, and Arrow/Parquet infrastructure.
9. **Versioned runtime configuration.** Separate desired state from actual state; validate and reconcile changes without unnecessary restarts.
10. **Observability by design.** Per-stream metrics, health states, integrity counters, and recovery diagnostics exist independently of Ratatui.
11. **Deterministic verification.** Preserve historical tests; add simulated transports, fault injection, state-machine tests, and reproducible end-to-end tests.
12. **Minimal core infrastructure, optional observability integrations.**
    MarketForge Live requires no external monitoring stack.
    Native Rust metrics, health evaluation, the runtime API, and
    Ratatui operate independently. An optional Prometheus exporter
    may expose existing metrics for externally managed Prometheus
    and Grafana deployments. Disabling the exporter must not affect
    market-data acquisition, integrity, recovery, or local monitoring.

---

## 3. System Architecture

```text
                        CONTROL PLANE
                Ratatui TUI / Control API
                            │
                            ▼
               Configuration / Registry
                            │
                            ▼
                    Runtime Reconciler
                            │
                            ▼
EXCHANGES              CONNECTION LAYER
    │                       │
    ├── WebSocket ───────────┤
    └── REST ────────────────┤
                            │
                            ▼
                     INGESTION LAYER
               Decode / Normalize / Validate
                            │
                   Book State / Recovery
                            │
                            ▼
                    CANONICAL EVENTS
                            │
                    Event Distribution
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
        Unix Socket     PostgreSQL      Retention
        Publication     Recording       RAM/Parquet
             │
             ▼
       Downstream Consumers

       Derived Streams:
       Source Dependencies → Merge / Conversion → Distribution

       Cross-cutting:
       Metrics / Health / Diagnostics / Resource Accounting
```

The diagram represents logical responsibilities, not a mandatory synchronous execution order.

### Principal Modules

| Module | Responsibility |
|---|---|
| `model` | Stream identities, cursors, generations, lifecycle states |
| `config` | Global defaults, per-stream overrides, revisions, validation |
| `exchange` | Protocol profiles, capabilities, subscription definitions |
| `transport` | WebSocket/REST communication, retry, rate limiting |
| `registry` | Source deduplication, demand tracking, lifecycle management |
| `ingest` | Decoding, normalization, canonical validation, book processing |
| `recovery` | Connection-aware trade recovery and L2 resynchronization |
| `derived` | Combined streams, adaptive ordering, currency conversion |
| `publish` | Unix-socket event publication and subscriber isolation |
| `retention` | Bounded buffering, Parquet spill, replay cursors |
| `recording` | PostgreSQL batch persistence and recording health |
| `metrics` | Per-stream metrics, integrity, latency, health snapshots |
| `control` | Management commands, queries, configuration updates |
| `runtime` | Dependency assembly, supervision, reconciliation, shutdown |
| `tui/live` | Independent Ratatui management client |

---

## 4. Fundamental Contracts

### Stream Identity and Ownership

A unique source stream is identified by:

```text
StreamKey =
    exchange
    + market
    + instrument
    + channel
    + acquisition parameters
```

`StreamRegistry` manages shared source ownership through demand handles.

Demand may originate from persistent configuration, recording, subscribers, derived streams, or future bootstrap retention sessions.

Releasing one demand must not terminate a stream still required elsewhere.

### Event Identity and Ordering

Canonical events retain:

- Stable source-stream identity.
- MarketForge-assigned stream cursor.
- Native exchange sequence/trade identifiers where available.
- Exchange event timestamp and local receipt timestamp.
- Canonical payload and provenance.

Connection, book synchronization, and retained-log generations are distinct.

**Native source streams:** Minimal-latency publication preserving accepted source order.

**Combined streams:** Arrival-order or adaptive event-time ordering with bounded buffering, watermarks, explicit late-event behavior, and deterministic tie-breaking.

Strict global event-time order cannot be guaranteed for unbounded live streams.

### Integrity

Trade continuity and L2 state validity are separate concerns.

Sequence gaps, duplicates, malformed events, missing snapshots, checksum failures, and unrecoverable intervals must be explicitly represented.

L2 publication requiring valid book state resumes only after successful resynchronization.

Recovery outcomes must distinguish restored current validity from complete historical event coverage.

---

## 5. Connectivity and Fault Tolerance

Generic infrastructure manages:

- WebSocket connection lifecycle and supervision.
- Dynamic subscriptions and acknowledgments.
- Heartbeats and silence detection.
- Reconnection with classified retries and jittered backoff.
- REST requests and shared weighted rate limits.
- Connection sharding and subscription restoration.
- Connection-generation fencing against stale asynchronous responses.

Recovery policies distinguish transient transport failures, rate limiting, protocol errors, invalid configuration, and data-integrity failures.

**Connection restoration does not imply data restoration.**

L2 gaps require invalidation and exchange-appropriate snapshot synchronization.

Recoverable trade gaps use supported REST endpoints and explicit coverage reconciliation.

Irrecoverable gaps remain visible in integrity state and metrics.

---

## 6. Configuration and Runtime Management

Configuration is hierarchical but deliberately shallow:

- **Global:** Logging, metrics, resource limits, publication, writer defaults.
- **Exchange/API pool:** Protocol requirements, connection policies, rate limits.
- **Source stream:** Acquisition, recording, applicable recovery and diagnostic overrides.
- **Derived stream:** Dependencies, ordering policies, conversion settings.

Values may inherit supported defaults or use explicit overrides.

Runtime changes follow:

```text
Proposed Configuration
        ↓
Structural/Semantic Validation
        ↓
Effective Configuration Diff
        ↓
Reconciliation Plan
        ↓
Apply Affected Changes
        ↓
Persist Accepted Revision
```

Initially hot-editable:

- Logging and supported metrics settings.
- Recording batching parameters.
- Subscriber limits.
- Source-stream acquisition and recording.
- Supported per-stream operational policies.
- Combined-stream creation/removal and ordering configuration.

Protocol definitions, parser semantics, WebSocket endpoints, and canonical schemas remain non-hot-reconfigurable initially.

The runtime exposes a versioned Unix control protocol; Ratatui is an independent client.

---

## 7. Distribution, Recording, and Retention

### Unix-Socket Publication

Use Unix domain sockets (`AF_UNIX`, `SOCK_STREAM`) with versioned binary framing.

Provide bounded per-subscriber queues, explicit backpressure policies, subscriber isolation, and disconnection of consumers that cannot maintain required throughput.

Market-data and control sockets remain logically separate.

### PostgreSQL

Reuse the existing database and live schema.

Recording is optional, asynchronous, and independently configurable.

Implement batching, commit tracking, appropriate idempotency constraints, failure classification, and explicit recording-lag metrics.

### Retention

Implement a reusable Live Retention Manager:

```text
Canonical Events
       ↓
Bounded RAM Batches
       ↓
Asynchronous Spill
       ↓
Immutable Parquet Segments
       ↓
Cursor-Based Replay
```

Reuse existing Arrow/Parquet serialization, validation, and transactional-publication primitives.

Retention supports configurable memory/disk budgets, segment manifests, committed cursor ranges, and future retention reservations.

**Memory-first Parquet spill protects against memory exhaustion but does not make unflushed events crash-durable.**

Strict durability requires a separate explicit commit mechanism; its implementation remains an open decision.

The future Bootstrap Engine consumes retention/replay interfaces rather than implementing its own live acquisition.

---

## 8. Metrics and Health

MarketForge Live provides native, lightweight Rust observability
through the Metrics Registry and HealthEvaluator.

The runtime API and Ratatui expose monitoring information without
requiring Prometheus or Grafana.

An optional Prometheus exporter exposes selected existing metrics
through a read-only HTTP endpoint.

Prometheus and Grafana are external, independently operated
applications and are never required for MarketForge startup,
operation, or recovery.

---

## 9. Ratatui Management Interface

Preserve the existing MarketForge Ratatui/Crossterm implementation and Tokyo Night theme.

Implement five primary views:

| View                | Functionality                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------------- |
| Dashboard           | Global runtime health, throughput, latency, errors                                                  |
| Streams             | Add/remove streams, recording controls, per-stream health                                           |
| **Derived Streams** | Create/manage merges and currency conversions, configure dependencies, ordering, reference policies |
| Recording           | PostgreSQL status, batching, retention utilization                                                  |
| Settings            | Global configuration, overrides, revisions                                                          |

Additional elements:

- Detailed stream-health inspector.
- Recent operational-event history.
- Validated configuration forms.
- Configuration diffs and revision history.

Adapt selected UI patterns from Statui and operational architecture from RustFinance.

The TUI must never own exchange connections or be required for daemon operation.

Bootstrap will have its own independent TUI in a later development phase.

---

## 10. Code Reuse

### Existing MarketForge

Reuse and preserve:

- Canonical trade/L2 models.
- Exact decimal and timestamp normalization.
- Instrument metadata and identity.
- `BookStore` and sequence-validation primitives.
- Historical integrity-accounting concepts.
- Arrow/Parquet schemas, writers, manifests, validation.
- Existing Ratatui theme and widgets.
- Historical test fixtures and regression tests.

### External Rust Projects

| Project | Intended use |
|---|---|
| Barter-Data / Barter-Integration | Exchange-stream abstractions, connection/subscription patterns; selective code reuse subject to compatibility |
| `tokio-tungstenite` | WebSocket transport |
| `reqwest` | REST transport |
| `backon` | Retry/backoff policies |
| `governor` | Rate-limiting primitives |
| Statui | Monitoring tables, sparklines, inspectors |
| RustFinance | Daemon/TUI separation, navigation and operational patterns |
| Ratatui / Crossterm | Terminal interface |

Avoid overlapping ownership of connection supervision and reconnection.

Verify dependencies, licensing, and integration suitability before adoption.

---

## 11. Testing Strategy

Testing is a first-class architectural requirement.

**Preserve all historical regression tests.**

Live verification includes:

1. Unit tests for models, normalization, configuration, ordering, and state transitions.
2. Deterministic fake WebSocket/REST transports.
3. Protocol fixtures for all supported exchange/market/channel families.
4. Connection lifecycle, heartbeat, retry, and generation-fencing tests.
5. Concurrent stream acquisition and deduplication tests.
6. Trade/L2 continuity, recovery, and invalidation tests.
7. Historical/live canonical-equivalence tests where source representations permit.
8. Unix-socket framing, reconnect, and backpressure tests.
9. PostgreSQL batching, idempotency, and failure-injection tests.
10. RAM/Parquet spill, cursor continuity, replay, retention, and crash-recovery tests.
11. Configuration reconciliation and concurrent-revision tests.
12. End-to-end simulated ingestion and performance regression benchmarks.

Tests must not require external exchanges except explicitly designated smoke/integration tests.

Metrics and UI must never alter processing results.

---

## 12. Implementation Strategy

**Approach: Contract-first, test-first, vertically integrated.**

### Phase 1 — Contracts

Finalize domain models, stream identities, cursors, generations, error taxonomy, configuration schema, lifecycle states, interfaces, and delivery guarantees.

### Phase 2 — Simulated Runtime

Build a complete deterministic pipeline using fake transports and test sinks.

Validate source ownership, normalization, integrity, routing, recovery, and control operations without external dependencies.

### Phase 3 — Production Ingestion

Integrate WebSocket/REST transports, exchange protocol specifications, subscription management, connection supervision, and recovery.

### Phase 4 — Distribution and Persistence

Implement Unix-socket publication, PostgreSQL recording, RAM buffering, Parquet spill, and cursor-based replay.

### Phase 5 — Derived Streams

Implement reusable combined streams, adaptive ordering, and optional spot currency conversion.

### Phase 6 — Operations

Implement runtime configuration reconciliation, control API, native metrics, health monitoring, and Ratatui management.

Interfaces required by later phases must already be represented in the foundational contracts; phases describe implementation milestones, not permission to redesign earlier subsystems.

### Phase 7 — Hardening

Run deterministic fault-injection suites, historical regression tests, sustained load tests, resource profiling, and crash-recovery verification.

---

## 13. Deferred Work

The following are outside initial Live implementation:

- Bootstrap orchestration and historical/live dataset assembly.
- Dedicated Bootstrap TUI.
- Python orchestration refactoring.
- Feature engineering, imbalance bars, ML, strategies, and execution.
- Options-market processing.
- Distributed deployment.
- Web frontend and external monitoring infrastructure.
- Unrestricted hot-reloading of exchange protocol definitions.
- Strict crash-durable retention, unless explicitly selected as a v1 requirement.

Live must preserve the interfaces necessary for later bootstrap integration.

---

## 14. Open Architectural Decisions

| Decision                                           | Status               |
| -------------------------------------------------- | -------------------- |
| Final Rust crate/module boundaries                 | Design pending       |
| Exact binary publication/control schemas           | Design pending       |
| Connection supervisor: custom or adapted library   | Evaluation pending   |
| Exchange protocol and recovery strategy interfaces | Design pending       |
| Precise cursor persistence and epoch semantics     | Design pending       |
| Event router delivery/acknowledgment policies      | Design pending       |
| PostgreSQL idempotency and commit-watermark design | Design pending       |
| Memory/Parquet retention guarantees                | Design pending       |
| Strict crash-durable capture mechanism             | Deferred / undecided |
| Final resource budgets and performance targets     | Benchmark dependent  |

Resolved decisions should be reflected in their subsystem specifications and updated here only when they change system-wide architecture.

---

## 15. Reference Documents

```text
00 — Master Plan.md
01 — Architecture and Module Boundaries.md
02 — Domain Models and Contracts.md
03 — Configuration System.md
04 — Exchange Protocols and Connectivity.md
05 — Stream Registry and Subscriptions.md
06 — Normalization and Integrity.md
07 — Fault Tolerance and Recovery.md
08 — Event Distribution and Unix Sockets.md
09 — Combined Streams and Currency Conversion.md
10 — Retention and Parquet Spill.md
11 — PostgreSQL Recording.md
12 — Metrics and Stream Health.md
13 — Runtime Configuration and Control API.md
14 — Ratatui Management Interface.md
15 — Testing and Validation.md
16 — Implementation Roadmap.md
17 — External Libraries and Code Reuse.md
```

**Architectural rule:** Every subsystem specification must define responsibility, ownership, interfaces, failure semantics, configuration scope, reuse opportunities, and testable invariants.

**Completion criterion:** MarketForge Live is a continuously operating, independently manageable Rust market-data service whose ingestion, integrity, publication, recording, retention, and recovery guarantees are explicit, tested, and independent of presentation or downstream applications.