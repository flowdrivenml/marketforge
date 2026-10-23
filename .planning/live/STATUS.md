# MarketForge Live — Modular Development Plan

**Goal:** Build a working MarketForge Live v1 using small, independently testable components that can later be connected together.

**Approach:** No unnecessary abstractions, no excessive infrastructure, no architectural expansion. Reuse existing Historical functionality wherever possible.

**Development model:** Independent building blocks, not sequential phases.

---

## 1. Core Models and Contracts

- [ ] Canonical live trade model
- [ ] Canonical L2 snapshot and update models
- [ ] Stream identity and instrument metadata
- [ ] Stream configuration models
- [ ] Data-quality and continuity events
- [ ] Component input/output interfaces

## 2. Exchange Connectivity

- [ ] Generic WebSocket connection handler
- [ ] Connection lifecycle and heartbeat handling
- [ ] Automatic reconnection and backoff
- [ ] Subscription and unsubscription handling
- [ ] REST request client and rate-limit handling
- [ ] Binance exchange adapter
- [ ] Bybit exchange adapter
- [ ] OKX exchange adapter
- [ ] Bitget exchange adapter
- [ ] Gate.io exchange adapter

Each exchange adapter should handle trades and L2 independently.

## 3. Normalization

- [ ] Reuse Historical instrument normalization
- [ ] Live trade normalization
- [ ] Live L2 normalization
- [ ] Exchange timestamp handling
- [ ] Spot, linear, and inverse quantity conversion
- [ ] Invalid message handling

## 4. Market Data Integrity

- [ ] Trade continuity and duplicate detection
- [ ] L2 sequence validation
- [ ] L2 snapshot synchronization
- [ ] L2 order-book state management
- [ ] L2 resynchronization
- [ ] Trade recovery using REST
- [ ] Recovery result and gap reporting

## 5. Stream Management

- [ ] Source stream registry
- [ ] Stream creation and removal
- [ ] Stream start, stop, and restart
- [ ] Stream enable/disable
- [ ] Stream status tracking
- [ ] Stream configuration persistence
- [ ] Independent stream lifecycle management

## 6. Event Distribution

- [ ] Internal event routing
- [ ] Bounded event queues
- [ ] Unix socket server
- [ ] Consumer connection and subscription handling
- [ ] Canonical event serialization
- [ ] Multiple independent consumers
- [ ] Slow consumer handling
- [ ] Delivery sequence and discontinuity reporting

## 7. Derived Streams

- [ ] Derived stream definitions
- [ ] Source stream dependency handling
- [ ] Live adaptation of Historical merging
- [ ] Event-time ordering and buffering
- [ ] Live adaptation of Historical transformations
- [ ] Live currency conversion
- [ ] Derived stream health and coverage reporting

Reuse Historical processing logic instead of implementing separate algorithms.

## 8. PostgreSQL Recording

- [ ] Reuse existing database schema where applicable
- [ ] Trade recording
- [ ] L2 recording
- [ ] Buffered batch writing
- [ ] Recording enable/disable
- [ ] Database failure handling
- [ ] Recording gap reporting

## 9. Monitoring and Health

- [ ] Connection health metrics
- [ ] Stream status metrics
- [ ] Data throughput metrics
- [ ] Processing latency metrics
- [ ] L2 synchronization status
- [ ] Recovery and gap metrics
- [ ] Queue utilization and dropped-event metrics
- [ ] Recording health metrics

## 10. Runtime Control

- [ ] Daemon startup and shutdown
- [ ] Runtime configuration loading
- [ ] Management API
- [ ] Stream management commands
- [ ] Recording management commands
- [ ] Graceful resource cleanup

## 11. Ratatui Interface

- [ ] Connect TUI to daemon management API
- [ ] Stream dashboard
- [ ] Source stream creation dialog
- [ ] Derived stream creation dialog
- [ ] Stream start/stop/restart controls
- [ ] Stream health and metrics display
- [ ] Recording controls
- [ ] Error and recovery status display

Reuse existing Live TUI components wherever possible.

## 12. Integration and Validation

- [ ] Exchange adapter protocol tests
- [ ] Normalization correctness tests
- [ ] L2 synchronization and recovery tests
- [ ] Trade recovery tests
- [ ] Unix socket consumer tests
- [ ] Derived stream correctness tests
- [ ] PostgreSQL recording tests
- [ ] Full pipeline integration tests
- [ ] Disconnect and reconnection tests
- [ ] Sustained operation and resource-limit tests

---

## Independent Development Strategy

These are **not 12 sequential development phases**.

Each component can be developed and validated independently.

Examples:

- **Binance adapter:** Connect to Binance and output canonical trades.
- **L2 engine:** Process recorded L2 messages and maintain a synchronized order book.
- **Unix socket server:** Distribute simulated canonical events to independent consumers.
- **PostgreSQL writer:** Receive generated market events and persist them.
- **Derived stream processor:** Process Historical fixtures through live-compatible logic.
- **Ratatui interface:** Operate against a mock management API.

### Definition of a Completed Building Block

Every component must have:

- [ ] Defined input contract
- [ ] Defined output contract
- [ ] Working implementation
- [ ] Explicit error handling
- [ ] Automated tests
- [ ] Independent validation
- [ ] Integration-ready interface

---

## Final Assembly

```text
Exchange WebSockets
        |
        v
Exchange Adapters
        |
        v
Normalization
        |
        v
Integrity / L2 / Recovery
        |
        v
Internal Event Router
        |
        +----------> Unix Sockets
        |
        +----------> PostgreSQL
        |
        +----------> Derived Streams
                           |
                           +----> Unix Sockets
                           |
                           +----> PostgreSQL


Stream Registry / Runtime Control
        |
        +----------> Management API
                           |
                           v
                        Ratatui