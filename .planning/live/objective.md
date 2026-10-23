# MarketForge Live — Objectives, Responsibilities and Limitations

**Status:** Draft  
**Scope:** MarketForge Live  
**Purpose:** Define the product's responsibilities, requirements, and limitations before designing its architecture.

---

## 1. Objective

MarketForge Live is a high-performance market-data acquisition, processing, and distribution system for cryptocurrency exchanges.

Its primary purpose is to provide normalized live market data to downstream applications, including quantitative trading, research, and feature-engineering systems.

MarketForge Live is responsible for acquiring, interpreting, processing, and distributing market data.

It is not responsible for making trading decisions or determining whether market-data quality is sufficient for a particular strategy.

The system prioritizes:

- Correct interpretation of market data.
- Reliable acquisition under ordinary operating conditions.
- Automatic recovery from common connectivity failures.
- Observable data quality and operational health.
- High throughput and low processing latency.
- Bounded resource consumption.
- Simplicity of implementation and maintenance.

Perfect historical continuity and guaranteed delivery are not primary objectives.

---

## 2. Core Responsibilities

### 2.1 Exchange Connectivity

MarketForge Live must:

- Connect to supported cryptocurrency exchanges.
- Support Spot, Linear, and Inverse market families where available.
- Establish and maintain market-data subscriptions.
- Handle exchange-specific connection and subscription protocols.
- Respect exchange API rate limits.
- Detect connection failures and timeouts.
- Automatically reconnect after recoverable network failures.
- Retry temporary exchange outages using bounded backoff.

The intended exchange scope is:

- Binance
- Bybit
- OKX
- Bitget
- Gate.io

Unsupported exchange capabilities must not be presented as supported.

### 2.2 Market-Data Acquisition

MarketForge Live must:

- Acquire live trade data.
- Acquire live L2 order-book data.
- Preserve relevant exchange timestamps.
- Preserve source and instrument identity.
- Detect malformed or unusable market-data messages.
- Detect available sequence or continuity information.
- Expose acquisition failures and interruptions.

Not every exchange provides sufficient information to detect every missing market event.

Therefore, the absence of detected gaps does not guarantee complete market-data history.

### 2.3 Market-Data Normalization

MarketForge Live must:

- Convert supported exchange-native messages into canonical market-data representations.
- Preserve correct instrument and contract semantics.
- Support Spot, Linear, and Inverse instruments.
- Preserve exact financial values where required.
- Preserve relevant source timestamps and identifiers.
- Reject or report data that cannot be interpreted correctly.

Normalization must not invent missing instrument economics.

Normalization is performed at the source-stream level, not at the derived-stream level.

Normalization rules should follow the same logic as implemented in the Historical engine of MarketForge.

### 2.4 L2 Order-Book Processing

MarketForge Live must:

- Process supported L2 snapshots and incremental updates.
- Maintain an internal order-book representation.
- Validate native sequence continuity where supported.
- Detect known synchronization failures.
- Expose current order-book validity.
- Attempt resynchronization using exchange-supported methods.

The previously defined canonical representation must be reused.

Existing database components should be reused where applicable.

When a sequence gap invalidates the current book, MarketForge must not present that book as synchronized.

A successful resynchronization restores a valid current book but does not necessarily recover missing historical updates.

Downstream applications must be able to distinguish:

- Currently synchronized book.
- Unsynchronized or invalid book.
- Book undergoing resynchronization.
- Known historical discontinuity.
- Unknown historical continuity.

### 2.5 Trade Recovery

MarketForge Live should attempt to recover missing trades through REST APIs where supported.

Recovery must:

- Respect exchange rate limits.
- Avoid knowingly duplicating recovered trades.
- Preserve source identity and timestamps.
- Report recovery success, partial recovery, or failure.

Trade recovery is best effort.

MarketForge does not guarantee that every missing trade can be recovered.

This responsibility concerns exchange-side market-data recovery, not recovery of events missed by downstream consumers.

### 2.6 Market-Data Distribution

MarketForge Live must distribute normalized source and derived market-data streams to downstream applications.

Publication follows a producer-first, best-effort model.

MarketForge must:

- Expose a single Unix-domain socket endpoint for local market-data publication.
- Keep market-data publication independent of the REST management API.
- Support multiple independent consumer connections.
- Allow consumers to subscribe to selected streams.
- Publish events to subscribed consumers as they become available.
- Preserve source identity, stream identity, and event ordering information.
- Expose available sequence and continuity information.
- Use independent, bounded outgoing queues for consumer connections.
- Prevent slow consumers from blocking market-data production or unrelated consumers.
- Disconnect consumers whose outgoing queues exceed their configured limits.
- Report known publication interruptions and consumer disconnections.

MarketForge produces data independently of downstream consumer processing speed.

A consumer that cannot keep up must not cause MarketForge to delay production, accumulate unbounded data, or reduce service quality for other consumers.

When a consumer's outgoing queue reaches its capacity, MarketForge disconnects that consumer rather than indefinitely buffering additional events.

Publication is push-based. Connected consumers receive events for their subscribed streams without explicitly requesting each event.

MarketForge does not maintain rolling publication buffers or consumer-specific event histories.

The publication interface does not provide historical replay or automatic recovery of events missed by consumers.

MarketForge does not guarantee that every acquired event reaches every subscriber.

Consumers must not assume that events remain available after publication.

PostgreSQL recording is independent of live publication and is not required for the publication subsystem to operate.

### 2.7 Data Quality and Operational Health

MarketForge Live must expose information about:

- Connection and subscription status.
- Data acquisition interruptions.
- Detected exchange-side sequence gaps.
- Trade recovery attempts and outcomes.
- L2 synchronization state.
- Reconnection attempts.
- Processing failures.
- Subscriber connections and active subscriptions.
- Subscriber queue utilization and overflow events.
- Subscriber disconnections and delivery interruptions.
- Recording failures and known data loss.
- Processing throughput and latency.

MarketForge must distinguish exchange-side market-data discontinuities from publication and delivery interruptions.

An exchange-side gap indicates that source market data may be missing.

A publication interruption indicates that a downstream consumer may not have received events that MarketForge processed.

These conditions must not be confused.

Where supported by the underlying data and publication protocol, sequence information should allow consumers to identify known discontinuities.

Data-quality information must be available to downstream applications, not only to the management interface.

Downstream applications determine whether degraded or incomplete data is acceptable for their purposes.

MarketForge must distinguish known failures from conditions whose completeness cannot be established.

### Data Integrity Monitoring

MarketForge Live must provide data-integrity monitoring for individual
source streams, independently of their operational health.

The integrity subsystem must:

- Track detected market-data gaps and discontinuities.
- Track the exact number of missing events when this can be reliably
  established from exchange-specific sequence semantics.
- Explicitly distinguish gaps with known missing-event counts from
  gaps whose magnitude cannot be determined.
- Preserve the timestamp of the most recently detected gap.
- Provide integrity statistics over four rolling windows:
  - 1 minute.
  - 5 minutes.
  - 30 minutes.
  - 6 hours.
- Report the number of detected gaps, exact missing-event counts,
  and gaps with unknown missing-event counts for each window.
- Maintain a bounded, in-memory history of individual gap events.
- Make recent gap history available for inspection through the REST API.
- Expose integrity summaries through the REST API.
- Support immediate integrity notifications through the existing
  market-data publication interface.

Integrity history must:

- Be maintained independently for each source stream.
- Reuse findings from existing sequence-validation and recovery components.
- Avoid duplicating gap-detection or recovery logic.
- Retain records for up to six hours.
- Enforce a maximum record count to ensure bounded memory consumption.
- Expose history coverage and truncation information when records are
  unavailable due to startup, stream restart, or capacity limits.
- Reset when the corresponding stream runtime generation is recreated.
- Operate independently of PostgreSQL.

Gap statistics represent discontinuities detected during acquisition,
regardless of whether missing events are subsequently recovered.

The integrity subsystem is not responsible for maintaining detailed
recovery histories, retry records, or recovery outcome classifications.

Exact missing-event counts must only be reported when supported by
the exchange's documented sequence semantics. Unknown quantities
must not be represented as zero.

The absence of detected gaps does not guarantee complete market-data
coverage.

Data-integrity monitoring must distinguish current stream health from
historical data continuity. A stream may be operating normally while
its recent history contains detected gaps.

For L2 streams, the existing order-book synchronization state remains
authoritative. Successful resynchronization restores current book
validity but does not erase previously detected discontinuities.

Integrity reporting is informational. MarketForge Live does not decide
whether downstream applications should continue processing or trading
when data integrity is degraded.

The integrity subsystem must remain lightweight and must not introduce
unbounded storage, per-event persistence, or significant overhead into
the market-data processing pipeline.

### 2.8 Stream Management

MarketForge Live must allow operators to:

- Add and remove source streams.
- Enable and disable streams.
- Inspect stream status.
- Restart failed streams.
- Request reconnection or L2 resynchronization.
- Configure persistent stream definitions.

Enabled streams operate independently of downstream subscriber connections.

Disconnecting a subscriber must not automatically stop an enabled source stream.

Streams without subscribers may continue acquisition and processing according to their configured lifecycle.

However, events without interested subscribers do not need to be serialized or queued for external publication.

### 2.9 Optional Market-Data Recording

MarketForge Live must support optional PostgreSQL recording.

Recording must:

- Be independently enabled or disabled.
- Preserve supported canonical market-data values.
- Report recording failures.
- Expose known recording gaps or incomplete coverage.
- Avoid presenting uncertain recording outcomes as confirmed.
- Operate independently of downstream consumer connections.
- Avoid indefinitely blocking market-data production during recording failures.

Recording is best effort.

MarketForge does not guarantee crash-lossless recording or complete persistence of every acquired event.

PostgreSQL may support initial application bootstrapping and historical data access.

However, PostgreSQL is not part of the live publication delivery mechanism.

MarketForge does not use PostgreSQL to guarantee delivery or automatically recover events missed by downstream consumers.

The publication subsystem must operate correctly when recording is disabled or unavailable.

### 2.10 Derived Streams

MarketForge Live must support combining source streams and applying transformations using the same core logic, semantics, and processing models as the Historical engine.

Derived streams must:

- Reuse existing Historical merging, transformation, and currency-conversion logic wherever applicable.
- Preserve the identity and provenance of contributing source events.
- Support event-time ordering consistent with Historical merging semantics.
- Expose input availability, health, and coverage.
- Continue processing healthy inputs when the operation remains meaningful.
- Report interruptions, missing data, and incomplete coverage.
- Allow downstream consumers to assess data quality.

Live-specific components should adapt existing Historical processing logic to continuous streaming rather than reimplementing it.

Nested derived streams are not required.

### 2.11 Runtime Management and Observability

MarketForge Live must provide:

- An HTTP REST API for runtime management and monitoring.
- Remote management capabilities.
- A Ratatui terminal interface that communicates through the REST API.
- Stream health and status information.
- Operational metrics.
- Recording controls.
- Stream creation, removal, and restart controls.
- Manual recovery operations, including reconnection and L2 resynchronization.
- Clear reporting of degraded or failed components.

The REST API must:

- Expose management operations through a consistent HTTP interface.
- Support both read-only queries and state-changing operations.
- Delegate operations to the components responsible for the corresponding state and lifecycle.
- Avoid maintaining independent authoritative runtime state.
- Return explicit operation results and errors.
- Support secure remote access.

The REST API is a management interface, not a market-data transport.

Market-data publication remains independent and uses Unix-domain sockets.

The REST API must not expose administrative operations to unauthorized clients.

Remote access must use appropriate authentication, authorization, and transport security, or be restricted to an authenticated private network.

The system should support ordinary management operations without requiring a complete daemon restart.

Global infrastructure changes may require restart.

### 2.12 Resource Management

MarketForge Live must:

- Bound memory consumption.
- Bound internal queues and buffers.
- Bound concurrent operations and retries.
- Respect exchange connection and request limits.
- Prevent uncontrolled resource growth.
- Report resource exhaustion and overload.

Publication must additionally:

- Enforce a maximum number of simultaneous consumer connections.
- Enforce bounded outgoing queues for each consumer connection.
- Disconnect consumers that exceed their queue capacity.
- Prevent consumer backpressure from propagating indefinitely into source or derived-stream processing.
- Bound resources used by event serialization and in-flight socket writes.

Consumer queue capacity is not a promise of temporary data retention or guaranteed delivery.

Resource exhaustion may cause observable data loss, stream degradation, consumer disconnection, or operation failure.

It must not silently produce invalid normalized data or falsely synchronized order books.

---

## 3. Explicit Limitations

MarketForge Live does not guarantee:

- Complete historical market-data continuity.
- Recovery of every missing trade.
- Recovery of missing historical L2 updates.
- Guaranteed delivery to every subscriber.
- Preservation of events for disconnected or slow consumers.
- Historical replay through the publication interface.
- Consumer-specific event recovery.
- Crash-lossless recording.
- Seamless replay-to-live transitions.
- Automatic resolution of every exceptional failure.
- Perfect detection of every exchange-side data gap.
- Continuous operation during every configuration change.
- Trading-strategy-specific data-quality decisions.
- Remote market-data publication through the management REST API.
- Direct remote access to Unix-domain publication sockets.

Publication is best effort.

Events may be unavailable to consumers because of:

- Consumer processing delays.
- Consumer queue overflow.
- Connection interruption.
- Consumer disconnection.
- MarketForge restart.
- Source-stream interruption.
- Other acquisition or processing failures.

MarketForge does not compensate for insufficient downstream processing capacity.

Exceptional failures may require operator intervention.

These limitations must be reflected honestly in system status and downstream data-quality information.

---

## 4. Responsibilities of Downstream Applications

Downstream applications are responsible for:

- Consuming published data at a sufficient rate.
- Allocating sufficient processing resources for their subscriptions.
- Maintaining their own processing state and tracking available sequence information.
- Detecting known market-data and publication discontinuities.
- Handling subscriber disconnections and reconnections.
- Resubscribing after connection interruptions.
- Reinitializing dependent state when continuity cannot be established.
- Evaluating whether market-data quality is sufficient for their intended purpose.
- Deciding whether to continue operating with incomplete or degraded market data.
- Implementing trading strategies and risk management.

Consumers must not assume that previously published events remain available.

If a consumer is too slow, its outgoing queue may overflow and MarketForge may disconnect it.

When events are missed, consumers determine whether to:

- Continue processing with incomplete data.
- Reset or rebuild their internal state.
- Obtain historical data through independently available mechanisms.
- Increase their own processing capacity.
- Reduce the amount of data they consume.
- Stop processing until their required data-quality conditions are restored.

MarketForge does not automatically recover events missed by downstream applications.

PostgreSQL may be available for initial bootstrapping or other historical-data operations, but consumers must not assume that it contains every missed live event.

Downstream applications are responsible for deciding whether their processing state remains valid following a publication interruption.

MarketForge Live provides market data and quality information.

It does not make strategy-specific decisions on behalf of downstream applications.


### Runtime Publication Queue Configuration

MarketForge Live must support adjusting `subscriber_queue_bytes`
at runtime through the REST management API.

- The value in `general.toml` defines the initial queue capacity.
- Runtime changes apply to all existing and future consumer connections.
- Increasing the limit must not interrupt active connections.
- Decreasing the limit must disconnect connections whose queued data
  already exceeds the new capacity.
- Changes must not restart or interrupt source or derived streams.
- Runtime changes are not persisted to `general.toml`.
- Restarting MarketForge restores the configured initial value.
- Queue capacity changes must respect bounded resource consumption.
- Concurrent configuration updates must be applied consistently.

The publication manager owns the authoritative runtime queue limit.
The REST API only exposes operations to inspect and modify it.

Queue capacity controls tolerance to temporary consumer slowdowns.
It does not increase processing throughput or guarantee event delivery.


### Shared Canonical Representation

MarketForge Live must reuse the existing Historical engine's canonical
data structures without introducing separate Live-specific equivalents.

- Source streams produce the existing `CanonicalEvent` types.
- Trade and L2 representations retain their existing structure and semantics.
- Financial values use exact `Decimal` arithmetic.
- Timestamps use nanosecond precision (`i64`).
- Normalization and contract-quantity conversion must reuse Historical
  logic wherever applicable.
- Derived streams preserve the original event's exchange, instrument,
  and source identity.
- Derived-stream publication identity is maintained separately from
  the original canonical event envelope.
- Live-specific metadata, such as publication sequence numbers and
  delivery status, belongs to the publication layer rather than
  the shared canonical representation.

Historical and Live must produce semantically equivalent canonical
events for equivalent source data.

### Operational Metrics and Latency Monitoring

MarketForge Live must provide lightweight operational metrics for
source streams, derived streams, publication, recording, and the
overall runtime.

Monitoring must include:

- **Source streams:** Message and event counts, throughput,
  normalization failures, reconnections, and last activity timestamps.
- **L2 streams:** Book synchronization state, bid/ask level counts,
  and resynchronization counts.
- **Derived streams:** Input availability, input/output throughput,
  queue utilization, overflows, late events, and conversion failures.
- **Publication:** Active connections, subscriptions, queued bytes,
  queue overflows, and consumer disconnections.
- **Recording:** Confirmed writes, failed write attempts, pending
  records, and last successful commit.
- **Runtime:** Uptime, stream counts, CPU and memory utilization,
  aggregate throughput, and internal queue overflows.

#### Latency Monitoring

MarketForge must measure latency in nanoseconds at the following
boundaries:

- **Source delay:** Exchange event timestamp to local receipt time.
- **Source processing:** Native message receipt to canonical output.
- **Derived processing:** Active merging and conversion processing.
- **Derived reorder waiting:** Time spent waiting for event-time
  ordering, where applicable.
- **Publication:** Subscriber queue admission to socket-write
  completion.

Source delay is an observed timestamp difference, not guaranteed
network latency, and may be negative due to clock differences.

Internal processing durations must use monotonic clocks.

Latency statistics must expose the latest observation, interval
mean, interval maximum, and sample count.

#### Collection and Exposure

- Metrics must be collected with minimal hot-path overhead.
- Cumulative counters and current gauges are the primary measurements.
- Throughput rates must be calculated from counter differences
  using actual elapsed monotonic time.
- Latency measurements must be aggregated into consistent intervals.
- A single periodic collector must publish immutable metrics snapshots.
- REST, Ratatui, and optional Prometheus endpoints must reuse these
  snapshots without independently resetting measurements.
- Stream lifecycle, dependency health, and L2 synchronization state
  remain owned by their authoritative runtime components.
- The existing integrity subsystem remains authoritative for gap
  statistics and history; operational metrics must not duplicate it.
- Metrics collection must not block market-data processing indefinitely.
- Metrics history does not require database persistence.

The initial implementation prioritizes operational diagnosis over
extensive profiling. Distributed tracing, detailed latency histograms,
per-event metric storage, and complex alerting are outside v1 scope.

Monitoring is observational and must not independently control
stream lifecycle or determine whether downstream applications
should continue processing or trading.

### WebSocket Connection Ownership and Message Routing

MarketForge Live should prefer one logical source stream per physical
WebSocket connection whenever supported and operationally reasonable.

- Dedicated connections are the default to simplify isolation,
  lifecycle management, and message processing.
- Multiple source streams may share a physical connection when required
  by exchange protocols, connection limits, or operational constraints.
- Shared connections must support generic message routing based on
  exchange-provided subscription identifiers.
- Message routing must occur before canonical normalization.
- Exchange-specific routing implementations should be avoided;
  reusable, configuration-driven strategies are preferred.
- Shared connections must maintain explicit subscription ownership
  and prevent stale messages from reaching restarted or removed streams.
- One overloaded logical stream must not indefinitely block other
  streams sharing the same connection.
- Connection sharing must not create duplicate normalization work.

Connection ownership and routing should remain internal to the
WebSocket infrastructure, without requiring a separate complex
routing framework.

The implementation should prioritize simplicity while retaining
support for shared connections where necessary. 



### Ratatui Management Interface

MarketForge Live must provide a terminal-based management and
observability interface implemented using Ratatui.

The TUI must expose all management and monitoring capabilities
available through the REST control API.

The TUI communicates exclusively through the REST API and must
not directly access or modify internal runtime state.

#### Main Views

The TUI must provide seven primary views:

1. **Dashboard**
   - Overall runtime health and uptime.
   - Source and derived-stream counts and statuses.
   - Aggregate source and derived throughput.
   - CPU and memory utilization.
   - Publication connection counts and queue utilization.
   - Recording status.
   - Summary of recent data-integrity problems.

2. **Source Streams**
   - List all registered source streams.
   - Filter and sort streams by exchange, market, symbol,
     channel, status, throughput, and latency.
   - Inspect individual stream definitions and runtime state.
   - Display connection health, processing metrics, and
     recent integrity statistics.
   - Create, enable, disable, restart, reconnect, and remove
     source streams.
   - Request L2 resynchronization where applicable.

3. **Derived Streams**
   - List all registered derived streams.
   - Inspect merge and currency-conversion configurations.
   - Display input dependencies and their health.
   - Inspect input/output throughput, processing latency,
     reorder waiting, queue utilization, and failures.
   - Navigate directly to contributing source streams.
   - Create, enable, disable, restart, and remove derived streams.

4. **Integrity**
   - Display data-integrity summaries across source streams.
   - Support the existing rolling windows of 1 minute,
     5 minutes, 30 minutes, and 6 hours.
   - Display detected gaps, exact missing-event counts,
     and gaps with unknown missing-event counts.
   - Inspect individual gap records and their available evidence.
   - Expose integrity-history coverage and truncation.
   - Distinguish current stream health from historical
     data discontinuities.

5. **Publication**
   - Display the Unix publication socket configuration.
   - List active consumer connections and subscriptions.
   - Show per-connection and aggregate queue utilization.
   - Display publication throughput, latency, and overflows.
   - Inspect the effective runtime subscriber queue limit.
   - Allow changing `subscriber_queue_bytes` through REST.
   - Do not provide manual consumer-disconnection controls.

6. **Recording**
   - Display PostgreSQL recording status and health.
   - Show confirmed writes, write failures, pending records,
     and the last successful commit.
   - Enable or disable recording at runtime.
   - Adjust `batch_max_records` and `batch_max_delay_ms`
     through the REST API.
   - Display the currently effective recording configuration.

7. **Settings**
   - Display runtime and infrastructure configuration.
   - Show REST management address and metrics settings.
   - Allow changing the runtime logging level.
   - Clearly distinguish runtime-adjustable settings from
     settings requiring a daemon restart.
   - Provide an explicitly confirmed graceful shutdown operation.

#### Interaction Requirements

- All runtime management operations must use the existing
  control API contracts.
- Source and derived-stream creation must reuse their existing
  request models.
- The TUI must support searching, filtering, sorting, and
  selecting registered streams.
- Destructive operations, including stream removal and daemon
  shutdown, must require explicit confirmation.
- Asynchronous operations must distinguish accepted requests
  from completed or failed operations.
- Management failures must be displayed clearly.
- Runtime configuration views must show effective active values,
  including changes made through REST.
- Closing the TUI must never shut down MarketForge Live.

#### Data Refresh and Performance

- The TUI should refresh operational metrics approximately
  once per second using the existing metrics snapshots.
- Stream lists and global monitoring data should be retrieved
  through bulk REST queries where appropriate.
- Detailed stream information and gap history should be
  requested on demand.
- The TUI must avoid issuing individual polling requests for
  every registered stream during normal operation.
- REST communication must not block terminal rendering.
- Temporary REST connection failures must not terminate the
  MarketForge daemon.
- The TUI must support reconnection to the management API.

#### Architectural Boundaries

The TUI is a presentation and management client only.

It must not:

- Implement exchange connectivity or market-data processing.
- Own authoritative stream definitions or runtime state.
- Duplicate metrics or integrity collection.
- Modify PostgreSQL directly.
- Implement independent recovery or lifecycle logic.
- Access the Unix market-data publication socket for management.
- Introduce additional management operations unavailable
  through the REST API.

The initial TUI should prioritize clear status tables,
stream inspection, diagnostics, and management actions.

Complex visualizations, graphical dependency editors,
persistent UI databases, and advanced dashboard customization
are outside the scope of v1.

The TUI may be implemented after the runtime infrastructure,
provided its required REST queries and management operations
are supported by the corresponding authoritative components.