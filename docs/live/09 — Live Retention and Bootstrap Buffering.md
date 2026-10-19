## 1. Purpose

The Live Retention subsystem temporarily preserves canonical market-data events for historical bootstrap, bounded replay, and recovery from interruptions in downstream consumption.

It bridges the difference between:

- The moment MarketForge begins acquiring live market data.
- The moment a historical bootstrap consumer finishes retrieving and processing historical archives.
- The moment that consumer can safely transition to continuously arriving live events.

Because historical acquisition and processing may take significant time, live events must remain available throughout the bootstrap interval.

The subsystem must support bounded memory usage and controlled spill to local Parquet files.

It must not assume that the entire bootstrap interval can fit in RAM.

### 1.1 Primary Responsibilities

The Live Retention subsystem must:

- Accept canonical events from the live processing pipeline.
- Preserve the accepted canonical publication identity.
- Retain events according to configured retention policies.
- Maintain per-stream retention boundaries.
- Provide bounded in-memory buffering.
- Spill retained events to Parquet when memory thresholds are reached.
- Maintain durable manifests for completed spill segments.
- Recover valid spill segments after process restart.
- Detect incomplete or corrupted segments.
- Serve retained events to authorized internal consumers.
- Support cursor-based access where available.
- Report expired or unavailable intervals explicitly.
- Coordinate bootstrap coverage requirements.
- Enforce disk and memory budgets.
- Release retained data only when its removal is permitted.
- Expose retention health and metrics.

### 1.2 Non-Responsibilities

The Live Retention subsystem does not:

- Download historical exchange archives.
- Decide historical bootstrap feature-engineering logic.
- Normalize exchange-native messages.
- Establish exchange WebSocket connections.
- Reconstruct invalid order books.
- Recover missing native exchange trades.
- Own IPC subscriber sessions.
- Guarantee that upstream exchange acquisition was complete.
- Write operational logs to PostgreSQL.
- Replace the optional PostgreSQL Recording Manager.
- Provide unlimited historical storage.

Historical archives remain owned by the historical data subsystem.

The Live Retention subsystem stores accepted live events required to bridge the historical-to-live transition.

### 1.3 Core Architectural Principle

Historical bootstrap and live acquisition execute concurrently.

Conceptually:

    Historical Archives
            |
            v
    Historical Processing
            |
            |
            +----------------------------+
                                         |
                                         v
                                  Bootstrap Join
                                         ^
                                         |
    Exchange WebSocket                   |
            |                            |
            v                            |
    Canonical Processing                 |
            |                            |
            v                            |
      Live Retention --------------------+
            |
            v
       Live Publisher

The bootstrap process may take longer than expected.

Live acquisition must not wait for historical processing to finish.

### 1.4 Fundamental Retention Invariant

For a protected bootstrap interval, MarketForge must not delete accepted events required by that bootstrap before the consumer has safely advanced beyond them.

Conceptually:

    Bootstrap Protected Frontier
                |
                v
    Required Retained Events
                |
                v
    Consumer Acknowledges Progress
                |
                v
    Retention May Advance

If storage capacity becomes insufficient, the subsystem must report that the required guarantee cannot be maintained.

It must not silently discard protected events.

### 1.5 Initial Storage Strategy

The initial retention architecture uses:

    Bounded RAM
        +
    Local Parquet Spill

The database is not required.

The design intentionally avoids:

- Unbounded in-memory event accumulation.
- Mandatory PostgreSQL persistence.
- External message brokers.
- Shared-memory ring buffers.
- A permanent historical-data database inside the Rust Live process.

Parquet is temporary retention storage, not a replacement for the historical archive system.

### 1.6 Recovery Limitations

Live Retention can preserve only events that entered its accepted retention boundary.

It cannot reconstruct messages that the exchange never delivered.

For example:

    WebSocket Disconnected
             |
             v
    Missing Exchange Trades

The Trade Reconciliation Engine owns native trade recovery.

If those executions are successfully recovered and admitted as canonical events, they may subsequently enter retention.

Retention must preserve their provenance and canonical publication identities.

---

## 2. Architectural Boundaries

Retention is downstream of canonical market-data acceptance and independent of subscriber-specific delivery.

### 2.1 Component Ownership

| Component | Responsibility |
|---|---|
| Stream Registry | Logical stream identities and desired state |
| Trade Processor | Accepted canonical trade events |
| BookStore | Accepted canonical L2 state transitions |
| Publisher | Subscriber routing and Unix socket delivery |
| Live Retention Manager | Temporary canonical-event retention |
| Memory Retention Store | Bounded in-memory event storage |
| Parquet Spill Manager | Temporary disk-backed event segments |
| Retention Manifest Manager | Segment metadata and recoverable coverage |
| Bootstrap Coordinator | Historical-to-live transition coordination |
| Historical Data Subsystem | Historical archive retrieval and processing |
| Recording Manager | Optional PostgreSQL market-data persistence |
| Runtime Resource Manager | Shared memory and disk admission |
| Recovery Coordinator | Cross-component failure recovery |
| Metrics Registry | Retention and bootstrap measurements |

Every component must retain one authoritative ownership boundary.

### 2.2 Data Flow

Accepted canonical events may flow to several independent consumers.

    Accepted Canonical Event
               |
       +-------+--------+
       |       |        |
       v       v        v
    Publisher Retention Recording
       |       |        |
       v       v        v
    Unix IPC  RAM/     PostgreSQL
              Parquet

These destinations have independent completion states.

An event written to a subscriber socket is not necessarily retained.

An event retained in Parquet is not necessarily recorded in PostgreSQL.

### 2.3 Retention Admission Boundary

The subsystem must distinguish:

    Canonical Event Accepted

from:

    Retention Event Committed

Retention admission establishes responsibility for preserving the event according to the configured policy.

Conceptually:

    Canonical Event
          |
          v
    Retention Admission
          |
      +---+---+
      |       |
      v       v
    Accept  Reject / Backpressure
      |
      v
    Retention Commit
      |
      v
    Event Available for Replay

The meaning of `Retention Commit` depends on the durability tier.

An event committed to RAM is not crash-durable.

An event committed to a validated durable Parquet segment may survive a process restart.

The subsystem must expose this distinction.

### 2.4 Relationship to Publication

The Publisher may request retained events for subscriber replay.

However, the Publisher does not own:

- Retention segment files.
- Memory eviction.
- Parquet compaction.
- Retention manifests.
- Retention coverage evaluation.

The Publisher requests a replay range and receives an authoritative availability result.

### 2.5 Relationship to Historical Bootstrap

Historical bootstrap requires a stronger retention contract than ordinary opportunistic subscriber replay.

A bootstrap consumer may explicitly protect a cursor interval while historical processing is underway.

Protected data must not be evicted merely because the ordinary retention window has expired.

Protection remains subject to finite resource budgets.

If those budgets cannot satisfy the protection requirement, the system must report failure or apply an explicit backpressure policy.

### 2.6 Relationship to Recording

Temporary retention and PostgreSQL recording are separate.

| Property | Live Retention | PostgreSQL Recording |
|---|---|---|
| Primary purpose | Bootstrap overlap and bounded replay | Optional market-data persistence |
| Initial storage | RAM and Parquet | PostgreSQL |
| Required for bootstrap | Yes, when bootstrap uses live buffering | No |
| Retention policy | Temporary | Configured recording retention |
| Crash durability | Depends on storage tier | Depends on committed transaction |
| Owner | Live Retention Manager | Recording Manager |
| Subscriber replay | Supported when coverage exists | Not implicitly supported |

The Publisher must not automatically query PostgreSQL to replace missing temporary retention data.

Any future cross-store replay requires an explicit source-selection and identity contract.

---

## 3. Retention Guarantees

Retention guarantees must describe what is preserved, for how long, and at which durability level.

The subsystem must never use the term `retained` without defining the relevant guarantee.

### 3.1 Retention Dimensions

The model distinguishes:

    Admission

    Coverage

    Ordering

    Durability

    Availability

    Protection

These dimensions are related but not interchangeable.

### 3.2 Admission Guarantee

An accepted retention event has passed the configured admission boundary.

The subsystem must not silently discard an accepted event while continuing to report the relevant retention guarantee as satisfied.

### 3.3 Coverage Guarantee

Coverage describes which canonical publication positions remain available.

Conceptually:

    StreamId
    CursorEpoch
    CursorRange

Example:

    Stream 15
    Epoch 7

    Available:
        Cursor 100 through 250

Coverage must be derived from retained event identities and validated storage metadata.

File existence alone is not proof of complete coverage.

### 3.4 Ordering Guarantee

Retention preserves canonical publication order within each source stream.

Example:

    Cursor 100
    Cursor 101
    Cursor 102

A reader must not observe these records in an arbitrary order.

Physical Parquet row groups or files may be organized differently, provided the read interface restores the required logical ordering.

### 3.5 Durability Levels

The initial conceptual durability levels are:

    Volatile

    SpillPending

    DiskCommitted

| Level | Meaning |
|---|---|
| Volatile | Accepted in memory but not crash-durable |
| SpillPending | Disk persistence in progress |
| DiskCommitted | Validated durable segment and required manifest state committed |

`SpillPending` does not imply successful persistence.

A process crash during spill may leave the affected events unavailable after restart.

### 3.6 Protection Guarantee

A protected retention interval cannot be evicted until:

- The protecting consumer releases its requirement.
- The protection expires according to an explicit policy.
- Or the retention guarantee fails and the affected consumer is informed.

Expiration of a protection lease is not evidence that its consumer processed the protected events.

### 3.7 No Silent Loss

When a required retained interval becomes unavailable:

    RetentionCoverageLost

must be reported.

The system must preserve the affected interval and known evidence.

It must not silently advance a consumer's replay cursor.

### 3.8 Bounded Guarantee

Retention capacity is finite.

Therefore:

    Protected Retention
        !=
    Unlimited Retention

A consumer requesting protection beyond available resources must receive an explicit admission result.

The runtime may reject the request, apply a supported admission policy, or reserve additional permitted capacity.

It must not promise protection without an enforceable resource budget.

---

## 4. Retention Domain Models

The following models describe the logical retention contract.

They are conceptual Rust-domain structures, not finalized serialization schemas.

### 4.1 RetentionManager

    RetentionManager {
        runtime_instance_id

        stream_states
        memory_store
        spill_manager
        manifest_manager

        protection_registry
        resource_budget

        configuration_revision
    }

The manager coordinates admission, retention state, storage transitions, and read access.

### 4.2 RetentionStreamState

    RetentionStreamState {
        stream_id
        cursor_epoch

        latest_admitted_cursor
        latest_memory_cursor
        latest_disk_committed_cursor

        available_intervals
        unavailable_intervals

        active_protections
        retention_status
    }

These cursor values must not imply that all intermediate positions are available unless the relevant coverage has been verified.

A single latest cursor is insufficient to represent arbitrary holes.

### 4.3 RetainedEvent

    RetainedEvent {
        canonical_event_identity

        event_type
        event_timestamp
        received_timestamp

        configuration_revision
        metadata_revision

        canonical_payload
        provenance
    }

Retention preserves the original canonical event identity.

The same event must not acquire a new publication cursor merely because it moves from RAM to Parquet.

### 4.4 RetentionInterval

    RetentionInterval {
        stream_id
        cursor_epoch

        start_cursor
        end_cursor

        boundary_semantics
        coverage_status
        durability_level
    }

The interval must explicitly define whether its endpoints are inclusive or exclusive.

Internal APIs should use one consistent convention.

The initial recommended convention is:

    [start_cursor, end_cursor)

### 4.5 CoverageStatus

Supported states:

    Available
    PartiallyAvailable
    Unverified
    Unavailable

A partially available interval must not be reported as fully replayable.

### 4.6 RetentionProtection

    RetentionProtection {
        protection_id
        consumer_id

        stream_id
        cursor_epoch

        protected_start
        protected_end

        protection_kind
        state

        created_at
        expires_at
        last_confirmed_progress
    }

The protection interval may have an open upper boundary while live events continue arriving.

Its exact interpretation must be explicit.

### 4.7 ProtectionKind

Initial categories:

    Bootstrap
    SubscriberReplay
    Recovery
    InternalConsumer

Bootstrap protection should receive an explicit admission and lifecycle contract.

Short-lived subscriber replay protection may use different priority and expiration rules.

### 4.8 ProtectionState

Supported states:

    Requested
    Active
    Advancing
    Releasing
    Released
    Expired
    Failed

A protection must not be considered active until required resource and coverage conditions are validated.

### 4.9 RetentionReadRequest

    RetentionReadRequest {
        stream_id
        cursor_epoch

        start_cursor
        end_cursor

        maximum_events
        maximum_bytes

        required_durability
    }

Reads must remain bounded.

The consumer must be able to paginate through long intervals.

### 4.10 RetentionReadResult

    RetentionReadResult {
        requested_interval

        returned_interval
        events

        next_cursor
        coverage_status
        durability_level
    }

A successful read response must distinguish:

    EndOfAvailableData

from:

    MissingRequiredData

These outcomes are not equivalent.

---

## 5. Retention Lifecycle

Every retained event moves through an explicit lifecycle.

### 5.1 Event Lifecycle

    Canonical Event Accepted
              |
              v
    Retention Admission
              |
              v
    Memory Retained
              |
              v
    Spill Eligible
              |
              v
    Parquet Spill Pending
              |
              v
    Disk Segment Committed
              |
              v
    Memory Eviction Eligible
              |
              v
    Disk Retention
              |
              v
    Expiration Eligible
              |
              v
    Retention Released

Not every event must pass through every stage.

For example, an event may expire from memory without being spilled when no active guarantee requires disk retention.

### 5.2 Retention Status

Conceptual states:

    Healthy
    UnderPressure
    Degraded
    Failed
    Stopping

These describe subsystem health rather than an individual event's durability.

### 5.3 Admission Requirements

Before accepting an event, the subsystem checks:

- Valid canonical event identity.
- Valid source stream.
- Supported canonical event type.
- Compatible cursor epoch.
- Applicable retention policy.
- Available memory capacity.
- Required protection constraints.
- Storage availability when disk durability is required.

Admission must not depend on a synchronous PostgreSQL operation.

### 5.4 Retention Commit

A memory retention commit occurs when the event has been safely inserted into the authoritative in-memory retention structure.

A disk retention commit occurs only when the required Parquet and manifest durability conditions have been satisfied.

These completion points must be independently observable.

### 5.5 Memory-to-Disk Transition

An event may exist in both memory and a Parquet segment during spill.

This is expected.

Conceptually:

    RAM Copy
       +
    Disk Copy

The original event identity remains unchanged.

The retention index must not expose the event as two distinct canonical events.

### 5.6 Memory Eviction

Memory may be released after disk commit when:

- The disk copy is validated.
- Active protection requirements remain satisfied.
- Relevant readers can safely continue.
- The event is no longer required in memory.
- Eviction does not violate ordering or replay guarantees.

The manager must not evict the only valid copy of a protected event.

### 5.7 Disk Expiration

A Parquet segment may be deleted only when:

- No active protection requires it.
- No active reader depends on it.
- The configured retention policy allows removal.
- Required coverage metadata can be updated consistently.
- Deletion will not silently invalidate an admitted bootstrap guarantee.

Expiration must be coordinated with manifest updates.

### 5.8 Lifecycle Invariant

For each accepted event:

    Exactly One Canonical Identity

may have:

    Zero or One RAM Representation

and:

    Zero or More Physical Disk Representations

Physical duplication during spill, recovery, or compaction must not create duplicate logical events.

The authoritative retention index resolves physical copies to canonical identities.

---
## 6. Memory Retention Store

The Memory Retention Store provides bounded, low-latency storage for recently accepted canonical market-data events.

It is the first retention tier.

Events may subsequently move to Parquet when memory pressure increases or when a protection policy requires disk-backed retention.

### 6.1 Responsibilities

The Memory Retention Store must:

- Accept canonical events in publication order.
- Maintain independent retention state for each source stream.
- Enforce per-stream and global memory limits.
- Preserve canonical event identities.
- Support efficient cursor-based reads.
- Coordinate event ownership with active readers.
- Identify spill candidates.
- Prevent eviction of protected events without another valid retained copy.
- Report memory pressure.
- Release memory deterministically.

The store must not perform filesystem writes directly.

Parquet persistence belongs to the Spill Manager.

### 6.2 Memory Store Architecture

Conceptually:

    Retention Manager
           |
           v
    Memory Retention Store
           |
       +---+---+----------------+
       |       |                |
       v       v                v
    Stream A Stream B        Stream C
       |       |                |
       v       v                v
    Segments Segments        Segments

Each source stream maintains its own logical event sequence.

Global resource accounting remains shared.

### 6.3 Per-Stream Buffers

The initial recommended implementation uses segmented append-only buffers.

Conceptually:

    Stream A
        |
        +-- Segment 1
        |      Cursor 100–199
        |
        +-- Segment 2
        |      Cursor 200–299
        |
        +-- Segment 3
               Cursor 300–350

Segments become immutable after sealing.

This permits readers to access stable event ranges while new events continue arriving.

### 6.4 MemorySegment

Conceptual representation:

    MemorySegment {
        segment_id

        stream_id
        cursor_epoch

        first_cursor
        last_cursor

        events

        event_count
        retained_bytes

        state
    }

Segment state:

    Open
    Sealed
    SpillPending
    DiskBacked
    EvictionEligible

Only one authoritative open append segment should exist per stream generation.

### 6.5 Segment Sealing

An open segment may be sealed when:

- Its maximum event count is reached.
- Its maximum byte capacity is reached.
- Its age exceeds a configured threshold.
- A controlled flush is requested.
- The runtime is shutting down.
- A protection or durability transition requires it.

Sealing makes the segment immutable.

New events continue entering a new open segment.

### 6.6 Immutable Event Storage

Canonical events should be stored through immutable shared references where practical.

Conceptually:

    Arc<CanonicalEvent>

This permits Publisher and Retention consumers to reference the same accepted event without unnecessary payload copying.

The precise representation is an implementation decision.

Reference ownership must remain bounded.

### 6.7 Event Ordering

Within one source stream, retained events follow their canonical publication order.

The Memory Store must reject or explicitly reconcile:

- Duplicate canonical event identities.
- Obsolete cursor epochs.
- Invalid cursor transitions.
- Conflicting records with the same identity.

Cursor continuity must follow the canonical contract.

The store must not assume that numeric cursor values are globally comparable across different streams or epochs.

### 6.8 Event-Time Ordering

Retention preserves publication order, not necessarily event-time order.

For example:

    Cursor 100:
        EventTime 12:00:05

    Cursor 101:
        EventTime 12:00:06

    Cursor 102:
        EventTime 12:00:03
        Provenance = HistoricalRecovery

The event with the older timestamp remains at cursor 102.

The Retention Manager must not rewrite canonical publication identities to make timestamps monotonic.

Historical bootstrap may require event-time reconciliation, but that belongs to the Bootstrap Coordinator.

### 6.9 Memory Read Operations

The Memory Store supports bounded reads by:

    StreamId
    CursorEpoch
    CursorRange

Reads must:

- Preserve publication ordering.
- Respect requested event and byte limits.
- Return explicit coverage information.
- Avoid blocking live ingestion for extended periods.
- Detect expired or unavailable cursors.

Large reads must be paginated.

### 6.10 Reader Isolation

An active reader must not prevent all future retention eviction indefinitely.

The implementation may use:

- Reference-counted immutable segments.
- Short-lived read leases.
- Bounded cursor iterators.
- Explicit reader deadlines.

A stalled reader must eventually release its resource reservation or fail according to policy.

### 6.11 Memory Store Invariants

    Every retained event has one
    canonical publication identity.

    Events remain ordered within
    their source-stream cursor domain.

    Segment memory is bounded.

    Readers never observe partially
    initialized segments.

    Protected events cannot disappear
    through ordinary memory eviction.

    A sealed segment is immutable.

---

## 7. Retention Resource Budgets

Retention must enforce both per-stream and runtime-wide resource limits.

A collection of individually bounded streams can still exhaust system memory if their combined allocations are not controlled.

### 7.1 Resource Budget Model

Conceptually:

    RetentionResourceBudget {
        maximum_memory_bytes
        maximum_disk_bytes

        maximum_stream_memory_bytes
        maximum_stream_disk_bytes

        maximum_open_segments
        maximum_pending_spills

        maximum_concurrent_readers
        maximum_concurrent_spills
    }

The exact configuration keys belong to **03 — Configuration System**.

### 7.2 Global Memory Budget

The global memory budget covers retention-owned allocations, including:

- Stored canonical events.
- Segment metadata.
- Retention indexes.
- Pending spill buffers.
- Reader-held segment references.
- Protection metadata.

Shared allocations must be accounted for consistently.

The implementation must avoid double-counting the same immutable event buffer while also avoiding undercounting retained references.

### 7.3 Per-Stream Memory Budget

Each source stream has a configurable memory allocation policy.

The system may support:

    FixedPerStreamBudget

or:

    SharedGlobalPoolWithStreamLimits

The recommended initial approach is a shared global pool with enforceable per-stream limits.

This avoids reserving large amounts of unused memory for inactive streams.

### 7.4 Disk Budget

The disk budget applies to temporary retention files and their required metadata.

Relevant storage includes:

- Parquet segments.
- Temporary spill files.
- Segment manifests.
- Retention checkpoints.
- Recovery metadata.

Disk reservations should account for estimated output size and temporary write amplification.

### 7.5 Shared Runtime Resources

Retention competes with other components for:

- Process memory.
- Filesystem capacity.
- File descriptors.
- CPU.
- Disk I/O.

The Runtime Resource Manager remains authoritative for shared resource budgets.

Retention maintains its own allocations within those limits.

### 7.6 Admission Before Allocation

Before allocating a new retention segment:

    New Event
        |
        v
    Estimate Required Capacity
        |
        v
    Check Stream Budget
        |
        v
    Check Global Budget
        |
        v
    Reserve Capacity
        |
        v
    Append Event

The subsystem must not allocate unbounded buffers and only discover capacity exhaustion afterward.

### 7.7 Memory Pressure Thresholds

Conceptual states:

    Normal
    Elevated
    Critical
    Exhausted

| State | Behavior |
|---|---|
| Normal | Accept events under ordinary policy |
| Elevated | Prefer sealing and asynchronous spill |
| Critical | Prioritize spill, restrict optional readers, apply admission policy |
| Exhausted | Reject unsafe allocations and escalate |

Thresholds are configurable.

The Retention Manager must not repeatedly oscillate between states because of small fluctuations around one threshold.

### 7.8 Resource Reservation

A reservation represents capacity committed to an operation.

Conceptually:

    ResourceReservation {
        reservation_id
        owner
        resource_kind
        reserved_amount
        state
    }

Reservations must be released when operations complete, fail, or are cancelled.

### 7.9 Disk Spill Priority

When memory pressure increases, spill selection should consider:

- Segment age.
- Segment size.
- Protection requirements.
- Available disk capacity.
- Number of active readers.
- Existing spill activity.

Protected segments may be prioritized for disk persistence because they cannot otherwise be safely discarded.

### 7.10 Resource Exhaustion

If memory is exhausted and disk spill is unavailable:

    Memory Exhausted
           +
    Spill Unavailable
           |
           v
    Required Retention Cannot Progress

The subsystem must report the failure.

It must not silently evict protected data.

The configured policy determines whether upstream backpressure, controlled stream degradation, or runtime escalation is required.

---

## 8. Retention Protection Leases

Retention protection prevents the eviction of events required by an active consumer.

The most important initial use case is historical bootstrap.

### 8.1 Protection Purpose

Suppose historical bootstrap begins at:

    12:00:00

Historical processing continues until:

    12:15:00

Meanwhile, live events arrive continuously.

The bootstrap consumer must be able to access the accepted live events required to bridge that interval.

Ordinary retention expiration must not remove those events before bootstrap reaches them.

### 8.2 Protection Identity

Every protection receives a unique:

    ProtectionId

The protection is associated with:

    ConsumerId
    StreamId
    CursorEpoch

A consumer may hold several independent protections.

### 8.3 ProtectionLease

Conceptual representation:

    ProtectionLease {
        protection_id
        consumer_id

        stream_id
        cursor_epoch

        protected_start
        protected_end

        acknowledged_frontier

        created_at
        expires_at

        state
        reservation_id
    }

A protection may have an open upper boundary.

For example:

    Protect from Cursor 100
    through the current live frontier.

The upper boundary advances as new events arrive.

### 8.4 Protection Admission

A protection must be explicitly admitted.

Conceptually:

    Protection Requested
             |
             v
    Validate Stream
             |
             v
    Validate Starting Cursor
             |
             v
    Check Existing Coverage
             |
             v
    Evaluate Resource Budget
             |
             v
    Reserve Protection Capacity
             |
             v
    Activate Protection

A request must fail if its required starting coverage has already expired.

### 8.5 Open-Ended Protection

An open-ended protection covers an interval whose end advances with the live publication frontier.

This is required for long-running historical bootstrap.

However, an open-ended protection must not be interpreted as a promise of unlimited storage.

Admission authorizes operation under a finite resource policy.

If future event volume exceeds available capacity, the subsystem must escalate explicitly.

### 8.6 Protection Progress

A consumer may report that it no longer requires events before a particular cursor.

Conceptually:

    AdvanceProtection {
        protection_id
        acknowledged_cursor
    }

The acknowledged frontier must advance monotonically.

The Retention Manager may then release older protected data when no other protections require it.

### 8.7 Protection Advancement Safety

The Retention Manager must distinguish:

    Event Delivered to Consumer

from:

    Event Safely Consumed

A bootstrap consumer should advance its protection only after establishing that the corresponding events no longer need to be replayed for its processing contract.

For example, receiving a batch in memory may not be sufficient if the consumer has not yet committed its output.

### 8.8 Protection Overlap

Several consumers may protect overlapping intervals.

Example:

    Bootstrap A:
        Cursor 100–500

    Bootstrap B:
        Cursor 300–700

The effective protected union is:

    Cursor 100–700

However, releasing Bootstrap A does not permit eviction of events still required by Bootstrap B.

Protection accounting must preserve individual ownership.

### 8.9 Protection Expiration

Protection leases may have deadlines.

Expiration is necessary to prevent abandoned consumers from retaining data forever.

However:

    LeaseExpired

does not imply:

    ConsumerCompleted

If a protection expires before the consumer safely advances, the system must report a protection failure.

The consumer must not continue assuming that its required events remain available.

### 8.10 Lease Renewal

An active consumer may renew its protection before expiration.

Renewal must validate:

- Consumer ownership.
- Current protection generation.
- Resource policy.
- Protection state.
- Applicable maximum lifetime.

Renewal must not silently bypass resource limits.

### 8.11 Consumer Failure

If a bootstrap consumer crashes:

- Its protection remains governed by the configured lease policy.
- The retention manager may preserve data until the lease expires.
- A restarted consumer may resume only through a valid identity and recovery contract.
- Expired coverage must not be reported as recoverable without evidence.

### 8.12 Protection Release

A consumer releases its protection when it no longer requires the retained interval.

Conceptually:

    ReleaseProtection {
        protection_id
        consumer_id
    }

Release must be idempotent.

Resources become eviction-eligible only when all applicable protections permit it.

### 8.13 Protection Invariants

    Protection cannot begin before
    its required coverage is validated.

    Protected events cannot be silently evicted.

    Progress acknowledgements are monotonic.

    One consumer cannot release
    another consumer's protection.

    Lease expiration is not proof
    of successful consumption.

    Open-ended protection remains
    subject to finite resource capacity.

---

## 9. Per-Stream Retention Policies

Different source streams may have different throughput, event sizes, and bootstrap requirements.

The Retention Manager must support per-stream policy selection without losing global resource control.

### 9.1 RetentionPolicy

Conceptual representation:

    RetentionPolicy {
        enabled

        memory_budget_bytes
        memory_retention_duration

        spill_enabled
        spill_threshold_bytes
        disk_budget_bytes

        disk_retention_duration

        protection_enabled

        configuration_revision
    }

The exact configuration structure belongs to document 03.

### 9.2 Policy Sources

Retention policy may be selected through:

    Global Defaults
          |
          v
    Exchange / Market Overrides
          |
          v
    Stream Overrides
          |
          v
    Effective Retention Policy

The Configuration System resolves these layers.

The Retention Manager receives the compiled effective policy.

### 9.3 Stream Throughput Differences

A high-volume trade stream may require more frequent spill than a low-volume stream.

Similarly, L2 event sizes may be significantly larger than individual trades.

Policies may therefore differ by:

- Channel type.
- Expected event throughput.
- Average payload size.
- Bootstrap priority.
- Configured memory allowance.
- Disk spill requirements.

### 9.4 Retention Disabled

A source stream may have retention disabled when no consumer requires it.

In that case:

    RetentionEnabled = false

The Publisher may still distribute canonical events.

However, the Publisher must not advertise cursor replay availability for events that were never retained.

### 9.5 Protected Stream Requirements

A bootstrap protection request may require retention to be active.

If retention is disabled, the bootstrap coordinator must obtain an explicit activation result before relying on it.

The Retention Manager must not silently begin protection from an earlier cursor that is no longer available.

### 9.6 Policy Changes

Some retention settings may be hot-editable.

Examples include:

    Memory Retention Duration

    Spill Threshold

    Disk Retention Duration

    Stream Memory Budget

Changes must be validated against active protections.

A new policy must not immediately delete data required by already admitted bootstrap consumers.

### 9.7 Budget Reduction

Suppose a stream has:

    Memory Budget = 1 GiB

and the operator changes it to:

    Memory Budget = 256 MiB

The Retention Manager must evaluate whether the existing retained state can satisfy the new limit.

Possible outcomes include:

- Spill eligible segments to disk.
- Gradually evict unprotected events.
- Reject the configuration change.
- Enter a controlled degraded state.

The subsystem must not delete protected events merely to satisfy the new limit.

### 9.8 Policy Revisions

Each retention policy has a configuration revision.

Retention metadata should preserve the effective policy revision when it affects durability or expiration semantics.

This permits diagnostics after configuration changes.

### 9.9 Policy Isolation

Changing retention policy for one source stream must not unnecessarily interrupt unrelated streams.

However, global budgets remain shared.

A stream consuming excessive resources may affect global admission capacity and therefore requires explicit resource accounting.

---

## 10. Memory Eviction and Spill Admission

Memory eviction and Parquet spill are separate operations.

An event may be evicted from RAM only when its required retention guarantees remain satisfied.

### 10.1 Eviction Eligibility

A memory segment becomes eviction-eligible when:

- It is sealed.
- No active reader requires its in-memory representation.
- No policy requires it to remain in memory.
- It has a valid disk copy if protected persistence requires one.
- All applicable protection constraints are satisfied.

### 10.2 Spill Eligibility

A segment becomes spill-eligible when:

- It is sealed.
- Spill is enabled.
- A valid disk reservation can be obtained.
- The segment has not already been committed to disk.
- The segment is not already being spilled.
- Its metadata is valid.

### 10.3 Spill Admission Sequence

    Sealed Memory Segment
              |
              v
    Evaluate Spill Policy
              |
              v
    Check Disk Capacity
              |
              v
    Reserve Disk Budget
              |
              v
    Mark SpillPending
              |
              v
    Submit to Spill Manager

The Spill Manager performs the filesystem and Parquet operations.

### 10.4 Eviction After Spill

After a successful disk commit:

    Memory Segment
          |
          v
    Disk Commit Confirmed
          |
          v
    Retention Index Updated
          |
          v
    Memory Eviction Eligible
          |
          v
    Release Memory

The disk commit must be validated before releasing the only required in-memory copy.

### 10.5 Spill Failure

If a spill fails:

- The segment remains available in memory if possible.
- The disk reservation is reconciled.
- The failure is reported.
- Retry policy is applied.
- Memory admission is reevaluated.
- Required retention guarantees remain explicit.

A failed spill must not advance the disk durability frontier.

### 10.6 Eviction Without Spill

Unprotected events may be evicted directly from memory when their retention policy allows expiration.

The Retention Manager must update coverage metadata accordingly.

Expired events must not remain advertised as replayable.

### 10.7 Eviction Ordering

Eviction should generally prioritize the oldest eligible data.

However, active protections and reader references may prevent strict age-based eviction.

The manager must evaluate eligibility rather than delete segments solely by timestamp.

### 10.8 Eviction Atomicity

Coverage metadata must not advertise an interval as available after its final physical copy has been removed.

Deletion and index updates must follow a crash-safe procedure where persistence guarantees require it.

### 10.9 Memory Pressure and Slow Spill

If event ingestion exceeds spill throughput:

    Ingestion Rate > Spill Rate
              |
              v
    Memory Usage Increases
              |
              v
    Memory Pressure
              |
              v
    Admission Restriction / Escalation

The subsystem must not assume that the disk writer can always keep up with live acquisition.

### 10.10 Backpressure Boundary

If retention becomes mandatory for a protected stream and the subsystem cannot admit the next event, upstream processing must receive an explicit failure or backpressure signal.

The runtime must not silently bypass required retention.

Importantly, applying backpressure to a live exchange feed does not guarantee that the exchange will preserve messages while the consumer is blocked.

If a WebSocket reader stalls and the exchange continues publishing, data may be lost upstream.

Therefore, the preferred response is to:

1. Preserve bounded transport and processing buffers.
2. Prioritize retention spill.
3. Restrict optional workloads.
4. Escalate before buffers are exhausted.
5. Mark required coverage incomplete if loss becomes unavoidable.

Backpressure is a resource-control mechanism, not a guarantee that the exchange will stop producing data.

---

## 11. Resource and Protection Coordination

Retention budgets, protection leases, and spill operations must be coordinated through one authoritative admission model.

### 11.1 Protected Event Admission

For an active protection:

    Incoming Canonical Event
              |
              v
    Determine Applicable Protections
              |
              v
    Reserve Retention Capacity
              |
              v
    Commit Event to Memory
              |
              v
    Update Protected Frontier

The protection frontier must not advance beyond events that the Retention Manager has actually accepted.

### 11.2 Resource Shortage

When protection requires additional storage but no capacity remains:

    Required Event
         |
         v
    Admission Cannot Succeed
         |
         v
    Protection Guarantee Impaired
         |
         v
    Failure Report
         |
         v
    Recovery Coordinator

The system must identify the affected stream and protected cursor interval.

### 11.3 Admission Fairness

A single high-throughput stream must not monopolize all retention resources without an explicit policy.

The admission controller should support per-stream limits and shared global capacity.

Priority may consider:

- Active bootstrap consumers.
- Required retention guarantees.
- Available disk spill capacity.
- Source throughput.
- Recovery urgency.
- Existing protected intervals.

### 11.4 Protection and Expiration

Ordinary retention expiration may proceed while a bootstrap protection is active.

However, expiration must exclude the protected interval.

Example:

    Retained:
        Cursor 100–500

    Protected:
        Cursor 300–500

    Expiration Eligible:
        Cursor 100–299

This assumes no other reader or protection requires the older records.

### 11.5 Protection and Replay

Subscriber replay may temporarily protect an interval while it is being read.

Such protection is generally shorter-lived than bootstrap protection.

The Retention Manager must prevent deletion of active reader data during the authorized read interval.

### 11.6 Restart Implications

An in-memory protection lease does not automatically survive a process crash.

If bootstrap requires protection continuity across restarts, the relevant lease and coverage metadata must be persisted through an explicit recovery contract.

The system must not reconstruct a protection from memory-only state that no longer exists.

### 11.7 Final Coordination Invariant

    Retention Admission
            +
    Memory Budgets
            +
    Disk Budgets
            +
    Protection Leases
            +
    Spill Completion
            |
            v
    One Consistent Coverage Model

No component may independently declare protected data safe to delete without consulting the authoritative retention state.

---

## 12. Parquet Spill Architecture

The Parquet Spill subsystem moves sealed memory segments into temporary disk-backed storage.

Its purpose is to prevent long-running historical bootstrap and subscriber replay from requiring unbounded RAM.

Parquet spill is not the permanent historical archive system.

### 12.1 Responsibilities

The Parquet Spill subsystem must:

- Accept immutable sealed memory segments.
- Encode canonical events into a versioned Parquet schema.
- Preserve exact canonical event identities.
- Preserve event ordering and provenance.
- Write temporary files without exposing incomplete segments.
- Validate completed Parquet files.
- Commit segment metadata through the Manifest Manager.
- Report durable completion.
- Detect and recover interrupted writes.
- Enforce disk-space and concurrency budgets.
- Support bounded reads.
- Delete expired segments safely.

### 12.2 Component Structure

    Live Retention Manager
              |
              v
       Spill Scheduler
              |
              v
        Spill Worker Pool
              |
              v
       Parquet File Writer
              |
              v
       Segment Validation
              |
              v
        Manifest Manager
              |
              v
       Durable Commit Result

The Spill Scheduler selects eligible segments.

Spill workers perform serialization and filesystem operations.

The Manifest Manager maintains authoritative disk-coverage metadata.

### 12.3 Separate Spill Execution

Spill processing must not block the canonical-event ingestion hot path.

The initial recommended implementation uses asynchronous coordination with a bounded pool of blocking or dedicated storage workers.

Parquet encoding and filesystem operations may consume significant CPU and disk I/O.

They should not run directly inside latency-sensitive WebSocket processing tasks.

### 12.4 Spill Scheduling

A spill job is created when a sealed memory segment satisfies the configured spill policy.

Possible triggers:

- Memory pressure.
- Segment size threshold.
- Segment age threshold.
- Active bootstrap protection.
- Explicit flush request.
- Graceful shutdown.
- Durability requirement.

The scheduler must avoid submitting duplicate spill jobs for the same authoritative segment.

### 12.5 Spill Job

Conceptual representation:

    SpillJob {
        spill_id
        segment_id

        stream_id
        cursor_epoch

        memory_segment_ref

        destination_path
        schema_version

        resource_reservation
        state

        created_at
        started_at
        completed_at
    }

The immutable memory-segment reference must remain valid until the spill attempt terminates.

### 12.6 Spill States

Supported states:

    Created
    PendingAdmission
    Scheduled
    Writing
    Validating
    Committing
    Completed
    Failed
    Cancelled

A spill operation must not be reported as completed merely because the Parquet writer closed its file handle.

The required durability and manifest conditions must also be satisfied.

### 12.7 Spill Concurrency

The Spill Scheduler must enforce:

    MaximumConcurrentSpills

    MaximumPendingSpillJobs

    MaximumSpillMemoryBytes

    MaximumDiskWriteConcurrency

These limits are configurable.

Concurrent spills for different streams may execute independently.

Within one stream, physical segment writes may execute concurrently, but authoritative coverage updates must preserve correct cursor-range relationships.

### 12.8 Spill Failure Isolation

A failed spill should affect the relevant retention segment and its dependent guarantees.

It must not automatically terminate healthy exchange connections.

However, repeated spill failures may eventually exhaust retention memory and require system-level escalation.

---

## 13. Parquet Segment Format

Each completed Parquet file contains a bounded range of canonical events from one source stream and cursor epoch.

The initial recommendation is to avoid mixing independent streams inside one physical segment.

This simplifies indexing, replay, protection, deletion, and crash recovery.

### 13.1 Segment Identity

Each Parquet segment receives a unique:

    RetentionSegmentId

Conceptually:

    RetentionSegmentIdentity {
        stream_id
        cursor_epoch
        segment_id
    }

The identifier must remain unique across process restarts.

A segment identifier is not an exchange-native sequence number.

### 13.2 Single-Stream Segment Rule

The initial format uses:

    One StreamId
          +
    One CursorEpoch
          +
    One Parquet Segment

A segment may contain multiple canonical event types only if they belong to the same logical source stream and the selected schema explicitly supports them.

For example, L2 snapshots and updates may require a tagged event representation.

### 13.3 Shared Historical Parquet Schema

The Live Retention subsystem must use the same canonical Parquet
data schema as the existing MarketForge historical pipeline.

The historical pipeline's Parquet schema is the authoritative
storage contract for canonical market-data records.

The Live Spill Manager must not introduce a second incompatible
market-data schema.

Compatibility requirements include:

- Identical market-data column names.
- Identical physical and logical column types.
- Identical decimal precision and scale semantics.
- Identical timestamp units and timezone semantics.
- Identical native identifier representations.
- Identical nullability and field semantics.
- Identical trade and L2 event representations.
- Shared schema versioning and validation rules.

Live spill files must be readable by the existing historical
Parquet reader without schema conversion.

### 13.4 Separation of Data and Retention Metadata

Live-specific retention metadata must remain separate from the
canonical historical Parquet data schema unless already included
in that shared schema.

Examples include:

    StreamCursor
    CursorEpoch
    RetentionSegmentId
    ProtectionState
    ManifestRevision
    SpillCommitState

Such information belongs in retention manifests, Parquet file
metadata, or dedicated sidecar indexes.

Per-event publication identities must remain recoverable when
required for exact cursor replay.

A segment-level cursor range alone is insufficient unless the
canonical cursor contract permits reconstructing individual
event identities without ambiguity.

### 13.5 Shared Schema Evolution

Historical and Live pipelines must use compatible versions of
the same canonical Parquet schema.

Schema changes must be coordinated between both pipelines.

A Live-specific schema change must not silently break the
historical Parquet reader.

Differences in compression, row-group size, file naming, and
physical segment layout are permitted when they preserve
the shared data schema and reader compatibility.
### 13.6 Exact Decimal Preservation

Prices and quantities must not be converted to floating-point values during Parquet encoding.

The schema must preserve canonical decimal semantics.

Supported approaches include:

- Arrow decimal types with sufficient precision and scale.
- Exact coefficient-and-scale encoding.
- Versioned binary canonical payloads.

If Arrow decimal precision limits are insufficient for a canonical value, the writer must use an explicitly supported lossless representation or reject the write.

It must not silently round the value.

### 13.7 Cursor Preservation

Every stored event retains its original canonical cursor.

Example:

    Memory:

        Cursor 100
        Cursor 101
        Cursor 102

    Parquet:

        Cursor 100
        Cursor 101
        Cursor 102

Spill must not assign replacement publication identities.

### 13.8 Event Ordering

Rows should be written in canonical publication order.

The segment metadata records the first and last cursor.

However, these boundaries alone do not prove continuity unless the cursor contract guarantees that every intermediate position represents a required event.

Coverage validation must follow the canonical cursor semantics.

### 13.9 Segment Metadata

A completed Parquet segment must have metadata describing:

    SegmentId
    StreamId
    CursorEpoch

    FirstCursor
    LastCursor

    EventCount
    FileSizeBytes

    CanonicalSchemaVersion
    ParquetSchemaVersion

    ConfigurationRevisionRange
    MetadataRevisionRange

    CreatedAt
    CommittedAt

    IntegrityChecksum

The checksum algorithm and exact fields belong to the finalized storage schema.

### 13.10 Segment Size

Segment sizing may consider:

- Maximum event count.
- Maximum uncompressed bytes.
- Maximum encoded bytes.
- Maximum segment age.
- Parquet row-group size.
- Expected event throughput.

No single segment may grow without bound.

### 13.11 Compression

Parquet compression is configurable.

The initial implementation should benchmark supported codecs against:

- Compression ratio.
- Encoding CPU cost.
- Decoding CPU cost.
- Spill throughput.
- Replay latency.

Compression must not affect canonical numerical precision.

### 13.12 File Naming

File names should be generated from stable, sanitized identifiers.

An illustrative layout:

    data/.runtime/live/retention/
        streams/
            <stream-id>/
                <cursor-epoch>/
                    <segment-id>.parquet

Temporary writes use a distinct extension:

    <segment-id>.parquet.tmp

The actual naming convention must avoid path traversal and collisions.

File paths are implementation details, not canonical event identities.

---

## 14. Atomic Parquet Spill Commit

A spill operation must not expose partially written Parquet files as valid retained coverage.

The commit procedure separates data-file creation from authoritative manifest publication.

### 14.1 Commit Sequence

    Immutable Memory Segment
              |
              v
    Reserve Disk Capacity
              |
              v
    Create Temporary File
              |
              v
    Encode Parquet Contents
              |
              v
    Flush File Data
              |
              v
    Validate Parquet Segment
              |
              v
    Synchronize File Durability
              |
              v
    Rename to Final Segment Path
              |
              v
    Synchronize Directory Metadata
              |
              v
    Commit Manifest Update
              |
              v
    Confirm DiskCommitted

The exact filesystem synchronization requirements depend on the selected durability policy.

For crash-durable `DiskCommitted` semantics, successful file closure alone is insufficient.

### 14.2 Temporary File Creation

Each spill writes to a unique temporary file.

The writer must not overwrite an existing committed segment.

Temporary file creation should use exclusive creation semantics.

This prevents two workers from accidentally writing the same destination.

### 14.3 Partial Write Protection

Until the file is complete and validated:

    SegmentState = Writing

The file must not appear in the authoritative retention index.

Readers must not consume incomplete temporary files.

### 14.4 Parquet Finalization

The writer must finalize the Parquet footer and required metadata before validating the file.

A file that exists but lacks a valid footer must not be considered committed.

### 14.5 Segment Validation

Validation should confirm:

- File can be opened.
- Parquet metadata is readable.
- Expected schema version matches.
- Expected stream identity matches.
- Expected cursor epoch matches.
- Event count matches.
- Cursor boundaries match.
- Required event identities are valid.
- Integrity checksum matches, where enabled.

Validation may combine writer-side checks with bounded post-write inspection.

The implementation must not require decoding the complete file multiple times unless the selected integrity policy requires it.

### 14.6 File Synchronization

For crash-durable storage, the writer must ensure the completed file is synchronized according to the operating system and filesystem guarantees.

On Unix-like systems, this generally requires appropriate file synchronization operations.

The contract must distinguish:

    WrittenToPageCache

from:

    SynchronizedToStorage

The latter is subject to the actual filesystem and hardware durability guarantees.

### 14.7 Atomic Rename

The temporary file is renamed to its final path only after successful validation and required file synchronization.

The rename must occur within a filesystem where the operation provides the required atomicity.

Cross-filesystem moves must not be treated as atomic renames.

### 14.8 Directory Durability

A crash-durable segment commit may require synchronizing the directory containing the renamed file.

Without this step, filesystem metadata changes may not survive a sudden crash even when the file contents were synchronized.

### 14.9 Manifest Commit

After the final Parquet file exists, the Manifest Manager commits the segment entry.

The manifest must not advertise a segment before its data file satisfies the required durability contract.

### 14.10 Commit Acknowledgement

A spill operation becomes:

    Completed

only after the required file and manifest commit conditions succeed.

The Retention Manager may then mark the segment:

    DiskCommitted

and evaluate memory eviction eligibility.

### 14.11 Crash Windows

The procedure must tolerate crashes at any point.

| Crash point | Expected recovery |
|---|---|
| Before temporary file creation | Memory-only data may be lost |
| During temporary write | Temporary file rejected or quarantined |
| After write, before validation | File requires validation |
| After validation, before rename | Temporary file reconciled |
| After rename, before manifest commit | Final file treated as unreferenced until reconciled |
| During manifest commit | Recover last valid manifest revision |
| After manifest commit | Validate committed segment and restore coverage |

A crash between final-file creation and manifest commit must not automatically cause the file to be deleted.

It may contain valid recoverable data.

### 14.12 Idempotent Spill Recovery

Recovery may encounter an already completed file for a spill operation whose completion acknowledgement was lost.

The subsystem must reconcile the existing file rather than blindly overwrite it.

A valid matching segment may be adopted into the manifest.

A conflicting file must be quarantined and reported.

---

## 15. Retention Manifest

The Retention Manifest records which disk-backed segments are authoritative and which cursor intervals they cover.

It is required for efficient restart recovery and bounded replay lookup.

### 15.1 Manifest Responsibilities

The Manifest Manager must:

- Register committed Parquet segments.
- Maintain per-stream disk coverage.
- Track segment paths and integrity metadata.
- Track schema versions.
- Support cursor-range lookup.
- Support safe deletion.
- Detect inconsistent metadata.
- Restore state after restart.
- Reconcile orphaned and incomplete files.

### 15.2 Manifest Model

Conceptually:

    RetentionManifest {
        manifest_version
        manifest_revision

        runtime_instance_id

        committed_segments
        coverage_intervals

        created_at
        updated_at
    }

The logical manifest may be represented physically as one or more files.

The implementation must avoid rewriting an unbounded global manifest on every segment commit.

### 15.3 Manifest Entry

Conceptually:

    RetentionSegmentEntry {
        segment_id

        stream_id
        cursor_epoch

        first_cursor
        last_cursor
        event_count

        file_path
        file_size_bytes

        canonical_schema_version
        parquet_schema_version

        checksum
        committed_at

        state
    }

### 15.4 Manifest Entry States

Supported states:

    Committed
    Deleting
    Deleted
    Quarantined

Temporary writes are not part of authoritative committed coverage.

### 15.5 Manifest Storage Strategy

The initial implementation may use:

    Versioned Manifest Snapshots

or:

    Append-Only Manifest Journal
             +
    Periodic Compaction

The selected approach must support:

- Atomic updates.
- Crash recovery.
- Bounded startup time.
- Efficient segment lookup.
- Safe deletion.
- Schema evolution.

A simple versioned manifest snapshot is acceptable initially if its update cost remains bounded.

### 15.6 Manifest Revision

Every successful manifest update advances:

    ManifestRevision

Readers may use this revision to detect changes during a long-running replay operation.

Manifest revisions are not canonical event cursors.

### 15.7 Coverage Index

The manifest maintains a searchable mapping:

    StreamId
        |
        v
    CursorEpoch
        |
        v
    Ordered Segment Ranges

Example:

    Stream 15
      Epoch 7

        Segment A: 100–199
        Segment B: 200–299
        Segment C: 300–399

The index must detect missing or overlapping ranges.

Physical overlap may be valid during controlled recovery or compaction.

Logical duplicate events must not be exposed to readers.

### 15.8 Coverage Verification

A valid segment manifest entry establishes that the segment's records are available under its storage contract.

It does not automatically prove that every native exchange event was acquired.

Retention coverage must remain distinct from source acquisition completeness.

### 15.9 Manifest Corruption

If manifest metadata is corrupted:

- Preserve existing Parquet files.
- Load the last valid manifest revision when available.
- Scan and validate candidate segments as required.
- Reconstruct recoverable coverage.
- Report unresolved inconsistencies.
- Avoid silently claiming complete retention.

### 15.10 Manifest Ownership

Only the Manifest Manager may publish authoritative manifest revisions.

Spill workers submit completed segment information.

Readers consume validated manifest snapshots.

Cleanup workers request manifest transitions through the manager.

---

## 16. Crash Recovery and Segment Reconciliation

After an unclean shutdown, the Retention Manager must reconstruct valid disk-backed coverage before advertising replay availability.

### 16.1 Recovery Objectives

Restart recovery must identify:

- Valid committed segments.
- Valid but unindexed segments.
- Incomplete temporary files.
- Missing manifest-referenced files.
- Corrupted Parquet files.
- Duplicate physical segments.
- Overlapping cursor ranges.
- Unknown schema versions.
- Incomplete deletion operations.

### 16.2 Recovery Sequence

    Runtime Starts
          |
          v
    Acquire Retention Storage Ownership
          |
          v
    Load Last Valid Manifest
          |
          v
    Inspect Retention Directory
          |
          v
    Reconcile Manifest and Files
          |
          v
    Validate Recoverable Segments
          |
          v
    Reconstruct Coverage Index
          |
          v
    Identify Coverage Gaps
          |
          v
    Publish Recovery Result
          |
          v
    Enable Retention Reads

Live ingestion may begin earlier if the architecture provides a separate new-runtime retention epoch and prevents conflicts with recovery of previous storage state.

Readers must not receive unverified historical coverage as though it were valid.

### 16.3 Temporary Files

Files ending in the configured temporary extension require inspection.

Possible outcomes:

    Valid And Recoverable
    Incomplete
    Corrupted
    Unknown

A recoverable temporary file may be finalized through the normal commit protocol.

An incomplete or corrupted file must not be indexed as valid coverage.

### 16.4 Orphaned Final Files

A final Parquet file that is absent from the manifest may result from a crash between atomic rename and manifest commit.

The recovery procedure must:

1. Validate the file.
2. Extract or verify segment identity.
3. Check for conflicting manifest entries.
4. Validate cursor and schema metadata.
5. Adopt the segment if safe.
6. Otherwise quarantine it.

### 16.5 Missing Files

A manifest may reference a file that no longer exists.

The subsystem must:

- Mark its coverage unavailable.
- Report the inconsistency.
- Reconcile the manifest.
- Identify affected protections or replay requirements.

The manifest must not continue advertising nonexistent data.

### 16.6 Corrupted Segments

Corrupted segments must be isolated.

Possible causes include:

- Interrupted writes.
- Filesystem corruption.
- Storage hardware failure.
- Unsupported schema interpretation.
- Checksum mismatch.

A corrupted segment must not be decoded into canonical events without successful validation.

### 16.7 Duplicate Segments

Two physical files may contain the same canonical event range.

This may occur after retry or interrupted cleanup.

The subsystem must not expose duplicate logical records.

The Manifest Manager determines the authoritative physical representation.

Conflicting duplicates require explicit reconciliation.

### 16.8 Overlapping Segments

Overlapping ranges may be legitimate after compaction or recovery.

Example:

    Segment A:
        Cursor 100–199

    Segment B:
        Cursor 150–249

The manifest must not treat overlap as proof of duplicate canonical identity without inspecting the applicable cursor semantics.

For identical cursor identities, payload conflicts must be reported as integrity failures.

### 16.9 Recovery of Volatile Events

Events committed only to memory before the crash may be permanently unavailable.

The restart procedure must not claim that these events were durable.

Any known loss must be reflected in retention coverage.

If exact boundaries cannot be reconstructed, the affected interval remains unverified.

### 16.10 Recovery Result

Conceptually:

    RetentionRecoveryResult {
        runtime_instance_id

        recovered_segments
        quarantined_segments
        missing_segments

        recovered_coverage
        unavailable_coverage

        manifest_revision
        recovery_status
    }

The result is reported to the Recovery Coordinator and Metrics Registry.

### 16.11 Restart Safety Invariants

    No incomplete Parquet file is
    advertised as committed coverage.

    No manifest entry is trusted
    without applicable validation.

    No valid orphaned segment is
    deleted without reconciliation.

    Physical duplicate files do not
    produce duplicate canonical events.

    Missing data is never fabricated.

    Previous runtime cursor epochs
    remain distinguishable.

---

## 17. Segment Deletion and Disk Reclamation

Temporary Parquet segments must eventually be removed to enforce finite disk usage.

Deletion must respect active protection leases, readers, and durable metadata consistency.

### 17.1 Deletion Eligibility

A segment may be deleted only when:

- Its retention period has expired.
- No active protection requires its events.
- No active reader requires the physical file.
- No required replay guarantee depends on it.
- Its deletion is permitted by the effective retention policy.

### 17.2 Deletion Sequence

    Segment Expiration Eligible
              |
              v
    Validate Protection State
              |
              v
    Validate Reader References
              |
              v
    Mark Segment Deleting
              |
              v
    Remove From New Read Admission
              |
              v
    Delete Physical File
              |
              v
    Synchronize Required Directory Metadata
              |
              v
    Commit Manifest Deletion
              |
              v
    Release Disk Budget

The implementation must define how in-flight readers are protected during deletion.

### 17.3 Deletion Crash Recovery

A crash may occur after a segment is marked for deletion but before the file is removed.

Recovery must reconcile the intended deletion state with actual filesystem contents.

The procedure must be idempotent.

### 17.4 Disk Budget Release

Disk capacity reservations are released only after the relevant physical storage is no longer allocated or after accounting is reconciled with actual filesystem state.

Removing a manifest entry does not prove that disk space was reclaimed.

### 17.5 Protected Segment Deletion

A segment required by an active bootstrap protection cannot be deleted through ordinary expiration.

If the protection has failed or expired, deletion may proceed only according to the configured retention policy and after the failure has been reported.

### 17.6 Cleanup Failures

Filesystem deletion may fail because of:

- Permission errors.
- Filesystem unavailability.
- Open-file constraints.
- Read-only remounts.
- Hardware errors.

Cleanup failures must be reported.

The subsystem must not decrement actual disk usage merely because deletion was attempted.

### 17.7 Final Storage Invariant

A segment is considered safely persisted only when:

    Parquet Data Valid
             +
    Required File Durability
             +
    Authoritative Manifest Commit

A segment is considered safely removed only when:

    Deletion Permitted
             +
    Physical Storage Reconciled
             +
    Manifest Coverage Updated

These conditions establish the storage contract for temporary disk-backed retention.

---

## 18. Historical Bootstrap Architecture

Historical bootstrap constructs an initial market-data history or processing state using archived exchange data while MarketForge Live continues acquiring new events.

The bootstrap process must not require live acquisition to stop.

The architecture separates:

- Historical archive retrieval.
- Historical archive processing.
- Live event acquisition.
- Temporary live retention.
- Historical-to-live reconciliation.
- Consumer state initialization.
- Continuous live consumption.

### 18.1 Component Ownership

| Component | Responsibility |
|---|---|
| Historical Data Subsystem | Historical archive retrieval and decoding |
| Bootstrap Coordinator | Bootstrap lifecycle and historical-to-live handoff |
| Live Retention Manager | Temporary protection and replay of accepted live events |
| Trade Processor | Canonical live trade production |
| BookStore | Authoritative synchronized live L2 state |
| Publisher | Distribution to live subscribers |
| Stream Registry | Source-stream identity and desired state |
| Recovery Coordinator | Cross-component failure coordination |
| Runtime Resource Manager | Memory, disk, and concurrency admission |

The Bootstrap Coordinator does not implement exchange connectivity, Parquet storage, or native sequence validation.

### 18.2 Bootstrap Data Flow

    Historical Archive Source
               |
               v
    Historical Archive Processing
               |
               v
      Historical Event Stream
               |
               |
               +--------------------------+
                                          |
                                          v
                                    Bootstrap Join
                                          ^
                                          |
    Live Exchange                          |
          |                               |
          v                               |
    Canonical Processing                  |
          |                               |
          v                               |
    Live Retention Manager ---------------+
          |
          v
      Live Publisher

The historical and live paths may use different acquisition mechanisms.

They must converge through a defined canonical identity and reconciliation contract.

### 18.3 Bootstrap Execution Order

The required ordering is:

    Prepare Live Source
             |
             v
    Establish Retention Protection
             |
             v
    Confirm Live Capture Readiness
             |
             v
    Establish Bootstrap Boundary
             |
             v
    Begin Historical Processing
             |
             v
    Reconcile Historical and Live Coverage
             |
             v
    Replay Protected Live Events
             |
             v
    Handoff to Continuous Live Consumption

Historical processing must not begin under the assumption that future live data will somehow be available later.

The retention protection and capture boundary must first be established.

### 18.4 Bootstrap Modes

The initial architecture distinguishes:

    TradeBootstrap

    L2StateBootstrap

These modes have different correctness requirements.

Trade bootstrap may concatenate or reconcile historical executions with subsequently captured live executions.

L2 state bootstrap requires sequence-compatible book state.

A collection of historical L2 updates cannot be joined to an arbitrary current snapshot without proving that the transition is valid.

### 18.5 Bootstrap Guarantees

The Bootstrap Coordinator may establish:

    HistoricalInputCoverage

    LiveCaptureCoverage

    HistoricalLiveBoundaryValidated

    ConsumerStateInitialized

    ContinuousDeliveryEstablished

Each guarantee must be evaluated independently.

A successful archive download does not establish live coverage.

A successful live retention read does not establish historical completeness.

---

## 19. Bootstrap Session Model

Each bootstrap operation is represented by a session with explicit source streams, coverage requirements, and lifecycle state.

### 19.1 BootstrapSession

Conceptual representation:

    BootstrapSession {
        bootstrap_id
        consumer_id

        source_streams
        bootstrap_kind

        historical_start
        historical_target

        retention_protections
        capture_boundaries

        historical_progress
        live_replay_progress

        state
        failure_reason

        created_at
        updated_at
    }

The exact Rust representation belongs to the Bootstrap Coordinator.

### 19.2 Bootstrap Identity

Every bootstrap receives a unique:

    BootstrapId

The identifier remains stable throughout retries belonging to the same logical bootstrap attempt.

A restarted bootstrap may require a new execution generation.

Conceptually:

    BootstrapId
    BootstrapGeneration

Obsolete completion notifications must not modify a newer bootstrap generation.

### 19.3 Bootstrap States

Supported states:

    Created
    Preparing
    CapturingLive
    ProcessingHistorical
    ReconcilingBoundary
    ReplayingLive
    HandoffPending
    Live
    PartiallyCompleted
    Failed
    Cancelled

| State | Meaning |
|---|---|
| Created | Bootstrap request registered |
| Preparing | Dependencies and protection requirements validated |
| CapturingLive | Live acquisition and retention boundary established |
| ProcessingHistorical | Historical archive processing underway |
| ReconcilingBoundary | Historical and live coverage being validated |
| ReplayingLive | Protected live events being consumed |
| HandoffPending | Preparing atomic transition to continuous delivery |
| Live | Consumer successfully transitioned to live processing |
| PartiallyCompleted | Some required guarantees remain unresolved |
| Failed | Bootstrap cannot satisfy its required guarantees |
| Cancelled | Operation intentionally terminated |

### 19.4 Bootstrap Admission

Before starting, the coordinator verifies:

- Source streams exist or can be activated.
- Required canonical schemas are compatible.
- Historical input is available.
- Retention is enabled.
- Protection can be admitted.
- Resource limits are sufficient for initial operation.
- Required source metadata is available.
- The consumer supports the selected handoff contract.

Successful admission does not guarantee unlimited future retention capacity.

### 19.5 Multiple Source Streams

One bootstrap may depend on several source streams.

Example:

    Bootstrap Session
           |
           +-- Binance BTCUSDT Trades
           |
           +-- Binance ETHUSDT Trades
           |
           +-- Bybit BTCUSDT Trades

Each stream retains an independent cursor and protection frontier.

The coordinator must not assume a single global publication cursor.

### 19.6 Bootstrap Cancellation

Cancellation must:

- Fence the bootstrap generation.
- Stop pending historical work where possible.
- Stop replay admission.
- Release protection leases.
- Release associated resource reservations.
- Preserve valid retained data according to ordinary retention policy.
- Report the final state.

Cancellation must not automatically deactivate live source streams required by unrelated consumers.

---

## 20. Establishing the Historical-to-Live Boundary

The most important bootstrap operation is defining where historical input ends and protected live input begins.

This boundary must be established using evidence rather than an arbitrary timestamp.

### 20.1 Boundary Dimensions

A bootstrap boundary may involve:

    Source Stream Identity

    Canonical Cursor

    Exchange Event Time

    Exchange Native Identity

    Capture Time

    Metadata Revision

These dimensions must not be treated as interchangeable.

### 20.2 BootstrapCaptureBoundary

Conceptual representation:

    BootstrapCaptureBoundary {
        stream_id

        cursor_epoch
        initial_live_cursor

        first_live_event_time

        first_live_native_identity

        capture_ready_at

        metadata_revision
        boundary_confidence
    }

The exact fields depend on the source stream and available exchange identifiers.

### 20.3 Why Timestamps Are Insufficient

Consider:

    Historical archive ends:
        12:00:00.000

    First captured live trade:
        12:00:00.150

There may have been executions between these timestamps.

The system must not assume that the intervals are continuous.

Likewise, equal timestamps do not prove that historical and live records contain identical executions.

### 20.4 Boundary Establishment Sequence

    Activate Source Stream
             |
             v
    Establish Retention Subscription
             |
             v
    Commit Protection Start Boundary
             |
             v
    Confirm Live Event Admission
             |
             v
    Record First Valid Live Evidence
             |
             v
    Begin Historical Archive Processing

The protection must cover the first live event required by the bootstrap.

There must not be a race between the first accepted live event and protection activation.

### 20.5 Capture Readiness

A live source may be:

    Connected

without being:

    Ready for Bootstrap

For trades, bootstrap readiness requires a valid live processing context and an established retention boundary.

For L2, readiness depends on the selected bootstrap strategy.

A synchronized current book may be necessary, but it does not establish historical delta continuity.

### 20.6 Boundary Evidence

Possible trade boundary evidence includes:

- Native execution identifiers.
- Exchange sequence identifiers.
- Verified archive/live overlap.
- Native continuity metadata.
- Matching canonical event identities.

The applicable evidence depends on exchange capabilities.

The Bootstrap Coordinator must not assume that all exchanges expose globally consecutive trade identifiers.

### 20.7 Unverified Boundary

If continuity cannot be proven:

    HistoricalLiveBoundaryValidated = Unverified

The bootstrap may continue according to a policy that accepts incomplete coverage.

However, it must not claim a complete historical-to-live transition.

### 20.8 Protected Capture Start

The initial protection start must be established at or before the required live capture boundary.

A protection request created after live capture has already advanced must validate that all earlier required events remain available.

It must not silently start from the current frontier.

---

## 21. Historical and Live Trade Reconciliation

Trade bootstrap combines archived executions with events accepted by MarketForge Live.

The join must preserve native execution identity and prevent duplicate canonical trade acceptance.

### 21.1 Trade Sources

Trade bootstrap may receive:

    Historical Archive Trades

and:

    Retained Live Trades

Historical archive events may have been normalized using the same canonical contract but may possess different acquisition provenance.

### 21.2 Identity Requirements

Trade reconciliation must use the native execution identity semantics established in **06.2 — Trade Processing and Continuity**.

The relevant identity may include:

    Exchange

    Market

    InstrumentId

    NativeExecutionId

    NativeIdentityScope

The bootstrap coordinator must not deduplicate trades solely by timestamp, price, and quantity.

Two distinct executions may legitimately share those values.

### 21.3 Historical and Live Overlap

The recommended bootstrap strategy intentionally requests historical coverage that overlaps the beginning of protected live capture.

Conceptually:

    Historical Coverage
    |-------------------------|

                    Live Capture
                    |------------------------------|

                    ^^^^^^^^^^^
                    Overlap

The overlap is used to establish continuity and deduplicate common executions.

### 21.4 Overlap Reconciliation

The join procedure must:

1. Validate historical archive coverage.
2. Validate live retention coverage.
3. Identify the overlapping interval.
4. Compare compatible native execution identities.
5. Remove duplicate representations of the same execution.
6. Preserve all distinct executions.
7. Evaluate continuity using supported exchange semantics.
8. Produce a verified or explicitly uncertain boundary.

### 21.5 Duplicate Executions

If an execution appears in both sources:

    Historical:
        ExecutionId = 500

    Live:
        ExecutionId = 500

The bootstrap output contains one logical execution, provided the identity scope and payload compatibility confirm equivalence.

The two source representations may retain separate provenance records.

### 21.6 Conflicting Executions

If identical native execution identities carry incompatible canonical economic values:

    Historical:
        ExecutionId = 500
        Price = 65000

    Live:
        ExecutionId = 500
        Price = 65100

The system must report an integrity conflict.

It must not silently select one representation without a defined reconciliation rule.

### 21.7 Recovery-Origin Live Events

A retained live event may have originated from the Trade Reconciliation Engine.

For example:

    Cursor 100:
        Live trade at 12:00:05

    Cursor 101:
        Recovered trade at 12:00:03

The bootstrap coordinator must preserve both canonical publication identities while deduplicating logical executions appropriately.

Publication order and event-time order are separate.

### 21.8 Historical Ordering

Historical bootstrap may require event-time ordering.

If so, the coordinator must use a defined ordering key and deterministic tie-breaking policy.

Possible dimensions include:

    Exchange Event Time

    Native Execution Sequence

    Native Execution Identity

    Source Provenance

The ordering policy must not assume that timestamps uniquely identify executions.

### 21.9 Trade Boundary Outcomes

The final reconciliation outcome may be:

    VerifiedContinuous

    VerifiedIncomplete

    PartiallyVerified

    Unverified

These describe evidence about historical-to-live trade coverage.

They do not replace the original canonical publication cursors.

---

## 22. L2 Bootstrap Semantics

L2 bootstrap requires a different approach from trade bootstrap.

Order-book state is sequence-dependent.

Historical updates cannot be joined to live state merely by sorting timestamps.

### 22.1 L2 State Requirement

To reconstruct a valid order book:

    Authoritative Snapshot
              |
              v
    Compatible Ordered Deltas
              |
              v
    Valid Book State

All sequence compatibility rules belong to the exchange-specific L2 processing contracts.

### 22.2 Historical L2 Reconstruction

If historical archives contain sufficient snapshot and delta information, a historical consumer may reconstruct past book states.

However, this reconstruction is independent of the current live BookStore generation until a valid synchronization relationship is established.

### 22.3 Historical-to-Live L2 Join

A historical L2 sequence may connect to a captured live sequence only when native protocol evidence establishes continuity.

Possible evidence includes:

- Compatible snapshot sequence.
- Consecutive update identifiers.
- Valid overlap conditions.
- Supported native sequence bridging.

The exact rules depend on the exchange.

### 22.4 Current-State Bootstrap

If historical-to-live delta continuity cannot be established, the consumer may initialize from a current authoritative BookStore snapshot.

Conceptually:

    Current BookStore Snapshot
              |
              v
    Snapshot-to-Live Handoff
              |
              v
    Continuous L2 Processing

This establishes valid current-state processing.

It does not reconstruct the missing historical order-book transitions.

### 22.5 L2 Coverage Classification

The coordinator must distinguish:

    HistoricalL2Coverage

from:

    CurrentBookState

For example:

    HistoricalL2Coverage = Incomplete

    CurrentBookState = Synchronized

This is a valid outcome.

### 22.6 L2 Generation Changes

If the BookStore generation changes during bootstrap handoff:

- Invalidate incompatible pending deltas.
- Cancel or restart the affected handoff.
- Obtain a snapshot from the current generation.
- Establish a fresh publication boundary.
- Preserve historical coverage diagnostics.

### 22.7 L2 Bootstrap Invariant

    A synchronized current book
    does not prove complete
    historical L2 event coverage.

The bootstrap process must preserve this distinction.

---

## 23. Protected Live Replay During Bootstrap

After processing historical input, the bootstrap consumer begins consuming protected live events.

This process uses the retention read interfaces defined earlier.

### 23.1 Replay Start

Replay begins from the validated bootstrap capture boundary.

Conceptually:

    Historical Processing Completed
              |
              v
    Resolve Boundary
              |
              v
    Acquire Retention Read Lease
              |
              v
    Replay Protected Live Events

The starting cursor must be explicit.

### 23.2 Replay Ordering

Replay preserves canonical publication order.

If the consumer requires event-time ordering, the Bootstrap Coordinator applies the separately defined historical/live ordering policy.

The Retention Manager must not reorder its stored canonical records.

### 23.3 Bounded Replay

Replay operations must enforce:

    MaximumReplayBatchEvents

    MaximumReplayBatchBytes

    MaximumConcurrentReplayReaders

    MaximumReplayReadMemory

These limits are defined through configuration.

Large protected intervals must be processed incrementally.

### 23.4 Protection Advancement

After the consumer safely processes a replay batch:

    Batch Read
        |
        v
    Batch Validated
        |
        v
    Consumer State Committed
        |
        v
    Advance Protection Frontier

The consumer must not release required retained events before its state is safe according to its processing contract.

### 23.5 Replay Progress

Conceptual state:

    BootstrapReplayProgress {
        bootstrap_id
        stream_id

        replay_cursor
        acknowledged_cursor

        current_live_frontier

        replay_lag_events
        replay_lag_time
    }

The cursor-based lag is authoritative for publication coverage where the cursor contract supports it.

Time-based lag is diagnostic.

### 23.6 Replay Catch-Up

During replay, new live events continue arriving.

The coordinator must repeatedly advance toward the live frontier.

Conceptually:

    Replay Cursor = 100

    Current Live Cursor = 1000

    Replay Progresses
             |
             v
    Replay Cursor = 900

    Current Live Cursor = 1100

    Replay Continues
             |
             v
    Replay Cursor Approaches Live Frontier

A consumer that processes retained events more slowly than new events arrive may never catch up.

The coordinator must detect this condition.

### 23.7 Replay Throughput

The coordinator should measure:

    Live Ingestion Rate

    Replay Processing Rate

    Remaining Backlog

    Retention Capacity

If replay processing throughput remains below live event arrival rate, the system must not assume that bootstrap will eventually complete.

### 23.8 Replay Resource Exhaustion

When retained backlog grows beyond available budgets:

- Prioritize required spill operations.
- Restrict optional replay workloads.
- Report increasing bootstrap lag.
- Apply bootstrap timeout or admission policies.
- Escalate if protected coverage cannot be maintained.

The Retention Manager must not silently discard protected events.

---

## 24. Bootstrap-to-Live Handoff

Bootstrap completion requires a consistent transition from retained-event replay to continuous live consumption.

The transition must preserve the applicable canonical publication boundary.

### 24.1 Handoff Requirement

Suppose:

    Last Replayed Cursor = 1000

while:

    Current Live Cursor = 1005

The consumer must not begin ordinary live delivery at cursor 1005 without accounting for cursors 1001–1004.

### 24.2 Handoff Mechanism

The recommended handoff uses the same ordered publication boundary mechanism required by subscriber replay in document 08.

Conceptually:

    Protected Replay
           |
           v
    Establish Live Handoff Boundary
           |
           v
    Continue Replay Through Boundary
           |
           v
    Reconcile Pending Live Events
           |
           v
    Activate Continuous Delivery
           |
           v
    Release Bootstrap Protection

The transition must be atomic from the consumer's logical delivery perspective.

### 24.3 Handoff Ownership

The Bootstrap Coordinator owns bootstrap readiness and completion.

The Publisher owns subscriber delivery when the consumer connects through Unix sockets.

The Retention Manager owns retained-event availability.

The Stream Registry owns source identity and desired state.

No component may independently declare bootstrap complete before the required handoff evidence exists.

### 24.4 Direct Internal Consumers

Not every bootstrap consumer must use Unix sockets.

A Rust-internal consumer may use an internal bounded event interface.

However, it must preserve the same:

- Canonical cursor identity.
- Replay-to-live continuity.
- Generation fencing.
- Resource limits.
- Protection advancement rules.

A separate internal transport must not weaken the bootstrap guarantee.

### 24.5 Handoff Cursor

The coordinator records:

    LastProtectedCursor

    FirstContinuousCursor

These values must form a valid transition according to the canonical cursor contract.

If the transition cannot be validated:

    HandoffFailed

The bootstrap must not be marked complete.

### 24.6 Handoff During Source Recovery

A source stream may disconnect while bootstrap is approaching the live frontier.

The coordinator must distinguish:

    Subscriber Handoff Readiness

from:

    Source Acquisition Health

The consumer may complete replay through the last available cursor while current acquisition remains degraded.

However, it must not claim continuously available live coverage until the required source guarantees are restored.

### 24.7 Handoff During L2 Resynchronization

If an L2 source changes book generation during handoff:

- Validate generation compatibility.
- Reject obsolete pending deltas.
- Establish a fresh authoritative snapshot when necessary.
- Restart the snapshot-to-live transition.
- Preserve historical coverage status.

The consumer must not mix incompatible book generations.

### 24.8 Handoff Completion Evidence

Conceptual representation:

    BootstrapHandoffResult {
        bootstrap_id

        source_streams
        final_replay_cursors
        continuous_delivery_cursors

        coverage_results
        consumer_state_revision

        completed_at
        status
    }

The result must identify the guarantees actually established.

### 24.9 Protection Release

Bootstrap protection may be released only after:

- Required replay events are consumed.
- Consumer state is committed.
- Continuous delivery is established.
- The handoff boundary is validated.
- No other bootstrap obligation requires the protected range.

Release must be idempotent.

### 24.10 Completion Outcomes

Supported outcomes:

    Completed
    PartiallyCompleted
    Failed
    Cancelled

`Completed` requires every guarantee defined as mandatory for the selected bootstrap contract.

`PartiallyCompleted` identifies explicitly accepted incomplete historical coverage or unresolved optional guarantees.

The coordinator must not report full historical continuity merely because continuous live processing has started.

---

## 25. Multi-Stream Bootstrap Coordination

A bootstrap consumer may require several independent source streams.

Each stream has its own cursor, event-time frontier, and recovery state.

### 25.1 Independent Stream Frontiers

Example:

    Binance BTCUSDT Trades
        Cursor 1000

    Binance ETHUSDT Trades
        Cursor 2500

    Bybit BTCUSDT Trades
        Cursor 800

These cursor values cannot be compared directly.

### 25.2 Multi-Stream Handoff

The coordinator must establish an independent valid handoff for each required source stream.

Conceptually:

    Stream A Handoff ----+
                         |
    Stream B Handoff ----+----> Consumer Ready
                         |
    Stream C Handoff ----+

A consumer requiring all three sources becomes fully ready only when each required handoff succeeds.

### 25.3 Event-Time Merging

A bootstrap consumer may require events from multiple streams in a common event-time order.

This requires a separately defined merge policy.

The Retention Manager does not implement cross-stream ordering.

The Bootstrap Coordinator must invoke the appropriate merge or reorder interface rather than comparing unrelated publication cursors.

### 25.4 Watermark Requirements

Cross-stream event-time merging may depend on:

    Source Watermarks

    Allowed Lateness

    Required Stream Set

    Missing Source Policy

These concepts belong to the combined-stream architecture.

Retention supplies source events and coverage information.

It does not independently decide when a cross-stream event-time watermark is safe.

### 25.5 Partial Multi-Stream Recovery

If one required source remains incomplete:

    Stream A = Ready

    Stream B = Ready

    Stream C = Coverage Unverified

The combined bootstrap cannot claim fully verified coverage.

A degraded-mode policy may permit partial operation, but the affected source must remain visible.

### 25.6 Multi-Stream Protection

Each required source receives its own retention protection.

Protection advancement is independent per stream.

A slow or failed source must not automatically prevent the release of unrelated stream intervals unless the consumer's processing contract explicitly requires coordinated retention.

---

## 26. Bootstrap Failure and Recovery

Bootstrap may fail independently of live acquisition.

The coordinator must preserve healthy live streams whenever possible.

### 26.1 Failure Types

Possible failures include:

    HistoricalArchiveUnavailable

    HistoricalArchiveCorrupted

    HistoricalCoverageIncomplete

    RetentionProtectionRejected

    RetentionCoverageExpired

    ParquetReadFailure

    HistoricalLiveBoundaryUnverified

    ReplayFallingBehind

    HandoffGenerationChanged

    ConsumerProcessingFailure

    ResourceExhaustion

### 26.2 Failure Isolation

Example:

    Bootstrap Failed

does not automatically imply:

    Live Source Failed

The live source may continue providing accepted canonical events to other consumers.

### 26.3 Retry Strategy

A failed bootstrap may be retried when:

- Historical input becomes available.
- Retention still contains the required live interval.
- Resource capacity is restored.
- Source-stream state is compatible.
- The consumer can safely restart its processing state.

A retry must validate its boundaries again.

### 26.4 Irrecoverable Coverage

If the historical archive ends before the first retained live event and the missing interval cannot be recovered:

    HistoricalLiveBoundaryValidated = Unverified

or:

    HistoricalLiveBoundaryValidated = VerifiedIncomplete

depending on available evidence.

The coordinator must not fabricate missing events.

### 26.5 Consumer Crash

If the bootstrap consumer crashes:

- The protection lease remains subject to its configured lifetime.
- Durable consumer progress may permit resumption.
- In-memory-only consumer progress may be lost.
- Restart must reconcile its last committed state.
- Old bootstrap generations must be fenced.

### 26.6 Repeated Failure

Repeated bootstrap failures may require:

- Longer retry intervals.
- Reduced concurrency.
- Additional diagnostics.
- Operator intervention.
- Explicit degraded completion.
- Permanent failure classification.

The Recovery Coordinator handles system-level escalation.

### 26.7 Failure Invariant

    Bootstrap failure must never
    silently change the contents
    or identities of accepted
    canonical live events.

---

## 27. Retention Read API

The Retention Manager exposes a unified read interface across memory and Parquet storage.

Consumers must not need to know whether a retained canonical event currently resides in RAM, on disk, or in both locations.

### 27.1 API Responsibilities

The retention read interface must:

- Resolve canonical cursor ranges.
- Validate cursor epochs.
- Determine actual data availability.
- Read from memory and Parquet.
- Preserve canonical publication order.
- Avoid duplicate events across storage tiers.
- Support bounded pagination.
- Enforce resource limits.
- Respect active protection leases.
- Report incomplete coverage.
- Detect storage corruption.
- Support cancellation.

### 27.2 Read Operations

The initial interface supports:

    GetRetentionCoverage

    ReadRetentionRange

    OpenRetentionReader

    AdvanceRetentionReader

    CloseRetentionReader

    GetRetentionStatus

These are conceptual operations, not finalized Rust function names.

### 27.3 Coverage Query

A consumer should be able to request:

    GetRetentionCoverage {
        stream_id
        cursor_epoch
        requested_interval
    }

The response must identify:

    AvailableIntervals

    MissingIntervals

    UnverifiedIntervals

    DurabilityLevels

    EarliestAvailableCursor

    LatestAvailableCursor

The presence of the latest cursor does not prove that every preceding cursor remains available.

### 27.4 Unified Read Path

Conceptually:

    Retention Read Request
              |
              v
    Validate Stream and Epoch
              |
              v
    Query Coverage Index
              |
              v
    Construct Read Plan
              |
       +------+------+
       |             |
       v             v
    Memory Reader  Parquet Reader
       |             |
       +------+------+
              |
              v
    Canonical Cursor Merge
              |
              v
    Identity Deduplication
              |
              v
    Bounded Result Batch

The merge operates on canonical publication identities.

It must not reorder events by exchange timestamps.

### 27.5 Read Planning

A read plan identifies the physical sources required to satisfy an interval.

Conceptually:

    RetentionReadPlan {
        stream_id
        cursor_epoch

        requested_interval
        memory_segments
        parquet_segments

        expected_coverage
        resource_reservations
    }

The planner must not advertise complete coverage before validating that all required portions of the interval are available.

### 27.6 Storage Tier Selection

An event may exist in both RAM and Parquet.

The read planner should select one authoritative representation.

For example:

    Cursor 100–199:
        Parquet

    Cursor 200–249:
        RAM and Parquet

    Cursor 250–300:
        RAM

The reader must return each canonical event once.

### 27.7 Read Pagination

Large reads must use bounded batches.

Conceptually:

    ReadRetentionRange {
        stream_id
        cursor_epoch

        start_cursor
        end_cursor

        maximum_events
        maximum_bytes
    }

The result includes the next continuation position when additional events remain.

### 27.8 Continuation Tokens

A continuation token may include:

    StreamId

    CursorEpoch

    NextCursor

    ManifestRevision

    ReaderGeneration

Tokens must be validated.

The server must not trust arbitrary client-supplied continuation state as proof of authorization or retention ownership.

### 27.9 Read Consistency

Readers require a consistent logical view of retained events.

An active reader must not unexpectedly lose access to a required segment merely because background cleanup begins.

The implementation may use:

- Immutable segment references.
- Read leases.
- Manifest snapshot references.
- Reference-counted file handles.

Reader lifetimes must remain bounded.

### 27.10 Read Failure Results

Possible outcomes include:

    Complete

    PartialCoverage

    CursorExpired

    InvalidCursorEpoch

    SegmentUnavailable

    SegmentCorrupted

    ResourceUnavailable

    ReaderCancelled

The interface must not return an ordinary end-of-stream result when required data is missing.

### 27.11 Read API Invariant

    A successful complete read
    establishes coverage of the
    requested canonical cursor interval
    under the defined retention contract.

It does not establish complete native exchange acquisition.

---

## 28. Retention Integrity Verification

Retention correctness depends on accurate event identity, coverage metadata, and physical storage validation.

The subsystem must detect inconsistencies rather than silently repairing them through unsupported assumptions.

### 28.1 Integrity Domains

The Retention Manager verifies:

    Canonical Identity

    Cursor Ordering

    Segment Metadata

    Physical File Integrity

    Coverage Index Consistency

    Manifest Consistency

### 28.2 Duplicate Canonical Identities

If two physical records share the same canonical event identity, the reader must determine whether they represent identical copies.

Identical copies may result from:

- Memory-to-disk overlap.
- Retry after uncertain spill completion.
- Controlled compaction.
- Crash recovery.

Conflicting copies must be reported as integrity failures.

### 28.3 Cursor Gaps

A missing numeric cursor may indicate a retention gap only when the canonical cursor contract guarantees consecutive assignment within that cursor domain.

If cursor values are sparse by design, the Retention Manager must use the defined continuity evidence.

The subsystem must not invent missing events from numeric differences alone.

### 28.4 Segment Integrity

A segment is valid only when its:

- File format is readable.
- Schema version is supported.
- Event identities are consistent.
- Stream identity matches.
- Cursor epoch matches.
- Recorded event count matches.
- Integrity metadata validates.

A Parquet footer alone is not sufficient evidence of full file integrity.

### 28.5 Manifest Consistency

The Manifest Manager must detect:

- References to missing files.
- Unindexed valid files.
- Duplicate segment identifiers.
- Conflicting cursor ranges.
- Invalid deletion states.
- Unsupported manifest versions.

Recovery must preserve evidence of unresolved inconsistencies.

### 28.6 Background Verification

The implementation may perform bounded background integrity checks.

These checks should prioritize:

- Recently committed segments.
- Segments required by active bootstrap protection.
- Segments selected for replay.
- Segments recovered after a crash.

Background validation must remain resource-bounded.

### 28.7 Integrity Failure Reporting

A confirmed integrity failure must include:

    StreamId

    CursorEpoch

    AffectedInterval

    StorageLocation

    FailureClassification

    AvailableEvidence

The Recovery Coordinator uses this information to determine escalation.

---

## 29. Retention Failure Recovery

Retention failures must be handled without corrupting canonical event identities or silently weakening protection guarantees.

### 29.1 Failure Categories

Relevant failures include:

    MemoryCapacityExceeded

    DiskCapacityExceeded

    SpillWriteFailure

    SpillValidationFailure

    ManifestCommitFailure

    SegmentCorruption

    SegmentMissing

    ReaderFailure

    ProtectionExpired

    CoverageUnavailable

    FilesystemUnavailable

### 29.2 Memory Pressure

When memory pressure increases:

    Memory Pressure
          |
          v
    Seal Eligible Segments
          |
          v
    Prioritize Spill
          |
          v
    Release Eligible Memory
          |
          v
    Reevaluate Admission

If spill cannot keep up, the Retention Manager must escalate before violating required capacity guarantees.

### 29.3 Disk Exhaustion

When disk capacity becomes insufficient:

- Stop admitting unsafe spill operations.
- Preserve existing committed segments.
- Reclaim expired unprotected segments.
- Restrict optional replay workloads.
- Report impaired retention guarantees.
- Continue memory retention only within configured limits.

The subsystem must not delete protected segments to conceal disk exhaustion.

### 29.4 Spill Write Failure

A spill failure leaves the affected memory segment in its last valid retention state.

The Retention Manager may retry when:

- Resource admission succeeds.
- The failure is recoverable.
- The segment remains available.
- The previous write attempt has been reconciled.

Retries must use idempotent segment identities.

### 29.5 Manifest Failure

A valid Parquet file may exist even when its manifest commit fails.

The subsystem must preserve the file and reconcile it later.

It must not mark the segment `DiskCommitted` until the complete commit contract is satisfied.

### 29.6 Corrupted Segment

When a segment becomes unreadable:

    Segment Corruption
          |
          v
    Remove From Verified Coverage
          |
          v
    Identify Affected Consumers
          |
          v
    Report Retention Gap
          |
          v
    Evaluate Recovery

A second valid physical copy may permit recovery.

Otherwise, the affected interval remains unavailable.

### 29.7 Reader Failure

A reader failure must not invalidate healthy source acquisition.

The affected bootstrap or replay operation may fail or retry.

The underlying retained data remains available unless storage integrity is also impaired.

### 29.8 Protection Failure

A protection failure occurs when the subsystem can no longer satisfy the interval promised to a consumer.

The system must notify the affected consumer and report:

    ProtectionId

    StreamId

    RequiredInterval

    AvailableInterval

    FailureReason

The consumer must not continue assuming complete protected coverage.

### 29.9 Retry Policy

Retries must distinguish transient from permanent failures.

Examples:

| Failure | Initial response |
|---|---|
| Temporary filesystem I/O failure | Bounded retry |
| Disk full | Reclamation and capacity reevaluation |
| Unsupported Parquet schema | Configuration or compatibility failure |
| Corrupted segment | Quarantine and coverage reconciliation |
| Invalid cursor epoch | Reject request |
| Expired protected interval | Explicit coverage failure |
| Temporary resource contention | Deferred admission |

Repeated failures escalate through the Recovery Coordinator.

---

## 30. Restart and Crash Recovery

The retention subsystem must reconstruct authoritative disk-backed coverage after process termination.

Memory-only retained events are not assumed to survive.

### 30.1 Restart Requirements

On restart:

- Establish a new runtime instance identity.
- Validate retention configuration.
- Acquire storage-directory ownership.
- Load the last valid manifest.
- Reconcile Parquet files.
- Validate committed segments.
- Identify missing or damaged coverage.
- Restore usable retention indexes.
- Reconstruct applicable persisted protection state.
- Expose verified replay availability.

### 30.2 Old Runtime Epochs

Previous cursor epochs may remain available in retained Parquet files.

The Retention Manager must preserve their identity.

It must not merge different epochs solely because they belong to the same logical stream.

### 30.3 Protection Restoration

A protection lease can survive restart only when its necessary state was durably persisted.

Relevant information includes:

    ProtectionId

    Consumer Identity

    StreamId

    CursorEpoch

    Protected Interval

    Last Committed Consumer Progress

    Lease Expiration

If this information is unavailable, the runtime must not fabricate an active protection.

### 30.4 Incomplete Spill Recovery

Temporary files require reconciliation.

The subsystem may:

    Finalize Valid Segment

    Quarantine Invalid Segment

    Delete Confirmed Abandoned Temporary File

Deletion must not precede the required ownership and recovery checks.

### 30.5 Recovery Availability

Retention reads may be unavailable while the relevant manifest and segment indexes are being reconstructed.

The runtime must expose:

    RetentionRecovering

rather than reporting incomplete recovery as normal availability.

### 30.6 Concurrent Live Ingestion

New live ingestion may begin before all old segments have been inspected if:

- A new cursor epoch is established.
- New writes use nonconflicting segment identities.
- Recovery and ingestion resource usage remain bounded.
- Old coverage is not prematurely advertised as valid.

This permits live acquisition to resume without waiting for exhaustive historical retention scans.

### 30.7 Crash Recovery Invariant

    Restart restores only
    the coverage supported
    by validated durable evidence.

Uncommitted memory-only events must not be assumed recoverable.

---

## 31. Retention Observability

The Retention Manager exposes operational metrics through MarketForge's internal Metrics Registry, API, and Ratatui TUI.

Operational logs remain local.

### 31.1 Retention Health Snapshot

Conceptually:

    RetentionHealthSnapshot {
        runtime_instance_id

        retention_state

        memory_used_bytes
        memory_budget_bytes

        disk_used_bytes
        disk_budget_bytes

        active_streams
        active_protections
        active_readers

        pending_spills
        failed_spills

        unavailable_intervals
        last_error

        evaluated_at
    }

### 31.2 Memory Metrics

| Metric | Description |
|---|---|
| `retention_memory_bytes` | Current retained memory |
| `retention_memory_budget_bytes` | Configured memory budget |
| `retention_memory_segments` | Active memory segments |
| `retention_events_in_memory` | Events retained in RAM |
| `retention_memory_evictions_total` | Completed memory evictions |
| `retention_admission_failures_total` | Failed event admissions |

### 31.3 Spill Metrics

| Metric | Description |
|---|---|
| `retention_spill_jobs_total` | Created spill operations |
| `retention_spill_completed_total` | Successfully committed spills |
| `retention_spill_failed_total` | Failed spill operations |
| `retention_spill_pending` | Pending spill jobs |
| `retention_spill_duration` | Spill operation duration |
| `retention_spill_bytes_total` | Bytes written to spill files |
| `retention_parquet_segments` | Committed Parquet segments |
| `retention_disk_bytes` | Disk space consumed by retention |

### 31.4 Protection Metrics

| Metric | Description |
|---|---|
| `retention_active_protections` | Active protection leases |
| `retention_protection_failures_total` | Failed protection guarantees |
| `retention_protection_expirations_total` | Expired protection leases |
| `retention_protected_bytes` | Estimated protected storage |
| `retention_protection_age` | Age of active protections |

### 31.5 Replay Metrics

| Metric | Description |
|---|---|
| `retention_read_requests_total` | Retention read requests |
| `retention_read_failures_total` | Failed reads |
| `retention_read_bytes_total` | Bytes returned |
| `retention_read_latency` | Read completion latency |
| `retention_active_readers` | Active readers |
| `retention_cursor_expired_total` | Requests for expired cursors |

### 31.6 Bootstrap Metrics

| Metric | Description |
|---|---|
| `bootstrap_sessions_active` | Active bootstrap operations |
| `bootstrap_sessions_completed_total` | Completed bootstrap operations |
| `bootstrap_sessions_failed_total` | Failed bootstrap operations |
| `bootstrap_historical_progress` | Historical processing progress |
| `bootstrap_replay_lag_events` | Remaining canonical cursor backlog where measurable |
| `bootstrap_replay_lag_seconds` | Time-based replay lag |
| `bootstrap_handoff_failures_total` | Failed historical-to-live handoffs |

### 31.7 TUI Integration

The Retention view should display:

    Retention State

    Memory Usage
    Disk Usage

    Active Spill Jobs
    Failed Spill Jobs

    Protected Streams
    Active Bootstrap Sessions

    Replay Readers
    Unavailable Coverage

A stream detail view may expose:

    StreamId
    CursorEpoch

    Earliest Available Cursor
    Latest Available Cursor

    Memory Segments
    Parquet Segments

    Protected Frontier
    Spill Status
    Last Retention Error

The TUI must use the runtime control API rather than directly accessing retention internals.

### 31.8 Local Logging

Relevant structured events include:

    RetentionStarted
    RetentionAdmissionFailed
    SegmentSealed
    SpillStarted
    SpillCompleted
    SpillFailed
    ManifestCommitted
    SegmentQuarantined
    RetentionCoverageLost
    ProtectionCreated
    ProtectionAdvanced
    ProtectionReleased
    ProtectionExpired
    BootstrapStarted
    BootstrapHandoffCompleted
    BootstrapFailed

No PostgreSQL operational-log storage is required.

---

## 32. Testing Strategy

The retention subsystem requires deterministic unit tests, filesystem integration tests, crash-recovery simulations, and sustained-load benchmarks.

Tests must establish correctness under concurrent ingestion, spill, replay, and cleanup.

### 32.1 Testing Categories

| Category | Scope |
|---|---|
| Memory tests | Buffer admission, sealing, eviction |
| Resource tests | Memory and disk budget enforcement |
| Protection tests | Lease ownership and advancement |
| Parquet tests | Encoding, precision, metadata |
| Manifest tests | Atomic updates and coverage indexes |
| Crash tests | Interrupted writes and restart recovery |
| Read tests | RAM/Parquet merging and pagination |
| Replay tests | Cursor availability and ordering |
| Bootstrap tests | Historical-to-live continuity |
| Concurrency tests | Readers, writers, spill, deletion |
| Property-based tests | Arbitrary state transitions |
| Performance tests | Ingestion, spill, replay, memory usage |

### 32.2 Test Infrastructure

Tests should use:

- Temporary filesystem directories.
- Deterministic clocks.
- Mock canonical-event producers.
- Mock historical archive sources.
- Configurable resource budgets.
- Injected filesystem errors.
- Forced process termination.
- Controlled concurrent readers.
- Mock bootstrap consumers.

Correctness tests must not depend on live exchange connectivity.

---

## 33. Memory and Protection Tests

### 33.1 Memory Admission

Verify:

- Valid event insertion.
- Per-stream ordering.
- Segment sealing.
- Memory budget enforcement.
- Duplicate event handling.
- Invalid cursor epoch rejection.
- Explicit admission failure.

### 33.2 Protected Eviction

Simulate:

    Retained:
        Cursor 100–500

    Protected:
        Cursor 200–400

Attempt eviction.

Expected:

- Unprotected eligible data may be removed.
- Protected data remains accessible.
- Coverage index remains accurate.
- No protected event disappears silently.

### 33.3 Overlapping Protections

Create:

    Protection A:
        Cursor 100–300

    Protection B:
        Cursor 200–500

Release Protection A.

Expected:

    Cursor 200–500

remains protected.

### 33.4 Lease Expiration

Simulate protection expiration while the consumer remains incomplete.

Expected:

- Protection becomes expired.
- Consumer completion is not inferred.
- Failure is reported.
- Subsequent expiration follows the retention policy.

### 33.5 Resource Exhaustion

Fill memory while preventing Parquet spill.

Verify:

- Admission limits remain enforced.
- Memory does not grow without bound.
- Protection failures are explicit.
- No silent event dropping occurs.
- Recovery escalation is triggered when required.

---

## 34. Parquet and Crash Recovery Tests

### 34.1 Parquet Round Trip

Verify exact preservation of:

- Canonical event identity.
- Publication cursor.
- Event timestamps.
- Exact decimal values.
- Native identifiers.
- Book generations.
- Configuration revisions.
- Metadata revisions.
- Recovery provenance.

### 34.2 Spill Commit Fault Injection

Inject crashes:

    Before Temporary File Creation

    During Parquet Write

    Before File Synchronization

    After File Synchronization

    Before Atomic Rename

    After Atomic Rename

    Before Manifest Commit

    After Manifest Commit

For each point, verify the recovered state.

### 34.3 Orphaned Segment Recovery

Create a valid final Parquet file without a manifest entry.

Expected:

- File discovered.
- File validated.
- Identity checked.
- Safe adoption into manifest.
- Coverage restored.

### 34.4 Corrupted Segment

Corrupt a Parquet file after commit.

Expected:

- Corruption detected.
- Segment removed from verified coverage.
- File quarantined according to policy.
- Affected intervals reported unavailable.
- No fabricated event records.

### 34.5 Duplicate Physical Segments

Create two physical files containing identical canonical event identities.

Expected:

- One logical event sequence.
- Duplicate physical representation reconciled.
- No duplicate replay output.

### 34.6 Segment Deletion

Simulate deletion while a reader is active.

Verify:

- Active reader remains protected.
- New read admission follows current coverage.
- Deletion completes after applicable references are released.
- Disk accounting remains accurate.

### 34.7 Repeated Crash Recovery

Simulate:

    Restart
      |
      v
    Segment Recovery
      |
      v
    Crash
      |
      v
    Restart Again

Recovery must remain idempotent.

---

## 35. Bootstrap and Handoff Tests

### 35.1 Live Capture Before Historical Processing

Verify that live retention protection is established before the historical processor begins consuming archives.

The test must reproduce the startup race that would otherwise create an unprotected live interval.

### 35.2 Historical/Live Trade Overlap

Create historical trades:

    100, 101, 102, 103, 104

and retained live trades:

    103, 104, 105, 106

Expected logical bootstrap sequence:

    100, 101, 102, 103, 104, 105, 106

The test must use a valid native execution identity model.

### 35.3 Boundary Conflict

Provide historical and live records with the same native execution identity but incompatible prices.

Expected:

    IntegrityConflict

The coordinator must not silently choose one record.

### 35.4 Missing Historical Coverage

Simulate an archive ending before the first captured live execution with no verifiable overlap.

Expected:

    BoundaryUnverified

or:

    VerifiedIncomplete

depending on available evidence.

Bootstrap must not claim full continuity.

### 35.5 Replay Catch-Up

Simulate continuous live ingestion while historical processing runs.

Verify that protected replay advances toward the live frontier without losing canonical cursor positions.

### 35.6 Replay Slower Than Ingestion

Configure:

    Live Ingestion Rate > Replay Processing Rate

Verify:

- Backlog growth is measured.
- Retention remains bounded.
- Protection capacity is enforced.
- Bootstrap failure or escalation occurs when necessary.

### 35.7 Atomic Live Handoff

Simulate:

    Last Replayed Cursor = 100

    Live Frontier = 105

Verify delivery of the required intervening events before continuous live processing begins.

### 35.8 L2 Generation Change

Change the authoritative BookStore generation during bootstrap handoff.

Expected:

- Obsolete delta chain rejected.
- New initialization snapshot acquired.
- Compatible handoff established.
- Historical L2 gaps remain explicit.

### 35.9 Bootstrap Consumer Crash

Crash the consumer during replay.

Verify:

- Persisted progress is reconciled.
- Protection lease behavior remains correct.
- No unverified continuity is claimed.
- Restarted bootstrap uses a new execution generation.

---

## 36. Property-Based Testing

Property-based tests should generate arbitrary sequences of:

    AppendEvent

    SealSegment

    RequestSpill

    CompleteSpill

    FailSpill

    OpenReader

    AdvanceReader

    CloseReader

    ProtectInterval

    AdvanceProtection

    ReleaseProtection

    ExpireProtection

    DeleteSegment

    Crash

    Restart

### 36.1 Identity Invariants

    One canonical event identity
    represents one logical event.

    Physical storage duplication
    does not create logical duplication.

    Cursor epochs remain distinct.

### 36.2 Coverage Invariants

    Advertised available coverage
    must be supported by valid storage.

    Missing required intervals
    must not be reported complete.

    Expired data must not remain
    advertised as replayable.

### 36.3 Protection Invariants

    Active protection prevents
    ordinary eviction of required events.

    Protection advancement is monotonic.

    One consumer cannot release
    another consumer's protection.

### 36.4 Resource Invariants

    Memory allocation remains bounded.

    Disk reservations remain bounded.

    Failed operations release resources.

    Readers cannot indefinitely prevent
    cleanup without an explicit lease.

### 36.5 Crash Invariants

    Incomplete files never become
    authoritative coverage.

    Manifest recovery is idempotent.

    Valid orphaned segments are
    reconciled before deletion.

    Memory-only events are not
    considered crash-durable.

### 36.6 Bootstrap Invariants

    Live protection precedes
    historical processing.

    Historical/live duplicates are
    reconciled using valid identities.

    Bootstrap handoff cannot skip
    required retained events.

    Current L2 synchronization does not
    prove historical L2 completeness.

---

## 37. Performance and Scalability

Retention must support sustained high-throughput market-data ingestion without becoming a bottleneck for canonical processing.

### 37.1 Benchmark Dimensions

Measure:

- Retention admission throughput.
- Per-event memory overhead.
- Segment sealing cost.
- Spill throughput.
- Parquet compression ratio.
- Disk write amplification.
- Manifest commit latency.
- Cursor-range read throughput.
- Replay processing throughput.
- Concurrent reader scalability.
- Memory eviction latency.
- Crash recovery time.

### 37.2 Workload Variation

Benchmarks should vary:

- Number of active streams.
- Trade event rate.
- L2 update rate.
- Average canonical event size.
- Number of protected streams.
- Historical bootstrap duration.
- Available RAM.
- Available disk capacity.
- Spill worker concurrency.
- Parquet row-group size.
- Compression codec.

### 37.3 Sustained Spill Benchmark

Test:

    Continuous Event Ingestion
              |
              v
    Memory Segments
              |
              v
    Parquet Spill
              |
              v
    Disk-Backed Retention

Verify that sustained spill throughput can support the target workload under the selected memory and disk budgets.

### 37.4 Bootstrap Stress Test

Simulate historical processing lasting significantly longer than the in-memory retention window.

Verify:

- Events spill to disk.
- Protected coverage remains available.
- Replay reads both RAM and Parquet.
- No canonical event identity changes.
- Bootstrap transitions to live correctly.

### 37.5 Performance Targets

Concrete throughput, memory, and disk performance targets must be established through measurement.

The specification must not assume that Parquet encoding, compression, or storage hardware can sustain every possible exchange data rate.

---

## 38. Implementation Acceptance Criteria

The Live Retention subsystem is complete when the following conditions are satisfied.

### 38.1 Architecture

- Retention is independent of optional PostgreSQL recording.
- The Retention Manager owns temporary canonical-event storage.
- Memory and disk tiers expose one logical retention interface.
- Retention remains downstream of canonical acceptance.
- Historical bootstrap and live acquisition execute concurrently.

### 38.2 Memory Safety

- Per-stream buffers are bounded.
- Global memory usage is bounded.
- Immutable segments are supported.
- Memory eviction respects active protections.
- Admission failures are explicit.
- Slow spill cannot cause uncontrolled memory growth.

### 38.3 Parquet Persistence

- Canonical identities survive Parquet serialization.
- Exact numerical values are preserved.
- Temporary files are not exposed as committed segments.
- File and manifest commits follow the durability contract.
- Incomplete spills are recoverable or explicitly rejected.
- Corrupted segments are detected.
- Segment deletion is crash-safe.

### 38.4 Coverage

- Per-stream cursor coverage is tracked.
- Cursor epochs remain distinct.
- Available and missing intervals are distinguishable.
- RAM/Parquet overlap does not create duplicate logical events.
- Expired events are not advertised as replayable.
- Coverage is never inferred solely from file existence.

### 38.5 Protection

- Bootstrap protection is explicitly admitted.
- Protected events cannot be silently evicted.
- Protection advancement is monotonic.
- Multiple overlapping protections are supported.
- Lease expiration is not interpreted as successful consumption.
- Protection failure is observable.

### 38.6 Bootstrap

- Live capture starts before historical processing.
- Historical/live boundaries are explicitly established.
- Native trade identities are used for overlap reconciliation.
- Missing historical coverage remains visible.
- L2 generation compatibility is enforced.
- Replay-to-live handoff is continuous under the selected contract.
- Bootstrap completion requires authoritative evidence.

### 38.7 Recovery

- Crash recovery reconstructs valid Parquet coverage.
- Orphaned segments are reconciled.
- Corrupted segments are quarantined.
- Memory-only data is not claimed durable.
- Disk exhaustion produces explicit degradation.
- Recovery operations remain idempotent.

### 38.8 Observability

- Retention memory and disk usage are exposed.
- Spill activity is measurable.
- Protection state is inspectable.
- Bootstrap progress is visible.
- Coverage gaps are reported.
- TUI and runtime API expose retention diagnostics.
- Operational logs remain local.

### 38.9 Testing

- Memory tests pass.
- Protection tests pass.
- Parquet round-trip tests pass.
- Atomic commit fault-injection tests pass.
- Manifest recovery tests pass.
- Cursor-read tests pass.
- Bootstrap overlap tests pass.
- Replay-to-live handoff tests pass.
- Concurrency tests pass.
- Property-based tests pass.
- Sustained-load benchmarks establish acceptable performance.

---

## 39. Final Architectural Contract

The Live Retention subsystem provides bounded, temporary storage for accepted canonical events.

Its principal responsibility is to preserve the live-data interval required while historical bootstrap is running.

The architecture is:

    Accepted Canonical Events
              |
              v
        Retention Manager
              |
              v
       Bounded Memory Store
              |
         +----+----+
         |         |
         v         v
    Live Readers  Spill Scheduler
         |         |
         |         v
         |     Parquet Workers
         |         |
         |         v
         |     Atomic File Commit
         |         |
         |         v
         |     Manifest Manager
         |         |
         +----+----+
              |
              v
       Unified Read Interface
              |
         +----+----+
         |         |
         v         v
    Subscriber   Bootstrap
      Replay     Coordinator
                    |
                    v
             Historical/Live Join
                    |
                    v
             Continuous Live Data

The subsystem guarantees:

- Explicit event admission.
- Stable canonical event identities.
- Bounded memory usage.
- Configurable Parquet spill.
- Crash-aware disk persistence.
- Recoverable segment metadata.
- Cursor-based retention access.
- Protection of admitted bootstrap intervals within the configured resource contract.
- Explicit reporting of unavailable coverage.
- Safe expiration and deletion.
- Historical-to-live handoff coordination.

It does not guarantee:

- Unlimited retention.
- Recovery of exchange events never acquired.
- Crash durability for memory-only events.
- Complete historical data from incomplete archives.
- Exactly-once application processing.
- Historical L2 continuity from an arbitrary current snapshot.
- Successful bootstrap when storage resources are insufficient.

The essential invariant is:

    No accepted canonical event required
    by an active, satisfiable protection
    may disappear silently.

The second essential invariant is:

    Bootstrap is complete only when
    historical coverage and continuous
    live delivery meet the explicitly
    selected correctness contract.

---

**End of Part 5 — Retention Read APIs, Failure Recovery, Observability, Testing, and Acceptance Criteria.**

**End of 09 — Live Retention and Parquet Spill.**