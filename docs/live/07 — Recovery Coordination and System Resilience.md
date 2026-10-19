## 1. Purpose

MarketForge Live must remain operational during network failures, exchange outages, invalid market data, resource exhaustion, process crashes, and partial subsystem failures.

The individual mechanisms required to handle these conditions are defined in earlier architecture documents.

This document specifies how those mechanisms cooperate to restore the runtime while preserving correctness, resource safety, and explicit data-integrity guarantees.

Its central component is the **Recovery Coordinator**.

The Recovery Coordinator evaluates failures, selects recovery actions, coordinates dependent components, tracks recovery progress, and escalates failures that cannot be resolved automatically.

It does not replace the lifecycle ownership of the Connection Manager, Subscription Manager, Trade Processor, or L2 Synchronization Coordinator.

### 1.1 Primary Objectives

The subsystem must:

- Receive structured failure reports from runtime components.
- Classify failures by scope, severity, and recoverability.
- Prevent duplicate or conflicting recovery operations.
- Select appropriate recovery strategies.
- Coordinate dependencies between recovery operations.
- Respect exchange rate limits and runtime resource budgets.
- Maintain recovery identity and generation safety.
- Track recovery progress independently for affected streams.
- Support partial recovery and degraded operation.
- Escalate repeated failures.
- Coordinate recovery after process and infrastructure failures.
- Preserve unresolved market-data coverage gaps.
- Expose recovery status through the runtime API and TUI.

Recovery must prioritize **restoring valid operation without misrepresenting data integrity**.

### 1.2 Fundamental Principle

MarketForge distinguishes restoring service from restoring historical completeness.

For example:

    WebSocket Reconnected
             |
             v
    Subscription Restored
             |
             v
    Live Processing Restored
             |
             v
    Historical Coverage Still Incomplete

This is a valid runtime condition.

The system must not claim that restoring connectivity also restores missing market-data events.

### 1.3 Recovery Guarantees

The architecture distinguishes:

| Guarantee | Meaning |
|---|---|
| TransportRestored | Physical exchange connectivity is operational |
| SubscriptionRestored | Required native subscriptions are established |
| ProcessingRestored | Normalization and integrity processing are operational |
| CurrentBookRestored | L2 book has a valid synchronized generation |
| HistoricalCoverageRestored | Required missing events have been recovered |
| PublicationRestored | Accepted canonical events can reach the publisher |
| RecordingRestored | Optional persistence is operational |

These guarantees are independent.

For example:

    TransportRestored = true

    SubscriptionRestored = true

    CurrentBookRestored = true

    HistoricalCoverageRestored = false

This means that current L2 state is valid, but historical incremental-update coverage remains incomplete.

The Recovery Coordinator must preserve these distinctions.

---

## 2. Architectural Boundaries

The Recovery Coordinator is a control-plane orchestration component.

It coordinates recovery actions through typed interfaces rather than directly implementing every recovery mechanism.

### 2.1 Existing Recovery Ownership

| Component | Recovery responsibility |
|---|---|
| Connection Manager | Physical reconnection, heartbeats, transport backoff |
| Subscription Manager | Native subscription restoration |
| Stream Registry | Desired-state reconciliation and lifecycle authority |
| Trade Processor | Trade integrity and continuity tracking |
| Trade Reconciliation Engine | Recovered execution matching and coverage evaluation |
| L2 Synchronization Coordinator | Snapshot acquisition, buffering, and synchronization |
| BookStore | Authoritative L2 state and generation validity |
| Publisher | Event delivery and subscriber recovery |
| Live Retention Manager | Temporary retained-event recovery |
| Recording Manager | Optional persistence recovery |
| Recovery Coordinator | Cross-component recovery decisions, dependencies, and escalation |

The Recovery Coordinator must not create competing implementations of these responsibilities.

### 2.2 Dependency Model

    Stream Registry
           |
           v
    Recovery Coordinator
           |
           +-------------------------+
           |                         |
           v                         v
    Connection Manager       Subscription Manager
           |                         |
           +------------+------------+
                        |
                        v
                 Stream Processing
                        |
                 +------+------+
                 |             |
                 v             v
          Trade Recovery   L2 Synchronization
                 |             |
                 +------+------+
                        |
                        v
                Integrity Evaluation
                        |
                        v
                Recovery Completion

This diagram represents orchestration dependencies, not ownership of every underlying operation.

The Connection Manager remains responsible for deciding how to execute a requested physical reconnection.

The Subscription Manager remains responsible for native subscription operations.

### 2.3 Control and Data Planes

Recovery coordination belongs to the control plane.

Market-data decoding, normalization, and publication remain in the data plane.

Conceptually:

    CONTROL PLANE

    Recovery Coordinator
           |
           v
    Recovery Commands
           |
           v
    Component Supervisors


    DATA PLANE

    Exchange Messages
           |
           v
    Normalization
           |
           v
    Integrity Validation
           |
           v
    Canonical Publication

The Recovery Coordinator must not become a bottleneck through which every market-data event passes.

It processes failure reports, recovery decisions, and recovery outcomes.

### 2.4 Design Constraints

- Rust-only runtime coordination.
- No Python dependency during normal recovery.
- No PostgreSQL dependency for operational recovery state.
- No direct TUI dependencies.
- No market-data processing inside the Recovery Coordinator.
- No independent WebSocket clients inside the coordinator.
- No independent trade normalization implementation.
- No independent L2 synchronization implementation.
- No unbounded recovery queues.
- No unlimited retry loops.
- No duplicate recovery jobs for the same authoritative failure.
- No stale recovery completion modifying current state.
- No automatic assumption that service restoration proves data completeness.

Recovery operations must remain observable, cancellable, and bounded.

---

## 3. Recovery Coordinator

The Recovery Coordinator maintains recovery state and orchestrates actions across runtime components.

Its authority is limited to recovery scheduling, dependency coordination, and escalation.

### 3.1 Responsibilities

The coordinator must:

- Accept failure reports.
- Identify affected resources.
- Determine failure scope.
- Classify recoverability.
- Identify the component responsible for remediation.
- Create or reuse recovery operations.
- Schedule recovery actions.
- Enforce concurrency limits.
- Respect applicable cooldowns and rate limits.
- Coordinate dependent recovery stages.
- Track recovery outcomes.
- Update system-level recovery status.
- Escalate unresolved failures.
- Report completion and partial completion.

### 3.2 RecoveryCoordinator State

Conceptual representation:

    RecoveryCoordinator {
        active_recoveries
        pending_recoveries

        recovery_generation_registry
        failure_registry

        dependency_state
        resource_budgets

        escalation_state
        recovery_metrics
    }

This representation is illustrative.

The implementation should avoid a global lock covering the full execution of recovery operations.

### 3.3 RecoveryOperation

Every recovery operation receives a unique identity.

Conceptually:

    RecoveryOperation {
        recovery_id

        target
        failure_id

        recovery_kind
        recovery_generation

        configuration_revision
        metadata_revision

        state
        priority

        created_at
        started_at
        deadline

        last_error
        result
    }

A recovery operation may target:

    Connection
    Subscription
    TradeStream
    L2Book
    Publisher
    Retention
    Recording
    RuntimeSubsystem

The exact target type must be explicit.

### 3.4 RecoveryOperation States

Supported states:

    Created
    PendingDependencies
    PendingAdmission
    Scheduled
    Running
    Validating
    Completed
    PartiallyCompleted
    Failed
    Cancelled

| State | Meaning |
|---|---|
| Created | Recovery operation registered |
| PendingDependencies | Required prerequisite operations are incomplete |
| PendingAdmission | Waiting for rate-limit or resource capacity |
| Scheduled | Eligible for execution |
| Running | Recovery action underway |
| Validating | Result being checked |
| Completed | Required recovery guarantees satisfied |
| PartiallyCompleted | Some guarantees restored, others unresolved |
| Failed | Recovery operation unsuccessful |
| Cancelled | Operation superseded or intentionally stopped |

A recovery operation must not transition to `Completed` merely because an underlying API request returned successfully.

### 3.5 Recovery Identity

A recovery operation is identified by:

    RecoveryId

Its authority is scoped by:

    RecoveryGeneration

The target's current runtime identity must also be validated.

For example, L2 recovery may reference:

    StreamId
    SubscriptionGeneration
    BookGeneration
    RecoveryGeneration

Historical trade recovery may refer to older execution intervals while still belonging to a valid current recovery session.

The coordinator must not reject historical records solely because their original exchange connection generation is obsolete.

### 3.6 Recovery State Ownership

The Recovery Coordinator owns the authoritative state of recovery operations.

It does not own the authoritative state of the recovered resource.

For example:

    Recovery Coordinator:
        L2 Recovery Completed

    BookStore:
        Book Generation 12 Synchronized

The BookStore remains authoritative for its synchronization state.

The coordinator's completion result must be based on confirmation from the responsible component.

---

## 4. Failure Reporting

Runtime components report failures through a common typed interface.

Failure reporting must be asynchronous and must not block market-data processing on expensive recovery decisions.

### 4.1 FailureReport

Conceptual representation:

    FailureReport {
        failure_id

        source_component
        affected_resource

        failure_class
        severity
        recoverability

        connection_generation
        subscription_generation
        processing_generation

        configuration_revision

        detected_at
        context
    }

Not every failure report requires every generation field.

The applicable identity depends on the affected resource.

### 4.2 Failure Classes

Initial failure classes:

    TransportFailure
    SubscriptionFailure
    ProtocolFailure
    NormalizationFailure
    TradeContinuityFailure
    L2SynchronizationFailure
    ResourceExhaustion
    PublicationFailure
    RetentionFailure
    RecordingFailure
    ConfigurationFailure
    InfrastructureFailure

The failure class identifies the subsystem and type of problem.

It is distinct from failure severity.

### 4.3 Failure Severity

Supported severity levels:

    Informational
    Warning
    Degraded
    Critical

| Severity | Meaning |
|---|---|
| Informational | No recovery action currently required |
| Warning | Potential problem requiring observation or validation |
| Degraded | Some operational or integrity guarantees are impaired |
| Critical | Required service or safety guarantees cannot be maintained |

Severity must not be inferred solely from the number of error messages.

One confirmed L2 sequence gap may require immediate book invalidation.

Several transient connection warnings may not require further escalation.

### 4.4 Recoverability

Failure recoverability is classified separately.

Supported values:

    AutomaticallyRecoverable
    ConditionallyRecoverable
    RequiresConfigurationCorrection
    RequiresOperatorIntervention
    Unrecoverable

Examples:

| Failure | Classification |
|---|---|
| Temporary connection reset | AutomaticallyRecoverable |
| REST request timeout | AutomaticallyRecoverable |
| Exchange rate-limit cooldown | ConditionallyRecoverable |
| Unsupported channel | RequiresConfigurationCorrection |
| Invalid contract metadata | RequiresConfigurationCorrection |
| Authentication rejection | RequiresConfigurationCorrection |
| Repeated disk exhaustion | RequiresOperatorIntervention |
| Missing historical trades beyond retention | Unrecoverable for that interval |

An unrecoverable historical coverage interval does not necessarily require terminating current live ingestion.

### 4.5 Failure Scope

The coordinator must determine whether a failure affects:

    Record
    Stream
    Subscription
    Connection
    Endpoint
    Exchange
    RuntimeSubsystem
    EntireRuntime

For example:

    One malformed trade
        -> Record scope

    One invalid L2 book
        -> Stream scope

    One WebSocket disconnect
        -> Connection scope

    Exchange-wide connectivity outage
        -> Exchange scope

    Global memory exhaustion
        -> Runtime scope

The selected recovery action must match the failure scope.

### 4.6 Failure Deduplication

Multiple components may report the same underlying failure.

Example:

    WebSocket connection closes
          |
          +-- Reader reports EOF
          |
          +-- Heartbeat reports timeout
          |
          +-- Writer reports failure

These reports must not create three independent physical reconnection operations.

The Recovery Coordinator groups related failures by:

    Affected Resource
    Failure Generation
    Failure Classification
    Causal Context

The Connection Manager remains the single owner of physical reconnection.

### 4.7 Failure Causality

The coordinator should preserve causal relationships between failures.

For example:

    Connection Failure
          |
          v
    Subscription Lost
          |
          v
    Trade Coverage Unverified

These are related conditions, not three unrelated root causes.

Similarly:

    Connection Failure
          |
          v
    L2 Subscription Lost
          |
          v
    Book Invalidated

The coordinator should track the causal chain without suppressing important downstream integrity consequences.

### 4.8 Failure Event Delivery

Failure reports must use bounded control-plane queues.

If the reporting queue is saturated:

- Do not block the market-data hot path indefinitely.
- Preserve critical recovery information through a bounded emergency mechanism where possible.
- Mark affected component health as impaired.
- Escalate when recovery coordination can no longer be guaranteed.

A failure-reporting overload must not silently leave invalid books classified as synchronized.

---

## 5. Recovery Planning

The Recovery Coordinator converts classified failures into explicit recovery plans.

A recovery plan defines the actions required to restore the affected operational guarantees, the dependencies between those actions, and the conditions under which recovery may be considered complete.

The Recovery Coordinator selects and schedules actions.

The component responsible for each action remains its execution owner.

### 5.1 RecoveryPlan

Conceptual representation:

    RecoveryPlan {
        plan_id
        failure_id

        target
        recovery_generation

        required_guarantees
        recovery_steps

        dependency_graph
        admission_requirements

        priority
        deadline

        current_state
        completion_evidence
    }

A recovery plan may contain one or several recovery operations.

Plans must remain bounded, cancellable, and observable.

### 5.2 Recovery Planning Inputs

A plan is constructed using:

- Failure classification.
- Affected resource identity.
- Current Stream Registry desired state.
- Current component lifecycle state.
- Connection and subscription generations.
- Configuration revision.
- Instrument metadata revision.
- Available recovery mechanisms.
- Exchange protocol capabilities.
- Applicable rate limits.
- Runtime resource availability.
- Required recovery guarantees.

The coordinator must not create recovery operations for resources that are no longer desired.

For example, a stream removed during an exchange outage must not be restored merely because an earlier recovery plan remains pending.

### 5.3 Recovery Action Types

The initial supported action types are:

    RestoreConnection

    RestoreSubscription

    RestartProcessor

    ReconcileTrades

    ResynchronizeBook

    RestorePublication

    RestoreRetention

    RestoreRecording

    ValidateConfiguration

    EscalateFailure

Each action delegates execution to the component that owns the corresponding mechanism.

For example:

    RestoreConnection
        -> Connection Manager

    RestoreSubscription
        -> Subscription Manager

    ReconcileTrades
        -> Trade Reconciliation Engine

    ResynchronizeBook
        -> L2 Synchronization Coordinator

The coordinator must not duplicate these implementations.

### 5.4 Recovery Plan Construction

Conceptually:

    Failure Report
          |
          v
    Classify Failure
          |
          v
    Determine Affected Resources
          |
          v
    Identify Required Guarantees
          |
          v
    Select Recovery Actions
          |
          v
    Resolve Dependencies
          |
          v
    Validate Recovery Plan
          |
          v
    Schedule Eligible Actions

An invalid or unsupported recovery plan must produce an explicit failure.

The coordinator must not silently substitute an unrelated recovery strategy.

### 5.5 Recovery Plan Validation

Before execution, the coordinator verifies:

- The target still exists.
- Recovery remains necessary.
- The requested desired state permits recovery.
- The configuration revision is compatible.
- Required component interfaces are available.
- The proposed actions can satisfy the requested guarantees.
- Dependency relationships are valid.
- Resource and admission requirements are defined.
- The plan does not create conflicting ownership.
- Recovery generations are valid.

Plans referencing obsolete runtime state must be cancelled or recompiled.

### 5.6 Recovery Plan Idempotency

Repeated failure notifications must not create independent plans that perform the same authoritative recovery action.

Example:

    Reader reports connection loss.

    Heartbeat reports timeout.

    Writer reports connection closure.

All three reports may refer to one failed physical connection generation.

The resulting connection recovery operation must have one authoritative owner.

Additional reports may enrich the existing failure context without creating duplicate reconnection attempts.

### 5.7 Recovery Completion Evidence

A recovery plan is complete only when the responsible components confirm the required guarantees.

Example:

    Requested Guarantee:
        CurrentBookRestored

Required evidence:

    Subscription operational
    +
    Authoritative snapshot established
    +
    Sequence synchronization valid
    +
    New book generation committed

A successful REST snapshot request alone does not satisfy the guarantee.

---

## 6. Recovery Dependency Coordination

Recovery operations frequently depend on other operations.

The coordinator must execute them in a valid dependency order.

### 6.1 Dependency Model

Recovery dependencies form a directed acyclic graph.

Conceptually:

    RecoveryPlan
        |
        +-- RestoreConnection
        |          |
        |          v
        |   RestoreSubscription
        |          |
        |          v
        |   RestoreProcessing
        |          |
        |     +----+----+
        |     |         |
        |     v         v
        | Reconcile   Resynchronize
        | Trades      Book
        |
        +-- RestorePublication

Not every recovery plan requires every action.

The required graph depends on the failure and affected resources.

### 6.2 Dependency States

Each recovery dependency may be:

    Pending
    Satisfied
    Failed
    Cancelled
    Unavailable

An operation may begin only when its required dependencies are satisfied.

### 6.3 Transport Dependency

Native subscription restoration requires a usable physical connection.

Therefore:

    RestoreConnection
            |
            v
    RestoreSubscription

However, the Connection Manager already owns automatic reconnection.

The Recovery Coordinator must observe or request that mechanism rather than launch another competing connection supervisor.

### 6.4 Processing Dependency

Normal stream processing requires a valid subscription assignment and compiled processing context.

Therefore:

    SubscriptionRestored
             |
             v
    ProcessorReady

The processing subsystem must independently validate its configuration and instrument context.

A successful subscription acknowledgement does not establish processing readiness.

### 6.5 Trade Recovery Dependencies

Trade-history acquisition does not always require the current WebSocket subscription to be restored.

For example, historical REST retrieval may proceed while the exchange's public WebSocket endpoint remains unavailable.

Therefore:

    Historical Trade Acquisition

may execute independently of:

    Current WebSocket Restoration

provided the relevant REST service, metadata, and recovery interfaces remain available.

Reconciliation with the current live frontier may require additional information from the restored stream.

The dependency graph must reflect the actual requirements rather than impose unnecessary serialization.

### 6.6 L2 Recovery Dependencies

Current L2 reconstruction requires an authoritative synchronization baseline.

For a REST-snapshot protocol:

    WebSocket Subscription
             |
             v
    Delta Buffering Active
             |
             v
    REST Snapshot Acquisition
             |
             v
    Snapshot Bridge
             |
             v
    Book Generation Committed

For a WebSocket-snapshot protocol:

    WebSocket Subscription
             |
             v
    Authoritative Snapshot
             |
             v
    Book Generation Committed

These procedures are owned by the Synchronization Coordinator.

The Recovery Coordinator tracks their completion without implementing their internal algorithms.

### 6.7 Publication Dependencies

Publisher recovery may be independent of exchange connectivity.

For example, a subscriber queue failure may require only subscriber-session recovery.

Conversely, restoring a processing pipeline after publication failure may require coordinated event replay or explicit delivery-gap reporting.

The dependency graph must distinguish:

    Source Acquisition Recovery

from:

    Event Delivery Recovery

The two must not be treated as equivalent.

### 6.8 Dependency Failure

If a prerequisite operation fails:

    Dependency Failed
           |
           v
    Block Dependent Actions
           |
           v
    Evaluate Alternative Recovery
           |
           v
    Retry / Escalate / Cancel

The coordinator must not execute a dependent action using invalid prerequisite state.

### 6.9 Dependency Revalidation

Dependencies may become invalid while a recovery plan is running.

Example:

    Connection restored.

    Subscription restoration begins.

    Connection fails again.

The coordinator must invalidate or suspend dependent recovery actions.

A previously satisfied dependency must not remain trusted after its authoritative generation becomes obsolete.

---

## 7. Recovery Scope and Shared Failures

One underlying failure may affect many logical streams.

Recovery should be coordinated at the narrowest scope that correctly represents the failure.

### 7.1 Recovery Scopes

Supported scopes:

    Record
    Stream
    Subscription
    Connection
    Endpoint
    Exchange
    RuntimeSubsystem
    Runtime

The coordinator must distinguish the affected resources from the root recovery target.

### 7.2 Shared Connection Failure

Example:

    Physical Connection A
            |
            +-- BTCUSDT Trades
            |
            +-- ETHUSDT Trades
            |
            +-- BTCUSDT L2

If Connection A fails, the system should initiate one physical reconnection operation.

The affected logical streams still require separate downstream restoration.

Conceptually:

    Connection A Failed
             |
             v
    One Connection Recovery
             |
             v
    Subscription Restoration
             |
       +-----+-----+
       |     |     |
       v     v     v
    Trade  Trade  L2
    Ready  Ready  Sync

The L2 stream does not become active until its book is synchronized.

Trade coverage may remain unverified after live acquisition resumes.

### 7.3 Shared Exchange Failure

An exchange outage may affect many endpoints and connections.

The coordinator should group related failures for:

- Exchange-level health assessment.
- Recovery prioritization.
- Rate-limit coordination.
- Operational reporting.
- Escalation.

It must not collapse all stream-level integrity outcomes into one exchange-level status.

For example, two trade streams affected by the same outage may have different historical recovery capabilities.

### 7.4 Failure Isolation

Failures should remain isolated wherever their ownership boundaries permit.

Examples:

    One malformed trade
        -> Trade stream integrity

    One L2 sequence gap
        -> Affected L2 book

    One WebSocket disconnect
        -> Assigned subscriptions

    One PostgreSQL outage
        -> Recording subsystem

A PostgreSQL recording outage must not automatically terminate healthy live acquisition when recording is optional and the configured delivery guarantees remain satisfiable.

### 7.5 Shared Recovery Deduplication

The coordinator should maintain a recovery-operation index keyed by the authoritative recovery target and generation.

Conceptually:

    RecoveryTargetKey {
        resource_identity
        recovery_kind
        resource_generation
    }

If an equivalent recovery operation already exists, a new request should reference that operation rather than create a duplicate.

### 7.6 Recovery Scope Expansion

A local failure may reveal a broader problem.

Example:

    One connection fails.
             |
             v
    Reconnection attempted.
             |
             v
    Several exchange endpoints fail.
             |
             v
    Exchange health becomes degraded.

The coordinator may expand failure scope when supporting evidence justifies it.

Scope expansion must not retroactively convert uncertain failures into confirmed exchange-wide outages without evidence.

---

## 8. Recovery Prioritization

Recovery operations compete for finite local resources and exchange API budgets.

The coordinator must schedule them according to explicit priorities.

### 8.1 Priority Objectives

Recovery scheduling should:

- Restore healthy current acquisition.
- Prevent further loss of observable market-data coverage.
- Restore valid L2 state.
- Recover missing trades while historical APIs remain available.
- Avoid overloading the exchange.
- Preserve optional recording and retention guarantees.
- Prevent starvation of lower-priority operations.

Priorities must remain configurable.

### 8.2 Priority Classes

Initial conceptual classes:

    Critical
    High
    Normal
    Background

| Priority | Typical operations |
|---|---|
| Critical | Restore failed shared infrastructure required for active streams |
| High | Restore current subscriptions and valid book state |
| Normal | Recover bounded historical trade gaps |
| Background | Nonurgent reconciliation and diagnostics |

These are initial scheduling defaults, not immutable rules.

For example, a trade-recovery endpoint with a short retention window may require higher priority than a routine L2 refresh.

### 8.3 Priority Inputs

Priority calculation may consider:

- Number of affected streams.
- Current acquisition availability.
- Required user-selected service guarantees.
- Historical recovery time sensitivity.
- Exchange endpoint availability.
- Recovery deadline.
- Failure severity.
- Number of dependent consumers.
- Recovery cost.
- Existing retry history.

The coordinator must not base priority solely on the order in which failure reports arrive.

### 8.4 Recovery Deadline

A recovery deadline limits how long an operation may remain active or pending.

The deadline does not necessarily imply that failure must permanently terminate the stream.

For example:

    Historical REST recovery deadline exceeded.

Possible result:

    Recovery operation failed.
    Live stream remains active.
    Historical coverage remains incomplete.

The coordinator must distinguish operation deadlines from persistent desired stream state.

### 8.5 Starvation Prevention

High-priority recovery operations must not indefinitely starve normal or background operations.

The scheduler should support a fairness mechanism such as:

    Priority Aging

or:

    Weighted Fair Scheduling

The precise scheduling algorithm may be selected during implementation.

It must remain deterministic under controlled testing.

### 8.6 Priority Changes

Recovery priorities may change during operation.

For example, a historical gap may become more urgent as the exchange's recent-trade retention window approaches expiration.

Priority changes must not invalidate already accepted recovery records.

The scheduler may reorder pending operations but must preserve each operation's identity and state.

---

## 9. Recovery Resource Budgets

Recovery workloads must remain bounded.

The Recovery Coordinator must cooperate with the existing Rate Limiter and Runtime Resource Manager.

### 9.1 Managed Budgets

Relevant budgets include:

    MaximumConcurrentRecoveries

    MaximumConcurrentRestRequests

    MaximumRecoveryMemoryBytes

    MaximumRecoveryBufferBytes

    MaximumPendingRecoveryOperations

    MaximumConcurrentBookSynchronizations

    MaximumConcurrentTradeReconciliations

    MaximumRecoveryCpuConcurrency

The exact configuration keys belong to **03 — Configuration System**.

No arbitrary numeric limits are hardcoded in this document.

### 9.2 Resource Admission

Before executing a recovery action:

    Recovery Operation
            |
            v
    Check Dependencies
            |
            v
    Check Local Capacity
            |
            v
    Acquire Required Permits
            |
            v
    Execute Through Owner

If admission is unavailable, the operation remains pending.

It must not consume unbounded memory while waiting.

### 9.3 Exchange Rate Limits

Recovery operations may share exchange quotas with:

- Connection establishment.
- Subscription requests.
- Snapshot acquisition.
- Public trade history.
- Other REST market-data queries.

The existing shared Rate Limiter is authoritative for those budgets.

The Recovery Coordinator must not maintain an independent exchange quota model that can disagree with the Rate Limiter.

### 9.4 Rate-Limit Cooldowns

Exchange-imposed cooldowns take precedence over local scheduling preferences.

If a REST endpoint returns an applicable cooldown:

    Recovery Deferred
           |
           v
    Respect Exchange Deadline
           |
           v
    Reevaluate Dependencies
           |
           v
    Retry When Admitted

The coordinator must not bypass the cooldown by creating additional equivalent recovery operations.

### 9.5 Recovery Memory

Recovery may temporarily retain:

- Buffered live trades.
- Retrieved historical executions.
- Reconciliation identities.
- Candidate L2 snapshots.
- Buffered L2 deltas.
- Diagnostic information.

Each component owns its corresponding data buffers.

The coordinator tracks admission and aggregate resource consumption without duplicating the underlying storage.

### 9.6 Resource Exhaustion

When recovery capacity is exhausted:

- Defer noncritical recovery operations.
- Preserve existing active acquisition when safe.
- Avoid unbounded queue growth.
- Report resource pressure.
- Respect operation deadlines.
- Escalate when required guarantees cannot be maintained.

The coordinator must not silently discard pending recovery work.

### 9.7 Resource Release

Resources must be released when an operation:

    Completes

    Fails

    Is Cancelled

    Is Superseded

Resource release must be idempotent.

Consumed exchange request quotas are not refunded merely because a recovery attempt fails.

---

## 10. Recovery Escalation

Recovery escalation determines what happens when ordinary recovery actions repeatedly fail.

Escalation must be explicit and based on failure classification.

### 10.1 Escalation Principle

Escalation should increase recovery scope only when necessary.

For example:

    L2 Sequence Violation
              |
              v
    Retry Synchronization
              |
              v
    Resubscribe if Required
              |
              v
    Replace Connection if Required
              |
              v
    Escalate Stream Failure

The specific progression depends on the exchange protocol and observed failure.

A sequence violation does not automatically justify replacing an otherwise healthy physical connection.

### 10.2 Escalation Levels

Supported conceptual levels:

    Normal
    Elevated
    Degraded
    Critical
    ManualIntervention

| Level | Meaning |
|---|---|
| Normal | Expected transient recovery |
| Elevated | Repeated failures require stronger measures |
| Degraded | Required guarantees remain impaired |
| Critical | Continued operation cannot satisfy essential safety or integrity requirements |
| ManualIntervention | Automatic recovery cannot safely proceed |

### 10.3 Escalation Inputs

Escalation may consider:

- Consecutive recovery failures.
- Failure duration.
- Repeated failure classification.
- Affected resource scope.
- Current service availability.
- Integrity consequences.
- Rate-limit restrictions.
- Available alternative recovery mechanisms.

The thresholds belong to runtime configuration.

### 10.4 Permanent Errors

Permanent configuration errors must not trigger unlimited retries.

Examples:

    Unsupported Channel

    Invalid Instrument Metadata

    Unsupported Quantity Semantics

    Authentication Rejection

    Invalid Endpoint Configuration

These require configuration correction or operator intervention.

The coordinator must preserve the failure reason and stop futile automatic recovery actions.

### 10.5 Repeated Transient Errors

Repeated transient errors may justify:

- Longer backoff.
- Reduced recovery concurrency.
- Temporary endpoint suspension.
- Connection replacement.
- Exchange-level degradation.
- Operator notification.

The coordinator must avoid turning a temporary exchange outage into a local reconnection storm.

### 10.6 Partial Recovery

A recovery operation may restore some guarantees while leaving others unresolved.

Example:

    TransportRestored = true

    SubscriptionRestored = true

    ProcessingRestored = true

    HistoricalCoverageRestored = false

The operation may be classified as:

    PartiallyCompleted

The remaining coverage problem must stay visible.

Partial success must not be collapsed into a misleading general `Recovered` status.

### 10.7 Recovery Exhaustion

A recovery action may become impossible because:

- Historical data is no longer available.
- The exchange lacks a compatible recovery endpoint.
- Required metadata revisions are unavailable.
- Resource limits prevent safe operation.
- Configuration is invalid.
- Repeated recovery attempts exceed policy limits.

The coordinator must terminate the affected recovery operation with an explicit outcome.

It must not continue retrying indefinitely.

### 10.8 Operational Escalation

Escalated failures must be exposed through:

- Runtime API.
- Ratatui TUI.
- Internal metrics.
- Local structured logs.

The coordinator must not require Prometheus, Grafana, or PostgreSQL for operational escalation.

---
## 11. Process Crash Recovery

MarketForge Live must recover predictably after unexpected process termination.

Possible causes include:

- Unhandled application failures.
- Operating-system process termination.
- Machine restarts.
- Power loss.
- Runtime resource exhaustion.
- Fatal dependency failures.
- Hardware failures.

A process crash destroys in-memory runtime state.

The recovery architecture must distinguish persistent declarations from transient operational state.

### 11.1 Restart Principle

After a crash, MarketForge must reconstruct its runtime from validated persistent inputs.

It must not assume that previously active connections, subscriptions, processors, or synchronized books remain valid.

The fundamental rule is:

    Persistent Configuration
              +
    Persistent Instrument Metadata
              +
    Recoverable Event State
              |
              v
       New Runtime Instance

A restarted process establishes new runtime generations.

### 11.2 Persistent and Transient State

| State | Persistence |
|---|---|
| Exchange protocol configurations | Local TOML |
| Trade and depth format configurations | Local TOML |
| Desired stream declarations | Persistent configuration |
| Instrument metadata | Python-managed PostgreSQL catalog |
| Instrument metadata snapshot | Versioned local cache |
| Physical WebSocket connections | Transient |
| Native subscription assignments | Transient |
| Processing tasks | Transient |
| Current L2 BookStore contents | Transient by default |
| Subscription acknowledgements | Transient |
| Recovery operations | Transient unless explicitly checkpointed |
| Temporary retained market data | Memory and Parquet, according to retention policy |
| Optional recorded market data | PostgreSQL |
| Operational logs | Local structured files |

Persistent declarations describe what the runtime should reconstruct.

Transient state describes what was previously happening.

The latter must not automatically become authoritative after restart.

### 11.3 Crash Recovery Sequence

    Process Starts
          |
          v
    Initialize Runtime Services
          |
          v
    Validate Local Configuration
          |
          v
    Load Instrument Metadata Snapshot
          |
          v
    Restore Desired Stream Declarations
          |
          v
    Inspect Recoverable Local State
          |
          v
    Initialize Registries and Managers
          |
          v
    Establish New Runtime Generation
          |
          v
    Reconcile Desired Stream States
          |
          v
    Restore Exchange Connectivity
          |
          v
    Restore Subscriptions
          |
          v
    Initialize Trade and L2 Processing
          |
          v
    Evaluate Historical Coverage
          |
          v
    Resume Normal Operation

The process must not report all previously active streams as operational immediately after loading their declarations.

Each stream must pass its normal activation and readiness requirements.

### 11.4 Runtime Instance Identity

Every process lifetime must have a unique runtime-instance identity.

Conceptually:

    RuntimeInstanceId

This identity distinguishes events, operations, and cursors produced by different process incarnations.

Example:

    Runtime Instance A
        Connection 12, Generation 4

    Process crashes.

    Runtime Instance B
        Connection 12, Generation 1

Even if numerical connection identifiers are reused internally, the complete runtime identity must remain distinguishable.

Persistent event provenance must not contain ambiguous generation identifiers.

### 11.5 Stream Identity Restoration

Logical StreamId values should remain stable across process restarts when the corresponding persistent stream declarations remain unchanged.

The Stream Registry reconstructs these identities from validated persistent mappings.

It must not create new logical streams merely because the process restarted.

However, all physical connection and subscription generations must be reestablished.

### 11.6 L2 State After Restart

The initial policy is:

    All L2 Books = Uninitialized

A previously persisted operational book snapshot does not automatically establish current synchronized market state.

Each required book must complete the configured synchronization procedure.

For example:

    Binance:
        REST snapshot + buffered deltas

    Bybit:
        WebSocket snapshot

    OKX:
        WebSocket snapshot

    Bitget:
        WebSocket snapshot

    Gate.io:
        REST snapshot + buffered deltas

The exact mechanism is selected by the compiled exchange configuration.

### 11.7 Trade State After Restart

Trade normalization can resume when its subscription and processing context are restored.

However, continuity across the crash interval is not automatically established.

The runtime must identify:

    Last Known Accepted Execution

    Last Durable Execution, if available

    First Execution After Restart

These boundaries may differ.

A trade accepted into memory shortly before the crash may not have reached durable retention.

The Recovery Coordinator must account for this distinction when evaluating recovery coverage.

### 11.8 Recovery After Unclean Shutdown

An unclean shutdown must trigger additional consistency checks.

These may include:

- Incomplete local Parquet segments.
- Unfinished retention writes.
- Uncommitted publication records.
- Incomplete recording batches.
- Missing runtime shutdown markers.
- Unresolved coverage intervals.
- Stale temporary files.

The responsible component performs each check.

The Recovery Coordinator coordinates their outcomes.

It must not independently parse or repair every storage format.

---

## 12. Persistent Recovery State

Not all recovery state needs to survive process termination.

However, information required to avoid misrepresenting historical coverage must remain recoverable.

### 12.1 Persistence Principle

Recovery state is divided into:

    Operational Recovery State

    Coverage and Durability Evidence

Operational recovery state includes:

- Pending retry timers.
- Active recovery tasks.
- Current connection operations.
- Temporary synchronization attempts.

These are normally transient.

Coverage and durability evidence includes:

- Known unresolved market-data intervals.
- Last durable publication boundaries.
- Retained segment coverage.
- Recording commit boundaries.
- Relevant metadata revisions.

These may require persistence.

### 12.2 Recovery Checkpoints

A recovery checkpoint may describe:

    RecoveryCheckpoint {
        runtime_instance_id
        checkpoint_revision

        stream_id
        last_durable_cursor

        coverage_intervals
        retained_segment_references

        configuration_revision
        metadata_revision

        checkpoint_timestamp
    }

This representation is conceptual.

The actual checkpoint schema belongs to the component responsible for the persisted information.

### 12.3 Checkpoint Limitations

A checkpoint may be stale when the process crashes.

Therefore:

    Last Checkpoint
        !=
    Exact Final Runtime State

On restart, the runtime must reconcile checkpoints against available durable data.

It must not assume that every event accepted after the checkpoint was lost or successfully persisted.

### 12.4 Idempotent Restart Recovery

Restart recovery must tolerate repeated execution.

For example:

    Process starts.

    Recovery begins.

    Process crashes again.

    Process starts again.

Repeating recovery must not create duplicate authoritative stream registrations or duplicate accepted canonical executions.

Where durable identity information is unavailable, the system must report the limitation rather than claim exactly-once recovery.

### 12.5 Storage Ownership

Persistent recovery metadata remains local unless another subsystem explicitly owns durable storage.

PostgreSQL must not become the default operational recovery database.

It remains reserved for optional market-data recording and the existing Python-managed instrument catalog.

The Recovery Coordinator must not create an additional PostgreSQL dependency for its own lifecycle management.

---

## 13. Local Disk Exhaustion

Disk exhaustion is a critical infrastructure failure because MarketForge may rely on local storage for temporary retained market data, Parquet spill files, checkpoints, and operational logs.

The runtime must handle disk exhaustion without corrupting accepted data or silently weakening durability guarantees.

### 13.1 Disk Consumers

Relevant consumers include:

- Temporary Parquet retention.
- Local metadata snapshots.
- Configuration files.
- Recovery checkpoints.
- Structured operational logs.
- Temporary atomic-write files.
- Other explicitly configured runtime artifacts.

Each component owns its storage behavior.

The Recovery Coordinator coordinates failures affecting multiple components.

### 13.2 Disk Health

The runtime should monitor:

    AvailableDiskSpace

    DiskUsage

    ConfiguredDiskBudget

    WriteFailureCount

    FilesystemAvailability

Additional diagnostics may include inode availability and filesystem error conditions.

Disk monitoring belongs to the runtime resource and metrics infrastructure.

### 13.3 Disk Pressure Levels

Supported conceptual conditions:

    Healthy
    Warning
    Critical
    Exhausted

| Condition | Meaning |
|---|---|
| Healthy | Storage capacity is sufficient |
| Warning | Available capacity is declining |
| Critical | Storage safety threshold is approaching |
| Exhausted | Required writes cannot be completed |

Thresholds belong to runtime configuration.

### 13.4 Disk Exhaustion Response

When a required write fails:

    Disk Write Failure
           |
           v
    Identify Storage Owner
           |
           v
    Determine Affected Guarantees
           |
           v
    Prevent Further Unsafe Writes
           |
           v
    Preserve Existing Valid Data
           |
           v
    Apply Component Recovery Policy
           |
           v
    Report Degradation

The runtime must not continue reporting successful retention when data can no longer be retained.

### 13.5 Temporary Parquet Retention

If Parquet spill becomes unavailable:

- The Retention Manager reports the failure.
- Existing valid files remain preserved.
- Incomplete segments are quarantined or recovered according to the storage contract.
- Additional memory buffering remains bounded.
- The runtime evaluates whether lossless retention is still possible.
- Required retention guarantees are degraded when they cannot be maintained.

The Recovery Coordinator does not independently decide to discard retained market events.

### 13.6 Recording Independence

Failure of temporary local retention and failure of optional PostgreSQL recording are separate conditions.

For example:

    Parquet Retention Failed

does not necessarily imply:

    PostgreSQL Recording Failed

Likewise:

    PostgreSQL Recording Failed

does not necessarily imply:

    Local Retention Failed

The coordinator must identify the guarantees affected by each failure.

### 13.7 Log Storage Exhaustion

Operational logs remain local.

If log storage becomes unavailable, the logging subsystem should:

- Report the condition through available runtime health mechanisms.
- Stop uncontrolled log-file growth.
- Apply a bounded fallback policy.
- Avoid recursively generating additional log-write failures.

A logging failure must not automatically terminate market-data processing unless the configured operational safety policy requires it.

### 13.8 Filesystem Recovery

After disk capacity is restored:

- Validate existing files.
- Inspect incomplete writes.
- Recover or quarantine temporary segments.
- Reconcile retained-event boundaries.
- Resume writes through the owning component.
- Verify restored storage guarantees.

Disk availability alone does not prove that previously failed writes were recovered.

---

## 14. PostgreSQL Recording Outages

PostgreSQL is an optional market-data recording destination for MarketForge Live.

A database outage must be isolated from live acquisition whenever the configured service guarantees permit it.

The Recording Manager owns database connections, batching, writes, and persistence recovery.

### 14.1 Recording Failure Types

Possible failures include:

- Database connection loss.
- Connection timeout.
- Transaction failure.
- Statement failure.
- Database restart.
- Authentication or permission failure.
- Storage exhaustion.
- Write latency exceeding operational limits.
- Recording batch rejection.

The Recording Manager classifies these failures and reports them through the common failure interface.

### 14.2 Recording Recovery Boundary

Conceptually:

    Canonical Event Accepted
              |
              v
    Recording Batch Prepared
              |
              v
    PostgreSQL Write Attempt
              |
         +----+----+
         |         |
         v         v
      Success    Failure
         |         |
         v         v
    Commit State  Recording Recovery

A successful canonical publication does not imply successful database persistence.

### 14.3 Optional Recording Policy

When recording is disabled:

    RecordingRestored = NotApplicable

No database connection is required for normal live operation.

When recording is enabled, the configured persistence guarantee determines how a database outage affects processing.

### 14.4 Recording Degradation

A database failure may result in:

    Live Acquisition = Operational

    Canonical Publication = Operational

    Recording = Degraded

This is a valid runtime condition when recording is optional and buffering or loss policies permit continued acquisition.

The Recording Manager must not silently drop events while reporting successful recording.

### 14.5 Recording Buffers

The Recording Manager may maintain bounded pending batches.

When PostgreSQL becomes unavailable, those buffers may temporarily retain unwritten events.

The limits must be explicit.

Possible outcomes after buffer exhaustion include:

    Backpressure

    DurableSpill

    RecordingGap

    RecordingFailure

The selected action depends on the configured recording contract.

No unbounded accumulation is permitted.

### 14.6 Ambiguous Database Commits

A database connection may fail after a transaction reaches PostgreSQL but before the client receives confirmation.

Therefore:

    Client Timeout
        !=
    Transaction Definitely Failed

The Recording Manager must use idempotent persistence identities, transactional reconciliation, or another supported mechanism to avoid duplicating accepted records after retry.

The Recovery Coordinator must not blindly retry ambiguous writes without the Recording Manager's reconciliation procedure.

### 14.7 Recording Recovery

After PostgreSQL connectivity returns:

    Database Available
           |
           v
    Validate Recording Session
           |
           v
    Reconcile Last Committed Boundary
           |
           v
    Resume Pending Writes
           |
           v
    Validate Persistence Results
           |
           v
    RecordingRestored

The Recording Manager reports completion.

The Recovery Coordinator records the restored guarantee.

### 14.8 Unrecoverable Recording Gaps

If events cannot be persisted after an outage, the Recording Manager must report an explicit recording gap.

The system must not fabricate database records or claim complete recording.

A recording gap is distinct from an exchange acquisition gap.

The same canonical events may have been received and published successfully while their database persistence failed.

---

## 15. Memory and Runtime Resource Exhaustion

MarketForge must prevent local resource exhaustion from turning into silent market-data corruption.

Relevant resources include:

- System memory.
- Runtime task capacity.
- File descriptors.
- Network sockets.
- Bounded queues.
- Processing buffers.
- Recovery concurrency.
- CPU processing capacity.

### 15.1 Resource Failure Classification

| Resource failure | Primary owner |
|---|---|
| Connection capacity | Connection Manager |
| Subscription capacity | Subscription Manager |
| L2 buffer overflow | Synchronization Coordinator |
| Trade recovery buffer overflow | Trade Processing / Recovery |
| Publication queue saturation | Publisher |
| Retention memory exhaustion | Retention Manager |
| Recording buffer exhaustion | Recording Manager |
| Runtime-wide memory pressure | Runtime Resource Manager |

The Recovery Coordinator must not override these owners' local safety mechanisms.

### 15.2 Resource Degradation

When resource pressure increases:

    Resource Pressure
           |
           v
    Apply Local Backpressure
           |
           v
    Restrict New Admission
           |
           v
    Defer Noncritical Recovery
           |
           v
    Preserve Essential Processing
           |
           v
    Escalate if Guarantees Fail

The initial preference is to preserve active acquisition and required integrity guarantees before starting optional recovery work.

### 15.3 Memory Exhaustion

A process may terminate abruptly if memory exhaustion occurs before the runtime can react.

Therefore, recovery architecture must include preventive resource admission and bounded queues.

The Recovery Coordinator cannot guarantee graceful handling of arbitrary operating-system out-of-memory termination.

Such an event is handled through process restart recovery.

### 15.4 File Descriptor Exhaustion

File descriptor exhaustion may prevent:

- New WebSocket connections.
- New REST connections.
- Local file creation.
- Parquet spill operations.
- Log-file creation.

The runtime must classify the shared resource failure.

It must not repeatedly attempt new connections without considering the underlying descriptor limit.

### 15.5 CPU Saturation

CPU saturation may cause:

- Processing latency.
- Queue growth.
- Delayed heartbeat responses.
- Delayed snapshot replay.
- Recovery timeouts.

The coordinator should avoid escalating every secondary symptom into an independent recovery storm.

For example, widespread heartbeat delays during severe CPU saturation may indicate a local runtime problem rather than simultaneous exchange outages.

### 15.6 Resource Recovery

After resource pressure is reduced:

- Reevaluate deferred operations.
- Restore admission gradually.
- Validate affected processing state.
- Reconcile interrupted coverage.
- Resume optional recovery tasks.
- Confirm restored service guarantees.

Resource availability alone does not establish market-data continuity.

---

## 16. Infrastructure Failure Isolation

Failures may affect one component, one exchange, or the entire runtime.

The recovery architecture must isolate failures where possible.

### 16.1 Isolation Boundaries

| Failure scope | Expected isolation |
|---|---|
| Individual trade record | Affected record and stream integrity |
| Individual L2 book | Affected book generation |
| Subscription | Affected logical stream |
| WebSocket connection | Subscriptions assigned to that connection |
| Exchange endpoint | Pools using that endpoint |
| Exchange outage | Affected exchange resources |
| Recording database | Optional recording subsystem |
| Local retention storage | Retention subsystem and dependent guarantees |
| Runtime resource exhaustion | Potentially multiple components |
| Process crash | Entire runtime instance |

### 16.2 Failure Propagation

Failure propagation must reflect actual dependencies.

Example:

    PostgreSQL Recording Failure
             |
             v
    Recording Manager Degraded
             |
             v
    Recording Guarantee Impaired

It must not automatically produce:

    Exchange Connection Failure

Another example:

    WebSocket Connection Lost
             |
             v
    Subscription Assignments Invalid
             |
             v
    Trade Coverage Unverified
             |
             v
    L2 Books Invalidated

These downstream consequences are legitimate and must remain visible.

### 16.3 Cascading Failure Prevention

The Recovery Coordinator must prevent recovery itself from increasing instability.

Examples of dangerous behavior include:

- Reconnecting every stream independently on a shared failed connection.
- Starting unlimited REST snapshot requests.
- Retrying PostgreSQL writes without bounded buffering.
- Rebuilding all books simultaneously after an exchange outage.
- Repeatedly creating recovery jobs for identical failures.
- Retaining obsolete recovery buffers indefinitely.

Recovery scheduling must remain bounded and dependency-aware.

### 16.4 Partial Runtime Availability

MarketForge may remain partially operational during infrastructure failures.

Example:

    Binance Spot       Operational

    Binance Linear     Degraded

    Bybit              Operational

    OKX                Recovering

    Bitget             Operational

    Gate.io            Operational

The runtime must expose this condition without collapsing all exchange health into one misleading global status.

### 16.5 Global Health

A runtime-level health assessment may summarize:

    Healthy
    Degraded
    Critical
    Stopping

This summary must not replace individual recovery and integrity states.

Operators must be able to identify which specific guarantees are impaired.

---

## 17. System-Level Recovery Guarantees

MarketForge Live must distinguish operational availability from market-data completeness, publication delivery, and persistence durability.

A recovery operation may restore one guarantee while leaving others unresolved.

The Recovery Coordinator must evaluate each guarantee independently.

### 17.1 Recovery Guarantee Model

The initial recovery model includes:

    TransportRestored
    SubscriptionRestored
    ProcessingRestored
    CurrentBookRestored
    HistoricalCoverageRestored
    PublicationRestored
    RetentionRestored
    RecordingRestored

Each guarantee is evaluated for the resource to which it applies.

Not every guarantee is relevant to every stream.

For example:

    CurrentBookRestored

does not apply to an ordinary trade stream.

Likewise:

    RecordingRestored

is not required when recording is disabled.

### 17.2 Guarantee Status

Each guarantee may have one of the following statuses:

    Satisfied
    Unsatisfied
    Recovering
    Unverified
    NotApplicable

| Status | Meaning |
|---|---|
| Satisfied | Required evidence confirms the guarantee |
| Unsatisfied | The guarantee is known not to hold |
| Recovering | An operation is attempting to restore it |
| Unverified | Available evidence is insufficient |
| NotApplicable | The guarantee is not required for this resource |

A guarantee must not be marked `Satisfied` merely because its recovery operation returned without an error.

The responsible component must confirm the relevant state.

### 17.3 RecoveryGuaranteeSnapshot

Conceptual representation:

    RecoveryGuaranteeSnapshot {
        resource_id
        runtime_instance_id

        transport
        subscription
        processing
        current_book
        historical_coverage
        publication
        retention
        recording

        evaluated_at
        recovery_generation
    }

The structure is illustrative.

The implementation may use a typed set of applicable guarantees rather than allocating every field for every resource.

### 17.4 Transport Restoration

`TransportRestored` requires confirmation from the Connection Manager.

Evidence includes:

- A current physical connection generation.
- Successful WebSocket establishment.
- Required handshake properties.
- Operational transport tasks.
- Applicable liveness requirements.

It does not establish successful native subscriptions.

### 17.5 Subscription Restoration

`SubscriptionRestored` requires confirmation from the Subscription Manager.

Evidence includes:

- Valid connection assignment.
- Current subscription generation.
- Successful native subscription establishment.
- Valid subscription parameters.
- Required acknowledgement or protocol-specific confirmation.

It does not establish complete market-data coverage.

### 17.6 Processing Restoration

`ProcessingRestored` requires confirmation from the processing subsystem.

Evidence includes:

- Valid compiled format.
- Valid instrument context.
- Current processing generation.
- Initialized normalization and integrity state.
- Operational message routing.
- Available processing capacity.

A trade stream may satisfy this guarantee while retaining historical coverage gaps.

### 17.7 Current Book Restoration

`CurrentBookRestored` applies to L2 streams.

It requires:

- An authoritative snapshot.
- Valid native sequence initialization.
- Successful synchronization.
- Atomic BookStore commit.
- Current book generation.
- Compatible metadata and configuration revisions.
- Required structural integrity checks.

A successful REST snapshot request alone is insufficient.

### 17.8 Historical Coverage Restoration

`HistoricalCoverageRestored` requires verified completeness of the relevant recovery interval.

The evidence depends on the data type and exchange protocol.

For trades, evidence may include:

- Exhaustive historical retrieval.
- Verified native identity continuity.
- Compatible live/recovery overlap.
- Historical archive coverage.

For L2, a new snapshot restores current state but generally cannot reconstruct every missed incremental update.

Therefore:

    CurrentBookRestored = Satisfied

may coexist with:

    HistoricalCoverageRestored = Unsatisfied

### 17.9 Publication Restoration

`PublicationRestored` requires confirmation from the Publisher.

Evidence includes:

- Operational publication queues.
- Valid event identities.
- Preserved required ordering.
- Available subscriber delivery mechanisms.
- Explicit handling of pending events.
- No unresolved violation of the configured publication contract.

Publisher availability does not prove that every subscriber received every event.

Subscriber delivery guarantees remain defined by the Publisher contract.

### 17.10 Retention and Recording Restoration

`RetentionRestored` and `RecordingRestored` refer to different storage systems.

Temporary retention uses the configured memory and Parquet infrastructure.

Optional recording uses PostgreSQL.

Each component must independently confirm its restored state.

A successful storage reconnect does not automatically establish complete durability for events affected by the outage.

---

## 18. Recovery Completion Semantics

Recovery completion must be evaluated against explicitly required guarantees.

The coordinator must not use a single ambiguous definition of success.

### 18.1 Recovery Completion Contract

A recovery operation is complete when:

    All Required Guarantees Satisfied

Optional or nonapplicable guarantees do not prevent completion.

However, unresolved required guarantees must produce an explicit partial or failed outcome.

### 18.2 Completion Outcomes

Supported outcomes:

    Completed
    PartiallyCompleted
    Failed
    Cancelled

**Completed**

All guarantees required by the recovery operation have been established.

**PartiallyCompleted**

Some guarantees have been restored, but others remain unresolved.

**Failed**

The operation cannot satisfy its required guarantees under the current recovery policy.

**Cancelled**

The operation was superseded, stopped, or rendered unnecessary.

### 18.3 Example: Trade Stream Recovery

After a WebSocket outage:

    TransportRestored = Satisfied

    SubscriptionRestored = Satisfied

    ProcessingRestored = Satisfied

    HistoricalCoverageRestored = Unverified

    PublicationRestored = Satisfied

The stream may resume normal live execution delivery.

However, the historical recovery operation remains incomplete.

### 18.4 Example: L2 Recovery

After a sequence gap:

    TransportRestored = Satisfied

    SubscriptionRestored = Satisfied

    ProcessingRestored = Satisfied

    CurrentBookRestored = Recovering

    HistoricalCoverageRestored = Unsatisfied

The L2 stream must not publish synchronized book state until a new authoritative generation is established.

After successful synchronization:

    CurrentBookRestored = Satisfied

The missed incremental-update interval may remain permanently incomplete.

### 18.5 Example: PostgreSQL Outage

When optional recording is enabled:

    TransportRestored = Satisfied

    SubscriptionRestored = Satisfied

    ProcessingRestored = Satisfied

    PublicationRestored = Satisfied

    RecordingRestored = Unsatisfied

Live acquisition may remain operational.

The Recording Manager must report the persistence failure and any unresolved recording gaps.

### 18.6 Completion Evidence

Every completed recovery operation must identify the component that established its result.

Conceptually:

    RecoveryCompletionEvidence {
        recovery_id
        target
        guarantee
        source_component
        resource_generation
        evidence_reference
        confirmed_at
    }

The evidence reference may point to an authoritative runtime state revision or a persisted coverage record.

The coordinator must not independently fabricate completion evidence.

---

## 19. Degraded Operation

MarketForge Live must support partial availability without misrepresenting the health of affected resources.

A degraded condition does not necessarily require terminating the entire runtime.

### 19.1 Degradation Principle

The system should preserve healthy functionality when failures can be isolated.

Example:

    Binance Spot:
        Operational

    Binance Linear:
        Recovering

    Bybit:
        Operational

    OKX:
        Degraded

    Bitget:
        Operational

    Gate.io:
        Operational

The runtime must not collapse this state into a single undifferentiated failure.

### 19.2 Degradation Dimensions

Degradation may affect:

| Dimension | Example |
|---|---|
| Connectivity | Repeated exchange disconnections |
| Subscription | Native channel unavailable |
| Processing | Normalization errors |
| Integrity | Unresolved trade coverage |
| L2 state | Invalid synchronized book |
| Publication | Subscriber delivery impairment |
| Retention | Parquet spill unavailable |
| Recording | PostgreSQL unavailable |
| Resources | Sustained memory or CPU pressure |

These dimensions are independent.

### 19.3 DegradationState

Conceptual classifications:

    Healthy
    Degraded
    Critical
    Unavailable
    Unknown

The classification must be derived from component state and applicable service guarantees.

It must not replace the underlying detailed failure information.

### 19.4 Healthy Current Acquisition With Incomplete History

A trade stream may satisfy:

    Current Acquisition = Healthy

while:

    Historical Coverage = Incomplete

This must be represented explicitly.

The system should continue live delivery when the selected processing and subscriber contracts permit it.

### 19.5 Invalid L2 State

A book with invalid sequence continuity must not be exposed as synchronized current state.

The system may retain its previous levels for diagnostics.

However:

    BookState = Invalid

must remain visible.

The Publisher must not silently present those levels as authoritative current liquidity.

### 19.6 Degraded Recording

When optional recording fails:

    Market Data Acquisition = Healthy

    Canonical Publication = Healthy

    Recording = Degraded

The runtime may continue operating if the configured recording policy allows it.

Any loss of required recording durability must remain explicit.

### 19.7 Degradation Escalation

A degraded subsystem may transition to a more severe state when:

- Failures persist.
- Recovery deadlines expire.
- Resource capacity is exhausted.
- More dependencies become unavailable.
- Required integrity guarantees can no longer be maintained.

Escalation must use configured thresholds and documented failure semantics.

### 19.8 Degradation Recovery

A degraded state may return to healthy only when the relevant guarantees are restored.

For example, a recovered WebSocket connection does not automatically remove a trade-coverage warning.

The warning must remain until the coverage interval is resolved or explicitly classified as irrecoverable.

---

## 20. Persistent Coverage Gaps

Market-data coverage gaps must remain identifiable after the immediate triggering failure has ended.

This applies to trade acquisition, L2 event history, publication, retention, and recording.

### 20.1 Coverage Domains

MarketForge distinguishes:

    AcquisitionCoverage

    TradeExecutionCoverage

    L2HistoricalCoverage

    PublicationCoverage

    RetentionCoverage

    RecordingCoverage

A failure may affect one domain without affecting others.

### 20.2 CoverageGap

Conceptual representation:

    CoverageGap {
        gap_id

        stream_id
        coverage_domain

        interval_start
        interval_end

        boundary_confidence
        evidence

        status
        recovery_id

        detected_at
        updated_at
    }

The exact schema belongs to the relevant integrity or retention component.

The Recovery Coordinator maintains references to unresolved gaps without duplicating their authoritative storage.

### 20.3 Gap Status

Supported statuses:

    Suspected
    Confirmed
    Recovering
    PartiallyRecovered
    Recovered
    Unrecoverable
    Unverified

These must remain distinguishable.

For example:

    Suspected != Confirmed

and:

    PartiallyRecovered != Recovered

### 20.4 Gap Boundaries

The exact beginning and end of missing coverage may not be known.

Example:

    Last accepted trade:
        12:00:00.100

    Connection failure detected:
        12:00:00.500

    Connection restored:
        12:00:03.000

    First new trade:
        12:00:03.200

The system must not automatically claim that missing executions occurred precisely between the connection failure and restoration timestamps.

The gap boundaries must preserve the available evidence and uncertainty.

### 20.5 L2 Historical Gaps

After an L2 sequence violation, a new snapshot may restore current state.

However, the missing delta interval remains unresolved.

Example:

    Sequence 1000 accepted
             |
             v
    Missing updates
             |
             v
    New snapshot at sequence 1200

The new snapshot establishes current state.

It does not reconstruct every book transition between sequences 1000 and 1200.

Therefore:

    CurrentBookRestored = Satisfied

    L2HistoricalCoverage = Incomplete

### 20.6 Publication Gaps

A publication gap occurs when accepted canonical events cannot be delivered according to the configured publication contract.

This is distinct from missing exchange messages.

For example:

    Trade received and normalized

    Trade accepted

    Publisher fails

The exchange acquisition may have succeeded even though downstream delivery did not.

### 20.7 Retention Gaps

A retention gap occurs when accepted events cannot be preserved according to the configured temporary retention guarantee.

The Retention Manager must report the affected cursor or event interval.

It must not infer completeness from the existence of adjacent Parquet files.

### 20.8 Recording Gaps

A recording gap occurs when canonical events cannot be persisted according to the configured recording policy.

The Recording Manager owns the relevant durability evidence.

The Recovery Coordinator tracks whether the gap remains unresolved.

### 20.9 Gap Persistence

Unresolved coverage information must survive process restart when required to preserve dataset-integrity guarantees.

The system may use local checkpoint files, retention manifests, or another defined persistent metadata mechanism.

The exact storage contract belongs to the relevant component.

Operational gap history must not be written into PostgreSQL merely for convenience.

### 20.10 Gap Resolution

A coverage gap may be resolved only when sufficient evidence establishes the required completeness.

Possible evidence includes:

- Verified recovered executions.
- Complete historical archive coverage.
- Successful retained-event replay.
- Reconciled recording commits.
- Another explicitly supported recovery mechanism.

A gap must not be marked recovered merely because the affected subsystem is currently healthy.

---

## 21. Recovery Observability

The Recovery Coordinator exposes system-wide recovery state through the internal Metrics Registry, runtime API, and TUI.

Operational logs remain local.

### 21.1 RecoveryHealthSnapshot

Conceptual representation:

    RecoveryHealthSnapshot {
        runtime_instance_id

        active_recoveries
        pending_recoveries
        failed_recoveries

        degraded_resources
        critical_resources

        unresolved_coverage_gaps

        exchange_health
        subsystem_health

        last_critical_failure
        evaluated_at
    }

The snapshot must not require copying all recovery history into each response.

### 21.2 Recovery Metrics

| Metric | Description |
|---|---|
| `recovery_operations_total` | Recovery operations created |
| `recovery_completed_total` | Fully completed recovery operations |
| `recovery_partial_total` | Partially completed operations |
| `recovery_failed_total` | Failed recovery operations |
| `recovery_cancelled_total` | Cancelled operations |
| `recovery_active` | Currently running recovery operations |
| `recovery_pending` | Pending recovery operations |
| `recovery_dependency_blocked` | Operations waiting for dependencies |
| `recovery_admission_deferred` | Operations waiting for capacity |
| `recovery_duration` | Recovery completion duration |
| `recovery_escalations_total` | Recovery escalation events |
| `recovery_duplicate_requests_total` | Duplicate requests consolidated |

Metrics must be scoped appropriately by failure type, resource, exchange, and recovery action.

### 21.3 Guarantee Metrics

The Metrics Registry should expose:

| Metric | Description |
|---|---|
| `transport_recovery_pending` | Connections awaiting restoration |
| `subscription_recovery_pending` | Subscriptions awaiting restoration |
| `processing_recovery_pending` | Processors awaiting restoration |
| `book_recovery_pending` | L2 books awaiting synchronization |
| `trade_coverage_unresolved` | Unresolved trade intervals |
| `l2_coverage_unresolved` | Unresolved L2 historical intervals |
| `publication_recovery_pending` | Impaired publication resources |
| `retention_recovery_pending` | Impaired retention resources |
| `recording_recovery_pending` | Impaired recording resources |

The Metrics Registry must not count nonapplicable guarantees as failures.

### 21.4 Recovery Events

The coordinator emits structured control-plane events.

Examples:

    RecoveryRequested
    RecoveryPlanned
    RecoveryDeferred
    RecoveryStarted
    RecoveryDependencySatisfied
    RecoveryEscalated
    RecoveryPartiallyCompleted
    RecoveryCompleted
    RecoveryFailed
    RecoveryCancelled

Each event should include:

    RecoveryId
    Target
    RecoveryKind
    RecoveryGeneration
    CurrentState
    FailureClassification
    Timestamp

Events must not contain credentials or sensitive configuration values.

### 21.5 TUI Integration

The Ratatui dashboard should display a concise recovery summary.

Suggested information:

    Active Recoveries
    Failed Recoveries
    Degraded Streams
    Invalid Books
    Unresolved Trade Gaps
    Recording Health
    Retention Health

The Streams view should expose stream-specific recovery status.

For example:

    Exchange: Binance
    Instrument: BTCUSDT
    Channel: L2

    Stream: Active
    Book: Synchronizing
    Recovery: REST Snapshot
    Attempt: 3
    Last Error: Sequence Gap

The TUI must not implement recovery algorithms.

Manual recovery requests must use the same control-plane interfaces as automatic recovery.

### 21.6 API Integration

The runtime API should expose read-only recovery state and supported recovery commands.

Conceptual operations:

    GetRecoveryStatus

    ListRecoveryOperations

    GetRecoveryOperation

    ListUnresolvedCoverageGaps

    RequestStreamRecovery

    CancelRecoveryOperation

The precise API transport and endpoint paths are defined separately.

A manual recovery request must pass through normal admission, generation validation, and rate-limit enforcement.

### 21.7 Local Structured Logging

Relevant events include:

    RecoveryOperationCreated
    RecoveryOperationStarted
    RecoveryOperationFailed
    RecoveryOperationCompleted
    RecoveryEscalated
    CoverageGapDetected
    CoverageGapPartiallyRecovered
    CoverageGapResolved
    InfrastructureFailureDetected

Logs must preserve useful causal identifiers.

For example:

    FailureId
    RecoveryId
    StreamId
    ConnectionGeneration
    RecoveryGeneration

Operational logs are stored locally.

PostgreSQL is not used for operational logging.

---

## 22. System Health Aggregation

The runtime exposes a global health assessment derived from component health and required guarantees.

### 22.1 Global Health States

Supported states:

    Healthy
    Degraded
    Critical
    Stopping

**Healthy**

Required runtime services and guarantees are operational.

**Degraded**

Some services or guarantees are impaired, but useful operation remains possible.

**Critical**

Essential safety, integrity, or runtime availability guarantees cannot currently be maintained.

**Stopping**

The runtime is intentionally shutting down.

### 22.2 Health Aggregation

Global health depends on:

- Connection availability.
- Subscription state.
- Processing state.
- L2 synchronization state.
- Trade coverage.
- Publisher availability.
- Retention status.
- Optional recording requirements.
- Resource health.
- Active recovery failures.

The aggregation policy must distinguish required and optional capabilities.

For example, disabled optional recording must not make the runtime unhealthy.

### 22.3 Scope-Aware Health

The global health summary must preserve access to:

    Runtime Health

    Exchange Health

    Connection Health

    Stream Health

    Processing Health

    Coverage Health

    Persistence Health

A single summary status must not conceal which guarantees are impaired.

### 22.4 Health Stability

Health aggregation should avoid rapid oscillation between states.

The implementation may use:

- Hysteresis.
- Minimum observation intervals.
- Recovery stabilization periods.
- Consecutive failure thresholds.

These policies must not delay mandatory safety actions.

For example, confirmed L2 sequence invalidation must take effect immediately, regardless of global health smoothing.

### 22.5 Final Health Principle

The runtime must never use:

    Connected

as a synonym for:

    Healthy

Similarly:

    Recovery Completed

must not be interpreted as:

    Every Historical Event Recovered

The health model must preserve the distinction between service availability, integrity, coverage, delivery, and durability.

---

## 23. System-Level Testing Strategy

The Recovery Coordinator requires deterministic unit tests, asynchronous integration tests, fault-injection tests, and full-runtime recovery simulations.

Testing must verify not only that individual components recover successfully, but also that their recovery operations remain coordinated.

The primary objective is to establish that MarketForge can restore valid operation without:

- Creating duplicate recovery operations.
- Violating component ownership.
- Exceeding exchange rate limits.
- Accepting obsolete processing generations.
- Publishing invalid market data.
- Concealing missing historical coverage.
- Losing committed publication identities.
- Leaking runtime resources.
- Causing cascading failures across unrelated streams.

### 23.1 Testing Categories

| Category | Scope |
|---|---|
| Unit tests | Recovery decisions, state transitions, failure classification |
| Dependency tests | Recovery action prerequisites and ordering |
| Concurrency tests | Simultaneous failures and competing recovery operations |
| Exchange outage tests | Shared failures across connections and streams |
| Data-integrity tests | Trade gaps and L2 synchronization failures |
| Infrastructure tests | Disk, memory, file descriptors, PostgreSQL |
| Crash recovery tests | Process restart and persistent-state reconciliation |
| Publication tests | Cursor consistency and recovery delivery |
| Fault-injection tests | Combined and repeated failures |
| Property-based tests | Arbitrary failure and recovery sequences |
| Performance tests | Recovery scheduling, concurrency, resource utilization |

Core correctness tests must run without live exchange connectivity.

External exchange integration tests supplement the deterministic test suite.

### 23.2 Test Infrastructure

The testing infrastructure should support:

- Mock Connection Managers.
- Mock Subscription Managers.
- Simulated exchange protocol adapters.
- Deterministic REST responses.
- Controlled WebSocket disconnections.
- Configurable execution delays.
- Mock clocks.
- Deterministic retry scheduling.
- Simulated resource limits.
- Simulated publisher failures.
- Simulated storage failures.
- Explicit recovery completion events.

Tests must avoid depending on arbitrary real-time sleeps for correctness.

### 23.3 Failure Injection Interface

Conceptually:

    InjectFailure {
        target
        failure_class
        failure_scope
        runtime_generation
        occurrence_time
        parameters
    }

The test framework must support failures occurring:

- Before operation admission.
- During operation execution.
- After local completion but before acknowledgement.
- During recovery validation.
- During cancellation.
- After an operation has been superseded.

These boundaries are important because asynchronous failures can produce ambiguous outcomes.

---

## 24. Recovery Coordinator Unit Tests

### 24.1 Failure Classification

Verify classification of:

    TransportFailure
    SubscriptionFailure
    ProtocolFailure
    NormalizationFailure
    TradeContinuityFailure
    L2SynchronizationFailure
    ResourceExhaustion
    PublicationFailure
    RetentionFailure
    RecordingFailure
    ConfigurationFailure
    InfrastructureFailure

Each classification must produce an appropriate recovery decision or explicit nonrecoverable outcome.

### 24.2 Recovery Operation Lifecycle

Verify valid transitions between:

    Created
    PendingDependencies
    PendingAdmission
    Scheduled
    Running
    Validating
    Completed
    PartiallyCompleted
    Failed
    Cancelled

Invalid transitions must be rejected.

A completed recovery operation must not resume execution.

### 24.3 Recovery Deduplication

Simulate several components reporting the same connection failure.

Example:

    Reader:
        Unexpected EOF

    Writer:
        Connection closed

    Heartbeat:
        Timeout

All reports refer to:

    ConnectionId = 12

    ConnectionGeneration = 5

Expected:

    One Authoritative Connection Recovery

The coordinator may preserve multiple failure observations.

It must not create three independent physical reconnection attempts.

### 24.4 Recovery Cancellation

Verify cancellation when:

- A stream is removed.
- A connection pool is closed.
- Runtime shutdown begins.
- A newer recovery operation supersedes an older one.
- Desired stream state changes to inactive.
- Configuration becomes invalid.

Cancelled operations must release their owned reservations.

### 24.5 Generation Fencing

Simulate:

    Recovery Generation 7 starts.

    Recovery Generation 8 supersedes it.

    Generation 8 completes.

    Generation 7 reports success.

The obsolete completion must not modify authoritative recovery state.

### 24.6 Recovery Completion Evidence

Verify that the coordinator requires confirmation from the responsible component.

Examples:

    RestoreConnection:
        Connection Manager confirmation required.

    RestoreSubscription:
        Subscription Manager confirmation required.

    ResynchronizeBook:
        BookStore synchronization confirmation required.

    ReconcileTrades:
        Coverage evidence required.

The coordinator must not infer completion solely from a successful command dispatch.

---

## 25. Recovery Dependency Tests

Recovery operations must execute only when their actual prerequisites are satisfied.

### 25.1 Dependency Ordering

Verify:

    RestoreConnection
            |
            v
    RestoreSubscription
            |
            v
    RestoreProcessing

Subscription restoration must not proceed through an unavailable physical connection.

### 25.2 Independent Historical Recovery

Historical trade acquisition may proceed while the live WebSocket endpoint remains unavailable if its REST or archive source is independently accessible.

Test:

    WebSocket unavailable.

    Historical REST available.

Expected:

    Historical acquisition may proceed.

The coordinator must not impose an unnecessary dependency on WebSocket restoration.

### 25.3 L2 Recovery Dependencies

For REST-snapshot protocols, verify:

    Subscription Active
            |
            v
    Delta Buffering Active
            |
            v
    REST Snapshot Acquired
            |
            v
    Sequence Bridge Valid
            |
            v
    Book Generation Committed

The coordinator tracks the final outcome.

The Synchronization Coordinator owns the actual reconstruction procedure.

### 25.4 Dependency Invalidation

Simulate:

    Connection restored.

    Subscription restoration begins.

    Connection fails again.

Expected:

- The old connection generation becomes invalid.
- Dependent restoration operations are suspended or cancelled.
- A new connection recovery proceeds.
- Obsolete completions cannot restore stream state.

### 25.5 Dependency Graph Validation

The recovery planner must reject:

- Cyclic dependencies.
- Unknown action owners.
- Missing required prerequisites.
- Incompatible resource generations.
- Unsupported recovery action combinations.

A malformed plan must not begin partial execution.

---

## 26. Exchange Outage Simulations

Exchange outages may affect hundreds of logical source streams.

The test suite must verify that recovery work is coordinated at the appropriate scope.

### 26.1 Shared Connection Failure

Simulate one connection containing:

    BTCUSDT Trades

    ETHUSDT Trades

    BTCUSDT L2

Disconnect the physical WebSocket.

Expected:

- One physical reconnection operation.
- Three affected logical stream assignments.
- Independent subscription restoration outcomes.
- Trade coverage marked unverified where necessary.
- L2 state invalidated.
- Fresh L2 synchronization required.
- No duplicate native subscriptions.

### 26.2 Exchange-Wide Outage

Simulate:

    Exchange: Binance

    Affected Connections: 20

    Affected Streams: 500

The coordinator must:

- Identify the common failure scope.
- Avoid redundant recovery plans.
- Respect shared exchange connection limits.
- Respect subscription request budgets.
- Schedule recovery gradually.
- Preserve stream-specific integrity states.
- Avoid exhausting local memory.

No arbitrary assumption should be made that all 500 streams can recover simultaneously.

### 26.3 Multi-Exchange Isolation

Simulate:

    Binance unavailable.

    Bybit healthy.

    OKX healthy.

    Bitget healthy.

    Gate.io healthy.

Expected:

- Binance recovery proceeds.
- Other exchanges remain operational.
- Global recovery scheduling does not block unrelated healthy streams.
- Shared local resources remain bounded.

### 26.4 Reconnection Storm Prevention

Simulate many connection failures at the same instant.

Verify:

- Bounded concurrent attempts.
- Exchange cooldown enforcement.
- Exponential backoff.
- Jitter.
- Shared rate-limit accounting.
- No duplicate supervisor ownership.

### 26.5 Repeated Exchange Instability

Simulate:

    Connect
       |
       v
    Disconnect
       |
       v
    Reconnect
       |
       v
    Disconnect

Repeat the sequence.

Verify that backoff is not reset prematurely.

A successful handshake alone must not erase repeated instability history.

### 26.6 Partial Exchange Recovery

Simulate an exchange where:

    WebSocket endpoint restored.

    REST trade history unavailable.

Expected:

    TransportRestored = Satisfied

    SubscriptionRestored = Satisfied

    ProcessingRestored = Satisfied

    HistoricalCoverageRestored = Unverified

The runtime must not report full recovery.

---

## 27. Trade and L2 Recovery Integration Tests

### 27.1 Trade Recovery With Live Continuation

Simulate:

    Trades 100–110 received.

    Connection interrupted.

    Trades 120–130 received after restoration.

    Recovery retrieves trades 111–119.

Verify:

- Live acquisition resumes.
- Recovery executes independently.
- Native identities remain stable.
- Missing executions are accepted once.
- Publication policy is respected.
- Coverage state reflects the verified recovery evidence.

The fixture must explicitly establish the native continuity guarantee required to identify the missing interval.

### 27.2 Partial Trade Recovery

Simulate a recovery endpoint returning only a subset of missing executions.

Expected:

    RecoveryOperation = PartiallyCompleted

    HistoricalCoverage = Incomplete

Current live acquisition may remain healthy.

### 27.3 L2 Sequence Gap

Simulate:

    Current sequence = 1000

    Incoming update begins at 1010.

The configured protocol confirms a sequence gap.

Expected:

- Current book generation invalidated.
- Invalid delta rejected.
- No publication of corrupted book state.
- Recovery Coordinator notified.
- Synchronization Coordinator establishes a new authoritative generation.

### 27.4 L2 Snapshot Failure

Simulate:

    Sequence gap detected.

    REST snapshot requested.

    Snapshot request times out.

Expected:

- The book remains invalid.
- Recovery operation remains unresolved.
- Retry policy is applied.
- REST rate limits are respected.
- No fabricated synchronization success.

### 27.5 Repeated L2 Failure

Simulate repeated synchronization failures.

Verify escalation according to policy:

    Retry Synchronization
             |
             v
    Resubscribe if Required
             |
             v
    Replace Connection if Required
             |
             v
    Escalate Persistent Failure

The coordinator must not replace a healthy physical connection unnecessarily.

### 27.6 Current-State vs Historical Recovery

Simulate successful L2 resynchronization after missing native deltas.

Expected:

    CurrentBookRestored = Satisfied

    HistoricalCoverageRestored = Unsatisfied

A fresh snapshot cannot be interpreted as reconstruction of missed incremental updates.

---

## 28. Infrastructure and Crash Recovery Tests

### 28.1 Process Crash

Simulate process termination during normal market-data acquisition.

After restart, verify:

- Persistent configuration loads.
- Instrument metadata snapshot validates.
- Desired streams are reconstructed.
- New runtime instance identity is established.
- New transport generations are allocated.
- Native subscriptions are restored.
- L2 books are reinitialized.
- Trade coverage is evaluated.
- Existing durable events are not blindly duplicated.

### 28.2 Crash During Recovery

Simulate:

    Recovery started.

    Historical records acquired.

    Process crashes before reconciliation completes.

After restart:

- In-memory recovery operations are not assumed complete.
- Available durable recovery evidence is inspected.
- Unresolved coverage remains identifiable.
- Recovery may safely restart.
- Duplicate canonical acceptance is prevented where persistent identity evidence permits it.

### 28.3 Repeated Crash

Simulate:

    Process starts.

    Recovery begins.

    Process crashes.

    Process starts again.

    Recovery begins again.

Verify that repeated restart recovery does not create duplicate authoritative stream declarations or inconsistent persistent cursor identities.

### 28.4 Disk Exhaustion

Simulate:

- Parquet spill failure.
- Checkpoint write failure.
- Metadata snapshot write failure.
- Local log write failure.

Verify:

- Existing valid files remain intact.
- Incomplete writes are detected.
- Resource pressure is reported.
- Retention guarantees are degraded explicitly.
- No unbounded memory buffering occurs.
- Recovery resumes only after storage validation.

### 28.5 PostgreSQL Outage

Simulate an outage while optional recording is enabled.

Expected:

    Acquisition = Operational

    Publication = Operational

    Recording = Degraded

provided the configured recording policy permits continuation.

Verify bounded recording buffers and explicit persistence-gap reporting.

### 28.6 Ambiguous Database Commit

Simulate:

    PostgreSQL transaction commits.

    Connection fails before client acknowledgement.

Expected:

The Recording Manager reconciles the transaction outcome using its defined persistence identity and commit contract.

The Recovery Coordinator must not blindly duplicate the write.

### 28.7 Runtime Memory Pressure

Simulate simultaneous:

- High trade throughput.
- L2 snapshot reconstruction.
- Recovery buffers.
- Publication congestion.

Verify:

- Resource limits are enforced.
- Noncritical recovery is deferred.
- Essential processing is protected where possible.
- No silent market-data dropping occurs.
- Failed guarantees are reported explicitly.

### 28.8 File Descriptor Exhaustion

Simulate failure to open new sockets and files.

Verify that the system identifies the shared resource condition.

It must not repeatedly launch futile connection attempts without considering the underlying exhaustion.

---

## 29. Publication, Retention, and Recording Tests

### 29.1 Publication Failure

Simulate an accepted canonical event that cannot enter the Publisher's required delivery boundary.

Verify:

- The failure is reported.
- Publication identity remains consistent.
- The event is not silently discarded.
- Recovery or explicit delivery-gap handling occurs.

### 29.2 Subscriber Failure

Simulate one subscriber becoming unavailable.

Verify:

- Other subscribers remain unaffected where possible.
- Accepted source-stream state remains valid.
- Subscriber delivery failure is distinguished from exchange acquisition failure.
- Replay or explicit delivery-gap reporting follows the Publisher contract.

### 29.3 Retention Failure

Simulate accepted events whose temporary Parquet retention fails.

Verify:

- Acquisition state remains independently classified.
- Retention failure is visible.
- Required retention guarantees are marked impaired.
- Existing valid retained segments are preserved.

### 29.4 Recording Failure

Simulate PostgreSQL recording interruption.

Verify:

- Recording status becomes degraded.
- Pending batches remain bounded.
- Retry behavior respects the Recording Manager's persistence contract.
- Ambiguous commits are reconciled.
- Unrecoverable recording gaps remain visible.

### 29.5 Independent Guarantee Restoration

Restore the Publisher while keeping recording unavailable.

Expected:

    PublicationRestored = Satisfied

    RecordingRestored = Unsatisfied

Restore recording without resolving an earlier acquisition gap.

Expected:

    RecordingRestored = Satisfied

    HistoricalCoverageRestored = Unsatisfied

Guarantees must remain independently evaluated.

---

## 30. Property-Based Testing

Property-based testing should generate arbitrary valid failure, recovery, cancellation, and completion sequences.

### 30.1 Recovery Ownership Invariants

    One authoritative recovery operation per
    target, recovery kind, and resource generation.

    Recovery operations never bypass
    component ownership boundaries.

    Connection recovery is executed by
    the Connection Manager.

    Subscription restoration is executed by
    the Subscription Manager.

    Trade reconciliation is executed by
    the Trade Reconciliation Engine.

    L2 synchronization is executed by
    the Synchronization Coordinator.

### 30.2 Generation Invariants

    Obsolete recovery completions cannot
    modify current authoritative state.

    Removed streams cannot be restored
    by delayed recovery operations.

    Recovery dependencies must match
    current resource generations.

    A cancelled recovery cannot later
    become authoritative.

### 30.3 Integrity Invariants

    Transport restoration does not imply
    historical completeness.

    Subscription restoration does not imply
    processing readiness.

    Current L2 synchronization does not imply
    complete historical L2 coverage.

    Recovered trades cannot bypass native
    execution identity validation.

    Invalid books cannot be published as
    synchronized current state.

### 30.4 Resource Invariants

    Recovery queues remain bounded.

    Recovery concurrency limits are respected.

    Exchange rate limits are never bypassed.

    Failed operations release reservations.

    Cancelled operations release resources.

    Recovery cannot create uncontrolled
    retry or reconnection storms.

### 30.5 Completion Invariants

    Completed recovery requires evidence
    for every applicable required guarantee.

    Partial recovery cannot be reported
    as complete.

    A successful REST request is not
    sufficient evidence of coverage.

    Optional disabled capabilities cannot
    cause false recovery failures.

### 30.6 Reference Model

A simplified reference Recovery Coordinator should be implemented for deterministic testing.

Generated operations may include:

    ReportFailure
    CreateRecovery
    AdmitRecovery
    SatisfyDependency
    FailDependency
    CompleteAction
    FailAction
    CancelRecovery
    ChangeDesiredState
    ReplaceGeneration
    Shutdown

For equivalent inputs, the reference and optimized implementations must produce compatible:

- Recovery operation states.
- Dependency states.
- Required guarantees.
- Completion outcomes.
- Resource reservations.
- Failure escalation decisions.

---

## 31. Performance and Recovery Scalability

Recovery coordination must remain efficient when many streams fail simultaneously.

It must not become a global bottleneck for otherwise healthy market-data processing.

### 31.1 Performance Requirements

The coordinator should minimize:

- Global lock contention.
- Duplicate failure processing.
- Redundant recovery plans.
- Unnecessary resource reservations.
- Repeated dependency scans.
- Unbounded scheduling queues.
- Excessive state copying.
- Expensive synchronous control-plane operations.

### 31.2 Recovery Scheduling Benchmarks

Benchmark:

- Failure-report ingestion.
- Recovery deduplication.
- Recovery-plan construction.
- Dependency resolution.
- Admission scheduling.
- Cancellation.
- Completion processing.
- Escalation evaluation.
- Recovery snapshot generation.

### 31.3 Scaling Dimensions

Benchmarks should vary:

- Number of active streams.
- Number of exchange connections.
- Simultaneous failure count.
- Recovery dependency depth.
- Pending recovery count.
- Available REST request capacity.
- Recovery concurrency limits.
- Failure notification duplication.
- Cancellation frequency.

### 31.4 Recovery Storm Benchmark

A representative benchmark should simulate:

    5 Exchanges

    15 Market Families

    Hundreds of Active Streams

    Multiple Shared Connection Failures

The coordinator must consolidate shared failures and schedule bounded recovery work.

Concrete performance targets must be established through measurement rather than arbitrary assumptions.

### 31.5 Isolation Benchmark

Saturate recovery processing for one exchange.

Verify that:

- Healthy exchange processing remains operational.
- Recovery scheduling remains responsive.
- Control-plane commands can still be processed.
- Runtime memory remains bounded.
- Unrelated connection pools are not blocked by recovery locks.

---

## 32. Implementation Acceptance Criteria

The Recovery Coordination subsystem is complete when the following conditions are satisfied.

### 32.1 Architecture

- The Recovery Coordinator has explicit ownership boundaries.
- Failure reporting uses structured typed events.
- Recovery planning is deterministic.
- Recovery operations have stable identities.
- Recovery generations prevent obsolete completions.
- Duplicate recovery requests are consolidated.
- Component-specific recovery mechanisms remain independently owned.

### 32.2 Dependency Coordination

- Recovery prerequisites are represented explicitly.
- Recovery dependency graphs are validated.
- Dependent actions wait for valid prerequisites.
- Obsolete dependencies invalidate affected operations.
- Independent recovery tasks can execute concurrently.
- Desired stream state is respected throughout recovery.

### 32.3 Resource Safety

- Recovery queues are bounded.
- Recovery concurrency is bounded.
- Exchange rate limits are respected.
- Local resource reservations are tracked.
- Failed and cancelled operations release resources.
- Shared failures do not produce duplicate recovery storms.
- Recovery workloads do not unnecessarily starve healthy acquisition.

### 32.4 Fault Tolerance

- Transient failures trigger appropriate recovery actions.
- Permanent errors stop futile automatic retries.
- Repeated failures escalate according to policy.
- Exchange outages are handled at the correct scope.
- Individual stream failures remain isolated where possible.
- Process crashes trigger validated restart recovery.
- Disk exhaustion is handled explicitly.
- PostgreSQL recording outages remain isolated when optional.
- Resource exhaustion does not produce silent integrity violations.

### 32.5 Data Integrity

- Trade coverage uncertainty remains visible.
- Confirmed gaps require supported evidence.
- Partial recovery is distinguished from complete recovery.
- Invalid L2 books cannot become authoritative.
- Current L2 restoration is distinct from historical coverage restoration.
- Publication gaps are distinct from acquisition gaps.
- Retention gaps are distinct from recording gaps.
- Recovery completion requires authoritative evidence.

### 32.6 Observability

- Recovery operation states are exposed.
- Failure classifications are available.
- Recovery dependencies are inspectable.
- Recovery escalation is visible.
- Unresolved coverage gaps remain identifiable.
- Runtime health reflects required service guarantees.
- TUI and API expose recovery status.
- Operational logs remain local.
- No Prometheus or Grafana dependency is required.

### 32.7 Testing

- Recovery Coordinator unit tests pass.
- Dependency coordination tests pass.
- Shared failure tests pass.
- Exchange outage simulations pass.
- Trade recovery integration tests pass.
- L2 resynchronization integration tests pass.
- Crash recovery tests pass.
- Disk and database failure tests pass.
- Publication and retention failure tests pass.
- Concurrency and generation-safety tests pass.
- Property-based tests pass.
- Recovery scalability benchmarks establish acceptable performance.

---

## 33. Final Architectural Contract

The Recovery Coordinator is responsible for restoring MarketForge Live's required operational guarantees after failures.

It does so by coordinating the existing components rather than duplicating their implementations.

The system-wide recovery architecture is:

    Runtime Failure
          |
          v
    Structured Failure Report
          |
          v
    Recovery Coordinator
          |
          v
    Failure Classification
          |
          v
    Recovery Plan
          |
          v
    Dependency and Resource Admission
          |
          v
    Component-Owned Recovery Actions
          |
          v
    Authoritative Result Validation
          |
          v
    Recovery Guarantee Evaluation
          |
          v
    Completed / Partial / Failed

The coordinator guarantees:

- Explicit failure classification.
- Correct recovery ownership.
- Bounded and dependency-aware scheduling.
- Prevention of duplicate recovery operations.
- Generation-safe asynchronous completion.
- Exchange-compliant recovery admission.
- Resource-aware failure escalation.
- Independent evaluation of recovery guarantees.
- Preservation of unresolved coverage gaps.
- Observable system health and recovery progress.

It does not guarantee:

- That every exchange outage can be resolved automatically.
- That missing trades are always recoverable.
- That a new L2 snapshot reconstructs missed historical updates.
- That all subscriber deliveries are exactly-once.
- That optional recording remains available during database outages.
- That arbitrary hardware or operating-system failures can be handled without process termination.
- That a recovered connection implies complete market-data integrity.

The essential invariant is:

    Recovery must never claim a guarantee
    that the responsible component
    has not established.

This principle applies to connectivity, subscriptions, normalization, L2 synchronization, historical coverage, publication, retention, and recording.

---
## 23. System-Level Testing Strategy

The Recovery Coordinator requires deterministic unit tests, asynchronous integration tests, fault-injection tests, and full-runtime recovery simulations.

Testing must verify not only that individual components recover successfully, but also that their recovery operations remain coordinated.

The primary objective is to establish that MarketForge can restore valid operation without:

- Creating duplicate recovery operations.
- Violating component ownership.
- Exceeding exchange rate limits.
- Accepting obsolete processing generations.
- Publishing invalid market data.
- Concealing missing historical coverage.
- Losing committed publication identities.
- Leaking runtime resources.
- Causing cascading failures across unrelated streams.

### 23.1 Testing Categories

| Category | Scope |
|---|---|
| Unit tests | Recovery decisions, state transitions, failure classification |
| Dependency tests | Recovery action prerequisites and ordering |
| Concurrency tests | Simultaneous failures and competing recovery operations |
| Exchange outage tests | Shared failures across connections and streams |
| Data-integrity tests | Trade gaps and L2 synchronization failures |
| Infrastructure tests | Disk, memory, file descriptors, PostgreSQL |
| Crash recovery tests | Process restart and persistent-state reconciliation |
| Publication tests | Cursor consistency and recovery delivery |
| Fault-injection tests | Combined and repeated failures |
| Property-based tests | Arbitrary failure and recovery sequences |
| Performance tests | Recovery scheduling, concurrency, resource utilization |

Core correctness tests must run without live exchange connectivity.

External exchange integration tests supplement the deterministic test suite.

### 23.2 Test Infrastructure

The testing infrastructure should support:

- Mock Connection Managers.
- Mock Subscription Managers.
- Simulated exchange protocol adapters.
- Deterministic REST responses.
- Controlled WebSocket disconnections.
- Configurable execution delays.
- Mock clocks.
- Deterministic retry scheduling.
- Simulated resource limits.
- Simulated publisher failures.
- Simulated storage failures.
- Explicit recovery completion events.

Tests must avoid depending on arbitrary real-time sleeps for correctness.

### 23.3 Failure Injection Interface

Conceptually:

    InjectFailure {
        target
        failure_class
        failure_scope
        runtime_generation
        occurrence_time
        parameters
    }

The test framework must support failures occurring:

- Before operation admission.
- During operation execution.
- After local completion but before acknowledgement.
- During recovery validation.
- During cancellation.
- After an operation has been superseded.

These boundaries are important because asynchronous failures can produce ambiguous outcomes.

---

## 24. Recovery Coordinator Unit Tests

### 24.1 Failure Classification

Verify classification of:

    TransportFailure
    SubscriptionFailure
    ProtocolFailure
    NormalizationFailure
    TradeContinuityFailure
    L2SynchronizationFailure
    ResourceExhaustion
    PublicationFailure
    RetentionFailure
    RecordingFailure
    ConfigurationFailure
    InfrastructureFailure

Each classification must produce an appropriate recovery decision or explicit nonrecoverable outcome.

### 24.2 Recovery Operation Lifecycle

Verify valid transitions between:

    Created
    PendingDependencies
    PendingAdmission
    Scheduled
    Running
    Validating
    Completed
    PartiallyCompleted
    Failed
    Cancelled

Invalid transitions must be rejected.

A completed recovery operation must not resume execution.

### 24.3 Recovery Deduplication

Simulate several components reporting the same connection failure.

Example:

    Reader:
        Unexpected EOF

    Writer:
        Connection closed

    Heartbeat:
        Timeout

All reports refer to:

    ConnectionId = 12

    ConnectionGeneration = 5

Expected:

    One Authoritative Connection Recovery

The coordinator may preserve multiple failure observations.

It must not create three independent physical reconnection attempts.

### 24.4 Recovery Cancellation

Verify cancellation when:

- A stream is removed.
- A connection pool is closed.
- Runtime shutdown begins.
- A newer recovery operation supersedes an older one.
- Desired stream state changes to inactive.
- Configuration becomes invalid.

Cancelled operations must release their owned reservations.

### 24.5 Generation Fencing

Simulate:

    Recovery Generation 7 starts.

    Recovery Generation 8 supersedes it.

    Generation 8 completes.

    Generation 7 reports success.

The obsolete completion must not modify authoritative recovery state.

### 24.6 Recovery Completion Evidence

Verify that the coordinator requires confirmation from the responsible component.

Examples:

    RestoreConnection:
        Connection Manager confirmation required.

    RestoreSubscription:
        Subscription Manager confirmation required.

    ResynchronizeBook:
        BookStore synchronization confirmation required.

    ReconcileTrades:
        Coverage evidence required.

The coordinator must not infer completion solely from a successful command dispatch.

---

## 25. Recovery Dependency Tests

Recovery operations must execute only when their actual prerequisites are satisfied.

### 25.1 Dependency Ordering

Verify:

    RestoreConnection
            |
            v
    RestoreSubscription
            |
            v
    RestoreProcessing

Subscription restoration must not proceed through an unavailable physical connection.

### 25.2 Independent Historical Recovery

Historical trade acquisition may proceed while the live WebSocket endpoint remains unavailable if its REST or archive source is independently accessible.

Test:

    WebSocket unavailable.

    Historical REST available.

Expected:

    Historical acquisition may proceed.

The coordinator must not impose an unnecessary dependency on WebSocket restoration.

### 25.3 L2 Recovery Dependencies

For REST-snapshot protocols, verify:

    Subscription Active
            |
            v
    Delta Buffering Active
            |
            v
    REST Snapshot Acquired
            |
            v
    Sequence Bridge Valid
            |
            v
    Book Generation Committed

The coordinator tracks the final outcome.

The Synchronization Coordinator owns the actual reconstruction procedure.

### 25.4 Dependency Invalidation

Simulate:

    Connection restored.

    Subscription restoration begins.

    Connection fails again.

Expected:

- The old connection generation becomes invalid.
- Dependent restoration operations are suspended or cancelled.
- A new connection recovery proceeds.
- Obsolete completions cannot restore stream state.

### 25.5 Dependency Graph Validation

The recovery planner must reject:

- Cyclic dependencies.
- Unknown action owners.
- Missing required prerequisites.
- Incompatible resource generations.
- Unsupported recovery action combinations.

A malformed plan must not begin partial execution.

---

## 26. Exchange Outage Simulations

Exchange outages may affect hundreds of logical source streams.

The test suite must verify that recovery work is coordinated at the appropriate scope.

### 26.1 Shared Connection Failure

Simulate one connection containing:

    BTCUSDT Trades

    ETHUSDT Trades

    BTCUSDT L2

Disconnect the physical WebSocket.

Expected:

- One physical reconnection operation.
- Three affected logical stream assignments.
- Independent subscription restoration outcomes.
- Trade coverage marked unverified where necessary.
- L2 state invalidated.
- Fresh L2 synchronization required.
- No duplicate native subscriptions.

### 26.2 Exchange-Wide Outage

Simulate:

    Exchange: Binance

    Affected Connections: 20

    Affected Streams: 500

The coordinator must:

- Identify the common failure scope.
- Avoid redundant recovery plans.
- Respect shared exchange connection limits.
- Respect subscription request budgets.
- Schedule recovery gradually.
- Preserve stream-specific integrity states.
- Avoid exhausting local memory.

No arbitrary assumption should be made that all 500 streams can recover simultaneously.

### 26.3 Multi-Exchange Isolation

Simulate:

    Binance unavailable.

    Bybit healthy.

    OKX healthy.

    Bitget healthy.

    Gate.io healthy.

Expected:

- Binance recovery proceeds.
- Other exchanges remain operational.
- Global recovery scheduling does not block unrelated healthy streams.
- Shared local resources remain bounded.

### 26.4 Reconnection Storm Prevention

Simulate many connection failures at the same instant.

Verify:

- Bounded concurrent attempts.
- Exchange cooldown enforcement.
- Exponential backoff.
- Jitter.
- Shared rate-limit accounting.
- No duplicate supervisor ownership.

### 26.5 Repeated Exchange Instability

Simulate:

    Connect
       |
       v
    Disconnect
       |
       v
    Reconnect
       |
       v
    Disconnect

Repeat the sequence.

Verify that backoff is not reset prematurely.

A successful handshake alone must not erase repeated instability history.

### 26.6 Partial Exchange Recovery

Simulate an exchange where:

    WebSocket endpoint restored.

    REST trade history unavailable.

Expected:

    TransportRestored = Satisfied

    SubscriptionRestored = Satisfied

    ProcessingRestored = Satisfied

    HistoricalCoverageRestored = Unverified

The runtime must not report full recovery.

---

## 27. Trade and L2 Recovery Integration Tests

### 27.1 Trade Recovery With Live Continuation

Simulate:

    Trades 100–110 received.

    Connection interrupted.

    Trades 120–130 received after restoration.

    Recovery retrieves trades 111–119.

Verify:

- Live acquisition resumes.
- Recovery executes independently.
- Native identities remain stable.
- Missing executions are accepted once.
- Publication policy is respected.
- Coverage state reflects the verified recovery evidence.

The fixture must explicitly establish the native continuity guarantee required to identify the missing interval.

### 27.2 Partial Trade Recovery

Simulate a recovery endpoint returning only a subset of missing executions.

Expected:

    RecoveryOperation = PartiallyCompleted

    HistoricalCoverage = Incomplete

Current live acquisition may remain healthy.

### 27.3 L2 Sequence Gap

Simulate:

    Current sequence = 1000

    Incoming update begins at 1010.

The configured protocol confirms a sequence gap.

Expected:

- Current book generation invalidated.
- Invalid delta rejected.
- No publication of corrupted book state.
- Recovery Coordinator notified.
- Synchronization Coordinator establishes a new authoritative generation.

### 27.4 L2 Snapshot Failure

Simulate:

    Sequence gap detected.

    REST snapshot requested.

    Snapshot request times out.

Expected:

- The book remains invalid.
- Recovery operation remains unresolved.
- Retry policy is applied.
- REST rate limits are respected.
- No fabricated synchronization success.

### 27.5 Repeated L2 Failure

Simulate repeated synchronization failures.

Verify escalation according to policy:

    Retry Synchronization
             |
             v
    Resubscribe if Required
             |
             v
    Replace Connection if Required
             |
             v
    Escalate Persistent Failure

The coordinator must not replace a healthy physical connection unnecessarily.

### 27.6 Current-State vs Historical Recovery

Simulate successful L2 resynchronization after missing native deltas.

Expected:

    CurrentBookRestored = Satisfied

    HistoricalCoverageRestored = Unsatisfied

A fresh snapshot cannot be interpreted as reconstruction of missed incremental updates.

---

## 28. Infrastructure and Crash Recovery Tests

### 28.1 Process Crash

Simulate process termination during normal market-data acquisition.

After restart, verify:

- Persistent configuration loads.
- Instrument metadata snapshot validates.
- Desired streams are reconstructed.
- New runtime instance identity is established.
- New transport generations are allocated.
- Native subscriptions are restored.
- L2 books are reinitialized.
- Trade coverage is evaluated.
- Existing durable events are not blindly duplicated.

### 28.2 Crash During Recovery

Simulate:

    Recovery started.

    Historical records acquired.

    Process crashes before reconciliation completes.

After restart:

- In-memory recovery operations are not assumed complete.
- Available durable recovery evidence is inspected.
- Unresolved coverage remains identifiable.
- Recovery may safely restart.
- Duplicate canonical acceptance is prevented where persistent identity evidence permits it.

### 28.3 Repeated Crash

Simulate:

    Process starts.

    Recovery begins.

    Process crashes.

    Process starts again.

    Recovery begins again.

Verify that repeated restart recovery does not create duplicate authoritative stream declarations or inconsistent persistent cursor identities.

### 28.4 Disk Exhaustion

Simulate:

- Parquet spill failure.
- Checkpoint write failure.
- Metadata snapshot write failure.
- Local log write failure.

Verify:

- Existing valid files remain intact.
- Incomplete writes are detected.
- Resource pressure is reported.
- Retention guarantees are degraded explicitly.
- No unbounded memory buffering occurs.
- Recovery resumes only after storage validation.

### 28.5 PostgreSQL Outage

Simulate an outage while optional recording is enabled.

Expected:

    Acquisition = Operational

    Publication = Operational

    Recording = Degraded

provided the configured recording policy permits continuation.

Verify bounded recording buffers and explicit persistence-gap reporting.

### 28.6 Ambiguous Database Commit

Simulate:

    PostgreSQL transaction commits.

    Connection fails before client acknowledgement.

Expected:

The Recording Manager reconciles the transaction outcome using its defined persistence identity and commit contract.

The Recovery Coordinator must not blindly duplicate the write.

### 28.7 Runtime Memory Pressure

Simulate simultaneous:

- High trade throughput.
- L2 snapshot reconstruction.
- Recovery buffers.
- Publication congestion.

Verify:

- Resource limits are enforced.
- Noncritical recovery is deferred.
- Essential processing is protected where possible.
- No silent market-data dropping occurs.
- Failed guarantees are reported explicitly.

### 28.8 File Descriptor Exhaustion

Simulate failure to open new sockets and files.

Verify that the system identifies the shared resource condition.

It must not repeatedly launch futile connection attempts without considering the underlying exhaustion.

---

## 29. Publication, Retention, and Recording Tests

### 29.1 Publication Failure

Simulate an accepted canonical event that cannot enter the Publisher's required delivery boundary.

Verify:

- The failure is reported.
- Publication identity remains consistent.
- The event is not silently discarded.
- Recovery or explicit delivery-gap handling occurs.

### 29.2 Subscriber Failure

Simulate one subscriber becoming unavailable.

Verify:

- Other subscribers remain unaffected where possible.
- Accepted source-stream state remains valid.
- Subscriber delivery failure is distinguished from exchange acquisition failure.
- Replay or explicit delivery-gap reporting follows the Publisher contract.

### 29.3 Retention Failure

Simulate accepted events whose temporary Parquet retention fails.

Verify:

- Acquisition state remains independently classified.
- Retention failure is visible.
- Required retention guarantees are marked impaired.
- Existing valid retained segments are preserved.

### 29.4 Recording Failure

Simulate PostgreSQL recording interruption.

Verify:

- Recording status becomes degraded.
- Pending batches remain bounded.
- Retry behavior respects the Recording Manager's persistence contract.
- Ambiguous commits are reconciled.
- Unrecoverable recording gaps remain visible.

### 29.5 Independent Guarantee Restoration

Restore the Publisher while keeping recording unavailable.

Expected:

    PublicationRestored = Satisfied

    RecordingRestored = Unsatisfied

Restore recording without resolving an earlier acquisition gap.

Expected:

    RecordingRestored = Satisfied

    HistoricalCoverageRestored = Unsatisfied

Guarantees must remain independently evaluated.

---

## 30. Property-Based Testing

Property-based testing should generate arbitrary valid failure, recovery, cancellation, and completion sequences.

### 30.1 Recovery Ownership Invariants

    One authoritative recovery operation per
    target, recovery kind, and resource generation.

    Recovery operations never bypass
    component ownership boundaries.

    Connection recovery is executed by
    the Connection Manager.

    Subscription restoration is executed by
    the Subscription Manager.

    Trade reconciliation is executed by
    the Trade Reconciliation Engine.

    L2 synchronization is executed by
    the Synchronization Coordinator.

### 30.2 Generation Invariants

    Obsolete recovery completions cannot
    modify current authoritative state.

    Removed streams cannot be restored
    by delayed recovery operations.

    Recovery dependencies must match
    current resource generations.

    A cancelled recovery cannot later
    become authoritative.

### 30.3 Integrity Invariants

    Transport restoration does not imply
    historical completeness.

    Subscription restoration does not imply
    processing readiness.

    Current L2 synchronization does not imply
    complete historical L2 coverage.

    Recovered trades cannot bypass native
    execution identity validation.

    Invalid books cannot be published as
    synchronized current state.

### 30.4 Resource Invariants

    Recovery queues remain bounded.

    Recovery concurrency limits are respected.

    Exchange rate limits are never bypassed.

    Failed operations release reservations.

    Cancelled operations release resources.

    Recovery cannot create uncontrolled
    retry or reconnection storms.

### 30.5 Completion Invariants

    Completed recovery requires evidence
    for every applicable required guarantee.

    Partial recovery cannot be reported
    as complete.

    A successful REST request is not
    sufficient evidence of coverage.

    Optional disabled capabilities cannot
    cause false recovery failures.

### 30.6 Reference Model

A simplified reference Recovery Coordinator should be implemented for deterministic testing.

Generated operations may include:

    ReportFailure
    CreateRecovery
    AdmitRecovery
    SatisfyDependency
    FailDependency
    CompleteAction
    FailAction
    CancelRecovery
    ChangeDesiredState
    ReplaceGeneration
    Shutdown

For equivalent inputs, the reference and optimized implementations must produce compatible:

- Recovery operation states.
- Dependency states.
- Required guarantees.
- Completion outcomes.
- Resource reservations.
- Failure escalation decisions.

---

## 31. Performance and Recovery Scalability

Recovery coordination must remain efficient when many streams fail simultaneously.

It must not become a global bottleneck for otherwise healthy market-data processing.

### 31.1 Performance Requirements

The coordinator should minimize:

- Global lock contention.
- Duplicate failure processing.
- Redundant recovery plans.
- Unnecessary resource reservations.
- Repeated dependency scans.
- Unbounded scheduling queues.
- Excessive state copying.
- Expensive synchronous control-plane operations.

### 31.2 Recovery Scheduling Benchmarks

Benchmark:

- Failure-report ingestion.
- Recovery deduplication.
- Recovery-plan construction.
- Dependency resolution.
- Admission scheduling.
- Cancellation.
- Completion processing.
- Escalation evaluation.
- Recovery snapshot generation.

### 31.3 Scaling Dimensions

Benchmarks should vary:

- Number of active streams.
- Number of exchange connections.
- Simultaneous failure count.
- Recovery dependency depth.
- Pending recovery count.
- Available REST request capacity.
- Recovery concurrency limits.
- Failure notification duplication.
- Cancellation frequency.

### 31.4 Recovery Storm Benchmark

A representative benchmark should simulate:

    5 Exchanges

    15 Market Families

    Hundreds of Active Streams

    Multiple Shared Connection Failures

The coordinator must consolidate shared failures and schedule bounded recovery work.

Concrete performance targets must be established through measurement rather than arbitrary assumptions.

### 31.5 Isolation Benchmark

Saturate recovery processing for one exchange.

Verify that:

- Healthy exchange processing remains operational.
- Recovery scheduling remains responsive.
- Control-plane commands can still be processed.
- Runtime memory remains bounded.
- Unrelated connection pools are not blocked by recovery locks.

---

## 32. Implementation Acceptance Criteria

The Recovery Coordination subsystem is complete when the following conditions are satisfied.

### 32.1 Architecture

- The Recovery Coordinator has explicit ownership boundaries.
- Failure reporting uses structured typed events.
- Recovery planning is deterministic.
- Recovery operations have stable identities.
- Recovery generations prevent obsolete completions.
- Duplicate recovery requests are consolidated.
- Component-specific recovery mechanisms remain independently owned.

### 32.2 Dependency Coordination

- Recovery prerequisites are represented explicitly.
- Recovery dependency graphs are validated.
- Dependent actions wait for valid prerequisites.
- Obsolete dependencies invalidate affected operations.
- Independent recovery tasks can execute concurrently.
- Desired stream state is respected throughout recovery.

### 32.3 Resource Safety

- Recovery queues are bounded.
- Recovery concurrency is bounded.
- Exchange rate limits are respected.
- Local resource reservations are tracked.
- Failed and cancelled operations release resources.
- Shared failures do not produce duplicate recovery storms.
- Recovery workloads do not unnecessarily starve healthy acquisition.

### 32.4 Fault Tolerance

- Transient failures trigger appropriate recovery actions.
- Permanent errors stop futile automatic retries.
- Repeated failures escalate according to policy.
- Exchange outages are handled at the correct scope.
- Individual stream failures remain isolated where possible.
- Process crashes trigger validated restart recovery.
- Disk exhaustion is handled explicitly.
- PostgreSQL recording outages remain isolated when optional.
- Resource exhaustion does not produce silent integrity violations.

### 32.5 Data Integrity

- Trade coverage uncertainty remains visible.
- Confirmed gaps require supported evidence.
- Partial recovery is distinguished from complete recovery.
- Invalid L2 books cannot become authoritative.
- Current L2 restoration is distinct from historical coverage restoration.
- Publication gaps are distinct from acquisition gaps.
- Retention gaps are distinct from recording gaps.
- Recovery completion requires authoritative evidence.

### 32.6 Observability

- Recovery operation states are exposed.
- Failure classifications are available.
- Recovery dependencies are inspectable.
- Recovery escalation is visible.
- Unresolved coverage gaps remain identifiable.
- Runtime health reflects required service guarantees.
- TUI and API expose recovery status.
- Operational logs remain local.
- No Prometheus or Grafana dependency is required.

### 32.7 Testing

- Recovery Coordinator unit tests pass.
- Dependency coordination tests pass.
- Shared failure tests pass.
- Exchange outage simulations pass.
- Trade recovery integration tests pass.
- L2 resynchronization integration tests pass.
- Crash recovery tests pass.
- Disk and database failure tests pass.
- Publication and retention failure tests pass.
- Concurrency and generation-safety tests pass.
- Property-based tests pass.
- Recovery scalability benchmarks establish acceptable performance.

---

## 33. Final Architectural Contract

The Recovery Coordinator is responsible for restoring MarketForge Live's required operational guarantees after failures.

It does so by coordinating the existing components rather than duplicating their implementations.

The system-wide recovery architecture is:

    Runtime Failure
          |
          v
    Structured Failure Report
          |
          v
    Recovery Coordinator
          |
          v
    Failure Classification
          |
          v
    Recovery Plan
          |
          v
    Dependency and Resource Admission
          |
          v
    Component-Owned Recovery Actions
          |
          v
    Authoritative Result Validation
          |
          v
    Recovery Guarantee Evaluation
          |
          v
    Completed / Partial / Failed

The coordinator guarantees:

- Explicit failure classification.
- Correct recovery ownership.
- Bounded and dependency-aware scheduling.
- Prevention of duplicate recovery operations.
- Generation-safe asynchronous completion.
- Exchange-compliant recovery admission.
- Resource-aware failure escalation.
- Independent evaluation of recovery guarantees.
- Preservation of unresolved coverage gaps.
- Observable system health and recovery progress.

It does not guarantee:

- That every exchange outage can be resolved automatically.
- That missing trades are always recoverable.
- That a new L2 snapshot reconstructs missed historical updates.
- That all subscriber deliveries are exactly-once.
- That optional recording remains available during database outages.
- That arbitrary hardware or operating-system failures can be handled without process termination.
- That a recovered connection implies complete market-data integrity.

The essential invariant is:

    Recovery must never claim a guarantee
    that the responsible component
    has not established.

This principle applies to connectivity, subscriptions, normalization, L2 synchronization, historical coverage, publication, retention, and recording.

---