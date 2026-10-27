# MarketForge Live — Recovery Manager Completion Handoff

**Status:** Infrastructure implemented and independently tested  
**Current milestone:** Stage 6.2 completed  
**Next dependency:** Source Stream Runtime  
**Decision:** Defer further Recovery integration until its dependent infrastructure and the Source Stream Runtime are implemented.

---

## 1. Current implementation

The shared Recovery Manager is implemented under:

`engine/src/live/recovery/`

Existing components:

| Module | Responsibility |
|---|---|
| `model.rs` | Recovery requests, operations, epochs, attempts, and identities |
| `key.rs` | Conservative REST acquisition identity and request coalescing |
| `registry.rs` | Registered requests, jobs, ownership, and cancellation |
| `scheduler.rs` | FIFO scheduling, concurrency limits, and job lifecycle |
| `executor.rs` | REST acquisition through the existing REST Facade |
| `coordinator.rs` | Shared asynchronous execution, completion delivery, and shutdown |
| `facade.rs` | Lightweight stream-bound recovery interface |

### Implemented capabilities

- One shared asynchronous Recovery Manager.
- Cloneable handles for individual source streams.
- Bounded request and job capacity.
- Concurrent REST acquisitions.
- FIFO scheduling.
- Coalescing of equivalent queued acquisitions.
- Shared REST responses using `Arc`.
- Real exchange REST execution through existing infrastructure.
- Reuse of the existing HTTP client and rate limiter.
- Individual request cancellation.
- Exact requester ownership checks.
- Exact source-epoch cancellation.
- Generation-based cancellation.
- Running acquisition abortion.
- Graceful manager shutdown.
- Typed recovery completions containing requester identity, epoch, and attempt.

### Test status

The Recovery integration suite passed **55 tests** following Stage 5.4.

Stage 6.1 and Stage 6.2 introduced additional facade and ownership tests. Their complete suite results were not explicitly reported in this handoff and should be reconfirmed before final integration.

---

## 2. Architectural ownership

The intended architecture is:

```text
WebSocket Manager
        │
        ▼
Source Stream Runtime ◄──── Recovery Manager
        │                          ▲
        ▼                          │
Normalization                Recovery Request
        │                          │
        ▼                          │
Integrity Facade ────────────────►─┘
        ▲
        │
Normalized REST Recovery Result
```

### WebSocket Manager

Owns physical connections, reconnections, subscriptions, native message routing, and transport availability.

It must not own market-data recovery or Integrity state.

### Integrity Facade

Owns market-data continuity validation, depth synchronization state, trade integrity state, and recovery admissibility.

It determines when recovery is required and whether recovered data can be accepted.

It does not perform asynchronous HTTP requests.

### Recovery Manager

Owns REST acquisition scheduling, request coalescing, concurrency, cancellation, and response delivery.

It returns exchange-native responses and does not determine whether historical coverage or depth synchronization has been restored.

### Source Stream Runtime

Will be the serialized execution owner connecting WebSocket input, normalization, Integrity, recovery requests, recovery completions, and publication.

It must not duplicate the Recovery Manager's scheduling machinery or Integrity's validation rules.

---

## 3. Work remaining to finish Recovery

### R1 — Runtime-issued recovery bindings

**Dependency:** Source Stream Registry / Runtime.

The current `StreamRecoveryBinding` contains source identity, configuration generation, and recovery epoch.

However, its constructor still accepts caller-supplied values.

Required integration:

- Create bindings from authoritative source-stream registration state.
- Ensure the instrument and exchange configuration match the registered stream.
- Assign trustworthy configuration and acquisition generations.
- Prevent obsolete stream instances from issuing new requests.
- Revoke or replace bindings during relevant lifecycle transitions.

**Completion condition:** Recovery requests are accepted only on behalf of the correct registered stream instance.

### R2 — Asynchronous completion delivery

**Dependency:** Source Stream Runtime event loop.

Recovery currently returns a `RecoveryTicket` containing a `oneshot::Receiver<RecoveryCompletion>`.

Required integration:

- Poll outstanding recovery tickets without blocking WebSocket processing.
- Deliver completed acquisitions into the owning stream's serialized processing context.
- Preserve requester identity and recovery attempt information.
- Handle failed, cancelled, and shutdown completions explicitly.
- Avoid silently losing completion events or leaving recovery state pending indefinitely.

**Completion condition:** WebSocket processing continues while REST acquisitions are outstanding, and completions reach the correct source-stream owner.

### R3 — Lifecycle and attempt fencing

**Dependency:** Source Stream Runtime lifecycle.

Required integration:

- Cancel obsolete requests during reconnects, resubscriptions, stream replacement, and shutdown.
- Validate stream, connection, subscription, and recovery-attempt identity before applying results.
- Reject late completions from obsolete attempts.
- Ensure a cancelled request cannot modify newer Integrity state.
- Define behavior for timeouts and failed acquisitions.

**Completion condition:** An obsolete recovery result cannot modify current stream state or authorize publication.

### R4 — Depth REST snapshot integration

**Dependencies:** Source Stream Runtime, Normalization, Integrity Facade.

Existing Integrity interfaces:

- `prepare_depth_recovery(...)`
- `on_recovery_result(...)`

Required integration:

1. Detect depth synchronization requirements through Integrity.
2. Prepare the authoritative REST synchronization attempt.
3. Submit the corresponding snapshot acquisition.
4. Continue processing and buffering WebSocket deltas as required by Integrity.
5. Receive and normalize the REST snapshot.
6. Associate it with the correct synchronization token and source epoch.
7. Deliver it to Integrity.
8. Respect Integrity's acceptance or rejection decision.
9. Resume publication only through the existing publication fence.

**Completion condition:** REST-based depth synchronization works end-to-end without losing buffered deltas or admitting obsolete snapshots.

### R5 — Trade recovery and coverage reconciliation

**Dependencies:** Source Stream Runtime, Integrity, additional trade-specific recovery logic.

The Recovery Manager already supports:

- Recent trades.
- Historical trade pages where supported by the exchange REST facade.

However, acquiring trade history is not equivalent to proving that missing trades have been recovered.

Required implementation:

- Define recovery coverage targets and reconciliation state.
- Select suitable REST acquisition strategies.
- Support bounded pagination where necessary and available.
- Normalize historical trade results.
- Reconcile overlapping WebSocket and REST trades.
- Deduplicate using established trade identities.
- Respect exchange-specific limits on historical coverage.
- Preserve uncertainty when complete coverage cannot be demonstrated.
- Release trade coverage holds only when the relevant Integrity contract permits it.

**Completion condition:** Trade recovery makes defensible coverage decisions without silently assuming completeness.

### R6 — Runtime recovery policy

**Dependencies:** Source Stream Runtime and recovery configuration.

Define and integrate:

- Acquisition timeouts.
- Retry and backoff policy.
- Maximum recovery attempts.
- Recovery escalation when REST repeatedly fails.
- Resource limits for pending recovery work.
- Behavior during rate limiting and exchange unavailability.

Reuse the existing REST transport and rate-limiter infrastructure.

Do not introduce a second HTTP retry scheduler without a clear requirement.

**Completion condition:** Every recovery attempt has bounded resource usage and an explicit failure or retry outcome.

### R7 — Publication integration

**Dependencies:** Source Stream Runtime and publication infrastructure.

Required integration:

- Invalidate downstream depth state when Integrity loses synchronization.
- Do not publish unsynchronized depth data.
- Apply accepted recovery transitions through the existing publication adapter.
- Respect preparation, handoff, acknowledgement, and reset protocols.
- Never treat REST HTTP success as permission to publish.

**Completion condition:** Consumers never receive recovered data before Integrity and publication fencing authorize it.

### R8 — Final end-to-end validation

**Dependencies:** R1–R7.

Once Source Stream Runtime integration exists, prioritize scenario-based tests covering:

- Initial Binance REST depth synchronization.
- WebSocket deltas arriving while REST is pending.
- Depth gaps requiring another recovery attempt.
- Reconnects during REST acquisition.
- Obsolete snapshots arriving after generation changes.
- Concurrent streams sharing one REST acquisition.
- Cancellation of one requester without disrupting others.
- Exchange REST failures and timeouts.
- Trade history overlap and deduplication.
- Incomplete historical trade coverage.
- Shutdown during pending recovery.
- Publication invalidation and restoration.

Reuse existing unit and integration tests rather than duplicating the same scheduler, registry, and executor cases.

**Completion condition:** The complete live recovery pipeline is verified against realistic asynchronous event ordering.

---

## 4. Development order

**Do not continue implementing Recovery-specific integration immediately.**

Recommended sequence:

1. Finish infrastructure required by the Source Stream Runtime.
2. Implement the Source Stream Registry and authoritative lifecycle ownership.
3. Implement the Source Stream Runtime and its serialized processing loop.
4. Connect WebSocket input, normalization, and Integrity.
5. Integrate the existing Recovery Facade and completion delivery.
6. Complete depth REST recovery.
7. Implement trade recovery and coverage reconciliation.
8. Integrate publication.
9. Run comprehensive end-to-end lifecycle tests.

---

## 5. Constraints for future implementation

Preserve the following architectural decisions:

- **One shared Recovery Manager per MarketForge runtime.**
- Individual streams receive lightweight, stream-bound handles.
- Physical WebSocket connections remain owned by WebSocket infrastructure.
- REST transport, HTTP clients, and rate limiters remain shared.
- Integrity remains the authority for market-data recovery correctness.
- The Source Stream Runtime serializes Integrity transitions.
- Recovery completions must preserve source epoch and attempt identity.
- Equivalent REST acquisitions may be shared; Integrity state must never be shared between unrelated streams.
- No recovery operation may bypass the publication fence.
- Do not duplicate existing components merely to simplify integration.

---

## 6. Handoff decision

**Recovery Manager infrastructure is sufficiently developed to pause.**

It is not yet a fully integrated live recovery system.

The remaining work primarily concerns authoritative runtime bindings, asynchronous result application, depth synchronization, trade coverage reconciliation, recovery policy, and publication safety.

**Resume Recovery development after the Source Stream Runtime foundation and its required dependencies are available.**