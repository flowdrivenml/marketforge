# MarketForge Live — Autonomous Plan: Canonical Stream Router

**Project:** MarketForge Live
**Repository:** `/home/pasha/projects/bybit-history-downloader`
**Branch:** `codex/stream-router`
**Milestone:** Canonical Stream Router
**Status:** Authorized for autonomous implementation
**Scope:** Rust Live subsystem only

---

## 1. Objective

Implement the production-quality Canonical Stream Router for MarketForge Live.

Source Stream is complete and has passed deterministic integration tests for all 14 supported BTC exchange profiles, including PostgreSQL persistence and restoration.

The Router is the next architectural component.

Its responsibility is to accept canonical market-data publications and distribute them to subscribed consumers while preserving:

- Stream identity and producer authority.
- Per-stream publication ordering.
- Subscription and demand ownership.
- Bounded memory and backpressure.
- Consumer isolation.
- Generation fencing.
- Explicit publication acknowledgment semantics.
- Correct lifecycle and shutdown behavior.

The Router must support direct in-process Rust consumers without serialization or interprocess communication.

Future consumers will include PostgreSQL recording, Unix-domain socket publication, Derived Streams, and potentially Spectra.

Only implement the Router core and its Source Stream integration in this milestone.

---

## 2. Architectural Authority

Before implementation, read:

- `.planning/live/AGENTS.md`
- `.planning/live/objective.md`
- `.planning/live/AUTONOMOUS_PLAN.md`
- Existing Rust implementation and public contracts.
- Existing integration tests.
- Existing PostgreSQL schema.

Retired architecture documents must not be restored or required.

Preserve existing architectural decisions unless a concrete correctness issue requires adjustment.

The completed Source Stream implementation is an established subsystem.

Do not redesign Source Stream normalization, integrity, recovery, acquisition, or persistence.

PostgreSQL remains mandatory infrastructure for MarketForge Live.

### Git Safety

Work exclusively on:

`codex/stream-router`

Before modifying code:

1. Inspect Git status and the current HEAD.
2. Confirm the Source Stream checkpoint exists.
3. Preserve all existing changes.
4. Do not reset, rebase, force-push, or overwrite unrelated work.

Use logically grouped commits.

Do not push unless explicitly authorized.

---

## 3. Existing Infrastructure

Inspect and reuse the existing modules:

    engine/src/live/stream/registry/
    engine/src/live/stream/persistence/
    engine/src/live/stream/request/
    engine/src/live/stream/source/

    engine/src/live/integrity/
    engine/src/live/recovery/
    engine/src/live/infrastructure/websocket/

In particular, examine:

    stream/registry/demand.rs
    stream/registry/graph.rs
    stream/registry/reconcile.rs
    stream/registry/runtime.rs

    stream/source/builder.rs
    stream/source/executor.rs
    stream/source/runner.rs
    stream/source/supervisor.rs

    stream/persistence/repository.rs

Reuse existing concepts for:

- Stream identity.
- Runtime generation.
- Demand ownership.
- Dependency relationships.
- Runtime lifecycle.
- Durable stream registration.
- Source publication.
- Acquisition fencing.

Do not implement parallel registries or competing identity systems.

The Router may maintain ephemeral producer bindings and delivery subscriptions, but they must integrate with existing infrastructure.

---

## 4. Target Architecture

    Source Stream ─────┐
                       │
    Source Stream ─────┤
                       │
                       ▼
                STREAM ROUTER
                       │
           ┌───────────┼────────────┐
           │           │            │
           ▼           ▼            ▼
       PostgreSQL     Unix       Derived
        Recorder     Socket      Streams
                     Output         │
                                    ▼
                                  Spectra
                                  (Future)

All destinations should use the same generic Router consumer interface.

The Router must not know whether its consumer is a recorder, socket publisher, derived processor, or financial-engineering system.

The Router is responsible for transport-independent canonical event distribution.

It must not implement:

- Exchange normalization.
- Trade or depth integrity calculation.
- REST recovery.
- Financial transformations.
- Feature engineering.
- Trading execution.

---

## 5. Public Router Facade

Provide a small public Rust facade.

Desired conceptual API:

```rust
pub struct StreamRouter;

impl StreamRouter {
    pub fn new(
        config: RouterConfig,
        context: RouterContext,
    ) -> Result<Self, RouterError>;

    pub fn attach_producer(
        &self,
        request: AttachProducerRequest,
    ) -> Result<ProducerHandle, RouterError>;

    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<SubscriptionHandle, RouterError>;

    pub fn status(&self) -> RouterStatus;

    pub async fn shutdown(&self) -> Result<(), RouterError>;
}
```

### Producer Handle

```rust
pub struct ProducerHandle;

impl ProducerHandle {
    pub fn try_publish(
        &self,
        publication: RouterPublication,
    ) -> Result<RouterAcceptance, PublishError>;
}
```

### Subscription Handle

```rust
pub struct SubscriptionHandle;

impl SubscriptionHandle {
    pub async fn recv(&mut self) -> Option<RouterDelivery>;

    pub async fn close(self) -> Result<(), RouterError>;
}
```

These signatures describe the desired API, not mandatory exact implementations.

Adapt them where necessary to preserve correctness and existing contracts.

### Facade Requirements

The facade must provide:

- Router construction.
- Authorized producer attachment.
- Consumer subscription.
- Synchronous nonblocking publication.
- Explicit consumer closure.
- Router status.
- Graceful shutdown.

Use simple request objects.

Avoid exposing internal locks, registries, channels, and task handles.

A downstream Rust component should need only a few imports.

---

## 6. Proposed Module Structure

Introduce:

    engine/src/live/stream/router/
    ├── mod.rs
    ├── facade.rs
    ├── event.rs
    ├── ingress.rs
    ├── producer.rs
    ├── subscription.rs
    ├── dispatch.rs
    ├── capacity.rs
    ├── lifecycle.rs
    └── status.rs

This is a suggested structure.

Adapt it to existing project conventions where appropriate.

Do not introduce unnecessary abstractions or excessive module fragmentation.

---

## 7. Canonical Router Events

Reuse existing canonical Rust types.

Do not introduce alternative trade or depth schemas.

The Router must support:

- Canonical trade publications.
- Canonical L2 snapshots.
- Canonical L2 deltas.
- Depth invalidation events.

Preserve relevant identity, provenance, sequence, and generation metadata.

Use immutable owned publication payloads.

Prefer shared ownership such as `Arc<T>` for efficient fanout when appropriate.

Do not deep-clone large depth snapshots for every subscriber unnecessarily.

### Atomic Publications

Each publication is an atomic admission unit.

A batch must be either completely accepted or completely rejected.

Never acknowledge partial acceptance.

Maintain ordering within each publication.

---

## 8. Publication Acknowledgment Contract

This is a critical requirement.

Source Stream currently commits integrity state after successful publication acknowledgment.

The Router must preserve this guarantee.

Define successful Router acceptance as:

**Complete acceptance into bounded volatile Router ownership.**

Successful acceptance does not imply:

- PostgreSQL durability.
- Successful external socket delivery.
- Completion of consumer processing.
- Delivery to every subscriber.

These are separate guarantees.

The Router must return an error when it cannot accept the complete publication.

Source Stream must not commit unpublished trades after failed Router admission.

### Synchronous Publication

Existing Source Stream publication callbacks are synchronous.

The Router must provide compatible nonblocking admission.

Do not indefinitely block Tokio workers waiting for consumer processing.

### Downstream Failure

Define explicit behavior when Router admission succeeds but downstream delivery subsequently fails.

Do not silently claim end-to-end lossless delivery.

Consumers requiring complete continuity must receive an explicit failure or desynchronization indication when delivery cannot be guaranteed.

---

## 9. Producer Ownership and Generation Fencing

Producer handles must be associated with validated stream identity and runtime authority.

Reject:

- Unknown streams.
- Unauthorized producer bindings.
- Detached producer handles.
- Revoked generations.
- Incorrect payload types.
- Publication after Router shutdown.
- Old-generation publication after producer replacement.

Do not confuse runtime stream generations with Source Stream acquisition epochs.

Preserve existing acquisition fencing.

Ensure queued stale publications cannot cross a revoked generation boundary.

Concurrent producer attachment and replacement must have deterministic semantics.

---

## 10. Consumer Subscriptions

Support multiple independent subscribers for one stream.

Each subscription must have:

- Stable identity.
- Explicit demand ownership.
- Bound stream identity.
- Bounded capacity.
- Lifecycle state.
- Defined continuity policy.
- Explicit cleanup.

Distinguish:

**Demand Registry:** Determines whether a stream is required.

**Router Subscription Registry:** Determines where publications are delivered.

Reuse existing demand infrastructure.

Removing one consumer must not stop a stream still required by another owner.

Removing the final ephemeral owner must release its demand.

Dropping a subscription must not leak ownership.

Preserve always-on demand where applicable.

---

## 11. Event Ordering

Preserve successful admission order independently for each stream.

For example:

    Producer:  A B C D
    Consumer:  A B C D

Never:

    Consumer:  A C B D

Define an explicit linearization point for concurrent publication.

Do not fabricate global event ordering between independent exchanges.

Maintain per-stream ordering across publication batches.

Consumers must not silently skip required events while remaining synchronized.

---

## 12. Fanout and Consumer Isolation

Support one producer and multiple consumers.

Example:

    Source
       │
       ▼
     Router
       │
       ├── Consumer A
       ├── Consumer B
       └── Consumer C

Consumers should receive shared immutable canonical events directly in Rust.

A slow or failed consumer must not corrupt unrelated consumers.

The Router must explicitly handle a full consumer queue.

Choose and document a coherent policy for partial fanout failure.

Acceptable designs may include:

- Atomic reservation before admission.
- Explicit consumer invalidation after failed delivery.
- Another bounded mechanism with equivalent guarantees.

Never silently claim successful delivery to all subscribers when some have missed events.

Do not execute arbitrary consumer computations on the Router dispatch path.

---

## 13. Resource Management

Every Router-controlled queue must be bounded.

Implement configurable limits for:

- Ingress pending publications.
- Ingress pending bytes.
- Consumer queue publications.
- Consumer queue bytes.
- Active subscriptions.
- Active producer bindings.

Enforce actual limits.

A depth snapshot may be much larger than an ordinary trade publication.

Therefore, queue-length limits alone are insufficient.

Account for retained event memory.

Use checked arithmetic where appropriate.

Avoid:

- Unbounded channels.
- Busy polling.
- Per-event Tokio task spawning.
- Global locks held across `.await`.
- Unnecessary serialization.
- Uncontrolled payload cloning.

### Backpressure

Required behavior:

- Reject admission when ingress capacity is exhausted.
- Explicitly handle exhausted consumer capacity.
- Prevent unlimited memory accumulation.
- Release capacity after consumption or cancellation.
- Release resources after shutdown.
- Preserve accurate operational counters.

Consumer continuity failures must never be silently hidden.

---

## 14. Depth Synchronization

Depth routing must preserve:

    Snapshot
       │
       ▼
    Delta
       │
       ▼
    Delta
       │
       ▼
    Invalidation
       │
       ▼
    Recovery Snapshot
       │
       ▼
    Delta

The Router must preserve publication ordering and book-generation authority.

### Late Subscribers

A consumer subscribing after the original snapshot cannot automatically treat subsequent deltas as a synchronized book.

Inspect existing Source Stream book and publication contracts.

Reuse an authoritative snapshot/bootstrap mechanism if available.

If no suitable mechanism exists, represent the consumer as unsynchronized until an authoritative initialization event becomes available.

Do not implement a competing order book inside the Router.

Do not fabricate complete snapshots from incomplete depth information.

### Recovery

After source invalidation:

- Old book authority must not remain valid.
- Old-generation deltas must not be accepted as current.
- Recovery snapshots must establish new authority.
- Consumer synchronization must be explicit.

---

## 15. Lifecycle and Shutdown

Implement lifecycle handling for:

- Router initialization.
- Producer attachment.
- Producer replacement.
- Producer detachment.
- Subscription creation.
- Subscription closure.
- Consumer failure.
- Source restart.
- Router shutdown.

Shutdown must stop new admissions, revoke authority, and resolve pending work according to documented guarantees.

Release subscriptions, demand ownership, and capacity accounting.

Do not report clean shutdown if required cleanup has failed.

Avoid deadlocks between publication, shutdown, and subscription removal.

---

## 16. Future In-Process Rust Consumers

The Router must support direct Rust consumers without Unix sockets.

This is required for future Derived Streams and potentially Spectra.

Example:

```rust
let mut consumer = router.subscribe(request).await?;

while let Some(delivery) = consumer.recv().await {
    process(delivery);
}
```

The interface must not require:

- JSON conversion.
- Socket serialization.
- A separate executable.
- A separate Tokio runtime.

Consumers may use their own workers or execution pools.

The Router must remain independent of those execution decisions.

Do not implement Spectra or CPU scheduling in this milestone.

---

## 17. Integration Tests

Create a dedicated Cargo test target:

`live_router`

Entrypoint:

    engine/tests/live/integration/router/main.rs

Suggested organization:

    engine/tests/live/integration/router/
    ├── main.rs
    ├── harness.rs
    ├── ingress.rs
    ├── dispatch.rs
    ├── subscriptions.rs
    ├── ownership.rs
    ├── backpressure.rs
    ├── lifecycle.rs
    ├── integrity.rs
    └── pipeline.rs

Use actual production Router components.

Use deterministic synchronization mechanisms rather than arbitrary timing delays wherever possible.

### Required Test Matrix

| Category | Required scenarios |
|---|---|
| Admission | Valid, invalid identity, wrong type, oversized publication |
| Atomicity | Complete acceptance or rejection, never partial |
| Ordering | Multiple batches, concurrent submissions, per-stream ordering |
| Fanout | One producer, multiple consumers, exact event contents |
| Isolation | Slow and failed consumers do not corrupt others |
| Ownership | Shared demand, final-owner removal, dropped handles |
| Fencing | Stale producer, revoked generation, producer replacement |
| Depth | Snapshot, deltas, invalidation, replacement snapshot |
| Late subscription | No false synchronization |
| Backpressure | Event and byte capacity exhaustion |
| Lifecycle | Startup, shutdown, reconnect, repeated lifetimes |
| Resources | No leaks after cancellation, failure, or shutdown |
| Pipeline | Real Source Stream to Router to multiple consumers |

### Exact Fanout Test

Start one producer and three consumers.

Publish 1,000 ordered canonical trade records.

Verify that each continuously synchronized consumer receives exactly the expected records in order.

Then fail one consumer deliberately.

Verify that remaining consumers continue correctly.

Confirm failed consumer status and resource cleanup.

### Depth Continuity Test

Publish:

    Snapshot
    Delta 1
    Delta 2
    Invalidation
    Recovery Snapshot
    Delta 3

Assert exact ordering, generation transitions, and consumer synchronization state.

### Backpressure Test

Use deliberately small capacities.

Force consumer queue saturation.

Verify bounded behavior, explicit failure semantics, and unaffected consumers where the selected policy permits isolation.

Do not use timing alone as proof of resource bounds.

---

## 18. Production Pipeline Integration

The final integration tests must exercise:

    Local WebSocket Server
             │
             ▼
    Production WebSocket Manager
             │
             ▼
    Production Source Stream
             │
             ▼
    Production Normalization
             │
             ▼
    Production Integrity
             │
             ▼
    Production Router
             │
             ▼
    Multiple Rust Consumers

Do not replace Source Stream or Router internals with mocks.

Use controlled local WebSocket and HTTP servers as appropriate.

Representative profiles:

- Binance Spot Trades.
- Binance Spot Depth.
- Bybit Spot Depth.
- Bitget Linear Depth.
- Gate.io Linear Trades.

Test actual canonical contents, ordering, acknowledgments, ownership, and lifecycle.

The Router is exchange-independent.

Do not unnecessarily duplicate all exchange-specific Source Stream tests.

Preserve the existing 14-profile regression coverage.

---

## 19. Unit Tests

Add focused unit tests covering:

- Producer validation.
- Subscription identity.
- Generation fencing.
- Admission state transitions.
- Atomic batches.
- Ordering.
- Capacity accounting.
- Overflow arithmetic.
- Consumer failure transitions.
- Demand ownership cleanup.
- Shutdown state.
- Invalid configuration.
- Resource release.

Tests should establish correctness through explicit assertions.

Avoid tests that merely check that an operation returned `Ok`.

---

## 20. Verification

Use Live features only.

Router integration tests:

```bash
cargo test \
  --manifest-path engine/Cargo.toml \
  --locked --offline \
  --no-default-features \
  --features live \
  --test live_router \
  -- --nocapture 2>&1 | cpout
```

Run focused Router unit tests.

Preserve Source Stream regression coverage:

```bash
scripts/verify-live-source.sh quick 2>&1 | cpout
scripts/verify-live-source.sh integration 2>&1 | cpout
```

Run relevant:

- WebSocket tests.
- Integrity tests.
- Recovery tests.
- PostgreSQL registration tests where affected.
- Existing Live integration tests.

Once implementation stabilizes, execute full Live verification.

Do not run multiple Cargo test processes concurrently against the same target directory.

Do not repeatedly execute expensive full suites while fixing isolated issues.

Never weaken or remove tests merely to obtain a passing result.

Document all failures and corrective changes.

---

## 21. Completion Criteria

The Router milestone is complete only when:

- Public facade is usable.
- Source Stream publication integration works.
- Producer authority is validated.
- Per-stream ordering is preserved.
- Multi-consumer fanout works.
- Subscription ownership is correct.
- Backpressure is bounded.
- Consumer continuity failures are explicit.
- Depth invalidation and generation fencing work.
- Producer replacement is safe.
- Shutdown releases resources.
- Deterministic production-pipeline integration tests pass.
- Existing Source Stream regressions pass.
- No unrelated architectural regressions are introduced.

Passing tests alone does not establish production readiness.

---

## 22. Explicit Exclusions

Do not implement:

- Spectra.
- Financial feature engineering.
- Derived Stream computation.
- PostgreSQL market-data recording.
- Unix socket publisher.
- Axum Control API.
- Ratatui management interface.
- Trading execution.
- Historical processing.
- A replacement canonical schema.
- A replacement Source Stream.
- A replacement PostgreSQL registry.

Preserve future compatibility without implementing future subsystems prematurely.

---

## 23. Final Report

After implementation, provide:

### Implementation Summary

- Public facade and types.
- Internal Router architecture.
- Existing components reused.
- Source Stream integration.
- Admission and acknowledgment guarantees.
- Consumer delivery semantics.
- Capacity and lifecycle policies.

### Verification Evidence

Report:

- Exact test commands.
- Test counts.
- Passed, failed, and ignored tests.
- Relevant regression results.
- Known limitations.
- Remaining architectural questions.

### Git State

Report:

- Branch.
- HEAD.
- Implementation commits.
- Working tree status.
- Modified paths.

Do not mark the milestone accepted on behalf of the user.

---

## 24. Execution Authorization

Codex is authorized to implement the Canonical Stream Router, create relevant integration tests, run focused verification, repair discovered defects within scope, and commit completed work to the designated development branch.

Proceed autonomously.

Preserve the existing architecture.

Prefer correctness, bounded resources, explicit ownership, and understandable code over unnecessary abstractions.

**The final Router must be a reusable infrastructure component, not an implementation specialized for Source Stream, PostgreSQL, Unix sockets, Derived Streams, or Spectra.**
