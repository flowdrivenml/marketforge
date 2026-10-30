# Canonical Stream Router — implementation and verification

Status: **Ready for Review**. Implementation and authorized verification are
complete. Milestone acceptance remains with the user/integration owner.

Branch: `codex/stream-router`. Source checkpoint `333cc76` remains an ancestor.
No push, merge, reset, rebase, database write, or public exchange request was made.
The pre-existing edits to `.planning/live/AGENTS.md` and
`.planning/live/AUTONOMOUS_PLAN.md`, and untracked `engine/.planning/`, were preserved.

## Architecture and public API

`live::stream::router` exports `StreamRouter`, `RouterConfig`, `QueueLimits`,
`RouterContext`, `AttachProducerRequest`, `SubscribeRequest`, `ProducerHandle`,
`SubscriptionHandle`, publication/delivery types, statuses, and typed errors.
The facade provides `new`, `attach_producer`, async `subscribe`, `status`, and async
`shutdown`. Producers provide synchronous nonblocking `try_publish`; subscriptions
provide cancellation-safe async `recv`, async consuming `close`, stable identity,
state, and queue/synchronization status. Handles clean up on drop.

A single dispatch task owns bounded volatile ingress and routes immutable
`Arc<RouterPublication>` payloads into independently bounded consumer queues.
Payloads reuse canonical `Trade`, `BookChange`, depth sequence baseline, and
accepted timestamp/provenance contracts. Initialization/change boundaries remain
explicit. Trade integrity observations, coverage flags, baseline, duplicate and
received counts are preserved; recovered trade publications are identified.
Fanout does not clone each subscriber's complete snapshot or trade batch.

The existing `StreamGraph` supplies accepted persisted identity and enabled state.
The existing `DemandRegistry` owns every subscription's demand; Router subscription
entries are delivery bookkeeping. Removing one owner preserves independent demand
and AlwaysOn activation. Consumer identity components are bounded to 4096 bytes.
No alternate persistent registry, log namespace, canonical schema, or order book
was introduced. Persistence repository and SQL schema remain unchanged.

`StreamRuntimeRegistry` issues opaque, revocable `RuntimePublicationAuthority`
leases when starting a runtime generation. A lease carries the established durable
definition and runtime generation and can be claimed exactly once, including
across Router instances. Stop, failure, restart, and registry drop revoke it.
Router attachment rejects mismatched definitions, unknown/disabled streams,
revoked authorities, duplicate claims, and exhausted binding capacity.
Replacement atomically retires old ingress and consumer queues. Old handle drop
cannot affect a newer binding. Runtime, source acquisition, and book generations
remain independent. An idle dispatch task observes revocation without polling.

`SourceRuntimeSupervisor::start_routed` integrates the actual prepared Source
executor and obtains its lease from the supervisor's existing runtime registry.
The original callback-based `start` still works. Source normalization, integrity,
recovery, acquisition, persistence, and runner implementations were preserved.
The callback adapters are isolated in `router/source.rs`.

## Guarantees and policies

- Admission linearizes at complete ingress insertion under Router and runtime
  authority guards. A batch is accepted in full or rejected without advancing
  publication sequence, depth authority, or queue capacity. Count and byte
  exhaustion, stale generations, invalid identities/types, and arithmetic
  exhaustion fail explicitly. Lock contention returns `Busy` without waiting.
- Successful acceptance means complete bounded **volatile Router ownership**.
  It does not acknowledge durability, every subscriber, or consumer processing.
  Source commits trade identities only after successful admission. A production
  Source runner test rejects a native trade at full ingress, publishes that same
  identity after capacity release, and verifies deduplication only after success.
- Publication order is preserved independently per stream, including concurrent
  callers and batches. Acceptance and delivery carry runtime generation and a
  per-binding sequence. The sequence is not a durable canonical log cursor.
  There is no fabricated global exchange ordering.
- Subscriber admission has a boundary: publications admitted earlier are excluded.
  Slow consumers fail with one terminal `ContinuityLost(ConsumerCapacity)` followed
  by EOF; queued data, demand, and capacity are released. Other consumers continue.
- Depth subscribers begin unsynchronized until an authoritative initialization.
  No bootstrap book is fabricated. Ordered invalidation/snapshot/delta events and
  source/book authority are preserved; obsolete deltas fail. Synchronization
  describes the consumer's delivery chain, never consumer-processed state.
- Ingress and consumer queues have configurable publication/byte limits, and
  producer/subscription counts are bounded. Retained canonical records, string
  allocations, observations, levels, shared headers, queue overhead, and pending
  stream identity are charged conservatively using checked arithmetic. Fanout
  charges full payload bytes to each consumer. Consumer-retained deliveries after
  `recv` are outside Router-owned queue budgets.
- Shutdown stops admission, cancels undelivered volatile work with explicit
  terminal failures, releases demand/capacity, and joins dispatch. Cleanup errors
  are reported. Drop cancels resources and aborts the worker as fallback.
  Source stop's final depth invalidation is acknowledged as cleanup when a
  terminal runtime/Router fence has already invalidated consumers; fresh market
  data is still rejected. No locks are held across awaits or user computation.

Detailed API and operational contract: `docs/live/stream-router.md`.

## Acceptance ledger

| Requirement | Evidence/outcome |
| --- | --- |
| Public facade/direct Rust consumers | Public handles, typed publications, production consumer tests |
| Atomic count/byte admission | Complete trade/depth rejection, oversized batches, sequence/authority unchanged |
| Ordering | 1,000 exact canonical records to three consumers, concurrent submitters, interleaved streams |
| Fanout/isolation | Shared Arc identity; deliberate consumer failure after 1,000 records; other consumers continue |
| Demand ownership | Independent owners, duplicate rejection, explicit close/drop, final owner removal, AlwaysOn preservation |
| Producer fencing | Single claim, foreign identity, idle revocation, ingress/delivery revocation, replacement backlog, stale handles |
| Depth | Snapshot/delta/delta/invalidation/recovery snapshot/delta; atomic groups; late subscription; stale generations |
| Resources | Publication/byte/binding/subscription limits, overflow, cancellation, terminal queue release, cleanup failure reporting |
| Source acknowledgment | Same native trade accepted after rejected Router admission; no false readiness |
| Production pipelines | Five requested representative profiles through real WS manager, Source, normalization, integrity, Router and three Rust consumers |
| Reconnection | Bybit depth acquisition invalidation and newer-generation recovery snapshot through production pipeline |
| Lifecycle | Startup, detach, replacement/restart, shutdown/drop, idempotent shutdown; Source repeated-lifetime regressions |
| Existing Source coverage | Existing 14-profile capability regressions preserved and executed |

## Verification

All listed final commands exited **0**. Cargo ran sequentially, with default
features disabled and offline/locked dependencies. No Historical feature was run.

| Command | Executed result |
| --- | --- |
| `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --lib live::stream::router` | 12 passed; 0 failed/ignored |
| `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --test live_router -- --nocapture` | 27 passed; 0 failed/ignored |
| `scripts/verify-live-source.sh quick` | Cargo check passed; 124 Source unit tests passed; 0 failed/ignored |
| `scripts/verify-live-source.sh integration` | 38 passed; 0 failed; 1 existing PostgreSQL test ignored |
| `scripts/verify-live.sh full` | All 15 gates passed; counts below |
| `cargo fmt --manifest-path engine/Cargo.toml --all -- --check` | Passed after final integration import cleanup |
| `git diff --check` | Passed |
| `git merge-base --is-ancestor 333cc76 HEAD` | Passed |

The full Live script now includes `live_recovery`, `live_source`, and `live_router`
in addition to its original gates.

| Full Live gate | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Library | 2029 | 0 | 4 |
| WebSocket | 69 | 0 | 0 |
| REST | 48 | 0 | 0 |
| Normalization | 9 | 0 | 0 |
| Integrity | 83 | 0 | 0 |
| Configuration | 14 | 0 | 0 |
| Metrics | 3 | 0 | 0 |
| Exchange contracts | 149 | 0 | 0 |
| Recovery | 63 | 0 | 0 |
| Source | 38 | 0 | 1 |
| Router | 27 | 0 | 0 |
| Doctests | 7 | 0 | 0 |
| **Total** | **2539** | **0** | **5** |

Formatting, Live library check, and network-target compilation also passed.
Network compilation executes no external smoke tests. The four ignored library
cases require isolated PostgreSQL recording access; the Source database case
requires scoped write authorization. No new SQL persistence behavior is claimed.

Production pipeline fixtures cover Binance Spot trades, Binance Spot depth with
controlled gated REST bootstrap, Bybit Spot depth including reconnect/recovery,
Bitget Linear depth snapshot bridging, and Gate.io Linear native trade routing.
These use existing local HTTP/WS infrastructure, native BTC fixtures, production
Source construction/attachment, normalization/integrity, runtime supervision,
publication acknowledgment, demand cleanup, and transport shutdown.

Logs:

- `engine/target/live-verification/router/unit.log`
- `engine/target/live-verification/router/integration.log` (final 27 cases with diagnostics)
- `engine/target/live-verification/router/source-quick.log`
- `engine/target/live-verification/router/source-integration.log`
- `engine/target/live-verification/source-20261029T222048Z-355343/`
- `engine/target/live-verification/source-20261029T222059Z-355639/`
- `engine/target/live-verification/20261029T222130Z-356379/` (full Live gate logs)

Development compilation exposed incorrect private-module imports, an initially
unfinished test module, a pinned receive-future borrow scope, and a nonexistent
`TradeIdentityLimits::default`. These were repaired using existing public
re-exports, completed modules, scoped cancellation, and Source initialization
limits. No behavioral assertion or existing test was weakened. All executed
Router behavioral suites passed after compilation. No baseline runtime test
failure or unresolved regression was observed. Shared fixture dead-code warnings
remain; they do not affect execution. Test runtimes are not throughput benchmarks.

## Changed paths and commits

Implementation commit: `756d03a` — bounded Router, runtime publication authority,
Source supervisor integration, unit tests, and API/operational documentation.

Integration commit: `42d5b17` — dedicated Cargo `live_router` target, production
pipeline/contract tests, and full Live verification integration.

This report is recorded in a subsequent local documentation commit; its identity
is available in the final Git log. All implementation/test changes were committed.
The remaining working-tree changes are the pre-existing planning edits/untracked
planning directory listed above.

Changed implementation paths:

- `engine/src/live/stream/router/{mod,event,facade,source,tests}.rs`
- `engine/src/live/stream/registry/{mod,runtime}.rs`
- `engine/src/live/stream/source/supervisor.rs`
- `engine/src/live/stream/mod.rs`
- `engine/tests/live/integration/router/{main,harness,contracts,pipeline}.rs`
- `engine/Cargo.toml`
- `scripts/verify-live.sh`
- `docs/live/stream-router.md`
- This evidence report.

## Limitations and stopping point

Delivery is volatile; there is no durable replay, end-consumer acknowledgment,
automatic missed-event recovery, or late-subscriber snapshot cache. Consumers
must handle continuity failures and wait for authoritative reinitialization.
Direct publishers may retry rejected admission while retaining their input;
Source currently fails closed on callback errors, including transient `Busy`.
The immutable durable graph and queue limits are fixed for a Router lifetime.
The shared demand mutex requires short registry operations without Router
reentry. Future Derived producer bindings require their own approved output
identity contract; no derived computation or adapter-specific execution exists.

Deterministic local pipeline success does not certify current public exchange
connectivity or establish historical completeness. Database opt-in tests were
not repeated without write authorization; no PostgreSQL persistence contracts
were changed. No performance/latency benchmark was claimed.

No unresolved architectural decision blocks this milestone's implemented scope.
Derived Streams, Spectra, Unix sockets, PostgreSQL market-data recording, Axum,
Ratatui, and Historical changes remain excluded. No next implementation package
is authorized by this report. The Router is **Ready for Review**, not Accepted.
