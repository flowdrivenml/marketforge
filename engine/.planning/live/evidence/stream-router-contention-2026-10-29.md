# Router Source publication contention correction

Status: **Ready for Review** for the requested reliability correction.
Milestone acceptance remains with the user. Branch: `codex/stream-router`.
The Source checkpoint `333cc76` and Router milestone commits remain intact.

## Independently confirmed production failure

The original `ProducerHandle::try_publish` rejected a contended Router state
mutex or runtime publication-authority mutex immediately with `PublishError::Busy`.
Both Source adapters called it and converted every admission error into `String`.
This was a transient synchronization failure, not evidence of a full ingress,
shutdown, or revoked producer.

For ordinary WebSocket trades, the complete failure chain is:

1. `SourceRuntimeSupervisor::start_routed` installs `publish_source_trades`.
2. A native WebSocket trade reaches `SourceStreamExecutor::process_message`,
   `SourceStreamRunner::data`, and `SourceProcessor::process_websocket`.
3. `StreamIntegrityHandle::process_trades_with` and the bound trade ingress invoke
   the synchronous publication callback before committing accepted identities and
   ordering baseline. The Router's immediate `Busy` becomes a callback error.
4. The processor wraps it as `SourceProcessorError::Handoff`; the runner/executor
   propagate it. `handle_depth_buffer_failure` does not handle trade handoff errors.
5. `SourceStreamExecutor::run` exits its processing loop, stops the runner, cancels
   recovery and detaches its WebSocket attachment. It preserves the original error.
6. `SourceRuntimeSupervisor::reap_finished` marks the runtime `Failed`; dropping
   its producer also fences Router consumers. No trade admission was acknowledged.

Depth's `complete_handoff` similarly marks the publication fence ResetRequired
and returns a handoff error. Ordinary WebSocket handoff failures propagate out;
REST-completion paths may attempt the existing publication reset/recovery protocol.
Neither provides a general reason to treat ordinary Router lock contention as a
terminal source or integrity failure.

This was reproduced through the actual production executor and supervisor using
the original immediate-publication callback style, for each lock independently.
Assertions verify the specific Busy error, Failed lifecycle, zero admissions,
transport detachment, and cleanup. This test remains as a witness of why direct
`try_publish` cannot be used as an infallible Source handoff.

## Can normal operations cause contention?

Yes, without a slow market-data consumer or exhausted ingress:

| Operation | Relevant locking |
| --- | --- |
| Dispatch | Router state, authority, consumer queues; overflow cleanup also demand |
| Subscribe | Router state while acquiring demand |
| Close/drop subscription | Router state and consumer queue while releasing demand |
| Router status | Router state while inspecting consumer queues |
| Receive | Router state and authority while extracting queued data |
| Producer attach/retire/shutdown | Router state; retirement releases demand |
| Runtime lease inspection/revocation | Authority guard |
| Idle revocation observation | Authority guard |

A deterministic test holds the application's demand mutex while an ordinary
`SubscriptionHandle::close` reaches demand release. The actual close operation
owns Router state at that point. A real WebSocket trade then witnesses Router
state contention. Releasing demand allows admission, and the Source stays Running.
This also demonstrates why dispatch or status/control activity can expose a
healthy producer to a transient critical-section convoy.

## Small correction and preserved guarantees

The public nonblocking `try_publish` contract is unchanged: contention still
returns `Busy` immediately, with no publication admitted. Source adapters instead
use a private `publish_for_source` path that shares the **same admission body**.

`RouterConfig::source_lock_timeout` is a lock-acquisition budget, default **50 ms**,
validated to be greater than zero and at most **one second**. One monotonic deadline
is computed for the entire state-plus-authority acquisition. It is not reset
between locks. Only these two private mutexes changed to `parking_lot::Mutex` to
support timed acquisition without polling or a retry loop. The already-locked
`parking_lot 0.12.5` dependency is now directly enabled by Live; no dependency
versions or transitive packages changed. Context demand and consumer queue
contracts remain unchanged.

After both locks are acquired, all existing identity, shutdown, generation,
capacity, payload, and sequence checks run, followed by exactly one complete
atomic ingress insertion. No error after acquisition is retried. A contention
deadline returns the distinct `PublishError::ContentionTimeout` without admission.
Capacity exhaustion, shutdown, and revoked authority retain their own errors.
The previous terminal-depth-cleanup behavior is unchanged.

Consequently:

- There are no extra publication tasks, queues, background retries, or deep payload
  clones for retries. Waiting uses the mutex's timed parking operation.
- Admission still linearizes at complete ingress insertion under both guards.
- Per-stream ordering, atomic batches, generation fencing, and capacity accounting
  retain the same implementation.
- Source publication acknowledgment remains synchronous. Source cannot advance
  integrity merely because it started waiting for a lock.
- A successful callback acknowledges one accepted publication; it cannot publish
  duplicates via retry. The trade test replays the same native identity after
  success and verifies that the next distinct trade gets sequence 2.
- A lock held beyond the budget still fails closed explicitly. The correction
  tolerates bounded transient contention; it does not promise healthy operation
  during an indefinitely stalled critical section.
- A synchronous callback may occupy its Tokio worker for the configured acquisition
  budget. This is bounded lock waiting, not waiting for consumer processing or free
  ingress capacity. OS scheduling is not a hard real-time guarantee.

Source executors, normalization, integrity, recovery, registration, persistence,
Historical code, and other subsystems were not changed.

## Deterministic tests

New tests live in `engine/tests/live/integration/router/contention.rs`. They are
compiled as a private Router unit module so they can hold the real locks without
adding public test hooks or exposing locks through the facade. The pipeline reuses
existing Source/local-server fixtures and executes the actual WebSocket manager,
Source builder/executor/supervisor, normalization, integrity, Router, and Rust
consumers. It does not replace Source or Router with mock implementations.

Seven production-path tests cover:

1. Original immediate Busy failure, independently for both mutexes.
2. Trade Source survives state-lock contention.
3. Trade Source survives authority-lock contention.
4. Depth Source survives state-lock contention.
5. Depth Source survives authority-lock contention.
6. Prolonged state/authority contention produces explicit timeout, Failed Source,
   zero admissions, terminal consumer fencing, and complete cleanup.
7. Actual subscription demand cleanup causes contention and the Source survives.

Success cases assert exact sequence and trade identity/depth boundary, fanout to
both consumers, readiness/Running lifecycle, duplicate trade suppression, and
cleanup. Bounded `cfg(test)` observation channels signal an actually failed immediate
lock attempt or actual timeout; guards release in response to those events.
There are no arbitrary sleeps or timing guesses used to create contention.
Two additional unit cases validate timeout configuration and rejection after an
elapsed acquisition deadline without consuming sequence or capacity.

An initial cleanup test tried inspecting the consumer queue while cleanup still
owned that queue mutex and was waiting for demand. Its test-control thread timed
out. The harness was corrected to observe the demand-release boundary through a
bounded test-only signal, then release demand. No production assertion or existing
test was weakened. All final suites passed. Reused fixture dead-code warnings
remain observational only.

## Verification

All final commands exited **0**, ran sequentially, and used Live features only.

| Command | Result |
| --- | --- |
| `cargo check --manifest-path engine/Cargo.toml --offline --no-default-features --features live --lib` | Passed; updated only the root package's dependency edge in Cargo.lock |
| `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --lib live::stream::router -- --nocapture` | **21 passed**, 0 failed/ignored, including all seven production contention tests |
| `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --test live_router -- --nocapture` | **27 passed**, 0 failed/ignored |
| `scripts/verify-live-source.sh quick` | Check passed; **124 Source unit tests passed**, 0 failed/ignored |
| `scripts/verify-live-source.sh integration` | **38 passed**, 0 failed, 1 existing PostgreSQL case ignored |
| `scripts/verify-live.sh full` | **15 gates passed**, 0 failed; **2548 tests passed**, 5 existing ignored database cases |
| `git diff --check` | Passed |
| `git merge-base --is-ancestor 333cc76 HEAD` | Passed |

Full Live includes library **2038 passed / 4 ignored**, WebSocket 69, REST 48,
normalization 9, integrity 83, configuration 14, metrics 3, exchange contracts 149,
Recovery 63, Source **38 passed / 1 ignored**, Router 27, and doctests 7.
Formatting, Live library check, and network-target compilation also passed.
No public exchange request or PostgreSQL write was made.

Logs:

- `engine/target/live-verification/router-contention/unit-and-pipeline.log`
- `engine/target/live-verification/router-contention/demand-cleanup.log`
- `engine/target/live-verification/router-contention/router-integration.log`
- `engine/target/live-verification/router-contention/source-quick.log`
- `engine/target/live-verification/router-contention/source-integration.log`
- `engine/target/live-verification/router-contention/full.log`
- `engine/target/live-verification/20261029T224751Z-389923/` — full gate logs
- `engine/target/live-verification/source-20261029T224656Z-388790/` — focused quick logs
- `engine/target/live-verification/source-20261029T224724Z-389243/` — Source integration log

## Changed paths and Git

- `engine/src/live/stream/router/{facade,source,tests}.rs`
- `engine/src/live/stream/registry/runtime.rs`
- `engine/tests/live/integration/router/contention.rs`
- `engine/Cargo.toml` and `engine/Cargo.lock`
- `docs/live/stream-router.md`
- This report.

Committed locally as one reliability correction; its hash is reported in the
final Git log. Pre-existing edits to `.planning/live/AGENTS.md` and
`.planning/live/AUTONOMOUS_PLAN.md`, plus untracked `engine/.planning/`, are
preserved. No push, merge, reset, history rewrite, or unrelated change was made.
No remaining blocker is known for the requested correction. It does not mark the
milestone Accepted or authorize any next implementation package.
