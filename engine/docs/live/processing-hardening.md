# Live processing runtime

The processing subsystem is constructed independently of the MarketForge application
runtime. It uses the restored `StreamRuntimeRegistry` and the existing `StreamRouter`;
it does not own another stream registry, dependency graph, demand registry or fan-out.
PostgreSQL remains a required Live feature. Tests restore definitions in memory and
perform no PostgreSQL writes or exchange connections.

## Ownership and public API

`live::infrastructure::processing` exports:

- `ProcessingPool`: fixed CPU worker startup and explicit `shutdown_until(Instant)`.
- `ProcessingHandle`: lightweight shared status, buffer budget, failure observation
  and exact-generation failure acknowledgment. Dropping a handle never shuts down workers.
- `Processor`: synchronous stateful execution; `try_run_turn` adds returned operational
  errors while preserving the existing `run_turn` interface. Implementations process
  at most the supplied number of complete input units and return true when another
  turn is needed; CPU progress and turn limits remain cooperative.
- `ProcessorInstance`, `ProcessorIngress`: ownership and typed admission for one exact
  registration. `start_with_handle` avoids retaining the worker owner. The legacy
  `start(Arc<ProcessingPool>, ProcessorToken, ...)` forwards to this implementation.
  The supplied token contributes the logical ID; the pool allocates a fresh generation.
  Always use `instance.token()` or `ingress.token()` for the resulting identity.
- `ProcessorMailbox`, `WorkBatch`, `MailboxLimits`, `MailboxStatus`, `BufferBudget`,
  `BufferStatus`, `StopOutcome`, `ShutdownOutcome`, and retained failure/status types.
- `ProcessingScheduler` and opaque, consumed `ExecutionLease` capabilities.

Raw pool registration, notification and token-based retirement are internal test
scaffolding, not a production admission API. Managed callers cannot pair arbitrary
mailbox callbacks with raw notifications. Standalone mailbox operations remain
available, but `push`/`push_and_notify` reject managed mailboxes, and `take_batch`
is forbidden on them. Managed processors use `take_work` and explicitly complete it.

`live::stream::processing` exports the owning facade:

- `ProcessingService::new(&RuntimeConfig, Arc<Mutex<StreamRuntimeRegistry>>,
  Arc<StreamRouter>)`, `handle`, and asynchronous, retryable `shutdown`.
- Cloneable `ProcessingServiceHandle`: `start(StartProcessor)`, `stop`,
  `stop_generation`, `status`, `wait_phase`, `wait_terminal`, and `take_report`.
- `StartProcessor`, `ProcessorContext`, `PreparedProcessor`, and `ProcessingStreamHandle`.
- `ProcessingInput`, `ProcessingOutput`, typed `OutputRejectionKind` and ownership-returning
  `OutputRejection`; association phases, terminal dispositions and `ProcessingReport`.

The future application runtime should restore its graph/registry, construct its Router,
then construct and own one service before accepting processing commands. It must keep
Tokio and the Router alive through service shutdown. A future processor factory receives
managed input and bounded output handles, constructs its state on Tokio's blocking
executor, and returns a processor plus an asynchronous readiness future. CPU turns run
only on the dedicated CPU workers. Readiness may await a signal from controlled Router
startup input; public ingress is returned only after readiness and registry confirmation.
Readiness futures must cooperate with asynchronous cancellation and must not block Tokio.

The compatibility `DerivedProcessorController` and failure adapter now live at the
stream boundary, with the original processing paths re-exported. Their synchronous
convenience methods are for standalone callers/tests; the service owns production
lifecycle serialization, cancellation and deadlines. Do not run synchronous convenience
startup/stop under a shared registry mutex or on a Tokio worker.

## Worker and association ownership

Each pool starts N CPU workers and one supervisor thread. The supervisor owns all worker
JoinHandles until they return, even if a deadline expires or the facade is dropped.
Partial thread-start failure stops and joins already-started threads before returning
`PoolError::WorkerStartup`. A transfer cell preserves the handles if supervisor creation
itself fails. Owner destruction requests shutdown; it never performs a blocking join.
Worker/supervisor thread identity prevents joining the current runtime thread.

One Tokio actor owns stream associations and lifecycle operations. The command queue,
active/starting/cleaning associations, retained reports, and factory/readiness futures
are explicitly bounded. No thread is created per stream, failure or retirement request.
Factory construction uses the existing Tokio blocking executor, with at most one
outstanding construction/readiness operation per association. Retirement is requested
without waiting and observed through retained state changes.

An association retains its stream ID, original registry generation, current operation
generation, unique processing registration, input mailbox, output fence/queue, Router
subscription and producer, readiness/construction operation, replies and terminal report.
The registry alone determines authoritative stream lifecycle and publication permission.

Canceling a startup future sets an exact-operation cancellation flag and wakes the actor;
it cannot orphan a pending instance. Canceling a stop/shutdown await leaves the actor
owning cleanup. Unexpected service-owner destruction marks abnormal teardown and fails
matching stream operations. A task-destruction guard also fences matching generations
if the actor itself is destroyed before cleanup completes. In that abnormal case,
worker supervision still owns unfinished CPU cleanup; status must not be read as a
clean completed lifecycle. Normal application shutdown must await the service before
tearing down Tokio.

## Admission, identities and lock order

Managed admission takes pool state, the registration's input/publication fence, then
mailbox state and the aggregate budget. It verifies the exact registration capability,
checks/reserves the entire event, inserts it, and notifies the scheduler before releasing
pool state. Registration reserves activation capacity, so that notification cannot fail
because another instance used its slot. Workers cannot acquire a turn between insertion
and scheduling. Closure uses the same pool/fence boundary: a racing event is accepted
before closure and retained, or rejected afterward with its original ownership returned.

Registration identities are allocated monotonically and are distinct from registry
runtime generations. Old ingress, retirement and acknowledgment messages cannot become
authoritative after reusing a logical ID. An execution lease is private, non-Copy and
non-Clone; completion consumes it. It cannot be replayed into another turn. Its scheduler
identity also rejects a capability from another scheduler with matching processor labels.
Compile-fail tests verify both replay and construction restrictions.

Pool locks never enclose processor turns, factories, user callbacks, blocking publication
or processor/payload destruction. Idle/ready retirement moves processor ownership to a
bounded disposal queue; CPU workers destroy it outside locks. Executing processors are
also destroyed outside locks before settlement is announced. Shutdown's supervisor moves
remaining objects out of pool containers and destroys them outside locks. Destruction
keeps a registration reservation until completion, and destructor panics are retained
failures rather than successful retirement.

Input closure outside a pool operation releases its mailbox/fence guards before
requesting retirement; it never holds a mailbox lock while acquiring pool state.
Tracked batch settlement takes mailbox state and then budget, never pool state. Output
uses the input/publication fence, output mailbox/budget, then releases mailbox/budget
locks before the Router attempt. It never acquires pool state. Registry operations
release registry access before resource cleanup, CPU observation or joining.
The short final readiness commit takes registry access before the admission fence
and invokes no processor code; it refuses to return a usable handle for a closed
registration. When registry and processing locks must coexist, registry access
precedes pool/fence access. No processing operation acquires the registry from a
held pool/fence lock. Construction, readiness futures and retirement/join waits
remain outside registry access. These are mutex-based operations, not lock-free
guarantees.

## Lifecycle phases

Startup:

1. Reserve an association slot and call `begin_start` under brief registry access.
2. Outside registry access, allocate bounded mailboxes, attach the explicit authorized
   Router producer and subscribe to the required upstream.
3. Construct/register the processor on the blocking executor; contain factory panics.
4. Admit only controlled upstream startup deliveries while awaiting readiness. Outputs
   may be retained locally but are not published during Starting.
5. Recheck the Router subscription observer for terminal or queued unsynchronized
   input, confirm the same registry generation with `mark_running`, then return
   public ingress. A read-only terminal observer also wakes startup cleanup while
   construction is still in progress, without consuming an upstream delivery.

Returned errors, panics, cancellation and supersession fence output/input and retain
cleanup. Pending factories cannot be forcibly interrupted; they remain owned and appear
as unfinished startup/cleanup. A late factory result is retired or rejected after runtime
shutdown. Readiness cancellation completes before releasing the association reservation.
No stale completion can mark a newer stream Running, Failed or Stopped.

Stop:

1. Begin the registry stop operation and revoke publication under registry access.
2. Release registry access; fence admission/output, detach input and request retirement.
3. Finish an already executing cooperative turn, cancel subsequent turns, and observe
   destruction/settlement. This is cancellation of queued work, not a full drain.
4. Finalize only the matching operation generation. A failure during the turn or its
   destruction produces Failed, including when it occurs during retirement.

Ordinary processor errors and panics automatically close the exact registration and
publication fence. A bounded watch channel carries coalesced changes; authoritative
failures remain in retained registration state and the bounded pool failure map. The
service revokes registry publication, retains work in its terminal report, then
acknowledges the pool failure. The exact registration independently retains its failure,
so earlier lifecycle acknowledgment cannot break `ProcessorInstance::stop`. Repeated
stop also retains the failed disposition while returning no duplicate queued events.
Unexpected worker exit is surfaced to supervision, fences registrations and fails the
service's associations instead of silently reducing the configured worker count.

## Accounted work and configuration compatibility

`ready_queue_capacity` still bounds ready plus running activations. Registration now
reserves that capacity even while idle. Pool registration, disposal and unacknowledged
failure reservations together cannot exceed it. The service uses the same number as
its association/report limit and command-queue capacity. Reports retain an association
slot until `take_report`; there is no unbounded failure history under fresh IDs.

Two optional centralized ProcessingConfig fields were added:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `buffered_events` | 65,536 | Aggregate retained accounting units |
| `buffered_bytes` | 67,108,864 | Aggregate retained payload-byte policy |

Both must be positive. Omitted fields use these defaults, and default-valued fields
are omitted on serialization so existing TOML files retain their field set. Explicit
nondefault values serialize and round-trip. Worker count and turn budget retain their
existing validation. Mailbox limits are supplied per processor and are subject to the
shared aggregate budget; a smaller aggregate budget deliberately rejects earlier.
Shutdown uses the existing positive `runtime.shutdown_timeout_seconds`. No second
loader, worker resizing or hot reload was added.

A unit is an entire input delivery or output publication, not an individual trade or
depth level. Byte charges use Router's retained-byte convention plus the input wrapper
and source identity. This includes variable depth arrays and canonical envelopes.
Sharing Arc data does not waive a charge; simultaneous input/output ownership may be
charged conservatively twice. These are accounting limits, not exact process RSS limits
or limits on arbitrary processor state, allocations or user-created clones.

Queued input, tracked in-flight input, pending output and retained terminal batches
remain charged while held. Batch payload destructors run before their reservation is
released. `complete` marks successful CPU processing; dropping an unfinished batch,
including unwinding after mutation, marks it indeterminate. It is never blindly replayed.
`abandon` records explicitly canceled work. The standalone instance StopOutcome
transfers queued events directly to its caller and records that transfer as abandoned
processing work; the service instead keeps batches charged in its report. Terminal-report batches have separate
retained-terminal counters and are not mislabeled as executing work. Taking a report
releases the association slot but does not release its batch charges; its recipient must
discard/process/drop those batches. Aggregate completion, abandonment, rejection and
indeterminate counters survive instance/mailbox destruction.

A Router delivery handed to the adapter is one transient complete event, separately
bounded by Router publication limits. It is immediately admitted or explicitly discarded
with stream/sequence/generation/byte metadata in the failed terminal report; it is never
put into an unbounded intermediary. Processor-created output rejected before local
admission is returned to the processor with a typed reason and rejection accounting.
Capacity/size rejection also retains an output failure marker and fences the common
admission gate even if processor code ignores the Result; the service fails the stream.

Status exposes configured/started/running/exited workers, unexpected exits, active
workers/executing processors, registrations/ready/retiring reservations, turns/errors/
panics/destructor panics, queued/in-flight/retained counts and bytes, per-association
pending outputs, rejected admissions, pending starts/cleanup, terminal reports and
shutdown progress. These Rust snapshots are the integration point for the existing
metrics/control mechanisms; no monitoring server or TUI is introduced.

## Router contract and output custody

`StreamRouter::attach_derived_producer(authority, input_stream_id)` is an explicit,
minimal identity-preserving producer origin. It requires exactly one declared direct
Source dependency in the restored derived definition, validates the exact definition
and registry authority, and retains the canonical Source's envelope identity. Routing
and delivery runtime generation belong to the derived binding. The ordinary Source
attachment and its schema/continuity validation remain in place.

This supports synthetic canonical transformations without inventing a general derived
financial schema. Multi-input Merge/Convert publication schemas and their algorithms
remain separate milestones; unsupported origins are rejected explicitly. Complete depth
snapshots/deltas and source/book generations pass through unchanged. An unsynchronized
publication, invalidation, EOF or Router continuity-loss notification fails processing;
it cannot drop depth updates and continue as an apparently valid stream.

Output is first accepted into a bounded local queue. The actor then uses the existing
Router producer with a fresh authority check at each admission. Router errors, including
Busy, capacity and revocation, are conservatively terminal in v1. The original publication
is returned to retained output, not silently lost or regenerated by re-executing mutated
processor state. Successful Router acceptance releases only the local reservation and
increments the custody counter. It does not establish subscriber execution or persistence.
Publication fencing cannot undo outputs already observed by a subscriber; those outputs
follow the Router's existing revocation/custody contract.

## Shutdown limits

Shutdown rejects new commands/admissions/registrations, fences all associations, cancels
future turns and retains queued/unpublished work in reports. Joining happens on the
blocking executor, with a configured observation deadline. A timeout returns
`complete: false` and unfinished worker/processor counts, while the actor and supervisor
retain ownership. A later call observes completion without an idempotency race.

CPU turns and payload destructors are cooperative Rust execution. A thread which never
returns cannot safely be force-killed. A deadline reports incomplete cleanup; it does
not prove a clean shutdown or release state still in use. The application's eventual
process-termination decision is outside this subsystem. Panic recovery assumes unwinding;
`panic=abort`, process crashes, production load and arbitrary processor RSS are not
verified by this milestone.

## Change scope

Continuation baseline: `main` at `5b50bd5` (the preceding hardening commit); original
milestone baseline: `e2263d8`. Related uncommitted startup/retirement implementation and
tests in controller.rs, instance.rs, pool.rs and registry/runtime.rs were preserved,
adapted and verified with this implementation. No unrelated files were included.

Changed production paths:

- `engine/src/live/config/general/{model,validation}.rs`
- `engine/src/live/infrastructure/processing/{controller,instance,lifecycle,mailbox,mod,pool,scheduler}.rs`
- New `engine/src/live/infrastructure/processing/regressions.rs`
- `engine/src/live/stream/mod.rs`
- New `engine/src/live/stream/processing/{mod,controller,lifecycle,pipeline,service}.rs`
- `engine/src/live/stream/router/{facade,tests}.rs` (the latter is a fixture-field adaptation)
- `engine/src/live/stream/registry/runtime.rs` (test adaptations/related existing tests)

Verification paths: `engine/Cargo.toml`,
`engine/tests/live/integration/config/processing.rs`, new
`engine/tests/live/integration/processing/main.rs`, and this document.

## Verification

The final command results and selected test counts follow below. The new integration
target exercises the actual Router, registry, worker pool, tracked mailbox and authorized
producer, including readiness data, cancellation, output/input saturation, ignored output
rejection, failure, stale generations, complete depth events and repeated cleanup cycles.
The core suite also tests partial worker startup, unexpected worker exit, self-join
prevention, destructor reentry/panic, reservation saturation and terminal memory charges.

An intermediate campaign caught a TOML serialization regression (22 configuration tests
passed, one failed). Default-field omission fixed it without weakening the assertion.
During development a queued-retirement test also exposed an ordering deadlock: it waited
for destruction while withholding the only worker. That run was terminated; the test now
requests retirement, verifies fencing and releases the worker, with bounded mock waits.

Clippy, slow Historical tests, benchmarks, production workloads, database-write tests and
public exchange tests were NOT RUN. No environment/dependency blocker was encountered.

Final verification: **PASS**, 168 tests passed, 0 failed, 0 ignored in the selected
executed tests; all three Cargo checks passed. Each command below exited **0**.
**FAIL:** none remaining. **BLOCKED:** none.

| Command | Result | Executed tests | Exit |
| --- | --- | ---: | ---: |
| `cargo check --manifest-path engine/Cargo.toml --locked --no-default-features` | PASS | — | 0 |
| `cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features live` | PASS | — | 0 |
| `cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features historical` | PASS | — | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --lib live::infrastructure::processing` | PASS | 51 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --lib live::stream::registry::runtime` | PASS | 22 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_config` | PASS | 24 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_router` | PASS | 27 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_processing` | PASS | 19 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --doc ExecutionLease` | PASS | 2 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_source pipeline::publication_capacity_failure_terminates_without_false_readiness` | PASS | 1 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_source depth::failed_depth_invalidation_is_not_reported_as_clean_shutdown` | PASS | 1 | 0 |
| `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --lib live::stream::router` | PASS | 21 | 0 |

The lease command executed two compile-fail tests; its separate ordinary-doctest
runner selected zero tests and is not counted as verification. The other filters
all selected and executed tests. Existing dead-code warnings were left unchanged.
Adding the Router terminal observer required updating one private Consumer test
fixture; the initial lib-test compile error was fixed by supplying its Notify,
without changing assertions. The affected lib commands and Router unit suite were
then executed successfully.

Implementation commit: `283601d51fa44c358cf89c2143e3a9be794d6817`.
Readiness/idempotency follow-up: `267f204532d097c7914a2db76b9efaed48107c14`.
The follow-up adds a closed-readiness regression and a repeated-failed-stop assertion;
Live check, processing, registry and processing integration checks were rerun successfully.
No push, merge, branch change, reset, rebase or dependency update was performed.

Exact implementation commit file inventory:

```text
engine/Cargo.toml
engine/src/live/config/general/model.rs
engine/src/live/config/general/validation.rs
engine/src/live/infrastructure/processing/controller.rs
engine/src/live/infrastructure/processing/instance.rs
engine/src/live/infrastructure/processing/lifecycle.rs
engine/src/live/infrastructure/processing/mailbox.rs
engine/src/live/infrastructure/processing/mod.rs
engine/src/live/infrastructure/processing/pool.rs
engine/src/live/infrastructure/processing/regressions.rs
engine/src/live/infrastructure/processing/scheduler.rs
engine/src/live/stream/mod.rs
engine/src/live/stream/processing/controller.rs
engine/src/live/stream/processing/lifecycle.rs
engine/src/live/stream/processing/mod.rs
engine/src/live/stream/processing/pipeline.rs
engine/src/live/stream/processing/service.rs
engine/src/live/stream/registry/runtime.rs
engine/src/live/stream/router/facade.rs
engine/src/live/stream/router/tests.rs
engine/tests/live/integration/config/processing.rs
engine/tests/live/integration/processing/main.rs
engine/docs/live/processing-hardening.md (documentation commit)
```
