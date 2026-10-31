# Processing hardening status

This is a partial hardening change, not completion of the Live processing runtime.

## Implemented contracts

`ProcessingScheduler::register` reserves an activation slot even while idle.
The existing `ready_queue_capacity` configuration continues to bound ready plus
running activations and now also limits registered instances. At the pool
boundary, registered instances plus retained failures cannot exceed this limit.
No TOML field or default changed. This deliberately rejects excess registrations
earlier than the previous implementation.

`ExecutionLease` has private fields, is neither Copy nor Clone, and is consumed
by `complete`. `processor()` exposes only the registration token. A scheduler
identity prevents passing a lease from another scheduler with matching tokens.
The compiler prevents safe callers from replaying a consumed lease. Logical
processor tokens still come from callers; preventing exact-token reuse across
registration lifetimes remains unfinished.

`PoolError::WorkerStartup` reports OS thread creation errors. Partial startup
closes the shared scheduler, wakes and joins already created workers, then
returns the error. No processors have been registered during this phase.
The failure injection test for this branch has not been implemented.

`ProcessorInstance::stop` now rechecks the exact failure token following a
successful retirement wait, preserving a panic that occurred during the wait.
This does not repair acknowledgment before cleanup.

## Ownership and boundaries still requiring implementation

The existing pool owns worker joins, while instances and ingress retain an Arc
of that same owner. A separate runtime owner and lightweight handles are still
required before application integration. Drop can still block on pool shutdown.
Do not construct this subsystem as a completed application runtime service.

The current three-phase controller fences registry publication, retires outside
the registry operation, then finalizes the operation generation. It is still a
manually driven helper, not an owning service. Automatic failure observation,
cancellation-safe association retention, readiness outside the registry lock,
and abnormal destruction reporting remain unimplemented.

Managed admission currently takes the mailbox mutex before the pool mutex.
The lifecycle race between notification and admission is not yet solved.
Processor destructors can still execute under the pool lock on removal; this
must be fixed before declaring the lock-order/destruction contract complete.

Mailbox limits account only queued payload bytes supplied by the caller, not
in-flight batches or pending outputs. They do not bound RSS. Complete-event
admission exists, but no reusable Router ingress/output adapters were added.
Router acceptance establishes custody only, not subscriber completion or
persistence. Authority rechecks and bounded pending output remain required in
the processing adapter.

CPU turns remain cooperative. Shutdown joins running threads without a deadline;
a processor that does not return can block shutdown. There is no force-kill
mechanism and no successful-shutdown guarantee for stuck processors.

## Verification

All commands used `--manifest-path engine/Cargo.toml --locked --no-default-features`.
Each completed with exit status 0:

- `cargo check` (no feature).
- `cargo check --features live`.
- `cargo check --features historical`.
- `cargo test --features live --lib live::infrastructure::processing`: 38 passed.
- `cargo test --features live --lib live::stream::registry::runtime`: 22 passed.
- `cargo test --features live --test live_config`: 21 passed.
- `cargo test --features live --test live_router`: 27 passed.

No dependency/environment blockers were encountered. Existing dead-code warnings
were left unchanged. Clippy, slow Historical tests, benchmarks, production data
workloads, exchange connections, and database writes were not run. No new
processing integration target was created. End-to-end synthetic derived
processing and the requested adversarial lifecycle campaign are NOT VERIFIED.

Starting branch: main. Baseline: e2263d8.
Existing uncommitted edits in controller.rs, instance.rs, pool.rs, and
stream/registry/runtime.rs were preserved and excluded from the hardening commit.
Only the new retirement failure recheck in instance.rs belongs to this change.
