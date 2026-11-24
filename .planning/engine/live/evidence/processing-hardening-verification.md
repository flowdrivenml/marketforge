# Live processing hardening verification

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
