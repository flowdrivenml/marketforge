# WP-10 — Canonical router final contract audit

**Status: ReadyForReview.** Date: 2026-10-21. Not Accepted.
Scope: the router component behind its frozen producer/destination ports.
Baseline: `4438d65`, branch `codex/wp06-completion`. This report supersedes the
chronological ledger; its previous receipts remain in Git at that checkpoint.

## Authority and disposition

Document 17, WP-10 requires ordered admission, registered destinations, policy
isolation, bounded resources, immutable sharing, correlated outcomes, cursor
integration, cancellation, partial fan-out, finite shutdown and native observations.
The matrix below covers every requirement and acceptance sentence in that entry.
DEC-016–018 and DEC-024–029 preserve canonical identity, grouping and provenance;
no financial, epoch, wire or durability guarantee was changed.

Document 17 §4.4 explicitly allows a component to work against deterministic
transport before the complete daemon exists. WP-10's real admission, registry,
queue, worker, tracker, reporting, shutdown and metrics owners are composed here;
only replaceable external adapters are deterministic test implementations.
The external work below retains its original roadmap ownership. No mandatory
WP-10 criterion remains blocked. This is component readiness, not verification
of a daemon, exchange, PostgreSQL, socket publisher or historical recovery.

## Requirement → implementation → verification matrix

Code paths below are relative to `engine/src/live/`. Test locations are relative
to `engine/tests/live/`: **C** = `router_runtime.rs` with function prefix
`composed_router_`; **R** = `router.rs`; **O** = `router_obligations.rs`;
**Q** = `routing.rs`; **W** = `destination_worker.rs`; **P** = `reporting.rs`.
All cited tests execute production router code in the registered `live_suite`.
“Verified” means no missing router-owned functionality for the listed criterion.

| Authoritative WP-10 requirement | Production implementation | Verifying tests | Outcome / remaining ownership |
|---|---|---|---|
| Per-stream ordered admission and cursor integration | `routing/{cursor,namespaces,ordered,ingress}.rs`; authoritative registered frontier, one preparation per namespace, contiguous offsets, checked exhaustion | C:`concurrent_producer_clones_share_one_frontier`; O:`ordered_frontier_drop_gap_namespace_and_exhaustion`, `registered_namespace_routes_without_resetting_frontier` | Verified; incoming cursors are preserved, never allocated/replaced by routing. Initial identities/epochs come from WP-05/06 and lifecycle composition. |
| Registered destinations and coherent membership changes | `routing/router.rs` commitment fence, revision checks, active/retired instance ownership; `RouterRuntime::{register_destination,unregister_destination}` | C:`destination_replacement_drains_original_instance_and_rejects_stale_completion`; R:`registry_retirement_bounds_repeated_same_id_instances_and_releases_each_charge`; `router_admission.rs::registry_revision_change_invalidates_prepared_fanout` | Verified; removed instances keep old charges/work. Persisted intent and reconciliation are WP-09/24, not membership policy invented inside the router. |
| Required / best-effort / explicit durability policies | `routing/{router,acknowledgement,port}.rs`; typed policy/stage validation; optional attempts excluded from required obligations | C:`recording_durability_requires_separate_exact_completion`; O:`router_excludes_best_effort_from_required_obligations`; Q:`registration_rejects_durable_publication`, `required_recording_cannot_finalize_while_queued` | Verified; queue acceptance is not Delivered/Committed/Durable. Actual storage guarantees remain adapter-owned. |
| Slow or failed best-effort subscribers cannot block unrelated sinks | Bounded round-robin `DestinationWorkers`, separate `SupervisionStep::BestEffort`, charged runtime optional-outcome ownership and worker-slot reservation | C:`stalled_optional_adapter_preserves_required_progress`, `optional_admission_outcomes_never_poison_required_correlation`, `registration_reserves_scheduler_slots_for_independent_sinks`; O:`optional_fanout_isolates_full_and_closed_queues_and_preserves_identity` | Verified for pending, full, closed, rejected and malformed optional responses. Optional outcomes never enter required-obligation correlation. |
| Bounded event/byte accounting, including waits and transferred ownership | `routing/{destination,queue,ordered,tracking_recovery,ingress_reporting}.rs`; finite destination/namespace/worker/tracker/journal limits; retained payloads keep original queue permits | Q:`event_and_byte_limits_are_independent`, `waiter_capacity_is_enforced`, `concurrent_admission_never_exceeds_capacity`; O:`bounded_ordered_admission_dropped_wait_releases_global_and_destination_capacity`; C:`dropped_and_shutdown_cancelled_waits_release_every_precommit_charge` | Verified. Runtime recovery/optional collections are bounded by charged live/retired queue capacities; slow external consumers exert explicit pressure. |
| Immutable batch sharing and accepted identity preservation | `ports/routing.rs::EventBatch`, shared canonical event Arcs; `routing/queue.rs` owns permits without rewriting payloads | C:`owned_port_delivery_completion_reporting_and_metrics`; Q:`queue_accepts_and_preserves_event_identity`; `router_commitment.rs::commitment_preserves_original_event_identity` | Verified, including Arc identity. Normalization/economic validation and restart-safe cursor production remain WP-05/06/12/16/17 responsibilities. |
| Result correlation and complete ordered destination trace | `routing/{worker,completion,acknowledgement,driver,runtime}.rs`; exact destination, cursor range, policy and separate completion checks | C:`owned_port_delivery_completion_reporting_and_metrics`, `recording_durability_requires_separate_exact_completion`, `destination_replacement_drains_original_instance_and_rejects_stale_completion`; W:`retained_delivery_rejects_bad_correlation_and_preserves_capacity` | Verified; stale/wrong-range reports cannot fulfill another obligation. Admission is not completion. |
| Partial fan-out must not become required success or rewind identity | `OrderedFanout`, tracked commitment/recovery, `IngressOutcomes`, conservative `RouterPort` receipt; public `RoutingAdmission` documentation makes non-rollback explicit | R:`router_port_partial_required_commit_never_returns_success_or_rewinds_identity`; O:`recovery_aware_partial_fanout_preserves_success_and_terminal_failure`, `ordered_fanout_partial_failure_advances_frontier_and_retains_payload` | Verified; detailed original outcomes remain owned even when the binary producer receipt is Rejected. No replacement cursor or blind replay is authorized. |
| Cancellation and admission deadlines | Context checks before waits, after waits and inside commitment fence; RAII rollback; dropped owned-port future cancels uncommitted preparation | C:`dropped_and_shutdown_cancelled_waits_release_every_precommit_charge`, `no_subscribers_preserves_order_and_expired_admission_has_no_effect`; R:`router_port_cancellation_deadline_and_empty_membership_do_not_commit`; O:`ordered_fanout_rechecks_deadline_inside_commitment_fence` | Verified; precommit charges/frontier are restored, accepted work is not revoked. |
| No subscribers / disabled optional sinks | `RouterRegistry::plan` rejects empty membership explicitly; only registered adapters are called, optional-only attempts report individual drops | C:`no_subscribers_preserves_order_and_expired_admission_has_no_effect`, `owned_port_delivery_completion_reporting_and_metrics` (publication only); R:`router_port_optional_overload_is_retained_as_attempt_only` | Verified. No destinations produces NoRequiredCapacity without commitment/frontier advance. An absent recording sink is not constructed or contacted. Database-free daemon startup is WP-24. |
| Required failures stay visible and retain recovery responsibility | Runtime failed-payload collection; terminal journal and ingress reporting are independent; charged explicit transfer and authenticated restoration to the original tracker | C:`failed_delivery_payload_survives_reporting_and_recovery_transfer`, `recovery_return_after_shutdown_keeps_identity_and_completes_once`, `recovery_return_rejects_full_and_foreign_owner_without_losing_charge`; R:`ingress_reporting_success_cannot_release_owned_tracking_repairs` | Verified. Full/foreign recovery returns preserve the payload. Uncorrelated/ambiguous results fail closed and remain charged for explicit intervention; the router never guesses a mapping or repairs history. |
| Outcome reporting and recovery acceptance boundary | Existing `IngressReporter`, `FinalizationReporter`, bounded retry/escalation, leased journal and `BoundedResolutionSink` receipt handshake; runtime exposes prepare/commit/retry without inventing progress | C:`operational_resolution_requires_sink_acceptance_and_keeps_failed_payload`; R:`tracking_ingress_reporter_integrates_charged_consumer_and_driver`; P:`failed_reporting_schedules_one_progress_turn_without_spinning`, `registered_reporting_preserves_escalation_when_retry_timer_is_unavailable` | Verified. Accepting an operational failure report never proves market-data repair; actual consumers/orchestration belong to WP-18/24/25. |
| Finite shutdown and cancellation preserve unresolved obligations | `routing/shutdown.rs` closes admission before deadline checks; registry reconciles unclaimed queues before driver closure; `RouterRuntime::shutdown`; Drop fences surviving producers | C:`deadline_retains_unclaimed_queues_and_resumes_shutdown`, `recovery_return_after_shutdown_keeps_identity_and_completes_once`, `owner_drop_fences_surviving_producer_handles`; R:`complete_router_shutdown_preserves_independent_reporting_ownership`; W:`delivery_driver_shutdown_retains_empty_preparation_and_in_flight_recovery_capacity` | Verified. DeadlineReached retains owners; Drained requires closed admission and every local charge/obligation cleared. Owner destruction is abandonment, never successful drain or crash durability. |
| Native Control-facing snapshots and router metrics already exist | Owned `RouterSnapshot`, namespace/delivery/ingress/complete drain snapshots; `RouterMetricSampler` with real `ScopedMetricsPort`; measurement update does not change accounting | C:`owned_port_delivery_completion_reporting_and_metrics`, `namespace_retirement_and_measurement_update_preserve_mandatory_state`; O:`router_snapshot_distinguishes_queue_depth_from_reserved_and_inflight_capacity`; R:`router_observation_requires_closed_admission_and_has_no_runtime_effects` | Verified. Observations do not drive integrity. Control transport/projection and daemon metric scheduling are WP-25/24. |
| Correct composition behind frozen ports, with one owner per mutable resource | Owned `RouterHandle: EventRouterPort`, existing `RouterPort`/`RouterIngress`, one delivery mutex, validated resource transfer including exact worker queue ownership | C:`owned_port_delivery_completion_reporting_and_metrics`, `invalid_resource_transfer_returns_queued_responsibility`, `resource_transfer_fences_equal_descriptor_foreign_worker_queues` | Verified. No parallel tracker/router, database-aware admission or competing scheduler was introduced. |

## External integration boundaries (not implemented by this audit)

Ownership follows Document 17's original package entries, not the previous
WP-10 diary's broad “remaining integration” wording. These are still required
for the overall v1 service, but do not move into WP-10 to close its component gate.

| Owning package and authoritative responsibility | WP-10 boundary and deterministic witness | Still unverified externally |
|---|---|---|
| WP-09: “Publish only committed desired state”; runtime execution remains the reconciler's responsibility | Registry register/remove revision fencing and explicit native measurement boundary. C:`destination_replacement_drains_original_instance_and_rejects_stale_completion`; C:`namespace_retirement_and_measurement_update_preserve_mandatory_state` | Full semantic compiler/configuration service, durable desired-state acceptance. WP-10 neither accepts configuration revisions nor treats candidates as committed. |
| WP-18: typed incidents, bounded recovery orchestration, partial outcomes and persistent coverage reporting | Original charged `RetainedDelivery`, failed payload and ingress/blocked-outcome transfers; return path authenticated to original tracker; reporting/escalation receipts. C recovery-return and operational-resolution tests above | Real repair scheduling, incident policy, restored-guarantee/coverage evidence. A failure acknowledgement or local transfer is not recovered historical coverage. |
| WP-20: RAM/Parquet retention, spill, replay and crash recovery | `EventDestinationPort`, shared `EventBatch`, DestinationKind::Retention and correlated Committed/Durable stages. Q:`retention_accepts_durable_commitment`; O:`router_obligations_preserve_destination_policies` | Actual Parquet/manifest/replay durability and retention consumers. |
| WP-21: optional PostgreSQL transactions, idempotent retry, ambiguous commit and coverage | Frozen destination descriptor/admission/completion contract; C:`recording_durability_requires_separate_exact_completion` exercises actual router stage enforcement | PostgreSQL execution, SQL correctness, timestamp mapping and real commit/coverage recovery. No database adapter or connection is fabricated. |
| WP-22: Unix publication/session/protocol lifecycle and slow-client policy | Same frozen destination port with Publication stages; C optional stall/error isolation and exact Delivered completion tests | Real sockets, framing, replay handoff and end-consumer behavior. Delivered is not consumer-processed. |
| WP-24: daemon construction, task supervision, committed reconciliation, per-resource revisions and finite service shutdown | Owned Arc-compatible producer; one bounded `drive` turn; `register_destination`/`unregister_destination`, namespace registration/retirement, metric scheduling, explicit completion and shutdown methods | Executable daemon/task adapters, startup, signals, accepted-to-applied configuration projection and real consumer scheduling. Caller supplies committed persistent updates; transient registrations remain independent. |
| WP-25: authorized Control dispatch, snapshots, operation tracking and client transport | Read-only native snapshots plus explicit retry/resolution commands using existing journal/sink tokens. C operational-resolution test rejects a foreign acceptance sink | Actual management authorization/audit/JSON transport and global snapshot projection. Native methods are not automatically external commands. |

Earlier WP-05 persistent identity/economic metadata, WP-06 C06 SQL verification,
and WP-07 C12 adapter-selection gates retain their existing statuses. They do not
block this in-process component's verified contracts. Real later consumers must
meet their own prerequisites and reopen WP-10 if integration exposes a defect.
No downstream work package was started or declared prerequisite-complete here.

## Changes made in this audit

- Completed recovery return through the same pending collection/supervisor/driver.
  Rejected returns preserve the original owner and charge. Opaque tracker identity
  prevents equal numeric IDs/ranges in another router from accepting a foreign
  delivery. No market cursor, generation, serialization or financial type changed.
- Validated transferred worker queues against exact active/retired registrations,
  preserving all input resources on failure rather than accepting a competing owner.
- Exposed existing namespace retirement, metric scheduling and terminal-resolution
  operations through the owned runtime; delegated to their existing authoritative
  implementations. Fenced surviving producers when their runtime owner is dropped.
- Clarified the existing partial-fan-out meaning of Rejected on the frozen producer
  receipt without adding variants or changing wire semantics. Added ten composed
  contract tests (18 total), including concurrent producer clones and failure paths.

Changed paths: `engine/src/live/routing/{acknowledgement,completion,driver,mod,
router,runtime,supervisor,worker}.rs`, `engine/src/live/ports/routing.rs`,
`engine/tests/live/router_runtime.rs`, STATUS.md and this report.

## Verification and limits

Cargo commands execute from `engine/`, locked/offline. No zero-test result is
counted as verification. Focused composed runs passed at 14, 16, 17 and finally
18 tests as the audit witnesses were added; no behavioral test failures occurred.

| Exact command | Exit / executed result | Receipt |
|---|---|---|
| `cargo test --locked --offline --no-default-features --features live --test live_suite composed_router` | 0; 18 passed | Final focused tool receipt; included in full suite below |
| `./scripts/verify-live.sh full` (repository root) | 0; five gates pass: formatting, live check, 137 library tests, 510 integration tests / 2 opt-in ignored, 1 doctest | `engine/target/live-verification/20261021T112625Z-2/` |
| `cargo check --locked --offline --no-default-features --features live,historical` | 0 | `/tmp/wp10-final-audit-combined.log` |
| `cargo check --locked --offline --no-default-features --features live,monitor-tui` | 0 | `/tmp/wp10-final-audit-tui.log` |
| `cargo check --locked --offline --no-default-features` | 0 | `/tmp/wp10-final-audit-base.log` |
| `./scripts/verify-live.sh historical` (repository root) | wrapper 1, Cargo 101; 151 passed / 1 baseline failure | `engine/target/live-verification/20261021T112456Z-2/` |
| `git diff --check` | 0 before checkpoint | Local checkpoint review |

The full wrapper executes `cargo fmt --all -- --check`,
`cargo check --locked --offline --no-default-features --features live --lib`, and
`cargo test --locked --offline --no-default-features --features live` with the
explicit targets `--lib`, `--test live_suite`, and `--doc`.
Historical baseline failure remains
`historical::merge::resources::tests::rejects_empty_streams`, at
`src/historical/merge/execution/shared/resources.rs:180`; historical execution
source is unchanged and feature-isolated. Existing dead-code warnings remain.
The two ignored opt-in PostgreSQL/performance tests were not run. No SQL,
exchange requests, benchmark, soak, deployment or crash-durability certification.

No Python/historical execution, dependencies, Cargo.lock, ignored configuration,
production data or accepted decisions changed. Pre-existing `.gitignore` edits,
untracked launcher and exposed target artifacts were preserved and excluded from
staging. One coherent local audit checkpoint; no push. Only the user may Accept.
**Next authorized work: none in this task; stop after this audit.**
