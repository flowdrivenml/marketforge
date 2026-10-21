# WP-10 cross-package responsibility reconciliation

Date: 2026-10-21. Documentation-only scope. Baseline: `d631b4d`, branch
`codex/wp06-completion`. **WP-10: ReadyForReview — Not Accepted.**
The [component audit](WP-10-implementation.md) remains the authoritative WP-10
implementation/verification record; this report does not replace or rerun it.

## Ownership findings and action matrix

Document 17 already includes every external responsibility identified by the
WP-10 audit. The missing planning detail was the named consumer interfaces,
explicit binding witnesses and completion boundaries, not a new work package.
The [execution plan](../AUTONOMOUS_PLAN.md#wp-10-consumer-integration-obligations)
now records required behavior, reuse, remaining work, acceptance tests and
prerequisites for each owner. Existing scope/acceptance criteria are retained.
No working code is moved and no external integration is marked implemented.

Interface paths below are relative to `engine/src/live/`. `RT` means
`routing/runtime.rs::RouterRuntime`; `D` means the frozen
`ports/routing.rs::{EventDestinationPort,EventBatch,DestinationDescriptor,
DestinationAdmission,DestinationCompletion}`. `C:name` means existing test
`engine/tests/live/router_runtime.rs::composed_router_name`.
Existing witnesses establish only the router boundary; the future tests in the
last column remain required acceptance work for the named consumer owner.

| Requirement / existing owning package | Authoritative source and original coverage | Exact interface to consume/reuse | Explicit integration obligation / required future acceptance test |
|---|---|---|---|
| Committed configuration acceptance — **WP-09**; application — **WP-24** | Document 17 WP-09 already requires persisted desired revisions, no premature effects and boundary tests; WP-24 owns accepted-to-applied execution. Doc 03 §§13–14 and Doc 13 §7 make that split explicit. | `ports/configuration.rs::{ConfigurationCompilerPort,ConfigurationStorePort,ConfigurationApplyPort}`. WP-09 hands plans/revisions to the apply port; its WP-24 implementation consumes `RT::{register_destination,unregister_destination,schedule_metrics}` and registry revision/instance fencing. No direct configuration-to-router dependency is required. | Finish semantic compilation/service, not just candidates/journal. Add persistence-fault/revision-conflict tests with zero premature router calls and unaffected-resource preservation; WP-24 tests real applied/adopted destination and measurement boundaries. Existing C:`destination_replacement_drains_original_instance_and_rejects_stale_completion` and C:`namespace_retirement_and_measurement_update_preserve_mandatory_state` are boundary witnesses only. |
| Failure/recovery responsibility — **WP-18**; local consumer wiring — **WP-24**; authorized manual dispatch — **WP-25** | Document 17 WP-18 already requires incidents, bounded orchestration, partial outcomes and coverage; Docs 07 §§12,20,27 preserve storage-evidence ownership and partial history. Docs 01 §7 / 13 §19 and DEC-031 keep wiring, authorization and local operational history separate. | `routing/ingress_reporting.rs::{IngressReporter,IngressRecord}`, `routing/reporting.rs::FinalizationReporter`; `RT::{take_ingress_for_recovery,take_failed_payload,take_delivery_for_recovery,restore_delivery_from_recovery,take_blocked_for_recovery,reconcile_unresolved_tracking}`; typed `RetainedDelivery`, `FinalizedDelivery`, `WorkerOutcome`; `ports/recovery.rs::RecoveryPort` is orchestration, not a payload-transfer receipt. | Bind typed failures to real bounded policy, retain every transferred charge, restore against the original tracker, keep ambiguous outcomes explicit and identify which guarantee/history remains incomplete. Add composed incident/retry/escalation/supersession and partial/unrecoverable-history traces. Existing C:`recovery_return_rejects_full_and_foreign_owner_without_losing_charge`, C:`recovery_return_after_shutdown_keeps_identity_and_completes_once` prove the boundary, not actual repair. |
| Real retention destination — **WP-20** | Document 17 WP-20 already includes RAM/Parquet, manifests, durability distinctions, protection/replay and real-file fault tests; Doc 09 §§14,38 and DEC-034 agree. | D with Retention kind and declared policy; `RT::apply_completion`; `ports/retention.rs::{SegmentStoragePort,RetentionReaderPort,RetentionControlPort}` remain retention-owned. Existing `routing.rs::retention_accepts_durable_commitment` and policy tests cover router stage acceptance. | Feed the actual router into real retention and replay. Test grouped-event identity, bounded RAM/disk, file/manifest publication faults, protected replay/restart, concurrent capture/handoff and deadline charges; withhold storage completion on failure. Retention owns durable evidence; recovery coordination does not invent it. |
| Real PostgreSQL destination — **WP-21** | Document 17 WP-21 already requires SQL atomicity, exact values, ambiguous commits, coverage, disabled recording and isolated database tests; Doc 11 §§15,17,35,39 and DEC-031 agree. | D with Recording kind; `RT::apply_completion`; `ports/recording.rs::RecordingBackendPort`; WP-11 demand and WP-20 replay/protection ports for catch-up. Existing C:`recording_durability_requires_separate_exact_completion` verifies that Committed cannot satisfy Durable. | Bind the actual recorder/SQL backend, reconcile unknown commits before claiming progress, preserve contiguous coverage and independent publication. Test the composed router with isolated real PostgreSQL, exact timestamps/decimals, grouped parent/levels, retries/restart/outage/protected cleanup and no connection while disabled. C06 mapping/fault, C12 adapter and test-isolation gates still apply; no migration authorization is inferred. |
| Real Unix publisher — **WP-22** | Document 17 WP-22 already requires local sockets, slow-client isolation, replay/L2 handoff and finite shutdown; Doc 08 §§22,36,46 and DEC-030 agree. | D with Publication kind; `RT::{apply_completion,take_optional_outcome}` and registered/retired queue ownership; WP-20 replay/protection and WP-17 book-cut ports. Existing C:`stalled_optional_adapter_preserves_required_progress` and C:`optional_admission_outcomes_never_poison_required_correlation` cover router isolation. | Bind real sessions and per-subscriber routing without duplicating the canonical router. Exercise actual local sockets with fragmented/invalid frames, stalled/disconnected peers and unrelated healthy sinks; verify ordering/generations, precise gaps, protected replay/book handoff and finite shutdown. Delivered/socket-write evidence is not consumer-processed; optional SQL is not a prerequisite. |
| Executable daemon, reconciliation and local reporting/metrics — **WP-24** | Document 17 WP-24 already requires construction, independent supervision, committed updates, revisions, startup and truthful shutdown. Doc 01 §7, Doc 03 §13, Doc 13 §§7–8,12 and roadmap §13.2 require incremental composition before final daemon completion. | `routing/runtime.rs::{RouterResources::validate,RouterServices,RouterHandle,RouterTurn,RouterStop}`; `RT::{producer,drive,register_namespace,retire_namespace,register_destination,unregister_destination,schedule_metrics,apply_completion,snapshot,shutdown}` and charged transfer APIs above; `routing/metrics.rs::RouterMetricSampler`, scoped metrics, reporter traits and `ports/lifecycle.rs::{TaskSpawnerPort,OwnerLifecyclePort,ShutdownReport}`. | Bind bounded real task/timer/report consumers and available sinks; translate persisted plans through `ConfigurationApplyPort`, preserving independent demand and desired/applied/adopted revisions. Test headless restart/disabled services, stale revision/replacement/worker failures, bounded report scheduling and deadline unfinished-work reports. Diagnostic drop/Disabled is not acceptance of market-data recovery responsibility; task join is not drained obligations. Existing C:`owned_port_delivery_completion_reporting_and_metrics` and C:`deadline_retains_unclaimed_queues_and_resumes_shutdown` are component witnesses, not a daemon. |
| Authorized Control transport and projections — **WP-25** | Document 17 WP-25 already requires authorization, dispatch to owners, operations, idempotency, freshness and slow-client isolation; Doc 13 §§6–8,19 and DEC-033 agree. Internal methods do not automatically become approved wire commands. | `ports/control.rs::{ManagementPort,ManagementClientPort}` and approved WP-07 command/schema types; native `RT::snapshot` / scoped metrics; supported actions delegate to `RT::{retry_ingress,retry_terminal_report,prepare_terminal_resolution,commit_terminal_resolution}` using `routing/resolution.rs::{BoundedResolutionSink,ResolutionAcceptanceReceipt}` and original journal tokens. | Bind actual server/client authorization/audit and owner operations, without mutable-lock access from clients. Test read-only denial, stale revisions, disconnect/retry idempotency, foreign/stale receipts and fresh snapshots while data delivery continues; reject unsupported commands explicitly. Existing C:`operational_resolution_requires_sink_acceptance_and_keeps_failed_payload` proves local acceptance cannot erase the original failure or certify payload repair. |

## Prerequisites and sequence validation

No package is reordered, redefined or marked Accepted. Relevant dependencies are
recorded per package in the plan; WP-05/06/07/09 partial/open gates retain their
existing statuses. Document 17 §4.4 permits component witnesses before a daemon;
§13.2 already specifies incremental composition and deliberate substitutions.

- WP-09 uses the compiler/store/apply contracts and validated inputs. It does not
  need the concrete WP-24 daemon to validate/persist a plan. C01/metadata/compiler
  ports can be exercised against verified deterministic fixtures before concrete
  WP-12 capability binding; unknown economic/exchange inputs remain fail-closed.
  This does not certify an unfinished semantic compiler or configuration service.
- WP-18 tests real coordination through existing backend/processor ports. Later
  real retention/SQL recovery evidence is completed with WP-20/21, not fabricated
  or made a backwards prerequisite for all incident orchestration.
- WP-20/21/22 consume the existing router and their earlier domain/demand/book/
  retention contracts. They need not wait for the final WP-24 daemon. Real sink
  binding evidence belongs to those package gates and the after-WP-22 checkpoint.
- WP-24 completes the concrete composition after owner implementations exist;
  WP-25 adds actual management transport. Local report acceptance must function
  without a Control client being online. WP-19 collector sinks remain deliberate
  acquisition witnesses, not replacements for the later storage/publisher owners.

The scoped consumer graph has no cycle: WP-10 → WP-18/20/21/22/24/25;
WP-09 → WP-24; WP-18/20/21/22 → WP-24; WP-24 → WP-25. These arrows describe
consumer integration, not a new exhaustive prerequisite graph or an instruction
to bypass other gates. In particular there is no WP-24 → WP-09/WP-10 edge.
No new architectural conflict was found in these seven ownership boundaries.

A pre-existing planning inconsistency remains outside them: the plan/roadmap
WP-27 exporter title refers to Prometheus, while DEC-032 excludes that stack and
STATUS.md names native observability integration. DEC-032 controls; this report
adds no exporter requirement and leaves that package unchanged. Reconcile its
labels/scope explicitly before WP-27 execution rather than silently inferring a
new monitoring requirement from a lower-priority title.

## Validation, status and checkpoint

Updated only AUTONOMOUS_PLAN.md, STATUS.md and this requested report. The plan
labels its old baseline historical and records the current documentation-only
assignment/stopping point. STATUS.md retains all older implementation notes but
marks old WP-10 and later historical architecture/workflow snapshots superseded.
Current WP-10 status remains **ReadyForReview — Not Accepted** throughout current
planning; the component report/audit evidence is preserved unchanged.

Validation is documentation-only: source/interface/test-symbol and local-link
checks, a scoped dependency-cycle check, `git diff --check`, and a staged-path
allowlist check. The read-only validation script exited 0: seven owner packets/status obligations,
seven local links, 21 runtime methods, 32 interface symbols and 11 existing
witness references matched source. Its scoped graph check passed for eight nodes
and 12 edges, and confirmed the approved sequence plus implementation/authority/
audit files were unchanged. `git diff --check` exited 0 before staging.
No Cargo tests were run or newly claimed; existing WP-10 receipts remain in its
component report. No Rust, Python, historical execution, schema,
exchange configuration, dependencies, accepted decisions or Document 17 changes.
Pre-existing `.gitignore`, launcher and generated artifacts are preserved and
excluded from staging. One local planning checkpoint contains these three files;
no push. Stop after reconciliation; no implementation package was started.
