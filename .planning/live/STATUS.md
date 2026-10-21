# MarketForge Live — Implementation Status

## Work Package Status

| Package | Scope | Status | Evidence |
|---|---|---|---|
| WP-00 | Establish the actual baseline | Accepted | WP-00A-baseline.md, WP-00B-feature-isolation.md |
| WP-01 | Create the module skeleton and working records | Superseded | — |
| WP-02 | Make shared depth/domain models available safely | Accepted | WP-02A-depth-event-sharing.md, WP-02B-trade-sharing.md |
| WP-03 | Extract compatible storage, arithmetic and UI primitives | Accepted | WP-03A-DWP-03B-parquet-contract.md, WP-03D-shared-parquet-primitives.md, WP-03E-shared-trade-builder.md |
| WP-04 | Finalize dependency and feature wiring | Accepted | WP-04D.md — feature/build verification |
| WP-05 | Implement domain identity, lifecycle and policy types | Partial — identity/metadata contracts verified; removed-ID lifecycle/history scope unresolved | [Implementation matrix](evidence/WP-05-implementation.md) |
| WP-06 | Freeze and implement event/delivery/replay contracts | Partial — basic real C06 witness passes; signed timestamp and fault gates remain | [Independent contracts](evidence/wp06-independent-contracts.md), [Observed C06 mapping and blockers](evidence/wp06-c06-connected.md) |
| WP-07 | Freeze service ports, management and observation surfaces | Contract implementation review pending; C12 adapter decision OPEN | [Contract implementation and witnesses](evidence/wp07-contracts.md) |
| WP-08 | Implement foundations and real native observability | ReadyForReview | [Implementation ledger](evidence/WP-08-implementation.md) |
| WP-09 | Implement the Configuration System | Partial — compiler/store/service composed; mandatory schema/economic/impact gaps remain | [Implementation ledger](evidence/WP-09-implementation.md) |
| WP-10 | Implement the canonical event router | ReadyForReview — Not Accepted | [Implementation ledger](evidence/WP-10-implementation.md) |
| WP-11 | Implement source registry and dependency ownership | Pending | — |
| WP-12 | Implement exchange-profile and format compilation | Pending | — |
| WP-13 | Implement WebSocket Connection Manager | Pending | — |
| WP-14 | Implement REST client and shared request scheduling | Pending | — |
| WP-15 | Implement Subscription Manager | Pending | — |
| WP-16 | Implement trade processing and continuity | Pending | — |
| WP-17 | Implement L2 synchronization and processing | Pending | — |
| WP-18 | Implement Recovery Coordinator | Pending | — |
| WP-19 | Verify the complete acquisition slice | Pending | — |
| WP-20 | Implement Live Retention completely | Pending | — |
| WP-21 | Implement PostgreSQL Recording completely | Pending | — |
| WP-22 | Implement Unix market-data publication completely | Pending | — |
| WP-23 | Implement derived streams completely | Pending | — |
| WP-24 | Finalize the production runtime and reconciler | Pending | — |
| WP-25 | Implement Control server and shared Control client | Pending | — |
| WP-26 | Implement the independent Ratatui Live client | Pending | — |
| WP-27 | Verify internal metrics, diagnostics, and observability integration | Pending | No Prometheus/Grafana |
| WP-28 | Verify the complete intended exchange matrix | Pending | — |
| WP-29 | Hardening, reproducibility and release | Pending | — |

## Current Development Position

### WP-05 authoritative documentation reconciliation — 2026-10-21

Documentation-only reconciliation records user-approved DEC-035–038 and updates
DEC-014/OPEN-001 to distinguish resolved catalog authority/classification from
missing inputs and downstream integration. OPEN-006–011 explicitly preserve
history-pruning/non-reuse, identical reintroduction, allocation, historical
bindings, activation freshness and concrete read-only SQL adapter ownership.
The latter package is **unassigned**: earlier WP-05 evidence suggesting WP-12
concrete adapter ownership is superseded by the current user-approved boundary.
WP-12 still owns catalog/protocol integration; WP-07 service ports, WP-09
compilation/durable identity enforcement, WP-11 operational reconstruction,
WP-23 derived semantic compilation/execution and WP-24 reconciliation are retained.
Earlier WP-09 notes about unresolved metadata authority/family mechanism are
historical: that architectural choice is resolved, while actual port/adapter
adoption and remaining acceptance functionality are still incomplete.

The five architecture documents, decision register and execution plan now
separate accepted architecture, verified checkpoint `63f2526` contracts and pending
integration. [Reconciliation report](evidence/wp05-documentation-reconciliation.md)
records changed sections, contradictions and validation. **WP-05 remains Partial,
not ReadyForReview or Accepted.** No implementation/test/schema/configuration/
dependency changes or new Rust test results are claimed. Preserve the existing
config-validator blank line and untracked runner script. One documentation-only
local checkpoint; no push. **Stop after reconciliation; no next package authorized.**
The earlier implementation assignment notes below are historical context.


### WP-05 identity and metadata contracts — 2026-10-21

**WP-05 remains Partial — Not Accepted.** The authoritative
[requirement/code/test matrix](evidence/WP-05-implementation.md) supersedes earlier
WP-05 reconciliation notes. Implemented immutable persistent identity evidence
from committed configuration revisions, canonical key/dedup and retained-history
reassignment checks, pure pre-commit validation, derived semantic generation
boundaries, and a typed read-only full-tuple metadata resolver with deterministic
snapshot fixtures. Metadata reuses existing financial types, preserves exact
values/provenance, and separates validity from normalization economic needs.
Explicit perpetual families now validate; absent families remain unresolved.

The user reaffirmed Option A and the unresolved permanent-removal/reintroduction
policy. No identity database/journal or permanent tombstones were introduced.
Current/retained mappings are verified; bounded history cannot prove bindings
before its missing parent. Known reintroduction is reported, not authorized.
This unresolved architectural decision keeps WP-05 Partial; its cursor/retained
history/replay/restart consequences are explicit in the evidence report.

WP-07 port binding, WP-09 durable enforcement/metadata adoption, WP-11 operational
registration/demand, WP-12 production metadata/protocol adapter integration,
WP-23 derived execution and WP-24 reconciliation remain their owners' work.
No PostgreSQL adapter/database access or production integration was fabricated.
Accepted decisions and historical instrument/catalog/job code are unchanged.
Existing FileRevisionJournal/FileEpochStore remain their respective authorities.
No frozen consumer implementation required changes; only test registration and
WP-05 models/exports were changed. Full Live verification exits 0:
137 library / 542 integration passed (2 ignored) / 1 doctest. Nineteen focused
WP-05 tests, 2 generation/cursor and 18 FileEpochStore tests pass. Live+historical,
Live+TUI and base-library compilation exit 0. Exact commands and final receipts
`engine/target/live-verification/20261021T125212Z-2/` are in the report.

Preserve/exclude the pre-existing config validator blank line and untracked
runner script. Local checkpoint only, no push. **Current scope and stopping point:
WP-05 only. No next package is authorized.** The older assignment notes below
are historical context and do not supersede this scope.


### WP-09 configuration implementation — 2026-10-21

**WP-09 remains Partial — Not Accepted.** The current authoritative
[requirement/code/test matrix](evidence/WP-09-implementation.md) supersedes earlier
candidate-only status. Implemented bounded canonical TOML/static loading,
inheritance provenance, fail-closed catalog/protocol compiler, semantic diffs and
supported source impacts, the frozen store adapter on a dedicated bounded worker,
and a management service for durable acceptance/history/query/restore/status.
The existing FileRevisionJournal remains the sole revision/persistence owner.
Only successful durable CAS produces a hand-off; the service cannot mutate live
owners and never claims runtime application/adoption. WP-24 executes plans.

Mandatory gaps: authoritative draft-to-DTO mapping/defaults; C01/OPEN-001 metadata
provisioning/classification; native/static and pure derived-economic compiler
inputs; derived impact closure/resource feasibility; frozen diff expressiveness
for global recovery/shutdown and profile removal. Exact requirements and owning
dependencies are recorded in the matrix; these are not reassigned to make WP-09
complete. Unsupported/unknown inputs fail explicitly. No real exchange or daemon
integration is claimed. WP-10 remains ReadyForReview — Not Accepted.

Full Live verification exits 0: 137 library / 523 integration passed (2 opt-in
ignored) / 1 doctest; final receipts `engine/target/live-verification/20261021T121532Z-2/`.
Live+historical, Live+TUI, base and historical-only library builds exit 0.
Nineteen focused WP-09 tests pass. Exact commands are in the report. One coherent
local implementation checkpoint includes this evidence; no push, DB access,
Python/historical edits, accepted-decision or ignored-configuration changes.
The pre-existing untracked autonomous-runner script is excluded.
**Current scope/stopping point: WP-09 only. No next package is authorized.**

The completed assignments below are historical context; their stopping/scope
statements do not supersede the current WP-09 assignment.



### Cross-package responsibility reconciliation — 2026-10-21

WP-10 has one current disposition: **ReadyForReview — Not Accepted**, supported
by audit checkpoint `d631b4d` and
[WP-10-implementation.md](evidence/WP-10-implementation.md). This documentation
reconciliation neither reruns that audit nor changes its acceptance status.
The [cross-package matrix](evidence/wp10-cross-package-reconciliation.md) traces
all seven external boundaries to their existing Document 17 owners and exact
interfaces. The action ledger, including required behavior, reuse, remaining work,
acceptance tests, prerequisites and completion boundaries, is in
[AUTONOMOUS_PLAN.md — WP-10 consumer integration obligations](AUTONOMOUS_PLAN.md#wp-10-consumer-integration-obligations).

| Owning package | Outstanding consumer obligation (all unverified) | Completion evidence still required |
|---|---|---|
| WP-09 (Partial) | Complete semantic acceptance/persistence and impact plans; hand committed revisions to the apply port, never mutate the router from a candidate | Invalid/failed persistence has zero runtime effects, correct revision conflicts and affected boundaries; actual application belongs to WP-24 |
| WP-18 (Pending) | Bind typed failures/charged ownership to incident and recovery policy; preserve original tracker on return | Real router-to-recovery transfer/retry/escalation and partial-coverage tests; actual backend repair verified when its owner exists |
| WP-20 (Pending) | Implement retention destination, RAM/Parquet/protection/replay and separately evidenced completion stages | Router-to-real-retention persistence, file/manifest faults, protected replay/restart and deadline accounting |
| WP-21 (Pending) | Implement recorder destination, real transactions, ambiguous outcomes, coverage and retention catch-up | Router-to-isolated-real-PostgreSQL tests; no Committed/Durable inflation; disabled recording creates no connection |
| WP-22 (Pending) | Implement publisher/session queues, local socket transport and replay/book handoff behind the destination port | Router-to-real-local-socket slow/disconnected peer isolation, ordering, generation fencing, handoff and finite shutdown |
| WP-24 (Pending) | Bind the existing router, actual adapters/report consumers/tasks, committed destination reconciliation and native observations | Headless composed service, stale-revision/owner failures, independent demand, disabled services and truthful unfinished-work shutdown reports |
| WP-25 (Pending) | Bind approved authorized Control commands and fresh projections to actual owners; preserve native receipt/token semantics | Real server/client authorization, correlation/idempotency, stale/foreign receipt, snapshot freshness and slow-client isolation tests |

These are pending integrations, not missing router-owned implementation and not
new completed packages. The approved sequence is unchanged; early components use
frozen port witnesses, real adjacent owners are composed as available, WP-24
finishes daemon wiring and WP-25 adds its transport. No dependency on a final
Control server is imposed on earlier owners. C01/C06/C12 and other recorded
prerequisite gates are retained, not waived. The known WP-27 title inconsistency
is recorded in the report with DEC-032 controlling; it is outside these seven
boundaries and no exporter work is authorized here.

Documentation-only validation: ownership/source/interface/test-symbol references,
links, a scoped dependency-cycle check and diff/scope checks. No new Rust/SQL/
exchange tests or implementation verification is claimed. Pre-existing .gitignore
and untracked artifacts are preserved. One local planning checkpoint, no push.
**Stop after this reconciliation. No implementation package is started.**


### WP-10 final contract audit — 2026-10-21

**WP-10: ReadyForReview, not Accepted.** The authoritative
[requirement-to-code-to-test matrix](evidence/WP-10-implementation.md) covers every
WP-10 requirement and acceptance criterion. Ten added composed tests (18 total)
verify recovery return after shutdown, full/foreign-owner rejection with charges
preserved, exact transferred-worker queue ownership, concurrent producer ordering,
dropped/cancelled waits, explicit durability, destination replacement/stale results,
operational resolution receipts, namespace/measurement boundaries and owner loss.
Recovery returns use the same tracker; no competing mutable owner was introduced.

Full Live verification exits 0: 137 library / 510 integration passed
(2 opt-in ignored) / 1 doctest. Receipts:
`engine/target/live-verification/20261021T112625Z-2/`.
Live+historical, Live+TUI and base compilation exit 0. Historical-only baseline
rechecked: 151 passed / 1 failed, `rejects_empty_streams`, unchanged historical
execution; receipts `engine/target/live-verification/20261021T112456Z-2/`.

No mandatory router-owned blocker remains. Persisted configuration acceptance
belongs to WP-09; recovery orchestration to WP-18; concrete retention/SQL/socket
adapters to WP-20/21/22; daemon reconciliation and supervision to WP-24; Control
transport/authorization to WP-25. Their actual integration remains unverified,
with deterministic boundary witnesses documented; no end-to-end service claim.
The earlier WP-05/06/07/09 statuses are unchanged. User retains acceptance.

One coherent local audit checkpoint; no push, database/exchange access, Python or
historical execution edits, dependencies/lockfile or ignored configuration changes.
Pre-existing .gitignore changes and untracked artifacts are preserved/excluded.
That audit ended at checkpoint `d631b4d`; WP-11 was not started. The current
reconciliation assignment and stopping point are stated above.

## Previous campaign notes (historical; superseded by the final audit above)

### Autonomous continuation — 2026-10-21

Recovered interrupted resolution.rs work in checkpoint `824e972`; both new
identity exhaustion tests and 52 resolution integration tests pass. Completed a
comprehensive WP-10 acceptance/dependency audit in the authoritative report.
The owned RouterRuntime now joins existing admission, delivery, ingress/terminal
reporting, metrics and shutdown; its Arc-compatible producer uses the existing
RouterPort. Failed payloads and optional outcomes retain original queue charges
until explicit consumer transfer. Optional worker responses no longer poison
required-obligation correlation. Eight actual composed-router witnesses pass.
Final full Live verification exits 0: 137 library / 500 integration passed
(2 opt-in ignored) / 1 doctest; receipts
`engine/target/live-verification/20261021T110956Z-2/`.
Live+historical, Live+TUI and base compilation exit 0. Historical-only baseline rechecked:
151 passed / 1 failed (`rejects_empty_streams`), unchanged historical source;
receipts `engine/target/live-verification/20261021T110756Z-2/`.
WP-10 remains **Partial**, not ReadyForReview: committed configuration destination
reconciliation and concrete operational reporting/recovery consumers remain;
daemon/Control bindings are later WP-24/25 consumers. WP-05/06/07/09 gates persist.
No new architectural decision is requested. Next authorized work remains WP-10;
WP-11 has not started. See [current acceptance checklist](evidence/WP-10-implementation.md).
No database/exchange access, Python/historical execution edits, dependencies,
lockfile or ignored configuration changes. Untracked launcher preserved; no push.



Checkpoint `0c240eb` contains terminal reporting adapter unwind as permanent
reporting failure, retaining the original outcome for explicit intervention.
The registered production-driver witness now covers twelve error/unwind,
open/closed and timer-admission scenarios, continued independent reporting,
no automatic retry after expiry and explicit reauthorization. Full Live gates
exit 0: formatting, check, 135 library / 492 integration (2 opt-in ignored) /
1 doctest; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T071014Z-3/`.
WP-10 remains Partial and next executable: complete concrete outcome/recovery
policy, runtime lifecycle and Control composition. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, Python/historical execution, dependency/lockfile or ignored
configuration changes. Untracked launcher preserved; no push. See WP-10 evidence.

Checkpoint `3dbf060` prevents ready terminal reports from parking after an earlier
report is deferred or escalated. Each non-idle reporting transition requests one
bounded follow-up turn; Idle does not self-wake. Eight deterministic scenarios
cover temporary/permanent failure, open/closed driver and available/rejected retry
timer admission. Independent ready reports complete while the original failed
record retains its identity and journal charge. Full Live verification exits 0:
formatting, check, 135 library / 492 integration (2 opt-in ignored) / 1 doctest;
Live+historical compilation exits 0. Final receipts:
`engine/target/live-verification/20261021T070749Z-3/`.
WP-10 remains Partial and next executable: concrete outcome/recovery policy,
runtime lifecycle and Control composition remain. WP-05/06/07/09 gates persist;
WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, Python/historical execution edits, dependency/lockfile or
ignored configuration changes. Untracked launcher preserved; no push.
See the WP-10 implementation evidence.



Checkpoint `f962ba2` closes the registered delivery scheduling gap after an
external recovery owner releases payload capacity. The existing capacity owner
retains one replacement-aware observer, armed before driver maintenance and
inspection; active and retired queues notify outside their capacity lock even
after closure. Latest-waker, retirement, preserved obligation and idle-no-spin
witnesses pass. Full Live verification exits 0: formatting, check, 135 library /
491 integration (2 opt-in ignored) / 1 doctest; Live+historical compilation exits
0. Receipts: `engine/target/live-verification/20261021T070346Z-3/`.
The final strengthened focused witness passes 1/1 (both queue cases).
WP-10 remains Partial and next executable: complete concrete outcome/recovery
policy, runtime lifecycle and Control composition remain. WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, Python/historical execution edits, dependency/lockfile or
ignored configuration changes. Untracked launcher preserved; no push. See WP-10
implementation evidence.

Checkpoint `48bb475` fixes parked delivery coordination after explicit recovery
transfer: successful retained or uncorrelated outcome transfers wake one turn;
empty lookups preserve the waiter. The original payload charge and independent
tracking obligation survive transfer, including after closure. Regression failed
before the fix and passes with production behavior. Full Live verification exits
0: formatting, Live check, 135 library / 491 integration (2 opt-in ignored) /
1 doctest; Live+historical compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T070036Z-7/`.
WP-10 remains Partial and next executable: concrete outcome/recovery policy,
runtime lifecycle and Control composition remain. WP-05/06/07/09 gates persist;
WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, Python/historical code, dependency/lockfile, ignored
configuration or launcher changes. No push. See the WP-10 evidence ledger.


Checkpoint `6e10fc5` adds explicit in-place retry of escalated ingress reporting.
The charged original record stays with the coordinator; intervention resets only
its bounded attempt cycle and wakes one turn. Idle/deferred/already-rearmed
coordinators reject intervention without changing accounting or retry deadlines.
Deterministic witnesses cover exhaustion, permanent failure, handler unwind,
closed-journal drain and independent required destination payload ownership.
Full Live verification exits 0: formatting, check, 135 library / 490 integration
passes (2 opt-in ignored) / 1 doctest. Final strengthened scheduler assertions pass
3/3; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T065806Z-2/`.
WP-10 remains Partial and next executable: concrete outcome/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 remain pending. Further autonomous implementation is safe.
Historical-only baseline was not rerun; the prior 151/1 finding remains recorded.
No SQL/exchange access, Python/historical execution, dependency/lockfile, ignored
configuration or launcher changes. No push. See WP-10 evidence.


Checkpoint `ac00c84` fixes ingress-reporting backlog scheduling: each successful
scheduled reporting turn requests one more bounded turn, so prequeued outcomes
cannot stall waiting for unrelated admission/delivery activity. The next idle turn
parks without self-waking. A deterministic real-ingress witness covers open and
closed journals, ordered outcomes, optional queue overflow, no retry timers and
independent destination payload ownership. Full verification exits 0: formatting,
Live check, 135 library / 489 integration passes (2 opt-in ignored) / 1 doctest,
and Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T065506Z-3/`.
Historical-only library baseline rechecked: exit 101, 151 passed / 1 failed,
`historical::merge::resources::tests::rejects_empty_streams`; no historical edits.
WP-10 remains Partial and next executable; full outcome/recovery policy, runtime
lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates persist;
WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, dependency/lockfile, Python/historical execution or ignored
configuration edits. Pre-existing untracked launcher preserved; no push.
See [WP-10 evidence](evidence/WP-10-implementation.md).


Checkpoint `ebea89c` adds delivery-coordinator notifications for producer tracking
handoff and successful explicit unresolved reconciliation, including after closure.
Notifications coalesce through the existing single waiter; failed/unchanged attempts
preserve it. Two strengthened production-interface witnesses cover latest-waker
replacement, mixed foreign/valid repairs, duplicate attempts and rejected correlation.
Full verification exits 0: formatting, Live check, 135 library / 488 integration
passes (2 opt-in ignored) / 1 doctest, plus Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T065151Z-2/`.
WP-10 remains Partial and next executable: complete outcome/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, Python/historical execution, dependencies, ignored
configuration or pre-existing launcher changes; no push. See WP-10 evidence.


Latest implementation checkpoint `10ad5d0` closes a generic ingress-reporting
acknowledgement gap: an unresolved reservation-count mapping now retains its
charged record even when the returned reservation list is empty. Explicit handler
handoff remains necessary. The existing production-consumer witness covers all
three repair/mismatch cases, repeated false acknowledgement, shutdown retention,
and explicit transfer. Full verification exits 0: formatting, Live check,
135 library / 488 integration passes (2 opt-in ignored) / 1 doctest, and
Live+historical check. Receipts:
`engine/target/live-verification/20261021T064838Z-2/`.
WP-10 remains Partial and next executable; concrete outcome/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No database/exchange access, dependency/lockfile, Python/historical execution or
ignored configuration edits. Pre-existing untracked launcher preserved; no push.
See [WP-10 evidence](evidence/WP-10-implementation.md).



Latest implementation checkpoints `d1dd223` and `535fb81` connect producer-owned
tracking repairs to the authoritative delivery owners and expose explicit
obligation-based reconciliation after admission closes. TrackingIngressReporter
performs nonwaiting handoff before calling runtime reporting policy, without
holding the driver lock. Foreign reservations and ambiguous count mappings stay
charged for intervention; successful transfers remain independently owned by
recovery. Four new deterministic witnesses cover ownership, retries, contention,
closure, unknown obligations and independent outstanding delivery obligations.
Final verification exits 0: formatting, Live check, 135 library / 488 integration
passes (2 opt-in ignored) / 1 doctest, and Live+historical check. Receipts:
`engine/target/live-verification/20261021T064550Z-2/`.
WP-10 remains Partial and next executable: complete outcome/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 remain pending. Further autonomous work can safely continue.
No SQL/exchange access, dependency/lockfile, ignored configuration, Python or
historical execution edits. Pre-existing untracked launcher preserved; no push.
See [WP-10 evidence](evidence/WP-10-implementation.md) for commands and limitations.


Latest implementation checkpoint `c4639c0` prevents ingress reporting success
from releasing records that still own unresolved tracking-repair reservations.
Both unresolved-update and reservation-count-mismatch outcomes remain charged
and report permanent failure until explicitly transferred. A deterministic
actual-ingress witness verifies repeated false success, shutdown retention,
transfer and independent repair/destination ownership.
Verification exits 0: formatting, Live check, 135 library / 484 integration
passes (2 opt-in ignored) / 1 doctest, and Live+historical check. Receipts:
`engine/target/live-verification/20261021T063849Z-2/`.
WP-10 remains Partial and next executable: concrete reporting/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 remain pending. Further autonomous work can safely continue.
No database/exchange access, Python/historical execution, dependency/lockfile,
ignored configuration or launcher edits. No push or acceptance claim.


Latest implementation checkpoint `8bee678` adds read-only producer reporting
snapshots and a combined router drain observation. Actual owner state exposes
attempt budget, retry deadline/timer, escalation and charged journal resources.
Transferred records prevent complete drain even after destination delivery drains.
Deterministic existing production-path witnesses now assert those observations.
Final full verification exits 0: formatting, Live check, 135 library / 483
integration passes (2 opt-in ignored) / 1 doctest; Live+historical check exits 0.
Receipts: `engine/target/live-verification/20261021T063459Z-7/`.
WP-10 remains Partial and next executable: concrete reporting/recovery policy,
full runtime lifecycle and Control wire composition remain. Earlier WP-05/06/07/09
gates persist; WP-11–29 pending. Further autonomous implementation can safely
continue. No SQL/exchange access, Python/historical execution changes, dependencies,
ignored configuration or launcher edits. No push or acceptance claim.



Latest implementation checkpoint `15b85ef` preserves and verifies the pre-existing
uncommitted combined router shutdown coordinator. Delivery and producer reporting
run bounded turns under one deadline poll; completion waits for both independent
owners, including transferred escalation records. A deterministic production-path
witness confirms expiry fencing without handler calls, delivery progress while
producer reporting escalates, exact one deadline poll per turn, and delayed drain
until the transferred charge releases.
Verification exits 0: full Live gates (135 library, 483 integration passed,
2 opt-in ignored, 1 doctest) and Live+historical check. Receipts:
`engine/target/live-verification/20261021T063204Z-2/`. After strengthening the
poll-count assertion, the focused witness (1 passed) and formatting pass again.
WP-10 remains Partial and next executable; concrete runtime recovery/reporting
policy, lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 remain pending. Further autonomous work can safely continue.
No database/exchange access, historical/Python execution, dependencies or ignored
configuration edits. Untracked launcher preserved. No push or acceptance claim.
See the WP-10 ledger for the complete verification limits and initial test fixes.


Latest implementation checkpoint `4054511` adds deadline-aware scheduled ingress
reporting shutdown. Admission is fenced immediately; retry state and charged
records survive expiry and escalation. Completion waits for preparing admissions
and externally held records; cancellation wakes the shutdown coordinator.
Verification exits 0: formatting, Live check, 135 library / 482 integration passes
(2 opt-in ignored) / 1 doctest, plus Live+historical check. Receipts:
`engine/target/live-verification/20261021T062755Z-2/`.
WP-10 remains Partial and next executable: concrete reporting/recovery policy,
complete runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09
gates persist; WP-11–29 pending. Further autonomous implementation can safely
continue within those gates. No database/exchange access or Python/historical
execution changes. Pre-existing untracked launcher preserved; no push or acceptance
claim. See WP-10 evidence for initial failed test iterations and final verification.

Latest implementation checkpoint `f607c06` adds bounded scheduled ingress reporting:
one retained retry timer, finite handler attempts, explicit escalation after permanent
failure/exhaustion/unwind, and charged ownership transfer. Timer admission faults
and early readiness preserve the record/deadline; authoritative time retires pending
timers when due. Closure preserves retry policy and unresolved charges.
Verification exits 0: formatting, Live check, 135 library / 480 integration passes
(2 opt-in ignored) / 1 doctest, plus Live+historical check. Receipts:
`engine/target/live-verification/20261021T062445Z-2/`.
WP-10 remains Partial and next executable: concrete reporting/recovery policy,
runtime lifecycle and Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Safe autonomous continuation remains possible.
No SQL/exchange access, Python/historical execution, dependency/lockfile or ignored
configuration changes. Pre-existing untracked launcher preserved. No push or
acceptance claim. See WP-10 implementation evidence.


Latest checkpoint `14c7643` adds explicit ingress-outcome draining: closes admission,
returns retained records, and reports completion only after all charges release.
A cancelled preparing admission wakes the drain coordinator even if it produces
no record; a committed reserved admission and consumer-held record delay completion.
Verification exits 0: formatting, Live check, 135 library / 478 integration passes
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T062135Z-3/`.
WP-10 remains Partial and next executable; concrete reporting/recovery policy,
scheduling and runtime/Control composition remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Safe autonomous continuation remains possible.
No SQL/exchange access or Python/historical execution changes; untracked launcher
preserved. See WP-10 evidence. No push or acceptance claim.



Latest implementation checkpoint `0c23ac0` adds a bounded ingress reporting
consumer over the existing charged journal records. Temporary/permanent handler
failures and handler unwind retain the pending record; successful handling releases
its charge. Explicit escalation transfers the same charged record, and mutable
outcome access permits recovery reservations to be reconciled without cloning or
releasing record accounting. Each turn handles at most one record; runtime owns
retry scheduling and concrete reporting/recovery policy.
Full verification exits 0: formatting, Live check, 135 library / 477 integration
passes (2 opt-in ignored) / 1 doctest, plus Live+historical check. Receipts:
`engine/target/live-verification/20261021T061849Z-2/`.
WP-10 remains Partial and next executable: concrete runtime reporting/recovery
handlers, scheduling, full lifecycle and Control composition remain. This adds a
consumer mechanism, not a running runtime or completed recovery integration.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous WP-10
implementation can safely continue. No SQL/exchange access, Python/historical
execution edits, dependency/lockfile or ignored configuration changes; preserved
untracked launcher; no push. See WP-10 implementation evidence.

Latest checkpoint `abc3f59` exposes the existing ingress through EventRouterPort,
with mandatory bounded outcome retention before returning a receipt. Only complete
required queue admission or a completed optional-only attempt returns Accepted;
partial/uncertain required outcomes reject conservatively while retaining their
identity and detailed outcomes. Rejection after partial commitment is not rollback;
producers must reconcile the journal before retrying. Cancellation and relative
per-call deadlines use the existing authoritative owners and clock.
Five deterministic consumer witnesses pass, including real partial queue commitment,
tracking-capacity rejection and dropped-wait rollback. Final full verification exits
0: formatting, Live check, 135 library / 475 integration passes (2 opt-in ignored) /
1 doctest. Live+historical check also exits 0. Receipts:
`engine/target/live-verification/20261021T061603Z-2/`.
WP-10 remains Partial and next executable: concrete runtime reporting/recovery
consumer and Control composition remain. Earlier WP-05/06/07/09 gates persist;
WP-11–29 pending. Further autonomous WP-10 implementation can safely continue.
No SQL/exchange requests, Python/historical execution, dependency/lockfile or
ignored configuration changes; untracked launcher preserved; no push.
See the WP-10 ledger for exact commands, failed test iterations and limitations.


Latest implementation checkpoint `9293c85` adds bounded ownership of contextual
ingress outcomes for runtime reporting. Reporting capacity is reserved before
admission; drop/cancellation releases it before commitment, and completed outcomes
are retained synchronously without an intervening await. Dequeued records retain
their charge until released. Three deterministic production-path witnesses cover
event-byte and record bounds, immutable sharing, rejection, cancellation, dropped
waits, latest-reporter wakeup and reserved admission completing after closure.
Final checks exit 0: formatting, Live check, 135 library / 470 integration passes
(2 opt-in ignored) / 1 doctest, plus Live+historical check. Full receipts:
`engine/target/live-verification/20261021T061040Z-2/`.
WP-10 remains Partial and next executable: the simple EventRouterPort projection,
concrete runtime/reporter and Control composition still remain. This journal is
in-memory outcome ownership, not durable reporting or a running daemon. Earlier
WP-05/06/07/09 gates remain; WP-11–29 remain pending. Further autonomous work can
safely continue. No SQL/exchange traffic, dependency/lockfile, ignored configuration
or Python/historical execution changes. Pre-existing untracked launcher preserved;
no push or acceptance claim. See WP-10 evidence for exact commands and limits.


Latest implementation checkpoint `2200236` adds contextual producer ingress over
the existing registry, namespaces, budget and delivery driver. No driver lock is
held across capacity waits; nonwaiting commitment contention returns the batch
and releases preparation resources. Required, optional and tracking-recovery
outcomes remain explicit. Four deterministic consumer witnesses cover immutable
batch sharing, optional overload, cancellation, bounded admission, drop rollback,
closure during waiting, contention, empty membership and retry of the same cursor.
Full verification exits 0: formatting, Live check, 135 library / 467 integration
passes (2 opt-in ignored) / 1 doctest, and Live+historical check. Receipts:
`engine/target/live-verification/20261021T060526Z-2/`.
WP-10 remains Partial and next executable. The detailed ingress requires callers
to retain/report outcomes; the simple EventRouterPort adapter, concrete runtime/
reporter and Control composition remain. WP-05/06/07/09 gates persist; WP-11–29
remain pending. Further autonomous work can safely continue. No SQL/exchange
access, ignored configuration, dependency/lockfile or Python/historical execution
changes. Pre-existing untracked launcher preserved. No push or acceptance claim.
See WP-10 evidence for exact commands and limitations.


Latest implementation checkpoint `e364dac` integrates bounded terminal reporting
and delivery retry scheduling into registered router shutdown, using the existing
driver/journal/registry owners. Admission closes before work; expiry performs no
adapter, timer allocation or reporter call. Queue reconciliation remains bounded;
temporary reporting failure and timer exhaustion preserve responsibility. Closing
an already-drained driver now releases its retained maintenance timer.
Two deterministic production-path witnesses cover two required queues through
one worker, correlated completions, reporting retry, timer exhaustion, expiry and
resume, and exact timer resource release on empty shutdown.
Final verification: formatting, Live check, 135 library tests, 463 integration
passes (2 opt-in ignored), 1 doctest, and Live+historical check all exit 0.
Receipts: `engine/target/live-verification/20261021T060056Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort facade,
concrete runtime/reporter and Control composition remain. Earlier WP-05/06/07/09
gates persist; WP-11–29 remain pending. Further autonomous implementation can
safely continue. No SQL/exchange access, dependency/lockfile, ignored config or
Python/historical execution changes. Pre-existing untracked launcher preserved.
See the WP-10 implementation ledger for commands and limitations.


Latest continuation fixes metric deadline crossings during pending timer polls.
The authoritative clock is now checked after either poll outcome, for both the
initial and replacement timer; due timers are released before observation.
The real native collector witness covers all four ready/pending crossing cases.
Verification: full Live gates exit 0 (135 library, 461 integration, 2 opt-in
ignored, 1 doctest), Live+historical check exit 0. Receipts:
`engine/target/live-verification/20261021T055653Z-2/`. After explicit replacement
timer release was added, focused witness (1 passed) and formatting exit 0.
WP-10 remains Partial and next executable; facade/runtime/reporter/Control
composition remains. Earlier WP-05/06/07/09 gates and WP-11–29 remain.
Further autonomous work can safely continue. No SQL/exchange access or
Python/historical execution changes. Pre-existing untracked launcher preserved.
Checkpoint: commit carrying this entry; see WP-10 implementation ledger.


This continuation fixes delivery retry scheduling against the authoritative clock.
Early-ready timer adapters return InvalidInput without coordinator self-wakes;
pending timers are retired when due, and deadline crossings inside fresh polls
schedule exactly one bounded turn. Deterministic production-driver witnesses
pass (2 focused tests). Full Live verification exits 0: formatting, Live check,
135 library / 461 integration (2 opt-in ignored) / 1 doctest; Live+historical
compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T055511Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort facade,
concrete runtime/reporter and Control composition remain. WP-05/06/07/09 open
gates persist; WP-11–29 pending. Further autonomous implementation can safely
continue. No database/exchange access, Python/historical execution edits,
dependency or ignored configuration changes. Pre-existing launcher preserved.
Checkpoint: commit carrying this entry and the WP-10 evidence continuation.


Latest implementation checkpoint `5cd2a74` fixes metric deadline crossings during
polling: readiness is followed by an authoritative clock check before rearming
or rejecting an early-ready timer. The existing native collector witness covers
crossings in both the initial and replacement poll, with exact timer counts and
next cadence. Verification exits 0: formatting, Live check, 135 library / 460
integration (2 opt-in ignored) / 1 doctest, combined Live+historical check.
Receipts: `engine/target/live-verification/20261021T055304Z-3/`.
WP-10 remains Partial and next executable: production EventRouterPort facade,
concrete runtime/reporter and Control composition remain. WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, push, Python/historical execution, dependencies or ignored
configuration edits. Pre-existing untracked launcher preserved.


Latest checkpoint `a8114ea` binds router metric cadence to the existing deadline
port. One retained timer is reused across polls and dropped on reconfiguration,
disabling or due sampling. Capacity failures preserve the boundary; early-ready
adapters cannot sample early or spin. The real native collector witness covers
pending reuse, disabling, timer rejection, early readiness and due sampling.
Full verification exits 0: formatting, Live check, 135 library / 460 integration
(2 opt-in ignored) / 1 doctest; combined Live+historical compilation exits 0.
Receipts: `engine/target/live-verification/20261021T055127Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort facade,
concrete runtime/reporter binding and full Control integration. This cooperative
poll binding does not constitute a running runtime. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Further autonomous implementation can safely continue.
No SQL/exchange access, push, historical/Python execution, dependency or ignored
configuration changes. Pre-existing untracked launcher preserved.


Latest implementation checkpoint `3e006a9` adds a cooperative bounded router metric
sampling cadence. Disabled/early turns construct no snapshots; delayed turns emit
one current sample without catch-up bursts. Wall/collector errors consume the
cadence; deadline overflow disables sampling explicitly. The existing real native
collector witness covers boundaries, failures, disabling and overflow.
Verification exits 0: formatting, Live check, 135 library / 460 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T054918Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort composition,
concrete runtime/reporter integration, runtime timer binding for the new cadence,
and full Control integration. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous implementation can safely continue. No SQL/exchange access,
push, Python/historical execution, dependency or ignored configuration changes.
Pre-existing untracked launcher preserved. See WP-10 evidence; no acceptance or
project completion claimed.


Latest implementation checkpoint `de3f4df` adds bounded registered router shutdown:
admission/namespaces/registry close immediately; delivery remains attachable until
unclaimed queues have reconciled. Capacity-limited schedulers drain/reap between
attachments. Closed fully drained queues are skipped to prevent worker recreation
from blocking convergence. Deadline expiry performs no attachment or adapter poll;
owners remain resumable. Two deterministic witnesses cover capacity exhaustion,
retired/active queues, externally held charges, no busy wakes, expiry/resume and
two committed required destinations through a one-worker scheduler with explicit
completion/report acknowledgement.
Verification exits 0: formatting, Live check, 135 library / 460 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T054648Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort composition,
concrete runtime/reporter integration, scheduled instrumentation and full Control
binding. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
implementation can safely continue. No SQL/exchange access, push, Python/historical
execution, dependency or ignored configuration changes. Pre-existing untracked
launcher preserved. See WP-10 evidence; no acceptance/project completion claimed.


Latest implementation checkpoint `baf09d3` connects ordered commitment to the
parked delivery coordinator. Accepted responsibility wakes the latest drive
waiter once, including tracking-recovery work that cannot rely on queue wakeups.
Precommit rejection preserves the waiter, frontier and empty obligation set.
The existing production-path integration witness now covers cancellation followed
by successful commitment without rearming the coordinator.
Verification exits 0: formatting, Live check, 135 library / 458 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T054320Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort composition,
concrete runtime/reporter integration and full instrumentation/Control binding.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is
safe. No SQL/exchange access, push, Python/historical execution, dependency or
ignored configuration edits. Pre-existing untracked launcher preserved.
See WP-10 evidence; no acceptance or project completion claimed.


Latest implementation checkpoint `8f348f4` fixes mixed monotonic timelines in
ordered fan-out: the admission owner's clock now reaches each queue reservation's
final deadline fence. Existing entry points retain system-clock behavior. Two
new deterministic witnesses verify a deadline valid only on the injected timeline
and expiry between destinations with complete reservation rollback. The existing
commitment-fence witness now explicitly arms expiry after preparation, preserving
its negative assertions independently of reservation clock-read count.
Final verification exits 0: formatting, Live check, 135 library / 458 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T054117Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort admission,
concrete runtime/reporter integration and full metric/control wiring remain.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is
safe. No SQL/exchange traffic, push, Python/historical execution, dependencies or
ignored configuration changes. Pre-existing untracked launcher preserved.
See WP-10 evidence. No package acceptance or project completion claimed.


Latest implementation checkpoint `5775ada` adds a finite native router metric
catalog and binds the authoritative owned router observation to six independent
gauges. Admission count/bytes, retained deliveries, tracked obligations, terminal
reports and escalated reports retain distinct meanings. Each sample returns its
own outcome; disabled/stale/unbound measurements cannot affect resource accounting
or shutdown. The real NativeMetrics witness covers units, distinct projection
values, wall timestamps, monotonic staleness, disabling, replacement and partial
binding. Final verification exits 0: formatting, Live check, 135 library / 456
integration (2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T053737Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort admission,
concrete runtime/reporter integration and full metric/control wiring remain. This
adds a usable binding, not a running coordinator or Control server. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is safe.
No SQL/exchange access, push, Python/historical execution edits, dependencies or
ignored configuration changes. Pre-existing untracked launcher preserved.
See WP-10 evidence. No acceptance or project completion claimed.


Latest implementation checkpoint `a2ccb14` exposes atomic admission budget
limits/usage/closure and a shared-reference combined router observation. Drain
requires the admission fence, preventing an empty open budget from being reported
as closed/drained. Existing shutdown uses the same observation implementation.
Two deterministic regression witnesses cover open/closed observation, retained
charges after closure/release, owned snapshot stability and no sampling effects.
Verification exits 0: formatting, Live check, 135 library / 456 integration
(2 opt-in ignored) / 1 doctest, combined Live+historical check. Receipts:
`engine/target/live-verification/20261021T053402Z-2/`.
WP-10 remains Partial and next executable: EventRouterPort admission composition,
concrete runtime/reporting and native metrics/control binding remain. The new
observation is available to consumers; no Control server is claimed. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is safe.
No SQL/exchange access, push, Python/historical execution edits, dependencies or
ignored configuration changes. Pre-existing untracked launcher preserved.
See WP-10 evidence. No package acceptance or project completion claimed.


Latest implementation checkpoint `0b0de81` connects ordered commitment directly
through `DeliveryDriver::commit_ordered`, using its authoritative tracker/recovery
owners and returning required plus optional outcomes. Closed owners return the
original preparation before any mutation; dropping it releases the order permit.
The existing registered-delivery/completion/reporting witness now uses this path.
Verification exits 0: formatting, Live check, 134 library / 455 integration
(2 opt-in ignored) / 1 doctest, combined Live+historical check. Receipts:
`engine/target/live-verification/20261021T053147Z-2/`.
WP-10 remains Partial and next executable: EventRouterPort admission composition,
concrete runtime/reporting integration and native metrics/control integration
remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 remain pending.
Further autonomous implementation is safe. No SQL/exchange access, push,
Python/historical execution changes, dependencies or ignored configuration edits.
Pre-existing untracked launcher preserved. See WP-10 evidence; no acceptance claim.


Latest checkpoint `358c34e` connects terminal reporting to timed delivery turns
through `DeliveryDriver::drive_registered_reporting`. One turn processes at most
one authoritative journal record; temporary failures keep their retry deadline,
permanent failures remain escalated, and reporting continues after admission
closes. Successful reporting wakes continued work and releases the retry timer on
complete drain. Scheduling failures remain explicit alongside reporting outcomes.
Two deterministic integration witnesses cover retry/no-spin/drain and permanent
failure retention during timer-capacity exhaustion.
Final verification exits 0: formatting, Live check, 134 library / 454 integration
(2 opt-in ignored) / 1 doctest; combined Live+historical check also exits 0.
Receipts: `engine/target/live-verification/20261021T052956Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, concrete reporting/runtime integration and native metrics/control
integration remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 remain pending.
Further autonomous implementation is safe. No database/exchange traffic, push,
Python/historical execution, dependency or ignored configuration changes.
Pre-existing untracked launcher preserved. See WP-10 evidence; no acceptance claimed.


Latest checkpoint `001633c` adds bounded periodic delivery maintenance through
`DeliveryDriver::drive_registered_with_retry`. One owned timer survives unrelated
notifications without postponement, releases capacity before rearming, and remains
active while a closed driver has outstanding payload charges. Zero intervals and
timer capacity/registration errors are explicit; independent delivery turns still
run. The real queue/worker witness exercises timer exhaustion, no busy wake,
expiry/rearm and shutdown after external payload release.
Verification exits 0: formatting, Live check, 134 library / 452 integration
(2 opt-in ignored) / 1 doctest, combined Live+historical check. Receipts:
`engine/target/live-verification/20261021T052731Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort composition,
actual reporting retries and native metrics/control integration remain. The timer
schedules maintenance and reporting handoff, not an external reporter or daemon.
Earlier WP-05/06/07/09 gates persist; WP-11–29 remain pending. Further autonomous
implementation is safe. No database/exchange traffic, push, historical/Python,
dependency or ignored configuration changes. Pre-existing launcher preserved.
See [WP-10 evidence](evidence/WP-10-implementation.md). No acceptance claimed.


Latest implementation checkpoint `077a78a` adds one bounded replacement-aware
journal capacity notification, armed by DeliveryDriver before polling. Successful
report acknowledgement or accepted escalation transfer wakes the current
coordinator; rejected operations preserve the waiter. Resolution notification runs
after the sink commitment lock is released. Deterministic production-path tests
cover reporting after close, rejected acknowledgement, replacement, failed/double
resolution, and reentrant sink observation.
Final verification exits 0: formatting, Live check, 134 library / 451 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T052449Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, bounded retry timer scheduling and native metrics/control integration
remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
implementation can safely continue. No SQL/exchange access, push, Python/historical
execution edits, dependency or ignored configuration changes. Pre-existing
untracked launcher preserved. See [WP-10 evidence](evidence/WP-10-implementation.md).
No acceptance claimed.


Latest checkpoint `8f7a2d5` adds one bounded, replaceable completion waiter to
DeliveryDriver. Successful correlated completion wakes the latest coordinator;
failed correlation preserves it, including while the closed driver drains.
The real ordered-fanout/retained-delivery witness proves replacement, rejected
correlation, shutdown completion and subsequent terminal payload release.
Verification exits 0: formatting, Live check, 134 library / 450 integration
(2 opt-in ignored) / 1 doctest, Live+historical check. Receipts:
`engine/target/live-verification/20261021T052247Z-2/`.
WP-10 remains Partial and next executable: EventRouterPort ordered composition,
reporting/retry timer scheduling and native metrics/control integration remain.
WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is safe.
No SQL/exchange access, push, Python/historical execution edits, dependency or
ignored configuration changes. Pre-existing untracked launcher preserved.
See [WP-10 evidence](evidence/WP-10-implementation.md). No acceptance claimed.


Latest continuation checkpoint `cd7f74f` composes registry maintenance, delivery,
and membership notification in `DeliveryDriver::drive_registered`. Each turn
attaches at most one queue, runs one existing recovery/supervision turn, and arms
membership observation against the revision captured before adapter polling.
Progress schedules another turn; capacity/correlation/shutdown observations do
not self-wake indefinitely. Three deterministic witnesses cover bounded attachment,
capacity retry/retirement/shutdown, adapter-time membership mutation, retained
uncorrelated payload ownership, and ordered tracked delivery through explicit
completion and terminal payload release.
Verification exits 0: formatting, Live check, 134 library / 450 integration
(2 opt-in ignored) / 1 doctest, plus Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T052023Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime completion/reporting/retry timer scheduling, native metrics
and control binding. This coordinator primitive is not a running daemon.
WP-05/06/07/09 open gates persist; WP-11–29 pending. Further autonomous WP-10
implementation can safely continue. No push, SQL/exchange access, Python/historical
execution edits, dependencies or ignored configuration changes. Pre-existing
untracked `scripts/run-autonomous-codex.sh` preserved. See
[WP-10 evidence](evidence/WP-10-implementation.md). No acceptance claimed.


Latest continuation adds bounded registry-to-delivery worker reconciliation and
wake-driven routing membership observation. One maintenance turn reaps drained
workers/retired slots and attaches at most one unclaimed queue, retired first;
capacity rejection releases only the fresh lease and preserves queued payloads.
One replacement-aware registry waiter wakes outside both membership/commitment
locks on registration, removal or shutdown. Three new deterministic witnesses
cover capacity retry, retired/replacement ownership, deterministic claims, failed
mutations, waiter replacement and reentrant callbacks.
Final verification exits 0: formatting, Live check, 134 library / 447 integration
(2 opt-in ignored) / 1 doctest, plus Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T051603Z-2/`.
Implementation checkpoints: `51bd142` (worker reconciliation), `b12bf59`
(membership notification). See [WP-10 evidence](evidence/WP-10-implementation.md).
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime retry/wakeup orchestration and native metrics/control binding.
WP-05/06/07/09 open gates persist; WP-11–29 pending. Further autonomous work can
safely continue. No push, SQL/exchange traffic, Python/historical execution edits,
dependency or ignored configuration changes. Pre-existing untracked launcher
preserved. No package acceptance or project completion claimed.



Latest continuation exposes bounded dynamic worker registration and drained-worker
reaping through WP-10's authoritative DeliverySupervisor/DeliveryDriver. Capacity
and shutdown failures return the complete worker; held payload charges fence
reaping, and same-ID open replacements remain independent. One deterministic
production-owner lifecycle witness covers capacity rejection/retry, retired charge
release, replacement preservation, idempotent reaping and shutdown registration
rejection. Full verification exits 0: formatting, Live check, 134 library / 444
integration (2 opt-in ignored) / 1 doctest; Live+historical check exits 0. Receipts:
`engine/target/live-verification/20261021T051151Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime wakeup/retry/registry synchronization and native metrics/control
binding. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
implementation can safely continue. Checkpoint: local commit carrying this entry.
Preserved pre-existing untracked launcher. No push, database/exchange traffic,
Python/historical execution edits, dependency or ignored configuration changes.



Latest continuation integrates independent same-ID retired/replacement workers into
WP-10's bounded scheduler. Closed old instances can drain alongside replacements;
two open instances remain rejected. A production registry/scheduler witness covers
capacity rejection with returned ownership, round-robin outcomes, retained charges,
closed-only removal, registry reaping and replacement preservation.
Full verification exits 0: formatting, Live check, 134 library / 443 integration
(2 opt-in ignored) / 1 doctest; Live+historical check exits 0. Receipts:
`engine/target/live-verification/20261021T050911Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime scheduling/retries/retired reaping and native metrics/control
binding. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
implementation can safely continue. Checkpoint: local commit carrying this entry
and matching WP-10 evidence. Pre-existing untracked launcher preserved. No push,
SQL/exchange traffic, Python/historical execution, dependencies or ignored config edits.



Latest continuation retains unregistered WP-10 destinations in a bounded retired
set. Removed queues close outside registry locks, remain observable and drainable
through exclusive retired-worker claims, and keep queued/reserved/consumer-held
charges visible to registry and composed shutdown. Active plus retired instances
share the destination limit; explicit drain-checked reaping releases slots without
finalizing independent delivery obligations. Same-ID replacements remain independent.
Three new deterministic witnesses and the updated shutdown witness pass.
Final full Live verification exits 0: 134 library / 442 integration (2 opt-in ignored)
/ 1 doctest; formatting, Live check and combined Live+historical check exit 0.
Receipts: `engine/target/live-verification/20261021T050714Z-2/`.
Checkpoint: `b658d5e` (bounded retired-destination ownership and shutdown drain).
WP-10 remains Partial and next executable: production EventRouterPort composition,
runtime scheduling/retry/retirement reaping and native metrics/control binding.
WP-05/06/07/09 open gates persist; WP-11–29 pending. Further autonomous work is safe.
No push, SQL/exchange traffic, Python/historical execution edits, dependencies or
ignored configuration changes. Pre-existing untracked launcher preserved.


Latest continuation repairs shutdown drain wakeups for WP-10 global preparation
charges and destination payload capacity. One bounded replacement-aware waiter
per authoritative accounting owner is registered atomically with the drain check;
final release wakes outside all accounting/registry locks. Registry and composed
router shutdown use these production owners. Deterministic tests prove replacement,
partial/final release, reentrant observations, close fencing and repeated drain.
Full verification exits 0: formatting, Live check, 134 library / 439 integration
(2 opt-in ignored) / 1 doctest; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T050256Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime retry scheduling and native metrics/control binding remain.
Earlier WP-05/06/07/09 gates persist; WP-11–29 remain pending. Further autonomous
implementation is safe. Checkpoint: local commit carrying this entry and the
matching WP-10 ledger. Preserved untracked launcher. No push, SQL/exchange traffic,
Python/historical execution, dependencies or ignored configuration changes.


Latest continuation composes WP-10 cooperative router shutdown across the existing
namespace registry, admission budget, destination registry and delivery driver.
Registry shutdown now closes queue/capacity admission outside registry locks;
queued and transferred payloads retain their charges. Combined drain observations
include pending cursor permits, admission charges, registered queue capacity and
all delivery responsibilities. Expiry fences admission without retrying tracking
or calling adapters; the same owners resume under a later deadline.
Final verification exits 0: formatting, Live check, 133 library / 438 integration
(2 opt-in ignored) / 1 doctest; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T050018Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
owner, runtime wakeup/retry scheduling and native metrics/control binding remain.
The shutdown coordinator is integrated with real production owners in deterministic
tests, not yet a running daemon. Earlier WP-05/06/07/09 gates persist; WP-11–29
pending. Safe autonomous continuation remains possible. Checkpoint: local commit
carrying this entry and matching WP-10 evidence. Preserved untracked launcher;
no push, SQL/exchange traffic, historical/Python changes, dependencies or ignored
configuration changes.

Latest continuation adds wake-driven WP-10 admission namespace shutdown. Closing
waits for outstanding cursor permits (including retired namespaces); commit/abort
wakes a bounded replacement-aware coordinator without rewinding accepted progress.
Full verification exits 0: formatting, Live check, 133 library / 436 integration
(2 opt-in ignored) / 1 doctest; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T045558Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime retry scheduling, metrics/control binding and composition of
namespace/budget/registry/delivery shutdown remain. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. Safe autonomous continuation remains possible.
Checkpoint: local commit carrying this entry and matching WP-10 ledger.
Pre-existing untracked launcher preserved. No push, SQL/exchange traffic,
Python/historical execution edits, dependency or ignored-configuration changes.


Latest continuation fences WP-10 delivery-driver admission on explicit close and
shutdown, including deadline expiry and already-drained owners. New admissions
return `DeliveryAdmissionError::Closed`; a restricted abort operation releases
existing preparations without reopening admission. Existing delivery recovery,
completion, reporting and resumed shutdown remain available. Full verification
exits 0: formatting, Live check, 133 library tests, 435 integration tests (2 opt-in
ignored), 1 doctest; Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T045336Z-3/`.
WP-10 remains Partial and next executable: production EventRouterPort ordered
composition, runtime wakeup/retry scheduling, metrics/control binding and full
admission-namespace shutdown remain. Earlier WP-05/06/07/09 gates persist;
WP-11–29 pending. Safe autonomous continuation remains possible. Checkpoint:
local commit carrying this entry and matching WP-10 evidence. Pre-existing
untracked launcher preserved; no push, SQL/exchange traffic, Python/historical
code, dependencies or ignored configuration changes.



Latest continuation composes WP-10 delivery responsibility under `DeliveryDriver`:
one owned supervisor/tracker/recovery/journal, a bounded recovery pass followed by
worker/finalization polling, correlated completions, explicit recovery transfers,
and resumable deadline shutdown checking the full delivery drain predicate.
Full verification exits 0: formatting, Live compilation, 133 library tests,
434 integration tests (2 opt-in ignored), 1 doctest; Live+historical check exits 0.
Receipts: `engine/target/live-verification/20261021T045121Z-3/`.
WP-10 remains Partial: production EventRouterPort admission/ordered composition,
runtime wakeup/retry scheduling, metrics/control binding and admission-namespace
shutdown remain. DeliveryDriver is a production composition component, not a
running router or daemon. Earlier WP-05/06/07/09 gates and WP-11–29 remain open.
Next executable package: WP-10; safe autonomous continuation remains possible.
Checkpoint: local commit carrying this entry and WP-10 evidence. Pre-existing
untracked launcher preserved; no push, database/exchange traffic, historical or
Python code changes, dependencies, or ignored configuration edits.



Latest continuation adds a capacity-bounded WP-10 tracking recovery pass:
independent obligations progress past a failed update, while successors of the
same obligation remain fenced and failed updates retain order and charges.
Full verification exits 0: 133 library, 431 integration (2 opt-in ignored),
1 doctest, formatting and Live check. Receipts:
`engine/target/live-verification/20261021T044817Z-7/`.
Live+historical compilation also exits 0. WP-10 remains Partial and next
executable: production EventRouterPort owner, automatic runtime recovery
scheduling, native metrics/control binding and whole-router shutdown remain.
Earlier WP-05/06/07/09 gates and WP-11–29 remain outstanding. Safe autonomous
continuation remains possible. Checkpoint: local commit carrying this entry.
Untracked launcher preserved; no push, SQL/exchange traffic, Python/historical
code, dependencies or ignored configuration edits. See WP-10 evidence.


Latest continuation bounds WP-10 prepared transaction ownership (including empty
transactions) before allocator mutation, and adds owned `DeliverySupervisor`
observations for worker charges, retained/blocked payloads, obligation/preparation
counts, recovery reservations and terminal reporting. The delivery drain predicate
requires all those responsibilities to clear; it does not certify whole-router
shutdown or external durability. Full verification exits 0: 133 library,
429 integration (2 opt-in ignored), 1 doctest, formatting/Live check and combined
Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T044555Z-2/`.
WP-10 remains Partial and next executable; production EventRouterPort owner,
recovery scheduling/lifecycle, native metrics/control binding and complete router
shutdown remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous implementation can safely continue. Checkpoint: local commit
carrying this entry and the matching WP-10 evidence. Existing untracked launcher
preserved; no push, SQL/exchange traffic, Python/historical code, dependencies or
ignored configuration edits.



Latest continuation adds WP-10 `DeliverySupervisor`: bounded real worker polling,
automatic obligation correlation, retained completion and terminal journal handoff,
explicit charged invalid-response recovery, and resumable deadline shutdown.
Full verification exits 0: 132 library, 428 integration (2 ignored), 1 doctest,
formatting/Live check and Live+historical compilation. Final receipts:
`engine/target/live-verification/20261021T044309Z-2/`.
WP-10 remains Partial: production EventRouterPort owner, recovery scheduling,
native metrics/control and complete router shutdown remain. The supervisor is
an integrated scheduling component, not a running daemon. Earlier WP-05/06/07/09
gates remain open; WP-11–29 pending. Further autonomous WP-10 work is safe.
See WP-10 evidence for scope, initial test fault and verification. Local checkpoint:
commit carrying this entry. Pre-existing untracked launcher preserved; no push,
SQL/exchange traffic, Python/historical execution, dependency or config edits.


Latest continuation adds bounded automatic terminal handoff with
`PendingDeliveries::finalize_next_into`: incomplete obligations are skipped,
one terminal payload is transferred per call, and journal/tracking failures
preserve retained payloads. Full checks exit 0: 132 library, 425 integration
(2 ignored), 1 doctest, formatting/Live check and Live+historical check.
Receipts: `engine/target/live-verification/20261021T043959Z-2/`.
WP-10 remains Partial and next executable; production EventRouterPort owner,
recovery scheduling, supervision, metrics/control and whole-router shutdown
remain. Earlier foundation gates and WP-11–29 remain outstanding. Safe to
continue autonomously. No SQL/exchange traffic or push. Checkpoint: local
commit carrying this entry; pre-existing untracked launcher preserved.



Latest continuation adds bounded WP-10 `PendingDeliveries`: automatic exact
worker-outcome/obligation correlation, independent completion ingestion, retained
payload ownership through failed finalization and explicit recovery transfer.
Unknown, ambiguous and duplicate matches fail closed with the original charge.
The real registered ordered fan-out witness now uses this owner end to end.
Full verification exits 0: formatting, Live check, 132 library / 424 integration
(2 opt-in ignored) / 1 doctest; Live+historical check exits 0. Final receipts:
`engine/target/live-verification/20261021T043756Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
recovery scheduling, runtime supervision, native metrics/control binding and
whole-router shutdown remain. Earlier WP-05/06/07/09 gates persist; WP-11–29
pending. Further autonomous WP-10 work is safe. No SQL/exchange traffic,
Python/historical execution, dependency/ignored-config changes or push.
Pre-existing untracked launcher preserved. Checkpoint: local commit carrying this
entry and the matching WP-10 ledger continuation.


Latest continuation connects WP-10 destination worker outcomes to authoritative
obligations with `RetainedDelivery`. Binding validates descriptor, exact cursor
range and actual admission response. Correlated completions use existing tracker
stage rules; admission acceptance invents no progress. Payloads retain their
original queue charges through errors, insufficient durability, full journals and
terminal journal handoff until explicitly released/transferred by the supervisor.
Three new negative/fault witnesses and the extended real ordered fan-out witness
pass. Full Live verification exits 0: formatting, Live check, 132 library / 421
integration (2 opt-in ignored) / 1 doctest; Live+historical check exits 0. Receipts:
`engine/target/live-verification/20261021T043439Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
automatic bounded outcome/obligation lookup and completion ingestion, recovery
scheduling, runtime supervision, native metrics/control binding and whole-router
shutdown remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further
autonomous WP-10 work is safe. No database/exchange access, Python/historical
execution, dependency or ignored-config changes; no push. Pre-existing untracked
launcher preserved. Checkpoint: local commit carrying this entry and WP-10 ledger.


Latest continuation adds WP-10 deadline-driven destination worker shutdown:
admission is fenced, each poll transfers at most one charged outcome, expiry
retains pending calls/queued payloads and later drain resumes without resubmission.
Two deterministic timer/production-worker witnesses pass. Full Live verification
exits 0: formatting, Live check, 132 library / 418 integration (2 opt-in ignored)
/ 1 doctest. Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T043100Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
completion/recovery correlation and payload ownership, runtime supervision and
native metrics/control binding remain. Worker drain does not establish independent
obligation/recovery drain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous WP-10 implementation is safe. No SQL/exchange traffic,
Python/historical execution changes, dependencies or ignored config changes;
no push. Existing untracked launcher preserved. Checkpoint: local commit carrying
this entry and matching WP-10 evidence.


Latest continuation adds bounded WP-10 `DestinationWorkers` scheduling over the
real exclusive workers: round-robin outcome handoff, slow-adapter isolation,
owner-preserving registration failures, close fencing and drained-only retirement.
Two deterministic scheduler witnesses pass. Final full Live verification exits 0:
formatting, Live check, 132 library / 416 integration (2 opt-in ignored) / 1 doctest;
Live+historical compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T042852Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
completion/recovery correlation and payload ownership, runtime task supervision,
native metrics/control binding and finite deadline shutdown remain. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Safe autonomous implementation
can continue. No SQL/exchange traffic, Python/historical execution changes,
dependencies, ignored configuration changes or push. Existing untracked launcher
preserved. Checkpoint: local commit carrying this entry and WP-10 evidence.


Latest continuation adds wake-driven WP-10 destination queue consumption through
`DestinationWorker::poll_next`: one bounded waiter per exclusive lease, atomic
queue inspection/registration, enqueue and close wakeups outside the queue lock,
and waiter cleanup on owner release. Existing nonwaiting polling remains intact.
Two deterministic witnesses cover both insertion paths, replacement wakers,
closed drain, pending adapter retention and transferred payload accounting.
Full Live verification exits 0: formatting, Live check, 132 library / 414
integration (2 opt-in ignored) / 1 doctest. Live+historical compilation exits 0.
Receipts: `engine/target/live-verification/20261021T042629Z-7/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
runtime scheduling composition, completion/recovery correlation and payload
ownership, native metrics/control binding and finite deadline shutdown remain.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Safe autonomous work
can continue. No SQL/exchange traffic, Python/historical execution, dependencies,
ignored configuration changes or push. Pre-existing untracked launcher preserved.
Checkpoint: local commit carrying this entry and matching WP-10 evidence.



Latest continuation binds WP-10 workers to the actual registered adapter, frozen
descriptor and queue under the membership fence. Eight worker witnesses now cover
ordered tracked fan-out, concurrent exclusive claims, replacement isolation,
shutdown drain, payload lifetime and adapter destruction before permit/lease release.
Adapter acceptance leaves authoritative obligations at Queued, never fabricated
Delivered/Committed/Durable. Worker checkpoint: `bd48878`; registry integration:
local commit carrying this entry and the matching WP-10 evidence continuation.
Final full Live verification exits 0: 132 library / 412 integration (2 opt-in
ignored) / 1 doctest, formatting and Live compilation; Live+historical check exits 0.
Receipts: `engine/target/live-verification/20261021T042417Z-2/`.
WP-10 remains Partial and next executable: production EventRouterPort owner,
runtime worker scheduling, completion/recovery correlation and payload lifetime,
metrics/control integration and finite deadline shutdown remain. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Autonomous implementation can
safely continue on WP-10. No SQL/exchange traffic, Python/historical execution,
dependency/lockfile or ignored-config changes; no push. Existing untracked
`scripts/run-autonomous-codex.sh` preserved.


Latest continuation implements the WP-10 exclusive destination admission worker.
It retains pending adapter calls, payloads and capacity across polling pauses;
validates destination/cursor correlation; and transfers outcomes with charged
payload ownership. Four deterministic witnesses cover FIFO, slow/failed adapters,
invalid responses, shutdown/drain observations and exclusive consumer fencing.
Full Live verification exits 0: 132 library / 408 integration (2 opt-in ignored) /
1 doctest, formatting and Live compilation. Live+historical check exits 0.
Receipts: `engine/target/live-verification/20261021T042138Z-2/`.
WP-10 remains Partial: production router owner, worker runtime scheduling,
completion/recovery integration, metrics/control binding and deadline shutdown
remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
WP-10 implementation is safe. No SQL/exchange traffic or Python/historical,
dependency/ignored-config changes. Pre-existing untracked launcher preserved.
Checkpoint: local commit carrying this entry and matching WP-10 evidence.


Latest continuation adds WP-10 owned recovery drain observations covering correlated
updates, unresolved outcomes and in-flight reservations. Queue-specific len/is_empty
semantics are preserved and documented; shutdown consumers can use the complete
snapshot instead. A deterministic real-owner witness covers rejected retry,
reconciliation, reservation release and independent obligation lifetime.
Full Live verification exits 0: 132 library / 404 integration (2 opt-in ignored) /
1 doctest, formatting and Live compilation; Live+historical compilation exits 0.
Receipts: `engine/target/live-verification/20261021T041749Z-2/`.
WP-10 remains Partial and next executable: production owner, workers, payload
lifetime, metrics/control binding and deadline shutdown remain. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Safe autonomous implementation
can continue. No SQL/exchange access, Python/historical/dependency/ignored-config
changes or push. Pre-existing untracked launcher preserved. Checkpoint: local
commit carrying this entry and matching WP-10 evidence.



Latest continuation adds WP-10 reconciliation of retained uncorrelated destination
outcomes against the authoritative acknowledgement tracker. Unknown obligations,
wrong destination/range and regressing progress preserve the outcome and capacity;
valid progress releases only the recovery reservation, preserving obligation
finalization responsibility. Two deterministic consumer witnesses pass. Full
Live verification exits 0: 132 library / 403 integration (2 opt-in ignored) /
1 doctest, formatting and Live compilation. Combined Live+historical compilation
exits 0. Receipts: `engine/target/live-verification/20261021T041624Z-3/`.
WP-10 remains Partial and next executable: production owner, workers, payload
lifetime, metrics/control integration and deadline shutdown remain. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Safe autonomous implementation
can continue. No SQL/exchange access, Python/historical/dependency/ignored-config
changes or push. Pre-existing untracked launcher preserved. Checkpoint: local
commit containing this entry; details in WP-10 evidence.


Latest continuation bounds WP-10 recovery batch allocation before allocating from
caller-supplied counts. Oversized/usize::MAX requests reject without panic or
capacity mutation; fallible allocation and existing reservation rollback remain
in effect. A deterministic production-interface witness covers occupied, empty,
exact-capacity and released batches. Full Live verification exits 0: 132 library,
401 integration (2 opt-in ignored), 1 doctest, formatting and Live compilation.
Combined Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T041427Z-2/`.
WP-10 remains Partial and next executable; production owner, workers,
completion/recovery payload lifetime, metrics/control binding and deadline
shutdown remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous implementation is safe. No SQL/exchange access, historical,
Python, dependency or ignored-config changes; pre-existing launcher preserved.
Checkpoint: local commit containing this entry and matching WP-10 evidence.


Latest checkpoint `b18da9c` repairs atomic WP-10 obligation preparation:
duplicate IDs reject before mutation rather than panicking, specification ID
exhaustion/collision preserves the allocator, and empty preparations retain a
real abortable/activatable transaction identity. Two new production-tracker unit
witnesses pass. Full Live verification exits 0: 132 library / 400 integration
(2 opt-in ignored) / 1 doctest, formatting and Live check. Combined
Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T041259Z-2/`.
WP-10 remains Partial and next executable: production port owner, bounded workers,
completion/recovery payload lifetime, metrics/control projection and deadline
shutdown remain. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous implementation can safely continue. No SQL/exchange access,
Python/historical/dependency/ignored-config changes or push. Pre-existing
untracked launcher preserved. See WP-10 evidence for exact checks.


Checkpoint `3155b01` closes a WP-10 commitment ownership gap: public plans can no
longer omit/duplicate required destinations, substitute an unrelated queue, or
forge the registered descriptor. Obligations also match the descriptor retained
at preparation; optional fan-out rejects omitted optional membership before any
write. Three deterministic witnesses pass across tracked, recovery-aware,
untracked and optional entry points, with actual queue identity/readback and
reservation rollback. Full verification exits 0: formatting, Live check,
130 library / 400 integration (2 opt-in ignored) / 1 doctest; Live+historical
compilation exits 0. Receipts:
`engine/target/live-verification/20261021T041111Z-2/`.
WP-10 remains Partial: production EventRouterPort owner, bounded workers,
completion/recovery payload lifetime, metrics/control projection and deadline
shutdown composition remain. Earlier WP-05/06/07/09 gates remain open;
WP-11–29 are pending. Next executable authorized package: WP-10. Further
autonomous implementation can safely continue. No SQL, exchange, historical,
Python, dependency or ignored configuration changes. Existing untracked launcher
preserved. See [WP-10 evidence](evidence/WP-10-implementation.md).


Latest continuation adds bounded owned namespace/frontier observations from the
actual admission owner, including pending cursor ranges, closure, retired
namespaces and offset exhaustion. Two deterministic witnesses verify real
ordered preparation/commitment, shutdown while prepared, queued outstanding work,
rollback and exhaustion. Full verification exits 0: formatting, Live check,
130 library / 397 integration (2 opt-in ignored) / 1 doctest; combined
Live+historical compilation exits 0. Receipts:
`engine/target/live-verification/20261021T040735Z-2/`.
WP-10 remains Partial; namespace observations do not establish worker completion
or a complete deadline shutdown. Production EventRouterPort owner, worker and
completion/recovery lifetime, metrics/control binding and shutdown composition
remain. Earlier WP-05/06/07/09 gates remain open; WP-11–29 remain pending.
Next executable authorized work: WP-10 completion. Autonomous work can safely
continue. No database/exchange requests, historical/Python/dependency/config
changes or push. Pre-existing untracked launcher preserved. Checkpoint: local
commit containing this entry and its matching WP-10 evidence continuation.


Latest continuation adds real owned router/destination snapshots, sorted and bounded
by the authoritative registry limit. Queue observations distinguish queued depth
from capacity retained by reservations and consumer-held batches; closure and
capacity shutdown remain distinct. Two deterministic consumer witnesses pass.
Final verification exits 0: formatting, Live check, 130 library / 395 integration
(2 opt-in ignored) / 1 doctest; combined Live+historical compilation also exits 0.
Receipts: `engine/target/live-verification/20261021T040447Z-2/`.
WP-10 remains Partial: production EventRouterPort owner, workers, completion and
recovery lifetime, metrics/control-port binding and deadline shutdown composition
remain. Earlier WP-05/06/07/09 gates remain open; WP-11–29 remain pending.
Next authorized executable work remains WP-10; safe autonomous work can continue.
No SQL/exchange access, historical/Python/dependency/ignored-config changes or
push. Pre-existing untracked launcher preserved. Checkpoint: the local commit
carrying this entry and the matching WP-10 evidence continuation.


Final verification: formatting, Live check, 130 library tests, 393 integration
tests (2 opt-in ignored), 1 doctest, and combined Live+historical compilation all
exit 0. Receipts: `engine/target/live-verification/20261021T040223Z-3/`.
Local checkpoint: commit carrying this continuation and its evidence.

This continuation adds bounded ordered optional-only queue attempts. Required
sinks cannot be bypassed; empty registries and cancelled/budget-exhausted calls
reject with the original batch and preserve the frontier. Completed best-effort
attempts report every queue failure and advance order even on total overload;
this is an attempt outcome, not proof of delivery. Two deterministic consumer
witnesses cover these cases and real queue identity/readback. WP-10 remains
Partial: production owner, workers, completion/recovery lifetime, snapshots and
shutdown composition are still pending. Next executable work is WP-10; WP-11–29
remain pending and earlier WP-05/06/07/09 gates remain open. Further autonomous
implementation can safely continue. See the WP-10 evidence continuation below.


Checkpoint `2fba098` adds ordered nonwaiting best-effort fan-out. Full/closed
optional queues produce individual outcomes without capacity waiters; required
tracking is preserved and the namespace order permit covers optional writes.
Three deterministic consumer witnesses pass. Final full Live verification exits
0: 130 library, 391 integration (2 opt-in ignored), 1 doctest; formatting, Live
check and combined Live+historical check also exit 0. Receipts:
`engine/target/live-verification/20261021T035844Z-2/`.
WP-10 remains Partial: production owner/worker/completion/recovery lifetime,
no-subscriber policies, observations and shutdown composition remain. Earlier
WP-05/06/07/09 gates remain open; WP-11–29 are pending. Further autonomous WP-10
implementation can safely continue. No SQL, exchange, historical, Python,
dependency or ignored configuration changes; pre-existing launcher preserved.
See WP-10 evidence for the checkpoint and exact checks.


Latest continuation adds `OrderedAdmissionBudget`: global event-count and retained-
byte limits are acquired before destination waiting and held through prepared
commitment/drop. Exhaustion explicitly rejects without a hidden waiter queue;
cancellation/drop releases both global and destination reservations. Two new
consumer witnesses pass. Full Live verification exits 0: 130 library, 388
integration (2 opt-in ignored), 1 doctest, formatting and Live check. Combined
Live+historical compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T035519Z-2/`.
WP-10 remains Partial: the production owner must use this bounded entry point;
worker/completion/recovery lifetime, policy composition and snapshots remain.
WP-11–29 remain pending; earlier WP-05/06/07/09 open gates persist. Further
independent autonomous WP-10 implementation is safe. Pre-existing untracked
launcher remains untouched; no SQL, exchange, dependency or historical changes.


Checkpoint `045d860` adds bounded explicit cursor namespace ownership and
`OrderedFanout::prepare_registered`, preserving the existing real tracked
fan-out. Retirement/shutdown fence cloned handles; bounded tombstones prevent
same-namespace frontier resets. Three new deterministic witnesses pass, including
concurrent registration and real destination queue readback. Full verification
exits 0: 130 library / 386 integration (2 opt-in ignored) / 1 doctest, formatting
and Live check; Live+historical compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T035235Z-2/`.
WP-10 remains Partial; production owner, global scheduling, destination workers,
completion/recovery ownership, best-effort policies and snapshots remain. This
runtime-local registry does not claim restart durability. WP-05/06/07/09 open
gates remain. Next authorized work: WP-10 completion; WP-11–29 remain pending.
No SQL/exchange requests, historical execution, Python, dependencies, ignored
configuration or user launcher changes. Further autonomous WP-10 work is safe.
See [WP-10 ledger](evidence/WP-10-implementation.md).


Checkpoint `133b72e` adds WP-10 `OrderedFanout`: the real required-destination capacity,
order permit, obligation activation and recovery-aware queue commitment now
compose through one cancellation/deadline-fenced operation. Dropping a prepared
operation or waiting future releases reservations and the stream permit without
creating obligations. The final deadline check runs inside the registry
commitment fence; partial outcomes retain their payload and advance the frontier.
WP-10 remains Partial: this is a required-sink coordinator, not the production
EventRouterPort service or worker/recovery runtime. Next executable work remains
WP-10 production owner, bounded scheduling and downstream completion integration.
Final verification exits 0: formatting, Live check, 130 library / 383 integration
(2 opt-in ignored) / 1 doctest, and Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T034918Z-2/`. Seven new deterministic
coordinator witnesses pass. Further autonomous WP-10 implementation is safe;
this continuation does not close its remaining acceptance requirements.
Pre-existing untracked `scripts/run-autonomous-codex.sh` was preserved.


Final cross-feature / opt-in receipts are in
`engine/target/live-verification/campaign-final-1792553404735277127/results.json`.
All four commands exit 0: base library check, Live+historical+TUI all-target check,
isolated PostgreSQL witness (1 test), release measurement baseline (1 test).
Historical library retains its independently rechecked 151/1 baseline result.
No production data, Python, historical execution, dependency/lockfile or ignored
exchange configuration changes; no remote push.


Latest verified milestones: `50f1009` (WP-08 ReadyForReview), `ba5ad39`
(WP-09 candidates/DAG and durable journal, Partial). WP-10 recovery-aware fan-out
now commits queues and preserves tracking responsibility, with explicit partial
outcomes, bounded registry and checked stream-order permits. WP-10 remains
Partial: production admit/worker/recovery/snapshot/shutdown composition is not
implemented. Final Live full run exits 0: 130 library / 376 integration / 1 doc;
2 opt-in tests are separate. Source identity validation additionally rejects a
definite derivative-versus-Spot mismatch without resolving OPEN-001 by guess.


Checkpoint `50f1009` completes WP-08 primitives as ReadyForReview. WP-09 now has
verified candidate changes, iterative bounded DAG validation and a real bounded
checksummed local revision journal, including all nine persistence fault points.
Full verification exits 0: 129 library / 371 integration / 1 doctest. WP-09 remains
Partial; authoritative metadata/capability compilation, TOML projection, worker
port and management integration are not certified. Independent WP-10 work may
continue against its explicit WP-06/WP-08 prerequisites.


WP-08 completion verification: full Live exits 0 (123 library, 366 integration,
1 doctest; two opt-in tests run separately). TUI and combined Live+historical
checks pass. Historical library baseline rechecked: 151 passed / 1 unchanged
zero-stream failure. Native metrics/health/diagnostics and deterministic
composition use real public ports. See WP-08 evidence for exact receipts and
release primitive overhead results. No package marked Accepted.


Checkpoints: `6c58c37` (WP-05 checked generations/cursors), `5b97f54`
(WP-06 real isolated PostgreSQL grouped transaction/readback/rollback witness).
WP-08 now has tested clocks, count/byte budgets, bounded task admission, scoped
native metrics, histogram, aggregation, capability health and redacted local
diagnostics. Latest full verification: 113 library, 363 integration, 1 doctest;
all gates exit 0. PostgreSQL opt-in witness separately passed 1/1. WP-08 primitives have since completed their package verification and are
ReadyForReview; see its current evidence ledger. WP-09 candidate/DAG work is
underway independently of the unresolved metadata/capability adapter gates.


Authorized scope is now WP-05 through WP-29, sequentially, one writer, local
commits permitted and no remote pushes. Initial branch `codex/wp06-completion`
was clean. WP-05 reconciliation adds checked advancement/exhaustion and exact
generation fences, plus namespace-aware cursor position comparison. Full Live
verification passes: 95 library, 363 integration, 1 doctest; all five gates exit 0.
See [WP-05 reconciliation](evidence/WP-05-implementation.md).

The isolated PostgreSQL identity was confirmed as database and authenticated
role `marketforge_live_test`. Although SQL CONNECT privilege on `marketforge` is
true, an actual connection with the test role is rejected by pg_hba.conf for both
SSL and non-SSL, establishing the server-side production connection fence. No
production credentials were used and no writes were made. C06 transaction
witnesses can now proceed in the isolated database. Signed timestamp/schema
compatibility and C12 adapter selection remain unresolved; prior reports
describing read-only authorization are historical.


### Accepted Foundations

The following work packages have been formally accepted:

- WP-00 — Baseline and feature isolation.
- WP-02 — Shared depth and trade models.
- WP-03 — Shared storage, arithmetic, and supporting primitives.
- WP-04 — Dependency and Cargo feature wiring.

WP-01 was superseded.

Acceptance of these packages is based on their existing evidence records.

### WP-05 — Domain Identity, Lifecycle and Policy Types

Status: In Progress

Implemented:

- Live source identities.
- Stream identity and cursor types.
- Log epoch and generation identity.
- SourceStreamKey validation.
- Lifecycle models.
- Integrity models.
- Demand ownership models.

Remaining:

- Reconcile implemented types against the authoritative WP-05 acceptance criteria.
- Identify any missing domain and policy contracts.
- Complete required verification.
- Record acceptance evidence.

Do not mark WP-05 Accepted until its formal criteria have been verified.

### WP-06 — Event, Delivery and Replay Contracts

Status: Partial. Independent contracts and the isolated real grouped transaction/
readback/rollback witness pass. Signed timestamp representability and broader
commit-fault/protected-cleanup verification remain open.

The immutable events, typed provenance, checked source cursor acceptance,
restart-safe epoch journal, exact versioned DTOs, protected replay and bounded
replay/follow/book-view cutover contracts remain implemented. The separate
filesystem journal audit and deterministic failure tests are retained.

On 2026-10-21 PostgreSQL connectivity succeeded using local libpq authentication.
All five live_canonical tables exist and are SELECT-accessible. Actual catalog
inspection replaces the former assumption that only historical schema-v9 tables
are available. No credential file was opened by the agent; no DDL or DML ran.

Implemented C06: exact relational row mapping and observed schema checks,
ordered grouped-level identity, defensive canonical decode, transaction write
plans, ambiguous outcome/retry contracts, bounded contiguous committed coverage,
protected reads and cleanup guards. Twelve new contract tests pass. The observed
SQL unsigned timestamp CHECKs cannot represent negative canonical timestamps;
the mapping explicitly rejects those without changing canonical validity.

The former read-only authorization gate is superseded for the isolated test DB.
Basic real transactions, exact fractional readback, duplicate rejection, atomic
rollback and fixture cleanup now pass. Production-driver commit faults and
concurrent protected cleanup remain unverified; no migration option is assumed
approved. The old schema proposal is retained as historical evidence.

Evidence: [independent contracts](evidence/wp06-independent-contracts.md),
[filesystem journal audit](evidence/wp06-epoch-journal-audit.md),
[connected C06 requirements, implementation and approval gates](evidence/wp06-c06-connected.md).

### WP-07 — Service, Management and Observation Contracts

Status: Contract implementation review pending; C12 production adapter decision OPEN.
Not Accepted; production adapter integration is not certified.

Started sequentially after C06 mapping/contract tests passed. Implemented narrow
ports, owner-specific dependency bundles, task/shutdown/update boundaries,
versioned JSON control framing/codec, trusted authorization, bounded idempotency,
operation completion evidence, resolved configuration/revisions/history/restore,
closed metrics/availability/health models, strategy/capability descriptors and
owned snapshots for all five views. Twenty-eight deterministic management,
configuration, strategy, resource and presentation tests pass.

C12 driver/runtime/TLS adapter choice is not inferred from illustrative library
lists or a successful psql connection. No new dependencies were added. The
implemented public ports remain independent of those concrete adapter types;
review/approve the remaining choice before the first real adapter consumer.

Final Live library 93/93, Live integration 363/363, doctest 1/1; combined
historical+Live integration 363/363. Default, historical, Live and combined
Live+historical+TUI all-target checks pass. Historical library remains 151 passed /
1 unchanged baseline failure: historical::merge::resources::tests::rejects_empty_streams.
Historical zero-stream behavior/test and existing WP-10 code were not modified.

Evidence: [contract ledger, constructor table, tests and limitations](evidence/wp07-contracts.md),
[exact final commands/logs](evidence/wp06-c06-wp07-verification/results-final.json),
[exact modified-file manifest](evidence/wp06-c06-wp07-verification/files.json).

### WP-10 — Canonical Event Router (superseded historical snapshot)

Status at that checkpoint (superseded): In Progress — Early Implementation

WP-10 implementation began ahead of the formal roadmap sequence.

This work is not evidence that WP-05 through WP-09 have been completed.

Implemented and tested independently:

- RouterRegistry.
- Destination registration and removal.
- Registry revision tracking.
- Destination queue management.
- Event and byte capacity accounting.
- Capacity reservations and ownership transfer.
- Asynchronous admission preparation.
- Required-destination fan-out preparation.
- Queue commitment.
- Complete and partial fan-out outcomes.
- AcknowledgementTracker.
- Delivery obligation preparation and activation.
- Transaction identity and membership validation.
- Transaction abort handling.
- FinalizationJournal.
- TrackingRecoveryCapacity.
- TrackingRecoveryReservation.
- TrackingRecoveryBatch.
- TrackingRecovery.
- Failed acknowledgement retention.
- Deterministic acknowledgement retry.
- Unresolved recovery outcome retention.
- Recovery-aware acknowledgement helper functions.

Existing tests have passed following the latest changes.

Implementation remains incomplete.

Known outstanding work:

- Integrate the repaired operational commit_tracked_with_recovery() into the production owner.
- Retain owned tracking failures and required delivery payloads through real recovery lifetime.
- Consolidate transitional commitment APIs.
- Prevent production routing from bypassing obligation tracking.
- Wire the tested StreamAdmissionOrder into production per-stream admission.
- Implement the canonical production EventRouterPort::admit() path.
- Integrate the router with its production runtime owner.
- Complete end-to-end routing and recovery tests.
- Verify concurrency, cancellation, shutdown, and backpressure behavior.
- Verify all WP-10 acceptance criteria.

Important:

Passing unit and integration tests demonstrates that tested components behave as expected.

It does not establish that the complete router is operational or production-ready.

## Implementation Architecture (historical; superseded where the WP-10 audit differs)

### Existing Routing Components

Primary implementation location:

`engine/src/live/routing/`

Relevant modules include:

- router.rs
- queue.rs
- destination.rs
- admission.rs
- acknowledgement.rs
- tracking_recovery.rs

Additional routing-related contracts and ports exist elsewhere under:

`engine/src/live/`

### Routing Lifecycle

The intended production lifecycle is:

1. Receive an EventBatch.
2. Resolve required destinations.
3. Prepare destination capacity.
4. Prepare delivery obligations.
5. Validate registry and transaction identity.
6. Reserve tracking recovery capacity.
7. Activate delivery obligations.
8. Commit destination queues.
9. Apply or retain delivery acknowledgements.
10. Preserve unresolved delivery responsibility.
11. Finalize completed or failed obligations.

The complete lifecycle is not yet connected through one authoritative production entry point.

### Delivery Guarantees

The implementation is designed around:

- No silent loss of required market events.
- Bounded resource consumption.
- Explicit backpressure.
- Destination-specific delivery obligations.
- Independent tracking of partial fan-out outcomes.
- Ownership-preserving capacity reservations.
- Explicit acknowledgement progression.
- Recovery responsibility for failed tracking updates.
- Per-stream ordering.

Not all guarantees have been verified end to end.

### Persistence Boundaries

PostgreSQL is reserved for optional market-data recording.

Application logs and operational event history remain local.

TrackingRecovery and FinalizationJournal currently provide in-process tracking and recovery mechanisms.

They must not be described as crash-durable without a verified persistence implementation.

## Implementation Workflow (historical; current policy is AUTONOMOUS_PLAN.md)

### Development Responsibilities

ChatGPT:

- Architecture interpretation.
- Implementation planning.
- Contract and invariant review.
- Acceptance criteria analysis.
- Independent code review.

Codex:

- Repository inspection.
- Rust implementation.
- Targeted refactoring.
- Compilation and test execution.
- Compiler-error resolution.
- Missing acceptance-test implementation.
- Reporting evidence and blockers.

Qwen/OpenCode:

- Optional repository inspection.
- Exact source extraction.
- Small mechanical modifications.

The user:

- Reviews implementation results.
- Approves architectural changes.
- Controls Git commits and merges.
- Confirms work-package acceptance.

### Autonomous Coding Policy

Codex may autonomously:

- Inspect relevant repository files.
- Implement approved work-package requirements.
- Modify files within the authorized scope.
- Run relevant formatting, compilation, and tests.
- Fix compilation errors.
- Fix failing tests without weakening assertions.
- Add tests required by acceptance criteria.

Codex must not:

- Change accepted architectural decisions without approval.
- Redesign unrelated modules.
- Remove required fault-tolerance guarantees.
- Introduce placeholder implementations as completed functionality.
- Delete or weaken tests merely to obtain a passing result.
- Modify historical behavior without explicit authorization.
- Run destructive Git operations.
- Commit, merge, or push without explicit authorization.
- Mark work packages Accepted without acceptance evidence.

Prefer completing the functionality already specified for a work package rather than repeatedly revisiting the same module to add known requirements.

### Verification Policy

Use the existing Cargo feature boundaries.

The Live test commands currently used are:

```bash
cargo test --locked --offline --no-default-features --features live --lib
