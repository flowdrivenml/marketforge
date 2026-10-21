# MarketForge Live — Implementation Status

## Work Package Status

| Package | Scope | Status | Evidence |
|---|---|---|---|
| WP-00 | Establish the actual baseline | Accepted | WP-00A-baseline.md, WP-00B-feature-isolation.md |
| WP-01 | Create the module skeleton and working records | Superseded | — |
| WP-02 | Make shared depth/domain models available safely | Accepted | WP-02A-depth-event-sharing.md, WP-02B-trade-sharing.md |
| WP-03 | Extract compatible storage, arithmetic and UI primitives | Accepted | WP-03A-DWP-03B-parquet-contract.md, WP-03D-shared-parquet-primitives.md, WP-03E-shared-trade-builder.md |
| WP-04 | Finalize dependency and feature wiring | Accepted | WP-04D.md — feature/build verification |
| WP-05 | Implement domain identity, lifecycle and policy types | Partial — reconciliation underway | WP-04A, WP-04A.1–A.3, WP-04B, WP-04B.1 — identity, validation, lifecycle, demand |
| WP-06 | Freeze and implement event/delivery/replay contracts | Partial — basic real C06 witness passes; signed timestamp and fault gates remain | [Independent contracts](evidence/wp06-independent-contracts.md), [Observed C06 mapping and blockers](evidence/wp06-c06-connected.md) |
| WP-07 | Freeze service ports, management and observation surfaces | Contract implementation review pending; C12 adapter decision OPEN | [Contract implementation and witnesses](evidence/wp07-contracts.md) |
| WP-08 | Implement foundations and real native observability | ReadyForReview | [Implementation ledger](evidence/WP-08-implementation.md) |
| WP-09 | Implement the Configuration System | Partial — candidates/DAG/journal verified; semantic compiler and service pending | [Implementation ledger](evidence/WP-09-implementation.md) |
| WP-10 | Implement the canonical event router | Partial — ordered tracked fan-out integrated; production owner pending | [Implementation ledger](evidence/WP-10-implementation.md) |
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

### Autonomous continuation — 2026-10-21


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

### WP-10 — Canonical Event Router

Status: In Progress — Early Implementation

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

## Implementation Architecture

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

## Implementation Workflow

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
