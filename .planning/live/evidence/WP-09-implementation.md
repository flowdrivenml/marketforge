# WP-09 — Configuration acceptance implementation

**Status: Partial — Not Accepted.** 2026-10-21. This matrix supersedes the
previous candidate/journal-only progress report. Mandatory compilation gaps below
prevent ReadyForReview; interfaces and deterministic protocol fixtures do not
establish real-exchange certification or complete runtime reconciliation.

Authority: Document 17 WP-09; Document 03 §§9–10, 14, 16–17; Document 13
configuration management/reconciliation; frozen `ports/configuration.rs` and
`ports/protocol.rs`. Accepted decisions remain unchanged.

## Requirement → implementation → verification

Paths below are relative to `engine/src/live/config/`. Test names are in
`engine/tests/live/wp09_service.rs` unless otherwise stated.

| Requirement / outcome | Implementation | Deterministic verification / remaining gap |
|---|---|---|
| TOML/schema loading — partial | `loader::{read_static,parse_static,read_desired,parse_desired}` | `wp09_toml_real_parser_versions_limits_unknown_fields_and_inheritance`: canonical v1, malformed/duplicate fields, unsupported versions, unknown fields, byte/depth limits, omitted inheritance. Draft file mapping remains blocked below. Static parsing is not capability approval. |
| Explicit shallow inheritance, immutable settings/origins — implemented | Existing `DesiredConfiguration::resolve`, `effective::EffectivePlan` private immutable fields | `wp09_compiler_verified_sources_affected_owners_and_exact_hot_boundaries`; existing `wp09_configuration::all_hot_global_categories_and_explicit_inheritance_have_no_base_effects`. Explicit settings survive changed defaults; global budgets remain singular. |
| Metadata/capability/economic/resource compilation — partial | `compiler::ConfigurationCompiler` implements frozen `ConfigurationCompilerPort`, reuses `build_candidate`, shared `InstrumentCatalog`, `ExchangeProtocolPort::compile`, capability/heartbeat/depth validation | Known catalog + verified trade fixture succeeds; absent catalog, unverified evidence and missing derived compiler fail explicitly. Production metadata/protocol/derived semantics and resource feasibility remain unverified; see blockers. |
| Source/derived DAG — implemented structurally | Existing iterative bounded `validation::dependency_order` | Existing `wp09_configuration` tests: unknown input, duplicate merge refs, cycles, long chain, nodes/edges. This does not certify derived economics. |
| Complete diff/affected-resource plan — partial | `diff::between`, compiler impact calculation against resolved settings | `wp09_diff_roundtrip_global_categories_and_unrepresentable_fields_fail` (global categories exercised); verified-source test proves inherited recording/metrics targets and explicit-owner isolation. Full derived/dependency closure and unrepresentable frozen changes remain blocked. |
| Optimistic revisions, no-op and restore as NEW revision — implemented for supported configurations | Existing sole `FileRevisionJournal`, worker CAS, manager restore | `wp09_service_durable_acceptance_noop_conflict_history_restore_restart_and_status`; `wp09_worker_concurrent_cas_has_exactly_one_durable_winner`: exactly one winner, loser conflicts, history restore gets new ID/parent, no-op preserves ID. |
| Atomic durable acceptance/recovery, no premature publication — implemented for supported configurations | Existing checksummed/locked file journal with temp sync/rename/directory sync, `worker::ConfigurationWorker`, `manager::accept` | Existing six journal unit tests cover all nine write fault points and corruption/recovery; composed service failure test proves no accepted hand-off, unchanged on-disk revision, fenced uncertain owner. Validation alone does not persist. |
| Bounded frozen store adapter and lifecycle — implemented | `ConfigurationWorker` implements `ConfigurationStorePort`, one native filesystem thread, bounded channel + shared count/byte reservations through queued/running/retained replies; existing bounded timer/cancellation | `wp09_worker_bounded_queue_cancellation_retains_obligations_and_drain`, `wp09_worker_deadline_during_compile_and_dropped_waiter_do_not_commit`, `wp09_worker_cancel_after_commit_entry_reports_unknown_and_retains_capacity`, `wp09_worker_cancel_deadline_generation_and_shutdown_are_effect_free`. Shutdown fences admission, drains charges and waits for journal lock release. |
| Management/history/query/diff/status — implemented for supported configurations | `manager::ConfigurationManager`, no duplicate accepted-state cache or journal | Composed service test exercises all queries/restore; `wp09_status_preserves_old_applied_and_adopted_while_desired_advances` preserves actual observed settings/revisions. No applied/adopted state is invented. |
| All approved hot categories/boundaries — partial | Existing frozen `ConfigChange::boundary`; compiler preserves explicit recording-default boundary, removes net no-ops, resolves affected owners; manager rejects ServiceRestart interactively | Global/source tests exercise Immediate, NextMeasurement, NextBatch, FutureAdmission, StreamTransition and unsupported budgets. Derived generation planning/activation and static native updates are not certified. Existing candidate tests cover global updates and atomic source registration failures; complete derived/update-category certification remains outstanding. |
| Invalid proposal/persistence failure has zero runtime mutation; independent demand preserved — acceptance side implemented | Manager has compiler/store only, returns `AcceptedConfiguration` after durable CAS; no apply/source/router/network/DB ports | Composed validation/rejection/failure tests; changing persistent desired state cannot mutate independent runtime claims here. Actual independent-demand reconciliation test belongs to WP-11/WP-24 and remains pending, not claimed complete. |

## Precise blockers and ownership

1. **Draft schema mapping (WP-09, consuming WP-05/07/12 contracts).** Inspected
   all 48 actual TOML inputs read-only: global, source/derived lists, 15 profiles,
   15 trade specs and 15 depth specs. The global draft includes socket paths,
   retention enable/ratio/path, publication enable and exporter settings absent
   from frozen `DesiredConfiguration`; required DTO task/FD budgets, recovery
   attempts and timing sampling are absent from that draft. Sources identify raw
   instruments; derived drafts omit bounded buffer fields required by the DTO.
   A versioned authoritative mapping/default contract is needed. The loader
   rejects these as canonical DTOs instead of dropping settings or fabricating
   defaults. WP-09 still owns the mapping implementation after resolution.
2. **Authoritative economics/classification (WP-05/07 C01, OPEN-001).** There is
   no frozen metadata resolver port. The existing shared catalog can resolve a
   complete `InstrumentIdentity`, but cannot resolve raw draft identifiers or
   establish missing perpetual family/contract economics. Unknown identities
   and unresolved classification return `Dependency`. No new metadata trait or
   accepted classification policy was invented.
3. **Static/native and derived compiler inputs (WP-07/12/23 dependency; WP-09
   retains acceptance ownership).** Frozen `ExchangeProtocolPort::compile`
   receives a resolved source key, not loaded profile/normalization documents.
   No production implementation yet consumes that bundle. No approved pure
   derived-economic compiler exists; all nonempty derived candidates return
   `Dependency` at `derived.economic_compiler`, including otherwise structural
   valid DAGs. Conversion/merge semantic acceptance, immutable native acquisition
   artifacts and dependent-generation impact closure remain incomplete in
   WP-09; WP-23 runtime execution is not a substitute for pure validation.
4. **Frozen diff expressiveness (WP-07 contract resolution; WP-09 implementation
   thereafter).** `ConfigChange` cannot represent global recovery/shutdown
   replacement or profile removal. `diff::between` explicitly returns
   `UnsupportedCapability`; order-only restores also fail explicitly instead of
   minting semantic revisions. Do not silently widen frozen DTOs. Complete
   history diff/restore of such configurations remains a mandatory WP-09 gap.
5. **Resource feasibility (WP-09 consuming approved owner/compiler costs).**
   Structural settings, DAG and loader/worker bounds are enforced. This is not
   complete feasibility validation of connections/tasks/FDs/native payloads,
   global retained bytes or derived buffers against owner allocations. Missing
   cost/capability inputs must be supplied through approved contracts; do not
   guess exchange limits or treat current worker limits as runtime admission.

WP-24 executes committed plans through `ConfigurationApplyPort`, integrates
source/destination owners and supplies observed applied/adopted status. WP-25
binds Control transport/authorization/idempotency. Neither belongs in this
service. WP-10 remains ReadyForReview — Not Accepted; its ports/code are unchanged.
There is no end-to-end daemon or exchange certification claim.

## Verification and scope

- Focused `cargo test --locked --offline --no-default-features --features live
  --test live_suite wp09_`: exit 0, **19 passed**, nonzero executed tests.
- `./scripts/verify-live.sh full`: exit **0**, all five gates; **137 library,
  523 integration passed / 2 opt-in ignored, 1 doctest**. Final receipts:
  `engine/target/live-verification/20261021T121532Z-2/`.
- From `engine/`, each command exits **0**:
  `cargo check --locked --offline --no-default-features --features live,historical --lib`;
  `cargo check --locked --offline --no-default-features --features live,monitor-tui --lib`;
  `cargo check --locked --offline --no-default-features --lib`;
  `cargo check --locked --offline --no-default-features --features historical --lib`.
  These are compilation checks, not historical behavioral test claims. The
  earlier historical `rejects_empty_streams` baseline failure was not rerun or
  changed in this WP-09-only campaign.
- Fixtures: sanitized canonical default JSON/TOML, temporary catalog and real
  journal directories, deterministic verified/unverified protocol port, manual
  clocks/timers, channel gates/barriers, local persistence fault fixtures. No
  arbitrary test sleeps, database or external exchange access.
- Dependency change is justified by actual TOML parsing: Live-only optional
  `toml_edit =0.25.17`, parse feature, using already cached/locked packages.
  Lockfile change adds only the crate's direct dependency edge; no versions moved.
- Existing unrelated warning: `routing/tracking_recovery.rs` retained reservation
  field appears unread. WP-10 is outside scope; the warning is not suppressed.
- Changed configuration compiler/loader/diff/effective/manager/store visibility,
  added bounded worker, config exports and composed tests; dependency manifests
  and this evidence/status. No Python, historical execution, accepted decisions,
  schema, ignored TOML, production data or remote writes. Preserve pre-existing
  untracked `scripts/run-autonomous-codex.sh` outside the checkpoint.

Historical evidence: earlier candidate/DAG/journal checkpoint receipts remain in
Git history and `engine/target/live-verification/20261021T032801Z-2` (130 library,
376 integration, 1 doctest). Those counts and the old “service pending” statement
are superseded by this report. **Next authorized package: none; stop after WP-09.**
