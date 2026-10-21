# WP-06 — Contract audit and blocked schema mapping gate

Date: 2026-10-21. Disposition: **Blocked; not Ready for Review and not Accepted.**

This is the retained **preimplementation** audit. Following the user-authorized
continuation, independent gaps are implemented and verified in
[wp06-independent-contracts.md](wp06-independent-contracts.md). Its requirement
mapping supersedes the independent missing-work entries below. The acknowledged
C06 database mapping gate remains BLOCKED; original baseline evidence is retained.

This is an audit and verification record, not an implementation completion report.
No Rust source, tests, Cargo manifests, lockfiles, historical behavior, or accepted
architectural decisions were changed. No Git commands were executed.

## Authoritative inputs

- `docs/live/17 — Implementation Roadmap.md`: sections 7, 7.1, WP-05,
  WP-06, WP-10, and sections 13–15. WP-06 is defined at lines 422–432;
  C03–C06 are defined at lines 282–285.
- `docs/live/02 — Domain Models and Contracts.md`: sections 4–6, 9,
  15–17, 18–20. These supply the relevant WP-05 identities, independent
  generations, cursor namespace, envelope, delivery, replay, and derived contracts.
- `docs/live/08 — Event Distribution and Unix Sockets.md`: sections
  23–25, including the snapshot/book-version/cursor boundary and ResumeAfterCursor.
- `docs/live/09 — Live Retention and Bootstrap Buffering.md`: protection
  ownership/expiry in section 8, replay/handoff in sections 23–24,
  restart recovery in section 30, and acceptance requirements in section 38.
- `docs/live/10 — Combined Streams and Currency Conversion.md`: section 6,
  derived identity and transformation provenance.
- `docs/live/11 — PostgreSQL Recording.md`: sections 7–10, particularly
  trade identity, grouped depth, full initialization, exact NUMERIC values,
  and global identity constraints with time partitioning.
- `.planning/live/STATUS.md` and `.planning/live/decisions.md`, especially
  DEC-004, DEC-006, DEC-009–018, DEC-024–029, DEC-031, DEC-034,
  and OPEN-003–005.
- Existing `.planning/live/evidence/wp06-*.md` records were located and relevant
  admission/dependency, cancellation, finalization, queue ownership, and resolution
  integration records inspected. These describe earlier component work, much of
  it now categorized as early WP-10. Current source and executed tests take
  precedence over their obsolete source extracts and historical test counts.
- Actual database DDL: `src/marketforge/database/schema.py`, `SCHEMA_VERSION = 9`.

No applicable AGENTS.md was found in the repository or its inspected ancestors.
Referenced WP-04A/B/C evidence names in STATUS.md are absent from the actual
planning tree; they were not treated as available verification evidence.

## Architectural blocker: C06 database mapping

The roadmap explicitly requires: “Confirm the actual database schema or mark
the database-specific mapping gate blocked; this does not authorize inventing
a replacement schema.”

The actual schema was found and inspected. Its market-data tables are:

- `live.trades` (generated database row identity and instrument foreign key).
- `live.l2_snapshots` and `live.l2_snapshot_levels`.
- `live.l2_updates` and `live.l2_update_levels`.

They do not provide the required `(StreamId, LogEpoch, offset)` identity,
BookGeneration, complete event provenance, or canonical event schema version.
There is no actual `depth_events` / `depth_levels` table pair or authoritative
canonical recording-identity registry in this DDL. A search of this schema found
no `stream_cursor`, `log_epoch`, `book_generation`, `provenance`, or `depth_events`.
The Rust `engine/src/live/recording/schema.rs` is a scaffold, not an alternative
implemented schema.

Document 11's proposed table structures are explicitly conceptual, not finalized
PostgreSQL DDL. Mapping canonical cursors onto generated row IDs would violate
DEC-016. Storing only level rows would lose the accepted logical-event unit.
Discarding provenance/generation fields would violate DEC-018 and DEC-034.
NUMERIC payload support alone does not solve any of those identity problems.

**Required unblock:** supply the approved recording schema/migration contract
that reconciles v9 with canonical stream/epoch/offset identity, grouped-event
identity and ordering, provenance/generation/version fields, and transactionally
enforced uniqueness. Alternatively, explicitly authorize designing that schema
migration. This run does not invent a substitute DDL or silently reduce C06.
A disposable database mapping witness is not claimed and no database was started.

This is the explicitly permitted blocked database-specific gate. It prevents
claiming full WP-06 completion. The independent contract gaps below also remain;
they must not be mistaken for implemented functionality just because this audit
stops at the schema gate.

## Existing functionality verified by source inspection

| Contract group | Existing implementation | Limits of that evidence |
|---|---|---|
| Identity and generations | `model/identity.rs`, `model/cursor.rs`; distinct StreamId, LogEpoch, connection/subscription/book/derived generations | Value types do not prove restart-persistent allocation or accepted-event ordering |
| Event unit | `model/envelope.rs`: LiveEvent owns the cursor; canonical Trade reused directly; LiveL2Group retains ordered levels, boundary, source operation and source metadata | One envelope can represent many levels, but no complete-initialization reconstruction witness exists |
| Event validation | `LiveEvent::try_new` rejects cursor/source mismatch and missing L2 book generation | Public fields and derived Deserialize bypass constructor validation; validation is not enforced by EventBatch |
| Batch ownership | `ports/routing.rs`: `Arc<LiveEvent>`, private `Arc<[SharedLiveEvent]>`, nonempty batch, one namespace, strictly increasing offsets | Strictly increasing is not proof of contiguous coverage; no serialization/storage DTO contract is frozen |
| Destination results | Distinct policies, stages, status and failure categories in `model/delivery.rs` | Domain values describe guarantees; they do not establish persistence |
| Delivery obligations | `routing/acknowledgement.rs`: inclusive range validation, bounded tracker, exact obligation/destination/range correlation and monotonic progression | These are existing contracts and state machines, not missing abstractions to recreate |
| Completion boundaries | Destination/policy-specific completion checks; durable publication/derived rejected; committed recording does not complete a durable obligation | No distributed socket/database/Parquet transaction is implied |
| Terminal ownership | FinalizationJournal, prepared obligations, recovery ownership, bounded resolution sink and receipt protocol | In-process ownership is not crash durability |
| Admission/fan-out | RouterRegistry, queues, event/byte capacity, reservations, cancellation/deadline primitives and independent destination outcomes | Production router integration remains WP-10 |
| Exact decimal primitives | Shared Decimal256 conversion and schema constants, precision 76 / scale 28 | Shared historical TradeBatchBuilder is not a full Live-envelope storage format |

Paths in this table are relative to `engine/src/live/`, except shared
Decimal256 code under `engine/src/storage/parquet/`.

## Requirement and acceptance ledger

| WP-06 requirement / acceptance criterion | Audit outcome and supporting evidence |
|---|---|
| Multi-level groups retain one cursor | Representable in LiveEvent/LiveL2Group; existing common fixture uses an empty Changes group. Missing named multilevel consumer/storage witness. Not fully verified |
| Rejected input consumes no accepted cursor | No source-owner acceptance/allocator contract is implemented; `routing/cursor.rs` is scaffold. Event construction errors alone do not establish this rule |
| Valid no-change depth outcomes follow approved rules | Shared DepthProcessingOutcome supports empty events; existing fixture accepts an empty Changes group. No documented/tested decision distinguishes valid sequence-only progress from an accepted empty event |
| Immutable payload ownership | Arc-based distribution and private batch storage exist. Event fields remain publicly mutable before sharing; mutation after unique Arc recovery is possible. No broader immutability enforcement claimed |
| Real WebSocket/REST/derived origin | EventProvenance requires connection/subscription generation fields; it cannot express their absence for non-WebSocket origins. Must freeze actual typed origin without fabricated transport metadata, per roadmap 7.1 |
| Derived consumer preserves source identity | `model/derived.rs` and `derived/provenance.rs` are scaffolds. Missing independent output identity/source-event references and transformation-provenance consumer witness |
| Invalid event inputs explicitly rejected | Identity/missing-generation checks exist. Deserialize and EventBatch do not call these checks; constructor invariants alone are insufficient. Timestamp consistency, sequence range, and canonical payload validation also need an explicit contract |
| Failure distinguishes accepted/committed/durable | Implemented and tested: contracts.rs stage distinctions; routing.rs `durable_recording_cannot_finalize_at_committed`, `required_recording_cannot_finalize_while_queued`, `durable_recording_finalizes_after_durable_stage` |
| Policy validation and delivery transitions | Existing `registration_rejects_durable_publication`, `registration_rejects_durable_derived_destination`, correlation/regression tests and prepared-spec validation. Preserve these rather than introduce a parallel tracker |
| Failure-injecting sink witness | Delivery failure/partial-fanout tests exist; shared `tests/live/support/sink.rs` is scaffold. No complete WP-06 consumer-witness suite exists |
| Replay preserves original identity | `publish/replay.rs`, retention reader/handoff modules and `ports/retention.rs` are scaffolds. Missing round-trip witness retaining payload, cursor, generation and provenance |
| Range endpoint semantics | DeliveryCursorRange explicitly inclusive. Replay ResumeAfterCursor and a separate inclusive read operation are not implemented; retain their distinction |
| Protection handles and lifecycle | `retention/protection.rs` scaffold. Missing owner identity, monotonic safely-consumed frontier, expiry/failure, idempotent release and finite-resource contract |
| Accepted/committed/durable retention positions | No RetentionCommit/position contract implemented. Must distinguish memory acceptance, committed storage and crash durability |
| Atomic replay/follow cut | Retention handoff and publisher replay scaffolds. Missing bounded capture-before-cut, replay-through-boundary and gap/duplicate/expiry/generation handling contract and simulated Bootstrap witness |
| Book-view cutover | No contract for consistent BookGeneration/BookVersion/StreamCursor plus configuration/metadata revisions. Document 08 section 23 requires these identities and an ordered capture boundary |
| Restart-safe cursor allocation | LogEpoch is a value type; no persistent namespace/checkpoint recovery interface or test implementation was found. Cannot claim restart safety from the in-memory acknowledgement counter |
| Full initialization reconstructs without older generation | Shared boundary enum expresses Initialization, but Live initialization module and depth tests are scaffolds. Full state must be emitted rather than a BookStore snapshot diff. Missing witness |
| Exact encoding and versioned DTOs | Shared Decimal256 primitives are available. Live wire/storage DTOs and compatibility fixtures are not implemented; Serde derives do not constitute a frozen format (OPEN-004) |
| Actual recorder encoding/schema probe | **Blocked** by actual v9 DDL mismatch described above. No approved migration or complete schema mapping is present |
| Public consumers and feature isolation | Existing Live-only and base-library checks pass. They do not establish missing replay/storage/derived consumers |
| Historical compatibility | Historical-only build passes. Library tests have a baseline failure described below; compatibility is not fully verified |

The scaffold `engine/tests/live/{depth,retention,recording,derived}.rs` files
contain no executable tests. Their registration is not evidence of coverage.
No new tests were added in this blocked audit.

## Verification

All commands executed from `engine/`, with locked offline resolution where
applicable. No dependency changes or network access were needed.

| Command | Exit | Result |
|---|---:|---|
| `cargo fmt --check` | 0 | Pass, no output |
| `cargo check --locked --offline --no-default-features --features live` | 0 | Pass; 6 existing dead-code/unused-field warnings in router/recovery code |
| `cargo test --locked --offline --no-default-features --features live --lib` | 0 | 72 passed, 0 failed, 0 ignored, 0 filtered |
| `cargo test --locked --offline --no-default-features --features live --test live_suite` | 0 | 272 passed, 0 failed, 0 ignored, 0 filtered |
| `cargo check --locked --offline --lib --no-default-features` | 0 | Pass |
| `cargo check --locked --offline --no-default-features --features historical` | 0 | Pass |
| `cargo test --locked --offline --no-default-features --features historical --lib` | 101 | 151 passed, 1 failed, 0 ignored, 0 filtered |

Complete test and additional build output, including executed test names and
warnings, is retained in `wp06-contract-audit/verification-1.log` through
`verification-5.log`. `wp06-contract-audit/verification.json` records the exact
commands and exit codes. The fmt and initial Live check outputs were captured
in the tool transcript; they were not repeated just to generate log files.

### Historical baseline failure

`historical::merge::resources::tests::rejects_empty_streams` fails at
`engine/src/historical/merge/execution/shared/resources.rs:180`:

```text
assertion failed: estimate_merge_stream_resources(0, &config()).is_err()
```

The function at that same file's lines 46–52 explicitly returns
`Ok(ResourceRequirements { memory_bytes: 384 * MIB, scratch_bytes: 0,
open_files: 0 })` for zero streams. This is a verified source/test mismatch,
observed before any Rust edits in this run. It is not attributed to a WP-06
regression. Resolving the historical zero-stream policy is outside this
contract audit; neither the test nor historical behavior was changed.

### Separate WP-10 finding

`FanoutCommitmentCoordinator::commit_tracked_with_recovery()` in
`engine/src/live/routing/router.rs` still drops the recovery reservations,
aborts prepared obligations, and rejects with ObligationCommitment after all
precommit checks pass (lines 801–819). This confirms the existing STATUS.md
WP-10 finding. It is not treated as missing WP-06 router functionality and
was not modified.

## Changed-file inventory and next review boundary

- `.planning/live/evidence/wp06-contract-audit.md`: this audit and blocker record.
- `.planning/live/evidence/wp06-contract-audit/verification.json`: command receipts.
- `.planning/live/evidence/wp06-contract-audit/verification-1.log`
- `.planning/live/evidence/wp06-contract-audit/verification-2.log`
- `.planning/live/evidence/wp06-contract-audit/verification-3.log`
- `.planning/live/evidence/wp06-contract-audit/verification-4.log`
- `.planning/live/evidence/wp06-contract-audit/verification-5.log`
- `.planning/live/STATUS.md`: WP-06 audit disposition and evidence link.

No architectural changes were made. DEC-009 Decimal256 precision/scale,
canonical historical payloads, existing custom concurrency infrastructure,
Cargo feature boundaries, and WP-10 ownership remain intact. No production
readiness, crash durability, database integration, or WP-06 acceptance is claimed.

After the schema gate is resolved, a coherent implementation order is event
validation/origin and consumer DTOs; accepted-event allocation and no-change
rules; replay/protection/cutover contracts; then all five consumer witnesses
and the complete acceptance/compatibility gates. This is a remaining-work
ledger, not authorization to begin WP-07 or WP-10.
