# WP-06 — Independent event, delivery and replay contract implementation

Date: 2026-10-21. **Independent WP-06 scope: Ready for Review.**
**Overall WP-06: BLOCKED at the acknowledged database-specific C06 mapping gate.**
No work package is marked Accepted. The user owns acceptance.

## Authorization, sources and scope

This implements the independent remaining-work ledger in
`wp06-contract-audit.md`, following the user's explicit continuation instruction.
The C06 schema blocker does not stop independent implementation.

Authoritative sources remain Documents 02, 08–11 and 17, shared canonical/depth
types, `.planning/live/decisions.md`, and the audit's source inventory. No applicable
AGENTS.md was found during the audit. Existing source, not obsolete evidence
extracts, controlled reuse of delivery/tracking contracts.

Canonical payloads, StreamId/LogEpoch/generation identities, existing obligation
tracking, bounded queues, acknowledgement progression, FinalizationJournal,
TrackingRecovery and custom concurrency infrastructure are reused. No concrete
WP-10 router/admission/recovery implementation was edited. No historical source,
test expectation, PostgreSQL schema, manifest, dependency or lockfile was changed.
No Git commands, commits, pushes, external services or network access were used.

## Implemented contract decisions and public consumers

### Event ownership, validation and actual origin (C03)

Owner: source or derived event producer. Consumers: existing EventBatch and
destinations, EventCodec, retention/replay, recorder encoding and derived witnesses.

`LiveEvent` remains the canonical Live envelope and `Arc<LiveEvent>` remains shared
ownership. Its fields now live in one private container exposed only through
shared Deref. There is no DerefMut. Existing read expressions such as
`event.cursor` remain valid, including all existing WP-10 consumers. Explicit
`into_fields()` creates mutable construction values; `TryFrom<LiveEventFields>`
and `try_new()` validate a newly constructed event. No second accepted-event
abstraction, cursor, financial payload, or metadata copy was added.

`LiveEvent::check() -> Result<(), LiveEventError>` and custom Deserialize reject
invalid source identity, empty identity, missing L2/unexpected trade book generation,
canonical trade/level validation failure, conflicting supplied timestamp/native
identity copies, reversed native sequence ranges, mixed-instrument groups,
duplicate initialization levels and initialization deletions. EventBatch revalidates
at its boundary. Native exchange continuity, deduplication and stale transport
completion fencing still precede acceptance in their responsible processors;
this package does not implement those processors.

`EventOrigin` distinguishes WebSocket generations, REST, and typed derived
provenance. REST needs no connection/subscription placeholders. Derived output
retains the source cursor and has an independent output stream/cursor, a
DerivedStreamId/generation, transformation/order revisions, and up to 32 exact
conversion steps with reference-event cursors. It does not overwrite source
values or fabricate historical dataset/task IDs. Reference chains identify
immutable prior events/definitions rather than embedding unbounded histories.
Transport generation labels are provenance in their source-owner context, not
globally ordered historical-coverage claims. A source-owner restart gets a new
persisted log namespace; a reconnect using the same owner does not.

`LiveL2Group::from_book()` emits the full initialized BookStore, including levels
unchanged relative to an older generation. It does not use the shared snapshot
diff as initialization output. Deterministic side/price iteration produces the
complete image; accepted group order is then preserved through every DTO/consumer.
An empty initialized book and a valid empty Changes event remain representable.

RetainedSize includes all newly owned derived identifiers, conversion-vector
capacity and nested strings, preserving existing queue byte-capacity accounting.

### Accepted-event cursor allocation (C04/C05 boundary)

Owner: one authoritative serialized source/derived owner, before EventRouterPort.
Public API: `EventAcceptor::start(&mut dyn EpochStore, StreamId)`;
`accept(LivePayload, EventProvenance, EventTiming) -> Result<Arc<LiveEvent>, AcceptanceError>`.

- Validation precedes offset advancement. Rejected input consumes no offset.
- A valid no-change depth message is **one accepted empty logical group**. Its
  source metadata/sequence progress remains replayable. It consumes one cursor,
  independent of the number of changed levels. Exchange duplicates/rejected
  source messages must not be sent to this acceptance boundary.
- Checked increment accepts offset u64::MAX once, then reports exhaustion.
  Downstream delivery failure never rolls back or reuses an accepted identity.
- `EpochStore::fresh_epoch()` explicitly requires serialized persistent non-reuse
  before namespace exposure. MemoryEpochStore is labeled a nondurable witness.
- FileEpochStore, isolated in the foundation filesystem adapter rather than
  the domain model, uses a stable sidecar file lock, a versioned bounded journal,
  atomic replacement, file sync and parent-directory sync. A new owner gets a
  fresh persisted epoch and starts offset zero. It never claims to reconstruct
  old committed event coverage or safely continue an unverified old offset.
- IO ambiguity fences further namespace allocation on that store. Unsupported,
  malformed or detected missing journals fail closed; epoch counters cannot wrap.
  The store can allocate namespaces for multiple independent stream owners.
- The registry remains responsible for one authoritative source owner. Journal
  storage must be retained, not rolled back or deleted while its identities can
  survive. This is not a replicated/host-loss-safe identity service or a runtime
  supervision implementation.

### Replay, retention positions and protection (C05)

Owner: retention owner; consumers: publisher/Bootstrap or an internal bounded
consumer. Public values are in `model/replay.rs`; the actual bounded read boundary
is in `ports/retention.rs`.

- ReplayRange has an inclusive start/end and optional open end. `resume_after`
  is a distinct checked operation. Namespace mismatch, reversed endpoints,
  exhaustion, unavailable coverage and cursor gaps are explicit errors.
- Replay pages require contiguous original cursors, not merely increasing offsets.
  Event/byte limits and the accepted frontier are validated. Continuations start
  at the checked next cursor. A checked end-of-available result cannot hide an
  earlier missing event. At u64::MAX there is no wrapped continuation.
- RetentionPositions distinguishes verified contiguous accepted, committed and
  durable positions; each must share a namespace and satisfy its boundary order.
  Memory acceptance is not labeled committed or durable.
- ProtectionLease owns consumer identity, token/generation, range, current
  protected frontier, safely-consumed acknowledgement, finite capacity, monotonic
  expiry and maximum lifetime. Admission requires finite verified existing
  coverage and sufficient initial event/byte reservation, including range
  cardinality. The storage owner must pin that coverage atomically with admission.
- Progress cannot regress or exceed the protected frontier. Delivery alone never
  advances the safely-consumed frontier. Renewal fences stale tokens and respects
  maximum lifetime. Release and cancellation are idempotent for the current owner.
  Expiration, cancellation, storage invalidation and resource exhaustion invalidate
  the guarantee; none means successful consumption. Individual overlapping
  protections remain independent. A finite lease cannot authorize an open read.
- Protection deadlines use caller-supplied monotonic Instant observations. No
  arbitrary sleeps, background expiry task, or unverified restart restoration is
  introduced. A later retention owner must persist a separate restoration record
  before promising restart-surviving leases.
- RetentionReaderPort uses the existing RoutingFuture shape and EventBatch type,
  a checked ReplayReadRequest with owner/token/range/count/byte limits, and typed
  outcomes. RetentionReadFailure retains the exact request, including affected
  namespace and reservation. Storage corruption/unavailability/spill/durability
  errors remain distinct from missing canonical delivery positions.

### Replay/follow and book-view cutover (C05)

`ReplayCutover::new(first, cut, book_generation, capacity)` is a bounded consumer
state machine, **not a production router**. The stream owner must install capture
and obtain its cut inside one serialized operation. A deterministic serialized
source-owner witness demonstrates that precondition.

Replay consumes every cursor through the inclusive cut while subsequent live
events are captured. Pre-cut repeats are replay-owned and do not appear again
in follow output. Missing positions, wrong namespaces/generations, cancellation
and event/byte overflow prevent completion. Failed/cancelled captures explicitly
transfer retained responsibility through `into_unresolved()`; a rejected capture
borrows its input so the caller retains ownership.

`follow_protected()` requires active owner/token-matched protection covering the
whole replay interval, in addition to completed replay. There is no public
unprotected replay-completion bypass. Safe consumption and protection release
remain separate from switching to follow mode.

BookViewBoundary contains a complete initialization **view**, accepted cursor
boundary, BookGeneration, separate typed BookVersion and configuration/metadata
revisions. It is not a fabricated accepted source event at an old cursor.
`for_book()`, `consume_view()` and `follow_view()` use the same ordered capture
state machine; a generation change invalidates the view and requires a fresh
handshake. A replay-mode capture cannot call the view-only completion operation.

### Versioned exact DTOs and available C06 encoding probe

`EventRecordV1`, PayloadDtoV1, TradeDtoV1 and LevelDtoV1 are explicit logical
serialization DTOs. EventCodec uses version 1 with bounded JSON interchange,
including exact decimal **strings**. Numeric JSON decimal tokens, exponents,
unrepresentable scale, unknown top-level fields, unsupported versions and invalid
canonical payloads are rejected. NULL and numeric zero remain different values.
The independently written golden fixture pins the full field mapping, opaque
native IDs, full u64 identities, optional fields and preserved decimal scale.

This is not a Rust-memory-layout encoding, a PostgreSQL row mapping, or the
publisher's final binary frame protocol. Domain Serde alone is not the persisted
format; consumers use the explicit versioned record and codec. A future change
to its embedded canonical metadata/origin mapping must be reviewed as a V1
compatibility change or use a new version. Shared Decimal256 precision 76 /
scale 28 is unchanged.

## Acceptance evidence and consumer witnesses

There are **51 new integration tests**, **3 new library boundary tests** and
**1 compile-fail immutability doctest**. Existing 272 Live integration tests remain
present and pass. All new integration tests are in `live::wp06_contracts`.

| Requirement / acceptance criterion | Supporting named evidence |
|---|---|
| One cursor for a multilevel group | `one_multi_level_event_consumes_one_cursor`, `recorder_encoding_probe_has_one_parent_and_ordered_level_identities` |
| Rejection and valid no-change rules | `rejected_input_consumes_no_offset_and_no_change_is_one_event`; library `last_offset_can_be_accepted_once_and_rejection_preserves_it` |
| Complete initialization independent of older generation | `full_initialization_reconstructs_without_previous_generation`, `initialization_rejects_duplicate_or_deleted_levels` |
| Original replay identity and provenance | `protected_retention_replay_roundtrip_and_ownership`, `grouped_depth_codec_preserves_native_metadata_and_order`, `parquet_retention_roundtrip_preserves_complete_envelope` |
| Failure-injecting sink | `failure_injecting_sink_distinguishes_admission_from_persistence`, plus existing independent partial-fanout/acknowledgement tests |
| Accepted / committed / durable distinguishable | `accepted_committed_and_durable_positions_are_distinct_and_checked`; existing `durable_recording_cannot_finalize_at_committed`, `required_recording_cannot_finalize_while_queued` |
| Exact recorder encoding probe, available without DDL | `recorder_encoding_probe_has_one_parent_and_ordered_level_identities`, exact trade/level DTO and golden-fixture tests; SQL mapping remains BLOCKED |
| Derived provenance consumer | `derived_provenance_consumer_preserves_source_and_exact_reference`, `duplicated_native_identity_fields_cannot_disagree`, `typed_rest_has_no_fabricated_connection` |
| Restart-safe identity / checked exhaustion | `file_epoch_restart_prevents_cursor_reuse`, `restart_epoch_exhaustion_is_explicit`, `failed_epoch_persistence_fences_owner`, `lost_epoch_journal_does_not_silently_reinitialize_identity`; library namespace/offset exhaustion tests |
| Protected replay / expiry / cancellation / ownership | `protection_checks_owner_monotonic_consumption_and_idempotent_release`, `overlapping_protections_keep_independent_ownership`, `expired_protection_never_reports_completion_or_allows_resume`, renewal/capacity/finite-read/cancellation tests |
| Gap-free bounded replay/follow cut | `bounded_atomic_replay_follow_cut_keeps_every_cursor_once`, `serialized_owner_installs_capture_at_the_same_cut_it_reports`, protected-handoff/expiry/generation/capacity/max-cursor tests |
| Book image publication cut | `book_view_capture_uses_cursor_boundary_without_fabricated_source_event` and complete reconstruction witness |
| Exact format / version / invalid input | `v1_golden_fixture_preserves_exact_scale_optional_fields_and_u64_identities`, `exact_codec_rejects_invalid_decimals_and_invalid_decoded_payloads`, codec limits/version tests and custom-deserialization tests |
| Immutable accepted event | Compile-fail example on LiveEvent; fields accessible only through shared Deref |
| Failure context and honest coverage | `retention_failure_retains_exact_request_context`, `end_of_available_data_does_not_hide_an_earlier_gap`, `cursor_page_requires_contiguity_not_merely_increasing_order` |

The Parquet witness writes a versioned encoded record in a binary Arrow column
and reads it through the actual shared Arrow/Parquet dependencies. It proves an
available storage consumer preserves the complete envelope. It is not presented
as a production segment/manifest format or a WP-20 implementation.

## Verification

All final commands executed from `engine/`, offline and locked where applicable,
using rustc 1.94.1. Complete output, test names, warnings, exit codes, start times
and elapsed times are in `wp06-independent-contracts/verification.json` and
`verification-01.log` through `verification-12.log`. Final layering review moved
only the concrete epoch filesystem adapter into foundation, keeping domain
contracts free of concrete file operations. The complete matrix was reverified:
`verification-final.json` and `verification-13.log` through `verification-24.log`
contain the final results below. All original logs are retained.

| Command | Exit / result |
|---|---|
| `cargo fmt --check` | 0, pass |
| `cargo check --locked --offline --no-default-features --features live` | 0, pass |
| `cargo test --locked --offline --no-default-features --features live --lib` | 0; 75 passed, 0 failed, 0 ignored/filtered |
| `cargo test --locked --offline --no-default-features --features live --test live_suite` | 0; 323 passed, 0 failed, 0 ignored/filtered |
| `cargo test --locked --offline --no-default-features --features live --doc` | 0; 1 passed, 0 failed |
| `cargo check --locked --offline --lib --no-default-features` | 0, pass |
| `cargo check --locked --offline --no-default-features --features historical` | 0, pass |
| `cargo test --locked --offline --no-default-features --features historical --lib` | 101; 151 passed, 1 baseline failure, 0 ignored/filtered |
| `cargo check --locked --offline --no-default-features --features live,historical` | 0, pass |
| `cargo check --locked --offline --no-default-features --features live,monitor-tui --bin marketforge-live-tui` | 0, pass |
| `cargo check --locked --offline --no-default-features --features historical,monitor-tui --lib` | 0, pass |
| `cargo check --locked --offline --no-default-features --features historical,live,monitor-tui` | 0, pass |

Existing router/recovery dead-code warnings remain visible and unsuppressed.
The historical failure remains exactly
`historical::merge::resources::tests::rejects_empty_streams`: the historical
estimator returns success for zero streams while its test expects an error.
Both source and test were left unchanged, as explicitly instructed. Historical
build compatibility passes; the historical test gate is not claimed fully green.

During development, a new corruption-context witness initially reported
49 passed / 1 failed because the fake reader mapped corrupt storage bytes to
InvalidEvent instead of SegmentCorrupted. The witness's failure classification
was corrected; its assertion was preserved. Intermediate compiler errors for
the private validation-module import and ambiguous File::by_ref were corrected.
Two initial Cargo invocations from the repository root failed before building;
all final verification above uses the required `engine/` directory. Those
development failures remain in the tool transcript; no failed assertions or
existing tests were deleted to obtain the final results.

## Exact changed files

Rust implementation:

- `engine/src/live/foundation/filesystem.rs` (epoch persistence adapter)
- `engine/src/live/foundation/mod.rs` (narrow FileEpochStore facade)
- `engine/src/live/model/envelope.rs`
- `engine/src/live/model/acceptance.rs` (new)
- `engine/src/live/model/replay.rs` (new)
- `engine/src/live/model/codec.rs` (new)
- `engine/src/live/model/memory.rs`
- `engine/src/live/model/mod.rs`
- `engine/src/live/ports/routing.rs`
- `engine/src/live/ports/retention.rs`

Tests/fixtures:

- `engine/tests/live/wp06_contracts.rs` (new)
- `engine/tests/live/fixtures/wp06-event-v1.json` (new, independent golden fixture)
- `engine/tests/live/mod.rs`
- `engine/tests/live/support/live_events.rs` (typed WebSocket origin migration)

Documentation/evidence:

- `.planning/live/STATUS.md`
- `.planning/live/evidence/wp06-contract-audit.md` (historical-audit cross-reference)
- `.planning/live/evidence/wp06-independent-contracts.md` (this record)
- `.planning/live/evidence/wp06-c06-schema-proposal.md` (proposal only)
- `.planning/live/evidence/wp06-independent-contracts/verification.json`
- `.planning/live/evidence/wp06-independent-contracts/verification-01.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-02.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-03.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-04.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-05.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-06.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-07.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-08.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-09.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-10.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-11.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-12.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-final.json`
- `.planning/live/evidence/wp06-independent-contracts/verification-13.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-14.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-15.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-16.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-17.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-18.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-19.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-20.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-21.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-22.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-23.log`
- `.planning/live/evidence/wp06-independent-contracts/verification-24.log`
- `.planning/live/evidence/wp06-independent-contracts/source-hashes.json`

No other package is started or declared complete.

## Remaining gates and reviewer disposition

- **BLOCKED: C06 actual PostgreSQL mapping/migration and database-specific
  transaction, ambiguous-commit/retry, contiguous-coverage, protected-read and
  compatibility witnesses.** The separate proposal identifies the v9 mismatch,
  three possible migration approaches, legacy compatibility implications and
  decisions requiring approval. None is implemented or treated as approved.
- **Separate baseline issue:** historical zero-stream estimator/test mismatch.
  No authorization to change either was inferred.
- Production retention storage/lease registries/crash-restoration services,
  publisher binary framing and runtime capture installation remain their existing
  WP-20/WP-22 owners. These implementations exercise frozen contracts and must
  not reinterpret the event unit, no-change policy, ownership, identity, decimal
  encoding or commitment boundaries. They are not independent missing WP-06
  contract implementations.
- The existing WP-10 recovery-aware commitment stub remains separately recorded
  in the original audit and was not changed.

No accepted architectural decision was changed. This supplies concrete,
reviewable representations for previously open C03–C05/OPEN-004 contracts within
the user's authorized WP-06 scope. Database migration choices still need user
approval. Independent acceptance is proposed for review; full WP-06 acceptance
must wait for its remaining gates and the user's decision.
