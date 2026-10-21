# WP-06 C06 — Observed canonical schema and recording contracts

Date: 2026-10-21. Disposition: **mapping implemented and tested;
overall database-specific C06 gate remains BLOCKED**. No package is Accepted.

This record supplements the earlier audit and schema proposal. The earlier
proposal describes historical schema version 9; it does not describe all tables
now present in the connected database. No schema or existing record was changed.

## Authorization and connectivity

The user authorized local authentication through libpq and read-only inspection,
then C06 implementation followed sequentially by WP-07. `psql --no-password
--no-psqlrc --host=127.0.0.1 --port=5432 --username=marketforge
--dbname=marketforge --set=ON_ERROR_STOP=1` connected successfully after the
sandbox network/authentication access escalation. Credential files were never
opened, printed, inspected or copied by the agent. psql handled authentication.

The transaction reported database/role `marketforge` and `transaction_read_only=on`.
All five `live_canonical` base tables exist, and the role has SELECT on each:

- `streams`
- `event_identities`
- `events`
- `l2_levels`
- `recording_coverage`

No additional database privilege is needed for the completed reads. This does not
assert that the role has write privileges or that writes are authorized.

The exact read-only query is [inspect.sql](wp06-c06-connected/inspect.sql).
The credential-free catalog and expression results are
[observed.jsonl](wp06-c06-connected/observed.jsonl). This query used only SELECT
inside BEGIN READ ONLY / COMMIT. It did not run DDL, DML, trigger calls or
transactional inserts that were later rolled back.

## Authoritative requirements and reconciliation

Audited against Document 17 WP-06/C06, Document 11 event identity, transaction,
coverage/reconciliation and protected-read requirements, the accepted decision
register, the original WP-06 audit, and the actual connected catalog.
Existing canonical LiveEvent, EventRecordV1, EventBatch, delivery failure,
ReplayRange, protection and cursor-position contracts are reused.

| C06 requirement | Implemented evidence | Remaining gate |
|---|---|---|
| Actual schema mapping | `recording/schema.rs`; observed catalog fixture with required columns, types, precision/scale, PK, FK and actual CHECK definitions | Live database transaction witness |
| Canonical event identity | Parent and every child preserve exactly `(StreamId, LogEpoch, offset)`; identity registry is the write plan's transaction prerequisite | Real atomic identity/parent/children/coverage insert |
| Logical L2 ownership | One event parent, exact ordered child ordinals; set/delete, null/zero and exact decimal distinctions; no regenerated cursor | Real relational round trip |
| Full envelope/version | EventRecordV1 stores exact validated payload/provenance/timing; relational headers and children are checked against it on decode | Negative timestamp limitation below |
| Transaction/ack ambiguity | Immutable RecordingWritePlan; committed versus durable acknowledged outcomes; Unknown maps to CommitOutcomeUnknown and never advances coverage | Injected actual PostgreSQL commit/ack interruption |
| Contiguous coverage | Checked bounded interval union, gaps preserved, out-of-order completion, MAX offset, atomic multi-range capacity rejection | Transactionally authoritative DB coverage writes |
| Retry reconciliation | Missing versus identical committed content versus conflict versus identity-only/corrupt indeterminate state | Real retry after ambiguous commit |
| Protected reads | Existing owner/token/expiry lease authorization, committed bounds, strict contiguous pages/continuation and count/byte limits | SQL reader under concurrent cleanup |
| Protected cleanup | Overlapping active protection after its safely-consumed frontier rejects cleanup | Atomic admission/cleanup serialization in actual backend |
| Historical compatibility | Decoding reconstructs shared canonical payloads without new identities; existing historical code/schema untouched | Full real database reader integration is unrun |

The newly observed tables directly represent the previously missing cursor and
level identities. The obsolete assumption that only schema-v9 `live.*` tables are
available is therefore reconciled by observation, without inventing a migration.
The original proposal is retained as a historical design record.

## Implementation

- `recording/schema.rs`: exact typed stream/event/level row projection, defensive
  decode, checked int4 child ordinals, stream/kind/instrument consistency,
  relational/JSON disagreement rejection and fail-closed schema inspection.
- `recording/commit.rs`: immutable transaction write plans, complete contiguous
  ranges for gapped batches, committed/durable/unknown/failed outcomes.
- `recording/coverage.rs`: bounded contiguous coverage interval accounting with
  checked MAX arithmetic and atomic staged multi-range updates.
- `recording/idempotency.rs`: content-aware reconciliation; duplicate identity
  alone is insufficient evidence of a successfully recorded event.
- `recording/reader.rs`: protected canonical reads and cleanup guards. Database
  owners must serialize these guards with admission and storage mutation.
- `tests/live/c06_recording.rs`: 12 deterministic tests covering these requirements,
  malformed metadata/projections, failure outcomes, gaps, limits and boundaries.
- `tests/live/fixtures/canonical-schema-observed.json`: the observed catalog
  snapshot, rather than an invented schema.

These are real contracts and pure encoders/readers, not a production SQL writer,
SQL connection pool, migration or WP-21 implementation. WP-10 was not modified.
C06 was tested before WP-07 work began (12 targeted tests, Live library 93/93,
Live integration 335/335 at that point).

## Explicit blocked criteria and decisions

1. **Read/write integration gate BLOCKED by authorization.** Disposable PostgreSQL
   transaction, constraint-conflict, ambiguous-commit, retry reconciliation,
   actual atomic coverage, protected cleanup and DB reader witnesses require
   writes. The current authorization permits only reads. They were not replaced
   with a claim that mocks prove real PostgreSQL transaction behavior.
2. **Timestamp representability gate BLOCKED.** Canonical timing is signed i64.
   The observed SQL header CHECKs require nonnegative NUMERIC(20,0). Valid
   canonical events with negative timestamps therefore receive explicit
   `UnsupportedTimestamp`; the canonical event and historical semantics are not
   restricted or mutated. The read-only probe tests the CHECK expression, not
   an INSERT that exercises the constraint. A requirement for total signed
   timestamp support needs an approved schema/capability decision.
3. Instrument FKs remain optional as observed. Canonical stream/cursor identity
   is never substituted by an instrument key or generated database row ID.
4. No DB rollback/crash claim is inferred from pure write-plan/interval tests.

For a concrete next verification step, approve a disposable test database (or an
explicit reserved fixture namespace) and transaction-only fixture writes to the
five named tables. The witness should insert a stream with nullable instrument,
identity+parent+ordered children+coverage in one transaction; read back through
the canonical decoder; force duplicate/conflicting content and statement failure;
interrupt commit acknowledgement and reconcile in a new session; and race
protected read/admission against cleanup. Existing production records must remain
out of scope. DDL changes remain separately unauthorized.

For signed timestamps, choose whether the backend must support the full canonical
signed domain (requiring approved CHECK changes) or explicitly advertise its
restricted storage capability. Neither decision has been silently accepted here.

## Verification

Final verification is shared with the following sequential WP-07 implementation:
[results-final.json](wp06-c06-wp07-verification/results-final.json), and the unchanged
historical failure is in [08.log](wp06-c06-wp07-verification/08.log).
The full exact file manifest and source hashes are in
[files.json](wp06-c06-wp07-verification/files.json).

No dependency, historical code/test, PostgreSQL DDL, credential, WP-10 code,
commit or push was changed/performed. Final acceptance remains the user's decision.
