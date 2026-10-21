# WP-06 C06 — Recording schema reconciliation proposal

2026-10-21 update: this is the retained **pre-connection** proposal for schema v9.
Read-only inspection now observes five additional `live_canonical` tables. See
[connected mapping evidence](wp06-c06-connected.md) for current implementation
and remaining database/representability gates. No migration was performed here.

Disposition: **PROPOSED; database-specific C06 mapping gate remains BLOCKED.**
No DDL, migration, SQL writer, database service, or existing schema was changed.
The user explicitly authorized this proposal and independent WP-06 implementation,
but prohibited implementing a migration or weakening canonical identity.

## Source of the mismatch

The actual schema is `src/marketforge/database/schema.py`, version 9. It contains
`live.trades`, `live.l2_snapshots`, `live.l2_snapshot_levels`, `live.l2_updates`,
and `live.l2_update_levels`, with instrument foreign keys into the existing catalog.
Trades and depth parents have generated database row IDs. Snapshot levels use
`(snapshot_id, side, level)`; update levels use `(update_id, change_index)`.

Document 11 sections 7–10 require accepted logical-event identity, complete
initialization/change boundaries, ordered levels, provenance, schema compatibility,
and transactionally enforced uniqueness even across event-time partitions.
Its tables are conceptual examples, explicitly not finalized PostgreSQL DDL.

Version 9 has exact NUMERIC payload columns, but lacks the complete Live envelope:

| Requirement | Version 9 mismatch |
|---|---|
| `(StreamId, LogEpoch, offset)` | Generated row IDs do not encode any of these domains |
| One cursor per logical L2 group | Physical snapshot/update parents are not mapped to accepted canonical identity |
| BookGeneration and initialization boundary | Native snapshot/update classification is not proof of canonical reconstruction-segment identity |
| Original ordered levels | Snapshot side/rank keys do not themselves preserve arbitrary canonical source-group ordering |
| Actual acquisition origin | Connection, REST, and derived provenance are not stored |
| Native optional identifiers | Some sequence columns exist, but do not substitute for the whole accepted source metadata |
| Complete canonical timing | Trade system timestamp and envelope receipt timing are not both represented |
| Event/schema compatibility | No complete versioned accepted-event envelope mapping |
| Contiguous committed coverage | No authoritative interval/commit ledger tied to canonical cursor identity |
| Cross-partition uniqueness | No canonical recording-identity registry enforcing the identity independently of event time |

The new WP-06 `EventRecordV1` is a logical, exact encoding probe. It is not a
PostgreSQL row schema, migration, or authorization to hide missing relational
identity behind a JSON blob.

## Required identities and invariants

1. The accepted event key remains exactly `(StreamId, LogEpoch, offset)`.
   If the column name `stream_cursor` is retained, it means only the offset.
   Neither instrument ID nor generated row ID is a replacement for StreamId.
2. Source/derived stream registration must refer to the authoritative stable
   identity mapping. Historical `EventEnvelope.stream_id` is payload metadata;
   it is not an independently assignable replacement for the Live cursor stream.
3. A logical depth parent owns one accepted key. Its levels use a deterministic
   ordinal subordinate to that key. Original boundary, source operation, sequence
   range/ordinal, native optional identifiers, BookGeneration, and timing survive
   reconstruction. No independent level cursor is created.
4. A complete initialization is persisted in full, including unchanged levels
   from an older book generation. A snapshot diff is insufficient.
5. Derived output has its own publication cursor and retains its source-event
   references, transformation/order revisions, and exact conversion observations.
   REST and derived rows must not acquire fictitious WebSocket generations.
6. All domain u64 values must survive their complete range. PostgreSQL signed
   BIGINT alone cannot encode every u64 epoch, offset, native sequence, or
   generation. An approved representation could use constrained `NUMERIC(20,0)`;
   this is an option for review, not implemented DDL. Native textual IDs stay opaque.
7. Prices, quantities, and conversion rates stay exact. NUMERIC must not impose
   an unapproved rounding or narrowing conversion. Preserve NULL versus zero.
   Parquet's accepted Decimal256 precision 76 / scale 28 remains unchanged;
   Rust canonical Decimal values retain their exact value and available scale.
8. The event identity registry, event payload/parent, all levels, and transaction
   coverage/commit records must commit atomically in PostgreSQL. No transaction
   across Unix delivery, PostgreSQL, and Parquet is promised.
9. A replayable committed frontier is established by verified contiguous coverage,
   not `MAX(timestamp)`, `MAX(row_id)`, or the highest completed batch offset.

## Migration approaches for approval

### A. Add an envelope/identity layer around the existing physical tables

Introduce an authoritative canonical identity/envelope relation and explicit
mapping to the existing trade and snapshot/update payload parents. Add any
missing level-order and provenance storage under that parent identity.

Benefits: retain existing physical payload tables and catalog foreign keys;
potentially incremental rollout and continued legacy reader compatibility.

Costs: more complex joins and constraints; canonical initialization must remain
distinct from native snapshot classification; mixed old/new writers need an
explicit legacy mode. Snapshot ordering and missing timing/provenance need
additional approved fields. Ordinary old inserts cannot silently become canonical.

### B. Introduce a versioned canonical recording namespace alongside v9

Keep v9 data and legacy readers intact; add a separately versioned canonical
event parent, ordered depth children, identity registry, and coverage ledger.
Reuse the existing instrument catalog rather than creating competing identities.
New Live recording writes only through the approved canonical path.

Benefits: clear event-unit/identity rules and a smaller compatibility surface;
legacy records can remain truthfully distinguishable. Makes global identity
enforcement and initialization semantics easier to review.

Costs: two readable formats during transition; additional storage/adapter work;
namespace selection and old-reader expectations need explicit configuration.
Any transitional dual write must be in the same database transaction or report
independent outcomes rather than imply atomicity.

### C. Rebuild the recording tables with an explicit legacy bridge

Adopt a new canonical layout in place and migrate available payload data through
an audited adapter, retaining legacy identities and unavailable-field markers.

Benefits: one eventual recording layout.

Costs: largest operational and compatibility change, possible maintenance window,
and careful rollback/reader-version coordination. Missing canonical metadata in
old rows still cannot be reconstructed by a table rewrite alone.

**Recommendation for review:** B provides the clearest separation between new
canonical guarantees and legacy rows. No approach is selected or implemented by
this document.

## Compatibility and recovery implications

- Existing v9 rows generally cannot be upgraded into original accepted canonical
  events losslessly: their original stream/log/offset and provenance were never
  recorded. Preserve them as legacy historical inputs unless an independently
  evidenced mapping exists. Do not number them with generated row IDs and call
  those numbers original accepted cursors.
- A future explicit reconstruction workflow may create newly identified output,
  but it must identify that reconstruction honestly; it must not impersonate an
  earlier accepted event or restore an unverified replay frontier.
- Existing Python catalog/schema initialization and older readers must have a
  defined version-negotiation/migration owner. Rust must not maintain competing,
  implicit DDL. Historical canonical financial semantics remain unchanged.
- Event-time partitioning requires a separate identity registry or an equivalent
  transactionally enforced mechanism for uniqueness across partition keys.
  Include late recovered events and retry reconciliation in the design.
- On ambiguous transaction outcome, reconcile canonical identities and content
  before retrying. `ON CONFLICT DO NOTHING` is not proof that existing content
  matches. Hashes can accelerate checks but must not replace content validation.
- Committed and durable acknowledgements require distinct evidence. The approved
  WAL/synchronous-commit/durability policy must define the Durable boundary;
  a queue enqueue or an unexplained successful method return is not sufficient.
- Cleanup and replay must coordinate protected reads with coverage withdrawal.
  A deleted or expired interval must not remain advertised as committed coverage.
- Epoch journal deletion/rollback cannot be used to reuse identities. The WP-06
  local journal fails closed after a detected missing/corrupt record and starts
  a fresh persisted epoch on new source-owner activation; it does not claim to
  reconstruct PostgreSQL committed event coverage.

## Decisions requiring user approval

1. Select A, B, C, or another explicit schema reconciliation design.
2. Approve the canonical stream identity registry and source/derived mapping,
   namespace/version policy, u64 SQL representation, and ordered-level keys.
3. Approve legacy-row disposition and compatibility expectations for existing
   writers/readers; default proposal is to retain them as legacy data.
4. Approve the identity/coverage transaction architecture and partitioning rules.
5. Approve the precise recording durability policy and supported database settings.
6. Assign schema/migration ownership and approve implementation of the migration
   and its disposable-database compatibility/ambiguous-commit/protected-read tests.

Until these decisions and the resulting schema-specific witnesses are completed,
**C06 remains BLOCKED**, regardless of the passing independent WP-06 contracts.
