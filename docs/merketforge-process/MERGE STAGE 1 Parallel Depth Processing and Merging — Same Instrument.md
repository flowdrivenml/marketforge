## Quick Navigation

- [Objective](#objective)
- [Core Principles](#core-principles)
- [Archive Boundary Metadata](#archive-boundary-metadata)
- [Sequence and Snapshot Reconciliation](#sequence-and-snapshot-reconciliation)
- [Parallel Processing Strategies](#parallel-processing-strategies)
- [Merging Optimization](#merging-optimization)
- [Integrity and Recovery](#integrity-and-recovery)
- [Implementation Plan](#implementation-plan)

## Objective

Enable fast parallel processing and merging of historical L2 order-book archives while preserving exact market data, reconstruction continuity, and source provenance.

MarketForge should avoid sequential processing whenever archives can be processed independently.

The central strategy is:

1. Process independent archives concurrently.
2. Preserve sequence, snapshot, and boundary-state metadata.
3. Reconcile adjacent archives using metadata.
4. Reprocess only archives or intervals with unresolved dependencies.
5. Merge compatible datasets without unnecessary decoding, sorting, or rewriting.

**Processing establishes valid canonical data. Merging reconciles independently processed datasets and preserves their chronological relationships.**

## Core Principles

### Canonical Representation

MarketForge uses one canonical `L2LevelUpdate` representation containing absolute price-level quantities.

Snapshots and incremental updates are normalized into the same canonical structure.

Source-specific semantics remain inside the processing layer.

Reconstruction boundaries are stored separately in metadata.

Canonical Parquet records do not require:

- Snapshot/update classifications.
- Native exchange sequence identifiers.
- Exchange-specific update actions.
- Reconstruction segment identifiers.

### Independent Reconstruction

An archive can be processed independently when it contains a complete authoritative initialization snapshot.

Archives without initialization snapshots may require a preceding book state.

State dependencies must be resolved before producing final canonical absolute-level output.

### No Fabricated Market Events

MarketForge must never represent inferred or synthetic state transitions as exchange-observed market activity.

Differences between authoritative snapshots may be calculated for reconciliation, validation, or internal state initialization.

Such differences must remain distinguishable from observed updates.

### Sequence Semantics

Native sequence identifiers are exchange-specific.

MarketForge must not universally assume:

```text
next_sequence = previous_sequence + 1
```

Sequence continuity, resets, duplicates, and overlap must follow validated source-specific rules.

Locally generated event identifiers establish deterministic ordering but cannot prove source completeness.

## Archive Boundary Metadata

Each independently processed archive should preserve:

```text
Archive Metadata
├── Exchange
├── Instrument / Stream
├── Source format
├── First / last timestamp
├── First / last source sequence
├── Sequence semantics
├── Initial snapshot availability
├── Initial book state
├── Final book state
├── Snapshot depth / coverage
├── Reconstruction segments
├── Continuity status
└── Integrity diagnostics
```

### Boundary Book States

Store the initial authoritative state and final reconstructed state when available.

Boundary states contain:

```text
BookState
├── Bids
│   └── Price → Normalized BookLevel
└── Asks
    └── Price → Normalized BookLevel
```

These states enable fast reconciliation between adjacent archives.

### State Fingerprints

Calculate deterministic fingerprints of boundary book states.

Fingerprints must account for:

- Side and price.
- Base, quote, and contract quantities.
- Order count.
- Instrument and stream identity.
- Snapshot depth and coverage.

Use deterministic ordering and exact decimal serialization.

Matching fingerprints provide a fast equality check for comparable states.

Different fingerprints trigger detailed state comparison.

**Matching fingerprints do not independently prove sequence continuity or source completeness.**

### Sequence Metadata

Preserve native sequence information when available:

```text
first_source_sequence
last_source_sequence
sequence_type
sequence_continuity
```

Different sequence domains must not be compared as interchangeable identifiers.

For example, Bybit update IDs and cross-sequence identifiers have different meanings.

### Reconstruction Segments

Retain the existing `segments.json` model.

Each segment identifies:

```text
segment_id
start_timestamp_ns
initial_event_offset
initial_level_count
```

Extend metadata later to record continuity status, sequence boundaries, and recovery information.

A reconstruction segment must begin with an authoritative initialization state.

## Sequence and Snapshot Reconciliation

The merger evaluates adjacent archives belonging to the same depth stream.

### Scenario A — Continuous Sequences Without a New Snapshot

```text
Archive A
    Snapshot 100
    Update 101
    Update 102
    Update 103

Archive B
    Update 104
    Update 105
```

If source-specific sequence rules confirm continuity, Archive B may continue from Archive A's final book state.

Archive B cannot necessarily produce final canonical output independently.

For absolute updates, source assignments can be prepared independently and finalized after resolving the incoming state.

Relative updates require the incoming state or a suitable intermediate transformation representation.

### Scenario B — Same Sequence, Matching Snapshot

```text
Archive A
    Final sequence: 103
    Final state: X

Archive B
    Snapshot sequence: 103
    Initial state: X
```

When sequence domains and snapshot coverage are comparable:

- Confirm matching boundary states.
- Identify the redundant overlap.
- Avoid duplicating already represented market activity.
- Preserve the snapshot as a reconstruction checkpoint if useful.
- Continue from the verified boundary.

No synthetic updates are necessary.

### Scenario C — Same Sequence, Different Snapshot

```text
Archive A
    Final sequence: 103
    Final state: X

Archive B
    Snapshot sequence: 103
    Initial state: Y
```

If the states are genuinely comparable, this indicates an integrity discrepancy.

Actions:

- Record a boundary-state mismatch.
- Preserve both source states for diagnostics.
- End the previous reconstruction segment.
- Initialize a new segment using the authoritative snapshot.
- Continue processing according to integrity policy.

Do not silently reconcile the discrepancy.

### Scenario D — Next Snapshot at Sequence i + 1

```text
Archive A
    Final sequence: 103
    Final state: X

Archive B
    Snapshot sequence: 104
    Initial state: Y
```

If the exchange guarantees consecutive sequence semantics, the snapshots represent adjacent logical states.

MarketForge may calculate:

```text
Patch = Difference(X, Y)
```

This patch establishes the transformation between the two states.

However, it does not necessarily preserve the original event timestamp, message structure, or ordering of individual changes.

Preferred behavior:

- Accept the authoritative snapshot.
- Start a new reconstruction segment.
- Preserve continuity metadata.
- Avoid inserting the derived patch as an observed market event.

### Scenario E — Sequence Gap With Recovery Snapshot

```text
Archive A
    Last sequence: 103

Archive B
    Snapshot sequence: 110
```

If source-specific rules establish a genuine gap:

- Preserve the valid interval ending at 103.
- Record the missing sequence interval.
- Start a new segment from snapshot 110.
- Continue from the authoritative state.

The missing intermediate activity cannot be reconstructed from boundary snapshots.

Snapshot differences may be retained as diagnostic reconciliation data.

### Scenario F — Overlapping Sequences

```text
Archive A
    Sequence 100 → 200

Archive B
    Sequence 180 → 250
```

Potential reconciliation:

```text
Archive A: 100 → 200
Archive B:           201 → 250
```

Procedure:

1. Verify compatible sequence domains.
2. Identify overlapping events.
3. Compare overlapping book states where possible.
4. Detect conflicting source observations.
5. Discard only verified redundant events.
6. Continue from the first valid new event.

Do not discard records solely because their numerical sequence identifiers are lower.

Sequence resets, repeated identifiers, and source-specific conventions must be considered.

### Scenario G — No Native Sequence Identifiers

MarketForge assigns deterministic local event ordinals.

```text
source_sequence: None
local_event_ordinal: 1, 2, 3, ...
continuity: Unverifiable
```

These ordinals preserve observed ordering.

They do not establish whether exchange messages were omitted.

Use available timestamps, authoritative snapshots, archive overlap, and book-state validation to detect inconsistencies.

Continuity must remain explicitly unverified when insufficient evidence exists.

## Parallel Processing Strategies

### Strategy 1 — Independent Snapshot Processing

Preferred fast path.

```text
Archive A ── Worker 1 ── Parquet A
Archive B ── Worker 2 ── Parquet B
Archive C ── Worker 3 ── Parquet C
```

Requirements:

- Complete authoritative initialization.
- Valid source-specific sequencing.
- Independent reconstruction.
- Compatible canonical normalization.

No cross-file state propagation is required.

### Strategy 2 — Boundary-State Propagation

For archives containing absolute updates without initialization snapshots.

Each archive is first analyzed independently to produce a state-transformation summary.

For absolute SET/DELETE operations:

\[
F_i(B) = (B \setminus K_i) \cup V_i
\]

Where:

- \(B\) is the incoming book.
- \(K_i\) contains modified price-level keys.
- \(V_i\) contains their final assigned values.

Transformations compose associatively:

\[
F_{A+B} = F_B \circ F_A
\]

This allows parallel prefix computation of incoming archive states.

Once incoming states are resolved, archives can be finalized concurrently.

### Strategy 3 — Relative Update Transformation

Relative updates may require symbolic state transformations.

For simple additive quantities:

\[
q_{out} = a q_{in} + b
\]

Where:

- \(a=1\) represents a relative transformation.
- \(a=0\) represents an absolute assignment.
- \(b\) represents the accumulated change or assigned quantity.

This model is only valid when source semantics permit it.

Depth truncation, deletions, nonlinear operations, and exchange-specific rules may require more complex transformations.

Use sequential processing when safe composition cannot be established.

### Strategy 4 — Parallel Archive Chunking

Large archives may be divided into independently analyzable chunks.

```text
Large Archive
    ├── Chunk A
    ├── Chunk B
    └── Chunk C
          ↓
    Parallel Analysis
          ↓
    Boundary Resolution
          ↓
    Parallel Finalization
```

Chunks must preserve complete source-message boundaries.

Compressed archives may require temporary decompression or a seekable intermediate format.

### Strategy 5 — Pipelined Execution

Overlap independent processing stages:

```text
Source Reader
      ↓
Decoder
      ↓
Normalization
      ↓
Ordered BookStore
      ↓
Arrow Batching
      ↓
Parquet Writer
```

Book mutations remain ordered within each stream.

Decoding, preparation, and serialization may execute concurrently where safe.

Use bounded queues and resource limits.

## Merging Optimization

### Logical Merging

When datasets are already canonical, compatible, ordered, and non-overlapping, avoid rewriting their Parquet contents.

Construct a logical dataset referencing existing files:

```text
Merged Dataset
├── Dataset A / part-000000.parquet
├── Dataset B / part-000000.parquet
└── Dataset C / part-000000.parquet
```

The manifest defines deterministic file order.

This can make compatible merges largely metadata operations.

The referenced files must remain available and immutable for the lifetime of the logical dataset.

### Physical Merging

When physical consolidation is required, prefer reusing compatible Parquet row groups where supported.

Otherwise, stream records through bounded Arrow batches.

Avoid unnecessary normalization and sorting.

### Overlapping Streams

For overlapping timestamps or multiple streams, use deterministic event-level merging.

Depth messages containing multiple canonical levels must remain atomic.

Preserve a compact source-event index:

```text
event_ordinal
canonical_row_offset
canonical_row_count
event_timestamp_ns
source_sequence
```

This enables merging complete source events without treating individual levels as independent market events.

### Metadata-Based Planning

The merger should determine its execution strategy before reading canonical rows.

Inspect:

```text
Stream identity
Timestamp ranges
Sequence ranges
Snapshot availability
Reconstruction boundaries
Boundary fingerprints
Continuity status
Parquet schema compatibility
```

Select:

```text
Independent datasets
    → Logical concatenation

Compatible overlapping datasets
    → Event-level merge and deduplication

State-dependent datasets
    → Boundary propagation and parallel finalization

Unresolved dependencies
    → Sequential fallback or recovery
```

## Integrity and Recovery

MarketForge never fabricates missing historical market activity.

When a genuine gap is detected:

1. Verify that the discontinuity is not a normal source-sequence behavior.
2. Check overlapping archives and available recovery sources.
3. Preserve the last valid reconstruction state.
4. Record the integrity violation.
5. Resume only when reconstruction can be established safely.

Possible continuity statuses:

```text
Verified
Unverifiable
GapDetected
RecoveredFromSnapshot
BoundaryMismatch
Invalid
```

These statuses are conceptual and should be mapped to the existing integrity architecture during implementation.

### Synthetic Reconciliation

Snapshot differences may be computed for:

- Boundary diagnostics.
- State comparison.
- Internal reconstruction.
- Recovery analysis.

They must not be indistinguishable from exchange-observed updates.

If persisted, synthetic operations require explicit provenance and must remain separate from observed canonical market events.

## Implementation Plan

### Phase 1 — Boundary Metadata

Extend depth processing to preserve:

- Native sequence ranges.
- Initial and final book states.
- Snapshot depth and coverage.
- Boundary fingerprints.
- Continuity status.
- Reconstruction dependencies.

Reuse the existing `BookStore`, `SequenceTracker`, and integrity infrastructure.

### Phase 2 — Independent Parallel Processing

Integrate depth into the existing global scheduler.

Process archives with authoritative initialization snapshots independently.

Preserve task-local reconstruction segments.

### Phase 3 — Boundary Reconciliation

Implement a metadata-driven reconciliation planner.

Support:

- Continuous boundaries.
- Matching snapshots.
- Boundary mismatches.
- Sequence gaps.
- Verified overlaps.
- Missing native sequences.

Do not rewrite canonical Parquet when metadata-only reconciliation is sufficient.

### Phase 4 — Logical Dataset Merging

Support ordered references to existing canonical Parquet files.

Preserve:

- File ordering.
- Reconstruction segments.
- Stream identity.
- Integrity metadata.
- Dataset provenance.

### Phase 5 — State-Dependent Parallel Processing

Implement compact state-transformation summaries.

Resolve incoming archive states using prefix composition where source semantics permit.

Finalize dependent archives concurrently.

Use sequential fallback when necessary.

### Phase 6 — Event-Level Merging

Preserve source-event boundaries through a compact event index.

Support deterministic merging of:

- Trades and depth.
- Multiple instruments.
- Multiple exchanges.
- Overlapping chronological streams.

Ensure atomic depth events are never interleaved with unrelated events.

## Final Design Decision

MarketForge should favor **metadata-driven parallel processing and merging** over sequential reconstruction of entire datasets.

The preferred strategy is:

```text
Parallel Archive Processing
          ↓
Boundary Metadata
          ↓
Sequence and State Reconciliation
          ↓
Resolve Dependencies
          ↓
Logical Merge / Parallel Finalization
          ↓
Canonical Dataset
```

The system must:

- Maximize independent processing.
- Minimize redundant decoding and serialization.
- Preserve exact canonical values.
- Maintain deterministic event ordering.
- Preserve reconstruction boundaries.
- Explicitly document uncertain or missing data.
- Never fabricate exchange-observed market activity.
- Fall back to sequential processing when required for correctness.

**Boundary metadata determines how datasets can be connected. Source-event metadata determines how they can be merged. Reconstruction segments determine where book state can be trusted.**


# Stage 1 — Same-Instrument Depth Merging

## Objective

Merge independently processed depth archives belonging to the **same exchange, instrument, and order-book stream** into one continuous logical dataset.

Prioritize metadata-based reconciliation to avoid unnecessary Parquet decoding, sorting, and rewriting.

## Core Architecture

```text
Daily Canonical Depth Datasets
          │
          ▼
   Boundary Validation
          │
          ├── Sequence continuity
          ├── Snapshot comparison
          ├── Book-state verification
          └── Overlap detection
          │
          ▼
    Logical Merging
          │
          ▼
  Unified Dataset Manifest
```

## Merging Scenarios

### 1. Continuous, Non-Overlapping Archives

```text
Day 1: Sequence 100–200
Day 2: Sequence 201–300
Day 3: Sequence 301–400
```

When source-specific sequence rules confirm continuity, simply reference the existing Parquet files in chronological order.

**No Parquet rewriting is required.**

### 2. Overlapping Archives

```text
Day 1: Sequence 100–250
Day 2: Sequence 200–350
```

Verify the overlap using sequence metadata and book-state consistency.

Retain only the valid, nonduplicated events.

Prefer logical row-range references:

```text
Day 1 → All rows
Day 2 → Rows corresponding to sequences 251–350
```

Avoid physically modifying Parquet files.

Recalculate reconstruction-segment offsets when excluding rows.

### 3. Sequence Gaps

```text
Day 1: Sequence 100–200
Day 2: Snapshot 210 → Updates
```

If a genuine gap exists:

- Preserve the valid preceding segment.
- Document the missing sequence interval.
- Initialize a new segment from the authoritative snapshot.
- Continue processing subsequent updates.

Never fabricate missing exchange events.

### 4. Boundary-State Mismatches

If two comparable snapshots represent the same sequence but contain different book states:

- Record an integrity violation.
- Preserve discrepancy metadata.
- End the previous reconstruction segment.
- Restart from the new authoritative snapshot, subject to integrity policy.

Synthetic SET/DELETE differences may be calculated for diagnostics but must not be represented as observed market events.

## Logical vs. Physical Merging

### Logical Merge — Preferred

Create a new manifest referencing existing Parquet files.

```text
Merged Dataset
└── manifest.json
    ├── Ordered file references
    ├── Included row ranges
    ├── Reconstruction segments
    ├── Sequence continuity
    └── Integrity diagnostics
```

Advantages:

- Minimal disk I/O.
- No redundant serialization.
- Fast merging of non-overlapping archives.
- Original canonical datasets remain immutable.

Referenced files must remain available and immutable.

### Physical Merge — Optional

When standalone consolidated Parquet output is required:

- Stream records through bounded Arrow batches.
- Preserve canonical event ordering.
- Reuse compatible Parquet row groups where possible.
- Rewrite only when necessary.

Parquet files should not be modified in place.

## Required Metadata

Each processed archive should expose:

```text
Archive Metadata
├── Exchange / Instrument / Stream
├── First / last timestamp
├── First / last native sequence
├── Sequence semantics
├── Initial / final book state
├── Boundary-state fingerprints
├── Snapshot availability and coverage
├── Reconstruction segments
├── Source-event boundaries
└── Integrity status
```

Source-event boundaries are necessary to avoid splitting atomic depth messages when trimming overlapping archives.

## Stage 1 Output

The output is a **unified logical canonical depth dataset**.

```text
Same-Instrument Merged Dataset
│
├── manifest.json
│   ├── Ordered Parquet references
│   ├── Included row ranges
│   ├── Reconstruction boundaries
│   ├── Sequence continuity
│   └── Integrity diagnostics
│
└── Existing canonical Parquet files
    └── Referenced without modification
```

The dataset must be readable as one deterministic chronological stream.

Logical merging does not imply that every archive boundary is continuous. Any unresolved gaps or independent reconstruction segments remain explicitly documented.

## Design Decisions

- Merge only archives belonging to the same exchange, instrument, and depth stream.
- Prefer metadata-only concatenation.
- Use logical row ranges for verified overlaps.
- Preserve original Parquet files unchanged.
- Validate source-specific sequence continuity.
- Compare boundary book states when applicable.
- Preserve authoritative reconstruction snapshots.
- Recalculate segment metadata after overlap removal.
- Never fabricate missing historical market events.
- Support optional physical consolidation.
- Produce a deterministic dataset manifest.

## Relationship to Other Stages

```text
STAGE 1
Same-Instrument Depth Merging
          ↓
Unified Logical Canonical Dataset
          ↓
STAGE 2
Cross-Instrument Conversion
          ↓
Common-Denomination Derived Datasets
          ↓
STAGE 3
Multi-Instrument Chronological Merging
          ↓
Unified Market Event Stream
```

**Stage 1 reconciles historical archives of the same depth stream into a unified logical dataset, minimizing physical data movement while preserving reconstruction correctness and integrity.**