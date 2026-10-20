# Live Envelope Correction Report

## Objective
Correct the event-envelope implementation from WP-04C before other MarketForge Live modules depend on it.

## Corrections Applied

### 1. Removed duplicate trade metadata
**Change**: `LivePayload::Trade(LiveTrade)` → `LivePayload::Trade(Trade)`

`LiveEvent` owns `cursor`, `provenance`, and `timing`. The `LiveTrade` wrapper redundantly carried all three. The canonical `Trade` is now wrapped directly in `LivePayload`, eliminating the duplication.

**Before**:
```rust
pub enum LivePayload {
    Trade(LiveTrade),   // LiveTrade carries cursor, provenance, timing
    L2Group(LiveL2Group),
}
pub struct LiveTrade {
    pub event: Trade,
    pub cursor: StreamCursor,           // DUPLICATE of LiveEvent.cursor
    pub provenance: EventProvenance,    // DUPLICATE of LiveEvent.provenance
    pub timing: EventTiming,            // DUPLICATE of LiveEvent.timing
}
```

**After**:
```rust
pub enum LivePayload {
    Trade(Trade),                         // No wrapper — LiveEvent owns metadata
    L2Group(LiveL2Group),
}
```

**Correctness**: `LiveEvent` is the sole cursor/provenance/timing owner. The `event_timestamp_ns()` accessor now delegates to `trade.envelope.event_timestamp_ns` for trades.

### 2. Removed duplicated L2 sequence metadata
**Change**: Removed `grouped_sequence_range: EventRange` from `LiveL2Group`.

`DepthSourceEventMetadata` already carries `sequence_start` and `sequence_end`. Having a parallel `EventRange(u64, u64)` field was redundant.

**Correctness**: Native sequence information remains accessible via `group.source.sequence_start` and `group.source.sequence_end`. Optional because not every exchange provides it.

### 3. Removed duplicated L2 book generation
**Change**: Removed `book_generation: BookGeneration` from `LiveL2Group`.

`EventProvenance` already has `book_generation: Option<BookGeneration>`. The authoritative source for book state identity is the envelope's provenance — not duplicated on the L2 group.

**Correctness**: Book generation remains available via `(group_event: &LiveEvent).provenance.book_generation`. This is one timestamp/identity, not two.

### 4. Removed unused `EventRange` type
**Change**: Removed `pub struct EventRange(pub u64, pub u64)` from envelope.rs and its re-export from mod.rs.

No external consumers exist. The type was only used by `grouped_sequence_range`.

### 5. Updated `LiveL2Group::new` signature
**Change**: Replaced `sequence_range: EventRange` parameter with `source: DepthSourceEventMetadata`.

**Before**:
```rust
pub fn new(
    boundary: DepthEventBoundary,
    operation: DepthSourceOperation,
    sequence_range: EventRange,
    book_generation: BookGeneration,
    levels: Vec<L2LevelUpdate>,
    source_timestamp_ns: Option<i64>,
) -> Self
```

**After**:
```rust
pub fn new(
    boundary: DepthEventBoundary,
    operation: DepthSourceOperation,
    levels: Vec<L2LevelUpdate>,
    source_timestamp_ns: Option<i64>,
    source: DepthSourceEventMetadata,
) -> Self
```

## Fields Remaining on LiveL2Group

| Field | Type | Source |
|-------|------|--------|
| `grouped_boundary` | `DepthEventBoundary` | Canonical processing boundary |
| `grouped_operation` | `DepthSourceOperation` | Exchange source type |
| `grouped_levels` | `Vec<L2LevelUpdate>` | Ordered canonical levels |
| `source_timestamp_ns` | `Option<i64>` | Exchange event timestamp |
| `source` | `DepthSourceEventMetadata` | Sequence + ordinal + timestamps |

All required information for publication, recording, retention/replay, and derived-stream processing is preserved.

## Serde Compatibility Analysis

### LivePayload discriminant (Trade variant)
- **Risk**: A previously serialized `LivePayload::Trade(<LiveTrade data>)` cannot be deserialized into the current `LivePayload` because `LiveTrade` no longer has `Serialize`/`Deserialize` derives (it was removed).
- **Severity**: None — live event serialization does not yet exist. No persisted data will encounter this.
- **Mitigation**: Document in release notes once production serialization begins. Consider `#[serde(remote = "LiveTrade")]` alias if backward compatibility is needed later.

### LiveL2Group field removals
- **`grouped_sequence_range` removed**: New serialized data has no this key. Old serialized data (if any) with this key is **gracefully ignored** by Serde's default deserializer (unknown fields are skipped). **Compatible.**
- **`book_generation` removed**: Same as above. **Compatible.**
- **Constructor parameter removal**: API-only change. No serialized data references constructor signatures.

### EventRange removal
- No external crates reference this type. Only internal use. **No compatibility risk.**

### serde(transparent) types (NativeSequence, NativeTradeId)
- Unchanged. No compatibility concern.

### Canonical types
- **Trade, L2LevelUpdate, EventEnvelope**: Not modified. Serde derives unchanged. **No impact.**
- **CanonicalEvent, DepthProcessingOutcome**: Not modified. **No impact.**

## Build Validation

| Check | Result |
|-------|--------|
| `cargo fmt --check` | ✅ Pass |
| `cargo check --lib --no-default-features --features live` | ✅ Pass (1 expected dead_code warning on LiveEvent::new) |
| `cargo check --lib --no-default-features --features live,historical` | ✅ Pass (12 pre-existing csv.rs warnings + 1 LiveEvent::new) |

## Non-blocking Notes
- `LiveEvent::new` dead_code warning: Private constructor, not yet called. Documented as expected.
- csv.rs dead_code warnings: Pre-existing in historical-only builds. Not introduced by this correction.
