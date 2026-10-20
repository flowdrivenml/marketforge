# WP-02A: Extract Shared Depth Event Contracts

## Objective
Extract `DepthEventBoundary`, `DepthSourceOperation`, `DepthSourceEventMetadata`, and `DepthProcessingOutcome` from `engine/src/formats/depth/outcome.rs` into a feature-free shared location so both Live and Historical can import without a `historical` feature dependency.

## Design Decisions
Decision: Keep the types in `outcome.rs` at its original location (`engine/src/formats/depth/outcome.rs`) — they are already feature-free (depend only on `crate::canonical::L2LevelUpdate`). Instead of extracting to a new module, remove the `historical` feature gate from the `formats` module in `lib.rs` and add targeted feature gates only to the sub-modules that actually depend on optional crates.

### Why not extract to a new module?
The four outcome types in `outcome.rs` depend only on `crate::canonical::L2LevelUpdate`, which is unconditionally compiled. The feature constraint comes from:
- `formats/depth/csv.rs` — uses `csv::ByteRecord` (optional `csv` crate)
- `formats/trades/*` — uses `csv::ByteRecord` (optional `csv` crate, 4 files)

All other `formats/*` sub-modules use only non-optional crates (`serde_json`, `rust_decimal`, `thiserror`).

### Minimal gate strategy
Instead of extracting to a new module (which would duplicate `outcome.rs`), the minimal approach is:
1. **Make `formats` unconditionally available** — the bulk of its dependencies (including `outcome.rs`) use only non-optional crates.
2. **Gate `formats::trades` behind `historical`** — it pulls in the optional `csv` crate.
3. **Gate `formats::depth::csv` behind `historical`** — the one sub-module with optional deps.

## Changes Applied

### 1. `engine/src/lib.rs`
**Before:**
```rust
#[cfg(feature = "historical")]
pub mod formats;
```
**After:**
```rust
pub mod formats;
```

**Rationale:** `formats` contains `depth` (feature-free core) and `trades` (historical-only). Making `formats` unconditional enables `marketforge_engine::formats::depth::DepthProcessingOutcome` to be accessible from `live` without `historical`.

### 2. `engine/src/formats/mod.rs`
**Before:**
```rust
pub mod depth;
pub mod trades;
```
**After:**
```rust
pub mod depth;

#[cfg(feature = "historical")]
pub mod trades;
```

**Rationale:** `trades/*` depends on `csv::ByteRecord` from the optional `csv` crate. The gate must remain here (at the `modules` level) since we removed the crate-level gate from `lib.rs`.

### 3. `engine/src/formats/depth/mod.rs`
**Changes:**
```diff
-mod csv;
+#[cfg(feature = "historical")]
+mod csv;

+#[cfg(feature = "historical")]
 pub use csv::DepthCsvAdapter;
```

**Rationale:** `csv.rs` is the only depth sub-module with optional deps (`csv::ByteRecord`). The `DepthCsvAdapter` re-export is gated to match.

### Files NOT Modified (intentionally)
- `engine/src/formats/depth/outcome.rs` — types remain at original location, no changes needed
- `engine/src/formats/depth/processor.rs` — depends on non-optional crates only
- `engine/src/formats/depth/snapshot.rs` — depends on non-optional crates only
- `engine/src/formats/depth/transforms.rs` — depends on non-optional crates only
- `engine/src/formats/depth/assembler.rs`, `relative.rs`, `scalar.rs`, `spec.rs`, `update.rs`, `json.rs` — all non-optional
- Historical callers (`engine/src/historical/normalize/worker/sink.rs`, etc.) — retain existing `crate::formats::depth::*` imports, which resolve correctly since `formats` is now unconditionally available
- `engine/Cargo.toml` — no dependency changes needed

## Verification Results

### cargo check configurations (all pass)
| Feature configuration | Result |
|----------------------|--------|
| `cargo check` (no features) | PASS |
| `cargo check --features historical` | PASS |
| `cargo check --features live` | PASS |
| `cargo check --features "live,historical"` | PASS |

### cargo fmt --check
PASS

### Import path preservation
All 6 historical callers retain existing import paths:
- `engine/src/historical/merge/depth/depth_stream.rs` — `formats::depth::DepthEventBoundary`
- `engine/src/historical/merge/depth/event_validation.rs` — `formats::depth::DepthEventBoundary`
- `engine/src/historical/merge/depth/partition_stream.rs` — `formats::depth::DepthEventBoundary`
- `engine/src/historical/normalize/worker/sink.rs` — `formats::depth::DepthProcessingOutcome`
- `engine/src/historical/normalize/worker/depth.rs` — `formats::depth::DepthEventBoundary`, `Formats::depth::DepthProcessingOutcome`
- `engine/src/historical/normalize/worker/depth.rs:629` — `crate::formats::depth::DepthProcessingOutcome`

These all resolve correctly because `pub mod formats;` in `lib.rs` is no longer feature-gated.

### Live-engine access
The depth event types are now accessible without `historical` enabled:
- `marketforge_engine::formats::depth::DepthProcessingOutcome`
- `marketforge_engine::formats::depth::DepthEventBoundary`
- `marketforge_engine::formats::depth::DepthSourceEventMetadata`
- `marketforge_engine::formats::depth::DepthSourceOperation`

## Constraints Satisfied
- ✅ No second L2 representation created
- ✅ No BookStore modifications
- ✅ No changes to canonical/processing/conversion/merging logic
- ✅ Existing import path `marketforge_engine::formats::depth::DepthProcessingOutcome` preserved
- ✅ No Cargo dependency changes
- ✅ No `cargo test` or `cargo bench` execution
- ✅ All 4 `cargo check` feature configurations pass
- ✅ `cargo fmt --check` passes
