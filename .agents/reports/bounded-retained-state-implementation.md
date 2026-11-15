# Bounded normalization retained state implementation

Status: **partial implementation, not acceptance complete**. Boundary books now have a bounded finalization lifetime, and report aggregation/manifest construction no longer make several large transient copies. Completed detailed task metrics and family-routing metric maps are **not yet disk-backed**. The full authoritative batch still fails the conservative 64-GiB admission check. No full-batch execution or performance benchmark was run.

## Ownership design and implemented behavior

The original ownership note was written here before implementation. It identified TaskExecution metrics, ProcessingMetricsReport task details/aggregate maps, family-routing observations, and persisted depth boundary books as separate lifetimes. Source EOF does not freeze reconciliation counters or output ownership.

### Boundary documents

The persisted V2 envelope remains `version: 2`, `continuity: independent_sources`, with a `sources` array containing V1 DepthBoundaryManifest values. BoundaryReadLimits and a serde seed/visitor process one source at a time through a fixed 8-KiB buffered file reader. The reader does not deserialize a stream-wide source Vec or allocate a whole-file String/JSON Value.

Before decoding can grow unboundedly, the guarded reader limits source bytes and encoded string tokens, while custom book-side visitors limit array growth. Book-side Vec capacity grows in bounded chunks; combined book level counts are checked within the bounded object. Prepared symbol/stream metadata has a separate, accounted identity allowance, including JSON escaping; it is not silently constrained to an input-record limit. Source count bounds the duplicate-ID HashSet, whose growth/rehash overlap has a compact per-source allowance. Arithmetic used to compute workspace bounds is checked before opening the reader.

Successful return validates the entire envelope, nested versions, duplicate fields/IDs, contiguous half-open row/event ranges, and trailing content. Header/source unknown fields remain prohibited; nested V1 unknown-field compatibility is preserved. Callbacks cannot return an early successful termination signal; callbacks are documented as validation-only and must not publish. A successfully visited prefix does not make an invalid suffix valid. Stream-level independent_sources is never promoted to verified cross-source continuity.

Production normalization validation checks expected source inventory, identities, event-index coverage, local event ordinals, independent initialization, total rows/events, and reconstruction segment correspondence. Initialization segments are compared while consuming events rather than accumulating another initialization list. Manifest construction performs identity validation within that same source visit, avoiding a second book scan. Merge inventory also visits boundaries, retaining only compact summaries, and correctness verification scans each boundary file once while retaining digest strings. The collecting convenience reader remains bounded and is no longer used by production normalization, merge inventory, or normalization verification. Standalone callers with custom bounds can use the explicit collector/visitor APIs; the legacy convenience signature inherits the active normalization limits or shared defaults. Generated admission JSON companions are present locally under this directory and are ignored by the repository’s existing JSON ignore rules.

The active writer already emits source boundaries incrementally; its book ownership remains under active processing admission. Persisted source books are now dropped after each validation callback, before finalization reservations are released. No returned manifest retains boundary books.

### Report-related transient copies

ScopedIntegrityMetrics::merge first validates all arithmetic using one CategoryStatistics scratch value, then applies the prevalidated updates. An overflow at a late daily/hourly entry leaves the complete original aggregate unchanged. ProcessingMetricsReport::add_task uses this preflight before committing counters, diagnostics, scopes, timestamps, and task ownership. It no longer clones the existing scope maps to obtain atomicity. Existing diagnostic sampling and ordering are preserved.

Scoped file/hour/day map deserialization inserts one entry at a time instead of first collecting an additional Vec of every entry. Duplicate scope/window keys still fail.

DatasetManifest now defaults to the same owned ProcessingMetricsReport type, while a generic borrowed form provides a serialization/validation view. Production finalization borrows context.report instead of cloning it. The authoritative report remains available if manifest validation, writing, or publication fails. The public owned constructor remains available. Persisted JSON fields and serialization order are unchanged, and report writing continues through BufWriter/serde_json::to_writer_pretty with explicit flush.

These changes reduce transient copying; they do **not** make the retained report maps themselves bounded.

### Admission ownership

Only the obsolete per-source boundary-book reservation was removed. Both session admission paths now include one conservative boundary-reader workspace in the phase-exclusive finalization requirement. The workspace covers bounded parser/token overlap, four bounded side-array capacities with allocator/transient headroom, fixed I/O, and a compact duplicate-ID index. The exact same identity/read limits drive production decoding and its admission calculation, including runtime overrides.

The memory budget, scratch budget, metric/depth limits, source selections, and authoritative input documents are unchanged. Active decoder/book, writer, reconciliation, safety, and retained-report allowances were not reduced. No new blocking growth or queues were introduced. Source processing and finalization remain sequential; the admission requirement is retained state plus max(active-unit requirement, finalization requirement), not their simultaneous sum.

## Calculated admission evidence

The nonexecuting example uses the production prepared-batch loader, profile resolver, stream planner, and estimator. It resolves metadata and stats source paths for accessibility; it does not decode archives, create staging, or launch normalization. JSON output explicitly identifies planned accounting, not measured RSS; live reservations, accounted peak, and measured RSS are null because no processing run exists.

Evidence files:

- `bounded-retained-state-admission-before.json`: complete-plan calculation before replacing boundary lifetime accounting.
- `bounded-retained-state-admission.json`: calculation after this implementation.

| Component | Before complete-plan calculation | After |
|---|---:|---:|
| Compact/prepared retained metadata | 13,571,445 | 13,571,445 |
| Conservative retained detailed reports | 74,423,050,224 | 74,423,050,224 |
| Per-source retained boundary books | 105,267,200,000 | 0 |
| Largest active execution unit | 7,270,368,288 | 7,270,368,288 |
| Phase-exclusive finalization allowance | 1,115,684,864 | 2,281,201,792 |
| Total retained + maximum phase | 186,974,189,957 | **81,706,989,957** |
| Configured memory | 68,719,476,736 | 68,719,476,736 |

The added boundary workspace is 1,165,516,928 bytes within finalization, including the compact duplicate index. It is not added again to the active execution phase. The complete-plan required reservation decreases by 105,267,200,000 bytes because the largest active phase remains larger than finalization. **No report-reservation reduction is claimed.**

The earlier recorded failure requested 183,512,345,789 bytes: retained trade/depth state plus the 3,808,524,120-byte active allowance reached before preflight rejected the batch. The complete nonexecuting diagnostic also examines later execution units; its 7,270,368,288-byte maximum explains the larger before-plan total. It does not indicate a change to active estimation. Full JSON evidence retains each unit's human-readable stream identity alongside its fingerprint and source count.

The current retained trade-job allowance is 21,937,110,165 bytes and the depth-job allowance is 52,499,511,504 bytes. Their sum plus the largest phase is the new requirement above. The remaining detailed-report allowance alone exceeds the configured memory limit. Therefore the batch still does not admit, and this implementation does not establish that full normalization succeeds.

Unchanged authoritative inputs:

- `engine/tests/historical/fixtures/normalize.json`: SHA-256 `c18aa92c680dc10c6639a28d596e90ac53587830626dcf1fb3d7b9e2eab37a8d`.
- `data/.config/historical/general.toml`: SHA-256 `4d958da9504e5528f8f8fede634b2e9002753b12c93e30c594beb00e6bb3f614`.

## Verification

Actual outcomes:

| Check | Outcome |
|---|---|
| Release historical normalization with `historical,historical-real-tests`, two test threads | **192 passed, 0 failed, 1 unchanged ignored**, 288.97 s in the final combined run |
| Historical normalization support with real-test feature | **56 passed, 0 failed**, 0.09 s |
| Final focused support run with `historical` | **54 passed, 0 failed** (two additional support tests are real-feature gated) |
| Final focused retained-memory regressions | **14 passed, 0 failed**, 0.10 s |
| Release `marketforge-normalize`, `historical,monitor-tui` | **Passed**, final build 18.83 s |
| Relevant Clippy | **Passed with existing engine warnings**, no new warnings in changed memory code |
| Scoped rustfmt | **Passed** |

The real-fixture run included the requested historical-normalize configuration plus the support and retained-memory targets. Focused support/retained-memory checks were repeated after the final bounded Vec-growth and defensive inventory-index checks. The authoritative batch was only resolved for admission diagnostics, never executed as a complete batch.

Synthetic coverage includes 2,000 source records in one document, constant live book count with separately allowed O(N) ID-index growth, allocator-tracked small merges into a 5,000-entry retained map, rejection before large nested string/array allocations, corrupt/truncated documents, invalid suffixes after successful callbacks, duplicate IDs/fields, unsupported versions, range errors, source/string/level/inventory limits, callback-error preservation, arbitrary envelope field order, late-overflow atomicity, absence of intermediate scope-entry inventories, exact policy denominators/window/unattributed behavior, borrowed/owned JSON equivalence, nondecoding admission against an invalid archive payload, identity allowances, and phase workspace/index growth.

The allocator tests measure allocations made by the tested component on its thread; they are not process RSS measurements or throughput benchmarks. They establish boundary/transient-copy bounds, not completed-report-store bounds.

Release normalization build with `historical,monitor-tui` passed. Relevant Clippy completed with existing warnings in other engine code; no new warnings were emitted for the changed memory code. Touched Rust files pass scoped rustfmt checks. Repository-wide cargo fmt still reports pre-existing formatting differences, starting in `src/bin/marketforge-live.rs`. Diff whitespace checking also identifies pre-existing trailing whitespace in Cargo.toml lines 70 and 117–119; unrelated formatting was not changed. Unrelated library-test targets were not repaired or used to hide failures.

Existing source/output regression tests cover deterministic stream ownership, canonical rows, indexes, book fingerprints, reconstruction digests, legacy manifests, worker-count equivalence, late job failures, and writer failure after successful normalization. The Bitget identity rule, overlap handling, OKX metadata, and existing ignored-test status were not modified.

## Changed files

Created:

- `engine/src/shared/persistence/parquet/boundary_reader.rs`
- `engine/src/historical/normalize/resources/admission.rs`
- `engine/examples/normalize-admission.rs`
- `engine/tests/normalization_retained_memory.rs`
- This report and its two admission JSON evidence files.

Modified in this task (pre-existing changes in these files were preserved):

- `engine/Cargo.toml` (new test/example targets only)
- `engine/src/shared/persistence/parquet/{mod,segments}.rs`
- `engine/src/historical/normalize/{streams_depth,executor,session}.rs`
- `engine/src/historical/normalize/manifest/{builder,models,persistence,validation}.rs`
- `engine/src/historical/normalize/metrics/{report,windows}.rs`
- `engine/src/historical/normalize/resources/{mod,estimate}.rs`
- `engine/src/historical/normalize/benchmark/verification.rs` (correctness verification; no benchmark execution)
- `engine/src/historical/merge/depth/inventory.rs`
- `.agents/progress.md` (append-only partial-progress record).

No files or retained test outputs were removed. TUI changes were preserved. No local completion commit was created because the overall retained-report implementation remains incomplete and shared files contain pre-existing user changes.

## Remaining implementation required for acceptance

1. Add session-owned, job/task-keyed completed-detail storage sharing the existing effective scratch pool, open-file budget, checksums/framing, ownership, cleanup, and failure-evidence retention. Worker-local Rc workspaces cannot simply be returned across workers or dropped while report references remain necessary.
2. Spill stable detailed observations during long streams, before all source reports accumulate. Keep mutable reconciliation counters, source attribution, output timestamps, and task status compact and amendable. Family-routing filtered integrity maps must also be bounded/spilled during routing; merely spilling TaskMetrics afterward is insufficient.
3. Replace full resident ProcessingMetricsReport aggregation and policy inputs with bounded report access plus exact global/per-file/per-stream/window aggregation. Preserve minimum-sample and unattributed rules, all-or-nothing updates, deterministic ordering and samples. The existing exact policy evaluator remains in use in this slice.
4. Stream success/failure report arrays from retained storage; remove the need for resident detailed task arrays/aggregate maps through validation and publication. Preserve the original error and disk evidence if failure-report writing fails.
5. Replace both session retained-report reservations and direct-call retained ownership with compact descriptors plus admitted report/cache workspace, including serialization/read/handoff overlap. Add deadlock, scratch/handle/disk exhaustion, job/task-ID collision, codec, spill amendment, and late-finalization failure regressions for the store.

Existing resident inventories still include prepared jobs/tasks and their indexes, source spans, bounded segment/file/row-group inventories, all completed detailed metrics, and aggregate scopes. There is no claim of constant total process memory or completed bounded reporting. The remaining report state is still O(tasks × permitted detailed-metric capacity).

No additional I/O from report spilling was introduced because report spilling is not implemented. Boundary files are read incrementally, and normalization validation combines the previous validation/identity scans. No throughput improvement or measured RSS reduction is claimed.

## Reproduction and later manual validation

Run the nonexecuting diagnostic (safe now; it currently reports memory_admission_fits=false):

```bash
cargo run --release --manifest-path engine/Cargo.toml \
  --no-default-features --features historical \
  --example normalize-admission -- .
```

Run synthetic and support regressions:

```bash
cargo test --release --manifest-path engine/Cargo.toml \
  --no-default-features --features historical \
  --test normalization_retained_memory --test historical_normalize_support \
  -- --test-threads=2
```

Run the requested real-fixture configuration:

```bash
cargo test --release --manifest-path engine/Cargo.toml \
  --no-default-features --features "historical,historical-real-tests" \
  --test historical_normalize -- --test-threads=2
```

Build the normalization executable:

```bash
cargo build --release --manifest-path engine/Cargo.toml \
  --bin marketforge-normalize --features "historical,monitor-tui"
```

Full-batch execution remains a subsequent **manual** action, after report retention/admission is completed. The independent Bitget identity issue may still prevent success. Use a new output root; do not reuse the failed run or overwrite datasets:

```bash
normalization_validation_root="data/tests/historical/normalize/full-run/memory-validation-$(date -u +%Y%m%dT%H%M%SZ)-$$"
engine/target/release/marketforge-normalize \
  --batch engine/tests/historical/fixtures/normalize.json \
  --config data/.config/historical/general.toml \
  --project-root . --output-root "$normalization_validation_root" --tui
```

No performance benchmark was authorized or run, and no Forge changes were made.
