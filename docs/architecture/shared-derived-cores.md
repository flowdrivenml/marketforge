# Shared conversion and chronological merge cores

## Conversion responsibility audit (before extraction)

The baseline is `54205985970eb13f1fe231be6d2d97388ae0c8f9` on `origin/main`.

| Existing Historical module | Responsibility | Destination |
| --- | --- | --- |
| `convert/shared/decimal.rs` | Decimal256(76,28), exact BigInt ratios, checked i128 fast path, half-even rounding, precision and division errors | Shared conversion decimal |
| `convert/planning/catalog.rs` instrument record | Dataset/stream identity plus existing Shared instrument identity and currencies | Shared conversion input record; Historical resolves it |
| `convert/planning/catalog.rs` resolver | Jobs, manifests, catalog lookup, internal/external trade reference selection | Historical |
| `convert/planning/graph.rs` | Direct/inverse edges, deterministic adjacency order, cycle-free bounded path enumeration | Shared conversion graph |
| `convert/planning/planner.rs` | Selection, identity/unresolved accounting, path limit, reusable plan | Shared planner; Historical validates job and translates selection |
| `convert/reference/state.rs` | Ordered observations, positive last trade, strict monotonic update, freshness and future rejection | Shared reference state |
| `convert/reference/evaluation.rs` | Reference identity checks, exact reduced rational path evaluation, failure classifications and provenance | Shared evaluation |
| `convert/trades/trade.rs`, `convert/depth/depth.rs` | Additional converted price and optional quote quantity, no intermediate rate rounding | Shared trade/depth transformations |
| `convert/reference/reader.rs`, `external.rs` | Indexed Parquet observations, timestamp alignment, cursor resume | Historical |
| Conversion execution, output and progress | Per-event status/path choice, atomic depth event handling, Arrow schemas/builders, checkpoints, partition scheduling, validation, publication | Historical |

No economic algorithm is redesigned. Currency and instrument identity remain the existing
Shared domain types. Historical public paths will re-export moved types/functions.
The Shared planner accepts resolved instruments, explicit selection, target currency
and hop limit; it cannot inspect jobs, filesystem paths or manifests.

Internal references are eligible only when their complete ordering key is strictly
less than the source key. External cursors consume only observations with a strictly
earlier timestamp, regardless of rank. Those transport/alignment rules remain in
Historical. State rejects nonpositive prices and nonadvancing keys; evaluation
preserves missing/future/stale/identity/arithmetic failure distinctions. Reference
prices remain last trades. No midpoint or alternate price policy is introduced.

Decimal arithmetic keeps Arrow's `i256` scalar representation without requiring
Arrow arrays or Parquet in the core. Scale 28, precision 76, half-even rounding,
arbitrary precision rational intermediates and the checked i128 fast path remain
unchanged. Transformation functions return additional values and leave canonical
inputs intact. Historical retains event-level atomicity and output status encoding.

## Conversion checkpoint

PASS: minimal library and Historical checks; 65 Shared cases run without Historical;
119 existing normal Conversion cases pass before and after extraction. The 81
other existing Conversion cases fail both before and after with absent local jobs
or datasets (all report `No such file or directory`); their dataset-level behavior
is BLOCKED on those fixtures, not claimed verified. All 200 existing names and
outcomes match. No test registration or ignored state was removed.

A new self-contained Historical kernel witness runs against pristine baseline
production code in an isolated detached worktree, then against the extraction.
Its checked-in `core-regression.txt` records exact Decimal256 mantissas, statuses,
failure codes, path currencies and reference provenance. The transcript matches
exactly for identity/direct/inverse/multihop, missing/stale/future/identity mismatch,
same-timestamp reference eligibility, unavailable paths, atomic depth overflow,
and combined trade/depth dispatch. No floating-point approximation is involved.
The original arithmetic/reference/value test bodies now live under Shared and are
included by their original Historical modules, preserving Historical discovery.

The only new unconditional dependencies are the existing `arrow-buffer` scalar
and BigInt arithmetic dependencies. Arrow arrays and Parquet remain optional.
There are no new per-event allocations, scans, synchronization or path discovery;
existing construction and evaluation boundaries remain intact.

## Merge responsibility audit and extraction boundary

`merge/core/kway_event.rs` and `kway.rs` duplicate a reverse BinaryHeap of
(timestamp_ns, stream_rank, task_rank, event_ordinal, source index), one pending
reference per source, global ordering checks and checked event counting. Unified
merging additionally checks a replacement source head immediately; depth-only
merging detects regressions when the regressing head is popped. Preserve that
observable error timing. Depth streams retain their established boundary and
regression handling before supplying heads.

Extract a generic head queue with owned, opaque event payloads and explicit
awaiting/ready/exhausted source slots. It contains the heap, pending payloads,
ordering validation and count checks. Historical pull coordinators retain source
traits, I/O advancement, skipped initialization reporting and regression counters.
A queue with an awaiting source reports Pending; only explicitly exhausted sources
permit final Exhausted. Future Live coordination may supply canonical events but
must establish safe heads itself; no watermark or asynchronous policy is inferred.

Both keys compare the identical four fields in the identical order, so Shared
Merge owns the canonical key and Conversion aliases it. Historical depth-event
key construction remains a Historical implementation on the re-exported type.
Event references remain opaque payloads: neither snapshots nor level vectors are
materialized. Dataset identity checks, stream inventory and rank assignment,
reconciliation, snapshot deduplication, reconstruction/checkpoints, partitions,
logical output, physical materialization, resource scheduling and publication all
remain Historical.

## Final Shared contracts

`shared::merge::{HeadQueue, MergePoll, MergeKey}` owns the queue policy. `push`
accepts a stable source index/rank, the complete nanosecond ordering key, and an
owned event payload of any size/type without Clone requirements. Sources have
one head each. Duplicate occupied source slots, invalid ranks/indices and updates
after explicit exhaustion are rejected. `next_head` returns Ready, Pending or
Exhausted. After Ready, the contributor alone must supply its next head or finish.
A Pending source blocks selection even if other heads are available: temporary
emptiness supplies no evidence that those heads are chronologically safe. Errors
are terminal for the consuming coordinator. Historical continues its synchronous
pull-ahead before returning each event and propagates I/O errors unchanged.

`shared::merge::ordering::MergeKey` is the canonical lexicographic four-field key.
`conversion::reference::ReferenceKey` aliases it. The conversion transcript witness
normalizes only the Rust Debug type label from MergeKey to ReferenceKey; all
mantissas, key fields, status/failure values and provenance remain exact baseline
text. Historical `MergeKey::from_depth_event` remains implemented in Historical.

`shared::conversion` exposes `decimal`, `graph`, `planner`, `reference`,
`evaluation`, `trades` and `depth`. The planner takes currencies, resolved Shared
instrument records, a selection predicate and hop limit. Its output preserves
source order and candidate path order. Evaluation takes a planned path, current
reference state, source key and maximum staleness and returns the exact rational,
rounded reporting rate, and contributing observations. Trade/depth functions apply
the rational directly to original price and optional quote quantity. Historical
retains candidate fallback loops and event-level Arrow buffer rollback.

The queue keeps the original heap and bounded pending storage with constant-time
source-state checks. It does not allocate or clone event payloads per event. It
adds no scans, threading, synchronization, async runtime or dispatch; the original
Historical trait-object source calls remain. PartitionDepthStream's existing
counter-and-drop policy for timestamp regressions and DepthStream's rejection
policy are unchanged. No performance benchmark was run, so throughput equivalence
is NOT VERIFIED; the execution and memory complexity are preserved by inspection.

Live integration points are these Shared plans, observations, transformations and
opaque event queue payloads (including DepthProcessingOutcome). Safe source-head
coordination, watermarks and lateness policies remain outside this implementation.
No Live Derived Streams implementation or Spectra integration is included.

## Completion and verification report

| Item | Result |
| --- | --- |
| Starting branch / fetched default | `main` / `origin/main` |
| Starting commit | `54205985970eb13f1fe231be6d2d97388ae0c8f9` |
| Development branch | `codex/shared-derived-cores` |
| Initial tree | PASS — clean, no unrelated modifications to preserve |
| Conversion extraction commit | `c0ee81d` (`refactor(shared): extract historical currency conversion core`) |
| Merge extraction commit | This report is included in `refactor(shared): extract historical chronological merge core`; final hash is supplied in the completion response |
| Minimal library | PASS — locked offline `cargo check --no-default-features --lib` |
| Historical-only | PASS — locked offline `cargo check --no-default-features --features historical` (including required binaries) |
| Live-only library and binary | PASS — locked offline `cargo check --no-default-features --features live` (also checked separately with `--lib`) |
| Normal integration compilation | PASS — `historical_normalize`, `historical_merge`, `historical_convert` |
| Slow integration compilation | PASS — all three `historical_slow_*` targets, `--no-run` only |
| Shared compilation without Historical | PASS — `shared_conversion`, `shared_merge`, library unit target |
| Production dependency audit | PASS — neither new core imports Historical, Live, Arrow arrays or Parquet |
| Numerical / reference / kernel transcript equivalence | PASS — self-contained baseline witness and extraction agree exactly (Debug alias label normalized only) |
| Merge order / event-count / error timing equivalence | PASS — baseline and extraction agree exactly |
| Complete real-dataset equivalence | BLOCKED — required local datasets/jobs are absent |
| Throughput equivalence | NOT VERIFIED — no benchmarks authorized |
| Slow test execution / benchmarks | NOT RUN |

Shared Conversion modules created: `mod.rs`, `decimal.rs`, `graph.rs`, `planner.rs`,
`reference.rs`, `evaluation.rs`, `trades.rs`, `depth.rs`. Shared Merge modules
created: `mod.rs`, `ordering.rs`, `kway.rs`.

Historical Conversion changes: `shared/decimal`, `planning/{catalog,graph,planner}`,
`reference/{state,evaluation}`, `trades/trade`, `depth/depth`. The planner is a job
validation/selection adapter; the resolver still supplies existing identities.
Other moved APIs are re-exported at their existing Historical paths, including
ResolvedConversionInstrument, ConversionPlan, PlannedConversionStream, decimal
constants/type, graph types, reference types/evaluation and trade/depth functions.
Historical Merge changes: `core/{ordering,kway,kway_event}`. KWayDepthMerger and
KWayEventMerger retain their constructors, metrics, source traits and return types;
MergeKey is re-exported and its depth-reference constructor stays Historical.
Neither source adapters nor execution/partition/materialization modules changed.

### Test discovery and execution

Every executed case used the existing bounded supervisor: a separate process with
an exact test name and 30-second deadline. No test timed out. Initial offline
compilation was BLOCKED by an empty dependency cache; locked online builds filled
that cache. Final matrix checks and test builds succeeded locked/offline. `cpout`
was unavailable; command output and Cargo JSON artifacts were captured directly
under `engine/target/shared-core-evidence/` with original exit codes retained.

| Target | Discovered | Executed result |
| --- | ---: | --- |
| Original Historical Convert baseline | 200 | 119 PASS, 81 FAIL (missing fixtures) |
| Final Historical Convert | 201 | 120 PASS, same 81 FAIL (missing fixtures) |
| Original Historical Merge baseline | 246 | 237 PASS, 9 FAIL (missing fixtures) |
| Final Historical Merge | 247 | 238 PASS, same 9 FAIL (missing fixtures) |
| Historical Normalize | 124 | NOT RUN; compiled only |
| Shared Conversion | 65 | PASS, both Historical-enabled and minimal builds |
| Shared Merge | 8 | PASS in minimal build; first 6 also passed Historical-enabled |
| Shared merge overflow unit | 1 selected | PASS in minimal build |
| Parquet decimal integration | 8 | PASS |
| Slow Normalize | 33 (2 ignored) | NOT RUN; compiled/discovered only |
| Slow Merge | 67 (3 ignored) | NOT RUN; compiled/discovered only |
| Slow Convert | 131 | NOT RUN; compiled/discovered only |

The minimal library unit target discovers 65 cases; only its selected Shared merge
overflow case ran. The remaining 64 unit cases were NOT RUN. Slow discovery lists
231 total cases, including the five existing ignored cases; listing did not run
any body. All 446 original normal Merge/Convert case names, ignore states and
outcomes are preserved. The two added Historical witnesses pass. Across the final
selected targets, 440 unique cases pass; the 90 missing-fixture failures are
unchanged from baseline. Repeat runs are not counted as additional unique cases.

The unavailable fixtures include `data/.jobs/convert/convert-multihop.json`,
`convert-combined-test.json`, `data/.work/convert-validation/*`, generated merge
manifests, `data/.jobs/merge/merge-200.json` and normalized trade datasets. All 90
baseline failing logs contain `No such file or directory` (os error 2), and every
one retains the same failing status after extraction. No test was removed,
ignored, rewritten to skip absent data, or supplied with fabricated production
outputs. The baseline/final comparison and case names are preserved in
[verification evidence](shared-derived-cores-verification.json).

Deterministic available cases cover exact Decimal256 scale/precision and rounding,
inverse/multihop ratios, missing/stale/future references, same-timestamp priority,
identity mismatch/provenance, trade and depth values, depth atomic overflow,
combined dispatch, source-local ordinals, equal keys, unique queue slots, empty
sources, temporary emptiness, exhaustion, non-Clone atomic depth outcomes, original
native metadata, source errors, reconstruction/checkpoint/boundary logic and
regression rejection. Dataset-dependent publication/replay cases remain BLOCKED;
the matching self-contained witnesses do not claim to validate absent datasets.

No push or merge was performed. No Live production code/configuration, Python,
Forge orchestration, PostgreSQL, secrets, AGENTS files or autonomous planning
sources were read or modified as part of the extraction. Live Derived Streams
remains future work.
