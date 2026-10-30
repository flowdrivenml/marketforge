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
