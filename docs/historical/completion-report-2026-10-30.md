# Historical modernization completion report

Status: **Ready for Review within the authorized bounded scope**, with the unsupported
profiles and unverified runtime categories below. This does not mark the milestone
Accepted. Branch: `codex/historical-modernization`, created from `7f9aaa4`.

## Architecture and changed files

The Historical TOML catalog is separate from Live. It validates deterministic
profile identity, physical schemas and source references, compatibility, versions,
duplicates and supported rules, then uses existing Shared normalizers. A v2 wire
job resolves into the existing v1 `ProcessingJob`/`WorkTask` execution model. Known
legacy v1 formats must agree with TOML; custom inline formats use the same Shared
compilers. Archive member and instrument/economic metadata remain runtime inputs.
There is no second normalization engine or Historical conversion refactor.

The complete contract, a job example, provenance, error policies, commands and
limitations are documented in [modernization.md](modernization.md).
Machine-readable inventories:

- [Profiles and rule counts](profile-inventory.json).
- [Every original Historical test target and its classification](test-inventory.json).
- [Preserved identities and new cases](test-preservation.json).

Added/modified implementation paths:

- `data/.config/historical/formats/{trades,depth}/*.toml` — 55 files.
- `engine/src/historical/config/{mod,catalog,job}.rs`.
- `engine/src/historical/mod.rs` and `job/load.rs`.
- `engine/src/historical/normalize/executor.rs` — resolved job snapshot in staging.
- `engine/src/shared/formats/depth/spec.rs`, `trades/{mod,spec}.rs`.
- `engine/src/shared/normalization/{decimal,timestamp}.rs`.
- `engine/src/historical/merge/execution/shared/resources.rs` — zero-stream rejection.
- `engine/Cargo.toml` — Historical TOML feature, grouped targets, opt-in slow feature.
- Group mains under `engine/tests/historical/{normalize,merge,convert}/` and
  `engine/tests/historical/slow/{normalize,merge,convert}/`.
- New `normalize/{configuration,equivalence}.rs`, nested depth helper imports and
  XLSX helper visibility; stale counter/fixture assumptions repaired in existing tests.
- Three checked-in conversion job parser fixtures and the v2 processing example.
- `engine/tests/shared/contracts/depth/depth_spec.rs` — current inventory assertions.
- `scripts/verify-historical.sh`, `engine/scripts/README.md`, and this documentation.

No Python source, Live implementation/configuration/tests/planning, or
`engine/src/historical/convert/` source was modified. Existing dirty Live planning
files and untracked `engine/.planning/` were preserved. No push, merge, reset,
database mutation, benchmark, public exchange request or production-scale archive
processing was performed.

## Configuration inventory and unresolved profiles

| Inventory | Count |
| --- | ---: |
| Raw-format records / complete profile identities | 36 |
| Normalization-rule records retained | 49 |
| Deduplicated physical definitions | 19 |
| Supported profiles | 31 |
| Compiled supported normalization rules | 42 |
| Unsupported profiles / retained uncompiled rules | 5 / 7 |
| Existing job files checked against TOML | 43 |

Supported trade/depth profiles preserve different instrument kinds and market
categories even when they share a physical parser. Rules stay separate for depth
snapshots versus updates. Contract values and value assets were not moved into
format files. Physical format, normalization policy and economic instrument
metadata remain separate.

Unresolved/unsupported:

- Bybit option trades and depth: existing product deferral; multi-instrument and
  option attributes are not implemented by current single-instrument processing.
- OKX option trades and depth: the same established option-processing boundary.
- Bitget inverse perpetual depth: quantity semantics are explicitly not established
  by the existing inspection. No base/quote/contracts encoding was invented.

All are discoverable with explicit rejection reasons and cannot execute. No
placeholder successful normalization was generated. External PostgreSQL export
files were not supplied/found; the unchanged local catalog seed definitions have
exactly 36 raw records and 49 rules and were reconciled with established jobs,
fixtures and inspection documentation. Equivalence against a separate database
export snapshot remains **unverified**, not assumed from matching counts.

## Job compatibility and provenance

The complete example is
[`historical-profile-v2.json`](../../engine/fixtures/jobs/historical-profile-v2.json).
The task identifies `format_profile` rather than repeating raw schema, normalizers,
format code and source ordering. It retains task/stream/input identity, authoritative
instrument metadata and archive member. Compression is profile-defined with only
explicitly permitted wrapper selection; source ordering cannot be overridden.
Job ordering, integrity policy, resources and output paths remain unchanged.

Python continues emitting v1. Existing registered definitions must exactly agree
with TOML, apart from absent versus null descriptive schema annotations; conflicts
are rejected. Unknown custom legacy formats retain inline support through Shared
compilers. Known format codes cannot use that escape to bypass compatibility.
Strict v2 serde rejects mixed embedded legacy fields. There are no two independent
normalization implementations.

The input wire protocol is v2, but resolution deliberately returns the established
v1 execution/snapshot model. Expanded runtime snapshots remain reloadable v1 jobs.
Each resolved task carries a profile ID and semantic SHA-256 fingerprint; the
processing executor commits `processing-job.json` with the existing staging-to-
dataset publication. It contains resolved rules and instrument metadata. Canonical
schemas and dataset manifest schema are unchanged. Optional v2 fingerprint pins
reject changed configurations.

## Shared work and confirmed defects

Shared exact parsing, quantity arithmetic, side transforms, extraction,
TradeProcessor, DepthProcessor, BookStore, archive readers and canonical types
remain the single production normalization infrastructure. Only validation and
confirmed exact-input defects changed. Scheduling, checkpoint ownership and archive
coordination remain Historical; no Shared-to-Historical dependency was introduced.
The unused private alternate CSV implementation is now test-only; all its original
unit witnesses remain present.

| Issue | Root cause and correction | Output impact |
| --- | --- | --- |
| Decimal input could round silently | Use exact mantissa parsing, preserving exact scientific notation support | Previously rounded overprecision inputs now reject; valid representable values unchanged |
| Seconds could truncate below nanoseconds | Reject fractional nanoseconds, convert exactly integral scaled Decimal with checked range | Such invalid input now rejects rather than changing its timestamp silently |
| Depth configuration errors accepted until processing / ignored | Strict rule fields, supported transformations, compatible fields/arrays/sequence/order validation in Shared compiler | Invalid configs fail before processing; valid migrated profiles unchanged |
| Invalid trade mapping/source declarations | Validate empty optional sources and canonical side-map targets at compile time | Invalid configs fail early |
| Zero-stream direct merge reservation accepted | Direct stream estimator now rejects as its existing contract/test requires; explicit empty-partition branch preserved | Invalid preflight behavior only; no market-data output change |
| Manifest/failure fixture counters inconsistent | Supply processed/skipped counts required by authoritative accounting equations | Test infrastructure repair; no weakened validator or assertions |
| Conversion parser tests depended on mutable generated references | Pin controlled production-shaped fixtures with original IDs/references/assertions | Test infrastructure only; conversion production code unchanged |
| Shared depth-spec test inventory stale | Include four current future-depth jobs: 19 jobs, 264 tasks, 519 specs | Test inventory repair only |

Previously processed data containing rounded excess precision or subnanosecond
input may require investigation/regeneration under an explicit integrity policy.
No replacement financial values are guessed. All tested valid legacy and TOML
paths produce exactly equal canonical results.

## Test organization, preservation and slow classification

| Registration | Before | After |
| --- | ---: | ---: |
| Historical integration targets | 160 | 3 normal + 3 opt-in |
| Package integration declarations (including unchanged other subsystems) | 197 | 43 |
| Original Historical integration cases preserved | 781 | 781 |
| Removed cases / changed ignore states | — | 0 / 0 |
| New config/equivalence cases | — | 13 |
| Historical integration cases total | 781 | 794 |
| Current library discovery | — | 155: 152 existing + 3 exact-input regression cases |

Original identities map to `<old target>::<old case>` under the appropriate group.
Normal groups have 112 normalize, 233 merge and 170 convert cases. Slow groups have
39 normalize, 80 merge and 160 convert cases (279 total; five retain existing ignore
attributes). Files remain in their responsibility directories, with explicit slow
module registration avoiding duplicate helpers or test bodies. Shared contract
and I/O tests remain under Shared targets.

The 60-second rule applies to individual cases, not total group duration. Slow is
also a conservative category for unverified external scans, stress/equivalence,
materialization and benchmarks. This is not a claim that every gated case takes
over 60 seconds. Known short real-fixture cases remain normal; the longest final
normal case was approximately 11.73 seconds.

Initial classification underestimated six indirect full-dataset scans. They reached
the existing 30-second supervisor deadline and were stopped:

- Combined reference checkpoint construction.
- Combined depth adapter/full-kernel comparison.
- Building a conversion output manifest from an external dataset.
- Preserving conversion configuration through that same heavy manifest fixture.
- Full converted dataset validation.
- Real depth event-index validation.

Related runs were stopped, incomplete/interrupted cases recorded, and those modules
and other uncertain full-data workloads isolated. No case was rerun after it became
known expensive. No final opt-in executable was invoked to execute test bodies and
no benchmark was run. Some bodies now isolated were attempted before classification;
their timeout/interruption results are **not passes**. All 279 opt-in cases remain
unverified for the final implementation; only compilation/discovery is claimed.
The 57 unselected library cases are also explicitly **not run** in final verification.

## Final verification and exact commands

Cargo processes ran sequentially, with locked/offline dependencies and default
features disabled. No unrestricted `--features historical` test execution,
`--all-features`, full Live integration run or slow-target execution occurred.

Normal compilation (exit 0):

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features \
  --features historical --test historical_normalize --test historical_merge \
  --test historical_convert --test depth_spec --test depth_processor \
  --test depth_transforms --test depth_scalar --test depth_csv_adapter \
  --test depth_xlsx_decoder --no-run --message-format=json
```

Normal per-case execution, from `engine/` (exit **0**):

```sh
python3 scripts/bounded_tests.py \
  target/historical-modernization/final-normal-artifacts.jsonl \
  target/historical-modernization/final-normal-run --run \
  --targets '(historical_(normalize|merge|convert)|depth_(spec|processor|transforms|scalar|csv_adapter|xlsx_decoder))'
```

| Final target | Passed | Failed | Timed out |
| --- | ---: | ---: | ---: |
| historical_normalize | 112 | 0 | 0 |
| historical_merge | 233 | 0 | 0 |
| historical_convert | 170 | 0 | 0 |
| depth_spec | 1 | 0 | 0 |
| depth_processor | 4 | 0 | 0 |
| depth_transforms | 14 | 0 | 0 |
| depth_scalar | 2 | 0 | 0 |
| depth_csv_adapter | 4 | 0 | 0 |
| depth_xlsx_decoder | 6 | 0 | 0 |
| **Total** | **546** | **0** | **0** |

Library compilation (exit 0):

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features \
  --features historical --lib --no-run --message-format=json
```

Focused library execution, from `engine/`:

```sh
python3 scripts/bounded_tests.py target/historical-modernization/final-lib-artifacts.jsonl \
  target/historical-modernization/final-lib-run --run --targets marketforge_engine \
  --cases '(shared::(normalization|formats|io)::.*|historical::(job|normalize)::.*|historical::merge::execution::shared::resources::.*)'
```

This selected **98 passed**, zero failed/timed out, and left **57 NOT_RUN**. The runner
accurately exits **1** for incomplete selection; it is not reported as an all-library
pass. A later focused job-example assertion passed once, with the other 111 cases
NOT_RUN (same selective exit 1), without changing production implementation.

Compile-only feature matrix (exit **0**):

```sh
scripts/verify-historical.sh compile
```

That runs:

- `cargo check --locked --offline --no-default-features --features historical`.
- `cargo check --locked --offline --no-default-features --features live --lib`.
- `cargo check --locked --offline --no-default-features --lib`.
- Normal/Shared target compilation above.
- `cargo test --locked --offline --no-default-features --features historical-slow-tests --test historical_slow_normalize --test historical_slow_merge --test historical_slow_convert --no-run --message-format=json`.

All passed. Slow discovery used `--list` only: 274 NOT_RUN, five existing ignored.
Historical TOML does not require Live; Live-only and minimal builds still compile.
Slow required-features are absent from Historical/default feature activation.
Authorized Rust formatting and `git diff --check` passed.

## Normalization equivalence evidence

The legacy baseline is deserialized directly from original embedded job definitions;
it is not first normalized through the TOML loader. V2 resolves independently from
the catalog, and both run the actual existing worker infrastructure.

- **18 trade profiles**, spanning five exchanges and their supported market/instrument
  categories. Existing gzip/ZIP archive input is sampled with hard 4 MiB/65,536-record
  read bounds, retaining eight matching native rows plus up to eight skipped identity
  rows. Tiny archive fixtures preserve wrapper and actual native fields. Futures
  family filtering remains authoritative. No entire large archive is processed.
- **13 depth profiles**, through tiny gzip, ZIP and TAR.GZ fixtures and the existing
  two-snapshot XLSX generator. They exercise initialization, absolute updates,
  relative updates, deletes, signed sides/quantities, timestamp sorting, sequence
  ranges and full book boundary snapshots.
- Complete canonical trade equality covers event count/order, timestamps, IDs,
  sides, exact prices, base/quote/contracts quantities and optional attributes.
  Depth comparisons include all canonical levels, original boundaries, operation,
  source ordinals/timestamps/sequences and complete boundary book metadata.
- Task integrity/counter reports are equal. Malformed depth input produces matching
  error/rejection behavior. Existing targeted trade-integrity and config-conflict
  tests retain their negative coverage. Configuration discovery checks every profile
  and rejects each unsupported identity explicitly.

No equivalence or live-compatibility claim is made for the five unsupported profiles,
large slow datasets, different database export versions or unexecuted targets.

## Evidence locations

Detailed, noncommitted logs are under `engine/target/historical-modernization/`:

- `final-normal-artifacts.jsonl`, `final-normal-build.log`, `final-normal-run/`.
- `final-lib-artifacts.jsonl`, `final-lib-build.log`, `final-lib-run/`.
- `example-artifacts.jsonl`, `example-build.log`, `example-run/`.
- `slow-discovery/`, `test-target-map.json`, `test-preservation.json`.
- Intermediate `normal-run-{1,2,3,4}/`, `interrupted-run.json`, and
  `interrupted-runs.jsonl` retain failures/timeouts/interruption evidence.
- `final-compile-matrix.log`, `format-check.log`.

Feature/slow compilation logs are under
`engine/target/historical-verification/20261030T001915Z-481455/`.
No skipped, timed-out, ignored or unexecuted case is counted as passing.

## Git commits and remaining limits

Local implementation commits:

- `e364518` — Historical TOML inventory, compiler validation and profile infrastructure.
- `8797aa1` — processing loader integration and committed resolved-job provenance.
- `f735689` — exact-input corrections, merge preflight and stale fixture repairs.
- `e48cf91` — checked-in conversion parser fixtures, preserving original assertions.
- `0dbaeb5` — grouped targets, slow isolation, configuration/equivalence tests and verifier.
- A subsequent documentation/evidence commit records this report and example.

Remaining limitations are intentional fail-closed unsupported definitions, absent
external export comparison, 279 unverified opt-in cases, 57 unselected library
cases, and environment-dependent existing real fixture paths in some older tests.
Initial conservative classification required corrections when indirect scans hit
the 30-second bound; those runs are not relabeled green. No other unresolved
failure remains in the final normal/selected Shared execution. Milestone acceptance
remains explicitly with the user.
