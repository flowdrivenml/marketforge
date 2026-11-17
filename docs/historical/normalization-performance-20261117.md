# Historical normalization performance implementation — 2026-11-17

Shared trade/depth optimizations are implemented and the final release binary successfully normalized the selected two-file trade and two-file depth workloads. This is evidence for these inputs, not every exchange or large-batch scaling.

Artifacts: `data/tests/historical/normalize/completion/normalization-performance/20261117-042647`. Production configuration, profiles, fixtures, raw sources, namespace mappings and identities were not modified. Initial working tree was clean at `10a96c585b4dd7c34d2c290e6ba7b75bacbb07ff`. No commit, reset, stash, extraction or Python orchestration change was made.

## Exact real investigation corpus

Exactly four distinct real archives were read throughout all trials and independent verification:

| Content | Task ID | Physical archive |
| --- | ---: | --- |
| trades | 13 | `/home/pasha/projects/marketforge/data/raw/7037/d5773da007c24ac8b13398dc987ebc5b/BTCUSDT-BTCUSDT_20261027_001.zip` |
| trades | 14 | `/home/pasha/projects/marketforge/data/raw/7038/51c2e2d4eab3498398a6ffb981bc72ae/BTCUSDT-BTCUSDT_20261027_002.zip` |
| depth | 171 | `/home/pasha/projects/marketforge/data/raw/5144/c9800e1e14864f31a4b31f286ec88fb1/BTC_USDT-2026102700.csv.gz` |
| depth | 172 | `/home/pasha/projects/marketforge/data/raw/5145/e5ce96a26d394258adb002cff0b317f2/BTC_USDT-2026102701.csv.gz` |

Trade stream: `bitget.linear.perpetual.BTCUSDT.trades`. Depth stream: `gateio.spot.spot.BTC_USDT.depth.incremental`. Derived batches have one job and exactly two concrete tasks each, preserving original source options, task IDs and catalog context. JSON guards run before every trial. Rust preparation proof also confirms unchanged task counts, paths, IDs, content and stream; retained reproduction runs execute that proof before launching normalization. No family expansion is possible for these concrete selections. Source sizes, mtimes and SHA-256 values are in `source-identities.json`; profile identities and exact copied TOML are retained.

Existing effective settings were preserved: workers 8, memory 64 GiB, scratch 128 GiB, reconciliation 8 GiB, record 4 MiB, depth 100,000 levels, handles 256, existing Parquet targets. Trades use existing earlier `prefer_source`, limit 1,000; depth uses existing `snapshot_recovery` and 2 ms timestamp-preserve tolerance. Both workloads actually excluded **zero** observations. Strict defaults remain unchanged in code; no permissive settings were enabled to obtain performance eligibility.

## Trades: complete CLI before/after

| Measurement | Baseline | Final verified binary |
| --- | ---: | ---: |
| Status | committed, Clean | committed, Clean |
| Wall seconds | 7.808 | 1.303 |
| User / system CPU seconds | 6.142 / 1.571 | 1.217 / 0.375 |
| Process peak RSS MiB | 194.89 | 195.21 |
| Workspace accounted peak bytes | 354,002,944 | 504,960 |
| Workspace scratch peak bytes | 17,362,446 | 8,681,421 |
| Execution path | general identity reconciliation | `ordered_run_append` |
| Rows / files | 200,000 / 1 | 200,000 / 1 |
| Unique / unverified exclusions / verified duplicates | 0 / 0 / 0 | 0 / 0 / 0 |

A matched Basic-diagnostics repeat measured 6.907 s baseline versus 1.202 s candidate. It captured exact workspace traffic:

| Structural work | Baseline | Checked append |
| --- | ---: | ---: |
| Scratch read / written bytes | 78,131,403 / 69,450,180 | 8,681,421 / 8,681,421 |
| Frames read / written | 1,800,000 / 1,600,000 | 200,000 / 200,000 |
| Sort runs generated | 4 | 0 |
| Merge passes | 0 (each sort fit one run) | 0 |
| Compressor initializations | 10 | 2 |

These traffic counters come from the explicitly identified diagnostic repeat, not retrospectively from disabled diagnostics. Baseline performs identity, chronological unique-union, native/loss and final-order sorts plus intermediate rewrites. General reconciliation now attaches frozen ranks while decoding the initial identity sort, eliminating its separate full priority rewrite. Checked append proves every source's native/time order plus disjoint ID ranges and reads sealed source spools directly into Parquet. Checked overlap merge verifies the entire chronological unique union and materializes only its sealed survivor spool.

All canonical fields, row-level source positions, identity mappings, ownership and quality equal the baseline. A separate streaming CSV/Decimal/integer oracle verified every actual trade ID, quantity, price, timestamp and optional field. IDs above 2^53 remain exact.

## Depth: censored baseline and completed candidate

| Measurement | Baseline | Final verified binary |
| --- | ---: | ---: |
| Status | timed out, no committed manifest | committed, Degraded |
| Wall seconds | 120.115, censored at watchdog | 24.858 |
| User / system CPU seconds | 117.382 / 2.585 | 26.256 / 5.586 |
| Process peak RSS MiB | 106.45, incomplete | 998.75, complete |
| Workspace accounted peak bytes | not captured | 1,182,215,744 |
| Workspace scratch peak bytes | not captured | 213,901,063 |
| Exact scratch traffic | not captured | disabled diagnostics; not captured |
| Logical process read / write bytes | not captured | 412,830,578 / 293,397,514 |
| Execution path | independent replay with full-book event checks | `independent_depth_continuation` |
| Committed rows / logical events | none | 2,211,066 / 2,109,457 |
| Unique / unverified exclusions / verified duplicates | not a completed outcome | 0 / 0 / 0 |

The baseline reached depth reconciliation/Parquet staging, evidenced by an unfinished `events.parquet`; exact event progress was not captured. It has no valid publication manifest. It was not rerun. Its RSS and CPU are censored, so neither a precise completed-workload speedup nor a complete-memory comparison can be derived.

The candidate performs two full-state comparisons total at initialization, visits 203,222 levels in those comparisons, and performs 2,109,455 changed-level checks. Payload events decode once (2,109,457); accounting visits 2,109,457 compact metadata records rather than decoding full level vectors again. It keeps a long-lived writer across both sources. Scratch framing/checksums/completion and publication validation remain enforced.

An independent streaming oracle replayed every source row using a separate Decimal/dictionary relative-level implementation and checked all canonical fields, original timestamps, sequences, source ordinals, initialization boundaries and event indexes. The source books remain valid. Completeness relative to supplied observations is preserved. Quality is degraded because continuity across the independent hourly histories is unproven; timestamps are honestly native source order. External complete market-history coverage is **not** claimed. See `verified-real-correctness.json`.

## Trial inventory and measurement conditions

Initial trials: one baseline and one candidate per workload, each with a 120-second process-group watchdog. Later targeted repeats collected trade diagnostics and verified successive final builds; all used the same four archives. There were **six trade trials and four depth trials total**, including the one censored baseline. No exchange fixture, full batch, default benchmark collection or production benchmark sweep ran.

| Immutable executable | Workload | Outcome | Wall s | CPU s | RSS MiB |
| --- | --- | --- | ---: | ---: | ---: |
| baseline | trades | committed | 7.808 | 7.713 | 194.89 |
| baseline | depth | 120 s timeout, incomplete | 120.115 | 119.968 | 106.45 |
| candidate | trades | committed | 1.101 | 1.395 | 195.04 |
| candidate | depth | committed | 21.615 | 27.167 | 1000.96 |
| baseline-diagnostics | trades | committed | 6.907 | 6.897 | 194.49 |
| candidate-diagnostics | trades | committed | 1.202 | 1.411 | 198.20 |
| final | trades | committed | 1.102 | 1.378 | 195.28 |
| final | depth | committed | 21.630 | 27.147 | 1001.78 |
| verified | trades | committed | 1.303 | 1.591 | 195.21 |
| verified | depth | committed | 24.858 | 31.842 | 998.75 |

Watchdogs terminate only their own child process group. Output roots are required absent; logs sit outside them. Abrupt SIGTERM can leave unpublished staging/scratch; no timed-out output was treated as a completed archive/dataset. Existing normal error/unwind cleanup remains tested. The unknown-PID original baseline's residual workspace was not guessed at or deleted.

Wall time includes process startup, complete CLI normalization, integrity checks and commit; build/data generation/independent verification are separate. `wait4` supplies process CPU and peak RSS. `/proc` I/O is sampled and last-observed, not physical scratch-device traffic. Ordinary warm/unknown OS cache was used, with no privileged cache manipulation. Physical reads in the later trials were cached; no equal cold-cache or statistical confidence claim is made. Some early development trials overlapped development activity; all raw measurements remain retained, and the final result is reported rather than choosing the fastest candidate.

All executables were optimized release builds with `historical,monitor-tui` and `CARGO_PROFILE_RELEASE_DEBUG=1` for usable symbols. They remain immutable:

| Binary | SHA-256 |
| --- | --- |
| `baseline` | `53d7eed29a66883446590dc11d19f0432195e810abe1ee94717513d829b93649` |
| `candidate` | `f163c3f850ab7e74cbde9ecd2fc0b91c82fb6919644cdd061d97557c25df9d32` |
| `final` | `867a9ecc133e0e0543ea9b8cc4a3da25f121f855606dd183206cfd2d4a74a59d` |
| `delivery` | `e3d7c6be23fe5bac3f5a71f95a33d46a626102002b72afd1e18527cd9a2fe7b6` |
| `verified` | `b2a734405731e072c40d27f7516fd886e720c18825fa59b5ed7a8d76ee22ea3c` |

The final `verified` binary is the delivery build. Earlier binaries remain for reproducibility; none was replaced while running. Source snapshots, dirty diffs/hashes, configuration/profile identities and build logs distinguish this checkout from its HEAD. The final production build completed in 30.18 s, separately from trial timings.

## Small generated representation experiment

Two synthetic Bybit-shaped ZIP sources (4,096 levels, 2,000 ordinary updates each) provide a completed depth baseline: 0.301 s / 0.250 CPU s versus initial candidate 0.201 s / 0.143 CPU s, with identical 12,192 canonical rows and 4,002 event indexes. The 100 ms watchdog polling granularity limits short wall-time interpretation; operation counts are stronger evidence.

Three additional generated-source delivery trials varied one dimension at a time from JSON/zlib, keeping profiles, policies, workers, budgets and Parquet targets fixed. They are not additional real archives or a Cartesian sweep:

| Representation / codec | Wall s | CPU s | Scratch read / written bytes |
| --- | ---: | ---: | ---: |
| json-zlib | 0.201 | 0.132 | 940,501 / 701,641 |
| json-raw | 0.101 | 0.112 | 8,498,934 / 7,409,658 |
| typed-zlib | 0.101 | 0.115 | 903,217 / 663,620 |

All three match baseline canonical fields, event indexes, source ownership and quality. Typed depth uses 4,002 typed encoding calls. Raw scratch costs substantially more I/O here. Single small trials do not justify changing production defaults. No eight-worker scaling or general codec speedup is claimed.

## Implementation and verification matrix

| Area | Implementation location / evidence | Status / limitation |
| --- | --- | --- |
| Explicit execution plans and checked trade continuation | `reconciled.rs`, `reconcile.rs`, `reconcile/external.rs`; `performance_checked_ordered_sources_match_complete_trade_oracle_and_general_path` compares full fields/ownership with forced general execution | Implemented; strict timestamp evidence required; bounded 128-byte evidence cache falls back without truncating IDs |
| Generic identity reconciliation | `external.rs`, `coverage.rs`; frozen rank table, coverage sweep; reference-oracle, Bitget missing/reset/conflict/discard tests | Implemented; four validation sort orders remain when the general contract needs them |
| Exact depth invariant | `reconciled_depth.rs`, shared `book/snapshot.rs`; 256/4,096/50,840-level structural tests, complete small-book replay | Implemented; ordinary deltas have changed-level cost; snapshots/transitions still require full states |
| Fragmented source ownership | `depth_partitions.rs`; source selected three times: 18 payload reads for 9 original events, 3 exact checkpoint restores; invalid source-boundary switches reject | Implemented, bounded forward partitions; checkpoint cost remains proportional to book size and switches |
| Depth accounting | `reconciled.rs`, `reconciled_depth.rs`; writer-bound sidecar, payload/metadata checks, native-order loss-span tests | Implemented; metadata does not replace payload/checksum/footer validation |
| Shared independent preparation | `preparation.rs`, `streams.rs`; two queues ready before either is drained, cancellation/permit test, memory pressure serial fallback | Implemented; maximum two pipelines per stream; families/shared paths use existing route |
| Scratch format and resources | `spool.rs`, `typed_depth.rs`, `typed.rs`, shared config; exact options/decimals/empty events, invalid tags/counts/truncation, codec equivalence and pressure | Implemented opt-in `typed_events`; unchanged frame cap/codec defaults; no in-memory spool path |
| Family/archive reads and profile parsing | Existing bounded routing, member inventory/safety and prepared contexts; 10 family routing + 10 family contract tests | Already adequate for bounded per-phase traversal; no new parser or combined discovery/routing cache |
| Long-lived Parquet and downstream schema | Existing writers retained; one output file across each real pair; exact Parquet/event-index comparisons and shared-consumer compile | Already adequate; Arrow builder/row-group targets unchanged; further allocator work unmeasured |
| Provenance and integrity bookkeeping | `manifest/positions.rs`, `metrics/windows.rs`, `worker/integrity.rs`; borrowed span index, reusable frame buffer, batched successful checks, window/overflow equivalence | Implemented; fatal observations remain immediate; spills stay streamed |
| Failure handling | Early native/domain/coverage evidence, whole-union validation before loss, canceled read-ahead observations retained even before sink creation | Improved; future-source definitive errors are surfaced on consumption rather than an asynchronous global early-abort protocol |
| TUI/telemetry | Existing bounded reporter and source preparation phases; 15 dashboard tests, 18 retained-memory/lifecycle tests | Integrated; no rendering-thread I/O/joins; fine phase CPU/raw-pass instrumentation remains incomplete |
| In-memory runs and discovery caching | Reviewed existing scratch and family ownership | Not implemented; sealed source evidence, bounded ownership and spill equivalence would need additional design/measurement |
| Large-batch throughput / hardware scaling | Two source progress handshakes plus four-archive trials | Not established; no all-exchange or eight-worker performance claim |

## Commands actually executed and results

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release --manifest-path engine/Cargo.toml \
  --bin marketforge-normalize --features historical,monitor-tui
cargo test --manifest-path engine/Cargo.toml --lib --features historical,monitor-tui historical::normalize
cargo test --manifest-path engine/Cargo.toml --lib --features historical,monitor-tui historical::normalize::reconcile::policy_tests
cargo test --manifest-path engine/Cargo.toml --lib --features historical,monitor-tui historical::config::batch::family_contract_tests
cargo test --manifest-path engine/Cargo.toml --lib --features historical,monitor-tui shared::historical::config
cargo test --manifest-path engine/Cargo.toml --test historical_normalize --features historical,monitor-tui
cargo test --manifest-path engine/Cargo.toml --test normalization_dashboard --test normalization_retained_memory --features historical,monitor-tui
cargo check --manifest-path engine/Cargo.toml --lib --features live,historical,monitor-tui
cargo clippy --manifest-path engine/Cargo.toml --lib --bin marketforge-normalize --features historical,monitor-tui
```

Results: release build passed; 160 normalization unit tests, 27 focused policy tests, 10 family contract tests, 9 runtime config tests, 159 synthetic integration/profile tests, 15 dashboard and 18 retained-memory tests passed. Overlapping test filters are not additive unique-test counts. No `historical-real-tests`/full/live test suite ran. Live/historical shared consumers compiled without execution. Clippy succeeded with 98 existing warnings; no new warning is suppressed. Changed-file rustfmt and `git diff --check` passed. Repository-wide `cargo fmt --check` is blocked by the pre-existing configured but absent `engine/src/bin/marketforge-tui.rs`; no dummy target or target removal was made.

## Bounded reproduction (zsh)

The retained harness validates the four-source allowlist, runs Rust preparation assertions, creates a fresh destination and owns its 120-second watchdog. From repository root:

```sh
run='data/tests/historical/normalize/completion/normalization-performance/20261117-042647'
stamp=$(date -u +%Y%m%dT%H%M%SZ)
python3 "$run/trial.py" "verified-repro-$stamp" trades
python3 "$run/trial.py" "verified-repro-$stamp" depth
MARKETFORGE_PERF_CANDIDATE=verified python3 "$run/verify.py"
```

The final verification command checks retained `verified-*` outputs against the baseline and independent raw-source oracles. It is separate from trial timing. To verify a fresh tagged output, set `MARKETFORGE_PERF_CANDIDATE="verified-repro-$stamp"`. Do not pre-create the output directories. The scripts retain JSON results and console logs outside the exact output roots. They use child-specific `wait4` status capture, not Bash-only `PIPESTATUS`.

Python acquisition/orchestration is unchanged. These are isolated measurement/verification artifacts, not a replacement production CLI or reporting system.
