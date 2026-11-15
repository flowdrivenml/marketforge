# Full prepared-batch normalization failures — 2026-11-15

Status: diagnosis complete; production behavior unchanged. No full real-data normalization or benchmark was executed. Sources were opened read-only. Existing worktree contains extensive pre-existing changes; these were preserved. No implementation commit was made for this diagnostic task. Reports are ignored by the repository's existing Git rules.

## Evidence and scope

Inputs: `engine/tests/historical/fixtures/normalize.json`, `data/.config/historical/general.toml`, and persisted `processing-job.json`, `processing-runtime.json`, and `failure.json` under `data/tests/historical/normalize/full-run/20261115-185513/staging/`.

The actual persisted jobs contain prepared-schema-version **4** metadata. This report follows the actual loader and files rather than older schema-version-3 descriptions. The runtime schema remains 1.

Companion artifacts:

- `full-batch-trade-evidence.json`: exact paths, row counts, actual min/max timestamps, missing-ID/regression counts for every affected archive, and all overlapping pairs with boundary-ID comparisons.
- `full-batch-trade-diagnostic.py`: read-only CSV scan used to produce that evidence (run from repository root).
- `full-batch-admission-diagnostic.rs`: temporary unit-test probe used to reproduce admission through the production batch loader. It resolves metadata and calculates estimates only; it never executes normalization. The probe was appended temporarily to `engine/src/historical/normalize/resources/estimate.rs`, executed with `cargo test --manifest-path engine/Cargo.toml --features historical --lib diagnostic_full_batch_admission_breakdown -- --nocapture`, and removed. Its exact source is retained here for reproducibility. Do not turn it into an unconditional real-data unit test.

## 1. Trade stream identification and failure

Job `7198016171337191544`, dataset `1335781940318399008`, stream fingerprint `c267594601432a594fc8057ce4786c9f00d55a0bfb3996927c310933693aceb7` is:

- Exchange: **Bitget**.
- Market: **linear perpetual**, USDT quoted.
- Symbol: **BTCUSDT**, concrete instrument ID **5**.
- Logical stream: `bitget.linear.perpetual.BTCUSDT.trades`.
- Profile: `bitget.perpetual.linear.trades`, format `BITGET-T1`; profile SHA-256 `1e965751ed2491123027395b6df8c99fb1067d08a74b81a3d9f7a45e8ef217c0`.
- Tasks **13–54**, 42 ZIP archives, containing CSV chunks for October 27–30.

Identification was independently reproduced with SHA-256 of compact sorted-key JSON matching `streams::stream_key`. That function hashes logical stream, exchange, instrument ID, symbol, instrument specification, normalizations, source ordering, namespace, profile identity/fingerprint and source options. It excludes source path, task ID and priority so compatible archives group together. This stream is **not family-routed**: its `scope` is null. Family planning uses a separate `family-routing` fingerprint, which correctly reproduces the depth fingerprint below.

### Actual archive contents

Read-only scan: **4,037,370 rows**, **zero missing IDs**, **zero within-source timestamp regressions**. Persisted failure metrics independently confirm all 42 accepted min/max ranges, ordering validation, row normalization counts and zero rejected records. Their actual file/hour/day map inventories contain only 3–9 entries per affected task (222 total), versus the 65,536-entry allowance per task. Source timestamps are milliseconds with observed second precision. The profile already documents that precision and numbered ZIP chunks. Actual overlapping accepted-time candidates occur at **37 adjacent chunk boundaries**, each an intersection of exactly one timestamp value. No pair has a positive-duration interval intersection. None of the IDs in one side of any intersecting bucket occurs in the other side. No cross-source duplicate or conflict was found in those buckets.

First overlapping boundary in chronological chunk order (the failure message does not record the exact row that triggered rejection):

- Task 13: `data/raw/7037/d5773da007c24ac8b13398dc987ebc5b/BTCUSDT-BTCUSDT_20261027_001.zip`, member `BTCUSDT_UMCBL_20261027_001.csv`.
- Task 14: `data/raw/7038/51c2e2d4eab3498398a6ffb981bc72ae/BTCUSDT-BTCUSDT_20261027_002.zip`.
- Actual timestamp intersection: **1793037823000 ms**. Seven IDs in the first archive and 35 in the second occupy that timestamp; **zero common IDs**.

All paths/ranges and all 37 responsible pairs are in the companion JSON and appendix. Comparisons use source `trade_id` strings, never timestamp/price/quantity/side as identity. The scan does not claim a universal uniqueness/reset guarantee or absence of ID reuse at different timestamps.

### Why reconciliation rejects it

`reconciled::observation` reads optional `raw_schema.marketforge_trade_identity`. It is absent in the persisted tasks and the current Bitget profile. `reconcile::reliable_source_identity` therefore falls back to `reliable_identity`, which recognizes Binance, OKX and UUID-shaped Bybit IDs, **not Bitget**. Present numeric Bitget IDs consequently do not establish a reliable cross-source identity domain.

`reconcile/external.rs::trades_with_progress` checks inclusive min/max interval intersection for an unreliable source (`start <= other_end && other_start <= end`). The shared second-resolution boundary satisfies that condition and raises the reported error. The message's “including missing IDs” is a generic explanation; missing IDs are not the cause here.

This is legitimate timestamp overlap between apparently distinct trades split across packaging boundaries. The engine's overlap detection is accurate at the available timestamp precision, and its refusal to infer identity is correct under the current policy. The configuration lacks an established identity contract for this feed. ID presence alone cannot authorize deduplication. Merely changing `<=` to `<` would lose this safety guarantee.

Source priorities 12–53 follow task/chunk order. Canonical tie order is deterministic by priority, source index and physical ordinal. Changing source ordering cannot establish an ID domain, and reversing priorities cannot resolve the failure. Source-local chronological append requires explicit source-local policy and **strictly disjoint** intervals; it cannot safely solve these shared boundaries. Family routing contributes nothing to this trade failure.

### Smallest correct resolution

Establish and document the native Bitget archive trade-ID/reset contract, then declare the narrowest justified explicit profile policy, normally `instrument_scoped` with a documented stable feed/instrument-lifetime reset domain if that guarantee is established. Keep exact cross-source identity reconciliation, conflict precedence, missing-ID rejection and deterministic ordering. Do not add a blanket exchange whitelist, guess a domain from dates/ID shape, label these source-local to suppress validation, drop boundary rows, or infer deduplication from payload equality.

The official [classic contract market documentation](https://www.bitget.com/docs/catalog/classic-contract-market/classic-contract-market) exposes `tradeId`, and the [download center documentation](https://www.bitget.com/legacy-docs/uta/data-download-center) identifies downloaded trades as public fills. The documentation inspected does **not** establish the historical CSV ID reset domain sufficiently to change production policy. The batch evidence likewise cannot prove a lifetime guarantee. Thus no profile correction was made. Until that authority is available, retaining the current failure is the smallest correctness-preserving operational outcome. A verified contract is the remaining prerequisite, not a Rust identity-validation relaxation.

## 2. Exact depth admission calculation

Job `2654568015303585734`, dataset `7336746276859674154`, fingerprint `163392b6eb32a21de0f15780b6bce020687f76adf053cfecabcdb5e19e680125` is the family execution unit for **OKX BTC-USDT-SWAP linear perpetual depth**, concrete tasks **249–251**:

| Task | Archive under `data/raw/` | Compressed bytes |
|---|---|---:|
| 249 | `6936/6a2ebc8b4a94495daa59c640f4497e6f/BTC-USDT-SWAP-L2orderbook-5000lv-2026-10-27.tar.gz` | 144,076,731 |
| 250 | `6937/4b1bcc2e03404c9cb4e7882e5a9a4958/BTC-USDT-SWAP-L2orderbook-5000lv-2026-10-28.tar.gz` | 146,068,639 |
| 251 | `6938/d4f4d692376b4052930d975219157985/BTC-USDT-SWAP-L2orderbook-5000lv-2026-10-29.tar.gz` | 203,919,718 |

The reported **183,512,345,789 bytes = 170.909190 GiB** was reproduced **exactly** with the production prepared-batch resolver, current configuration and native Rust sizes. Deserializing persisted jobs produces a slightly different number because prepared-memory accounting inspects String/Vec capacities; using the production resolver without cloning its tasks reproduces the original number.

### Active execution reservation

`resources/estimate.rs::estimate_breakdown` uses filesystem metadata only to check accessibility. It does **not** use compressed bytes, decompressed bytes, record count, total days, or archive length as the working-set estimate. For each of these tasks, symbol plus logical stream identity length is 80 bytes.

| Component | Formula | Bytes |
|---|---|---:|
| Record/transformation/decompressor allowance | `4,194,304 * 64 + 33 MiB` | 303,038,464 |
| Depth state/transient allowance | `100,000 * (80 * 4 + 1,024)` | 134,400,000 |
| Active source subtotal | previous two | **437,438,464** |
| Parquet batch/encoding allowance | `268,435,456 * 4` | 1,073,741,824 |
| Footer metadata allowance | `65,536 * 16,384` | 1,073,741,824 |
| Oversized depth outcome allowance | `100,000 * 2 * (80 + 512)` | 118,400,000 |
| Segment state/clones | `min(268,435,456, 65,536 * 32) * 3` | 6,291,456 |
| Event/index/encoder allowance | `16 MiB` | 16,777,216 |
| Writer subtotal | preceding five | **2,288,952,320** |
| Task finalization headroom | `8 MiB` | 8,388,608 |
| Single active task | source + writer + headroom | **2,734,779,392** |
| Shared reconciliation allowance | configured quota | 1,073,741,824 |
| Stream task metadata | `3 * (48 + 920)` | 2,904 |
| Stream reservation | sum | **3,808,524,120** |

`streams::estimate` takes the **maximum**, not sum, of source task requirements, then adds one reconciliation allowance and per-task metadata. All three archives are covered by one stream admission, but **not as three simultaneously resident decoded archives**. The estimated file requirement is 14 (fan-in 8 + 6), and session file headroom adds 8, well below 256. The stream scratch estimate is zero upfront; actual spool growth is charged against the shared 128-GiB scratch quota and can fail safely if exhausted.

### Session-wide retained-state reservation

`session.rs` first sums retained allowances for **all tasks of a job**, then checks each planned stream against that sum plus retained allowances of earlier eligible jobs and the maximum execution/finalization requirement. It follows BTreeMap fingerprint order, so this stream labels the rejection even though the dominant reservation belongs to the entire batch. The trade job's later reconciliation failure is not yet known during admission, so its retained allowance remains included.

`retained_task_memory`, for each task, computes:

```
scope = logical_stream_length + absolute_input_path_length
entry = sizeof(CategoryStatistics) + scope + 256
reports = 6 * (entry * metric_entries
               + 20 * (1024 + sizeof(IntegrityDiagnostic))
               + sizeof(TaskMetrics) + scope * 4)
          + 3 * prepared_task_memory
retained = reports + (depth ? depth_levels * 4096 : 0)
```

Native sizes verified: CategoryStatistics 112, IntegrityDiagnostic 72, TaskMetrics 808 bytes. The factor six covers report/aggregate/clone/manifest/serialization overlap. Prepared values are recursively walked with additional object/node allowances; this is an estimate, not allocator measurement.

| Retained component | Trade job: 107 tasks | Depth job: 257 tasks |
|---|---:|---:|
| Full metric-entry allowance × six | 21,918,646,272 | 52,453,441,536 |
| Diagnostics × six | 14,072,640 | 33,800,640 |
| TaskMetrics structs × six | 518,736 | 1,245,936 |
| Scope strings × 24 | 392,784 | 931,680 |
| Prepared metadata × three | 3,479,733 | 10,091,712 |
| All-source depth boundary allowance | 0 | **105,267,200,000** |
| Job retained total | **21,937,110,165** | **157,766,711,504** |

For reproduction: summed scope lengths are 16,366 and 38,820; summed prepared-memory estimates are 1,159,911 and 3,363,904 respectively. Each depth task adds **409,600,000 bytes**, regardless of its actual boundary book size. Task 249 alone reserves 631,958,182 retained bytes, of which 409,600,000 is boundaries.

Job finalization reserves 1,090,519,040 bytes for trades and 1,115,684,864 for depth (`65,536 * 16 KiB + 16 MiB`, plus 12 segment inventories for depth). Both are smaller than this active stream reservation, so they do not add a second concurrent term.

Exact check:

```
21,937,110,165 + 157,766,711,504 + 3,808,524,120
= 183,512,345,789 requested
> 68,719,476,736 permitted
```

Even the depth retained allowance alone exceeds 64 GiB. Changing workers from eight to one does **not** change this preflight requirement. Workers affect how many independently admitted units can run concurrently after preflight; the shared budget gates that concurrency.

## 3. Genuine state versus excessive buffering

The 171-GiB number is **not measured RSS**, nor evidence that complete archives are currently held in RAM. It is mostly pessimistic, multiplied retained-capacity accounting. Nevertheless, simply deleting those terms would invalidate admission safety:

- Real task metrics survive worker completion until job finalization. Their file/hour/day maps can grow to the configured entry cap. Reserving six full 65,536-entry maps for every task is extremely conservative for ordinary daily/hourly archives, but reflects possible retained growth and cloning under current contracts. Neither filenames nor unvalidated timestamp spans justify a smaller bound.
- Depth boundary allowance is charged per source across **all 257 depth tasks**, although writers serialize boundaries incrementally. `streams_depth::validate_output` and `shared/persistence/parquet/segments.rs::read_depth_stream_boundaries` read the entire stream boundary JSON into a byte buffer and deserialize all source boundaries together. `manifest/builder.rs` also reads boundary inventories. Thus some all-source boundary memory is real in finalization, but session-wide reservation across every depth task is much wider than that lifetime. The 4096 bytes per allowed level is heuristic and not a measured requirement.
- Active-source and writer estimates are themselves conservative. For example, 65,536 allowed footer entries contribute 1 GiB even when a stream writes few groups. Correctly accounting for actual growth requires a safe quota/fallback, not removing headroom based on typical observations.

### Source decoding and writing

The implementation inspected already has bounded disk-backed processing:

1. `family::route_with_progress` walks physical archives sequentially and decodes each source/member once. It routes selected native symbols to disk spools. Filtered/out-of-scope rows contribute accounting/diagnostics and are not retained as input records.
2. Production `worker/decoded.rs` uses `DiskCsv`/`DiskJson` iterators. In-memory decoded `Vec` variants exist only under `cfg(test)`.
3. `reconciled::execute` normalizes tasks sequentially into outcome spools. It does not retain full normalized event vectors across archives. One record/outcome and book/transient state remain bounded by record, depth and reconciliation limits.
4. `publish_depth` opens one concrete writer at a time, consumes spooled outcomes incrementally, deletes consumed outcome/boundary spools, and streams boundaries through `ParquetDepthWriter::write_boundary`. Parquet batches flush incrementally; source decoding is decoupled from final Parquet replay by scratch storage.

All three selected raw archive spools can exist **on disk** after family routing, and all source outcome spools can exist on disk before publication. This costs scratch and extra I/O, not archive-sized resident vectors. No unnecessary whole-source in-memory input retention was found in this production path. This inspection did not execute or profile the three real depth archives; it does not claim a measured peak or that every library allocation is covered exactly by heuristics.

Resident **event-data** memory can remain independent of total archive bytes under these limits. Current retained metrics, segment/footer inventories and consolidated boundary readers still scale with bounded metadata/source counts and can hit quotas as workloads grow. Streaming finalization is needed for a stronger archive/source-count-independent bound. Scratch and output bytes naturally grow with input and remain separately budgeted.

## 4. Recommended corrections and expected effects

These are architectural proposals, not implemented behavior:

1. **Stream boundary validation and manifest aggregation.** Introduce a bounded source-boundary reader and validate one source at a time while checking row/event offsets, task identities, initialization segments and completeness. Preserve the existing output contract. Replace `fs::read` plus all-source `Vec` at both validation and manifest consumers. Then replace per-session/per-source boundary reservation with a bounded active-finalization reservation and small source inventories. Do not reduce accounting before all consumers are converted.
2. **Bound retained metrics by lifetime.** Persist completed task reports in versioned, quota-accounted scratch artifacts and stream job aggregation/manifest emission; release completed task maps. Alternatively implement explicit aggregate retained-state leases with bounded spill before growth. A worker must never wait for memory growth while holding capacity needed for draining; admission/spill design must prevent that deadlock. Keep exact integrity observations and reports. Do not set a smaller global cap just to admit this batch.
3. **Separate reservations by phase.** Reserve bounded task decoding/writer/reconciliation working sets, compact retained descriptors and the maximum truly concurrent finalization workspace. Release each lease at its actual lifetime boundary. Preserve checked arithmetic, accounting of clones/serialization and global budget enforcement. Serializing jobs would reduce cross-job retention but does not by itself solve this depth job's 146.9-GiB allowance.
4. **Improve admission diagnostics.** Report active-source, writer, reconciliation, retained-metrics, boundaries, prior-job totals, finalization and source count separately. Label session-wide rejection explicitly so a particular fingerprint is not mistaken for a 171-GiB single-stream working set.
5. **Trade contract correction only after authority.** Add the narrow explicit Bitget policy and evidence described above. Use the existing strict reconciliation route; shared boundary timestamps remain overlaps. Re-resolve profile fingerprints/provenance through the loader.

Expected effects, not benchmark claims: boundary streaming reduces finalization peaks and eliminates whole-boundary-file buffering; report spill bounds retained RAM at additional sequential I/O/serialization cost. Existing depth routing/outcome spooling already adds decode → scratch → replay I/O. Keeping it preserves deterministic output and avoids simultaneous archives in RAM. Direct incremental depth publication might reduce extra I/O for proven chronological nonoverlapping sources, but is a separate optimization requiring recovery/overlap/provenance proof. A reliable Bitget identity contract still uses bounded identity and chronological external sorts; it does not automatically enable the source-local append fast path. Faster processing or a numeric RSS reduction has not been measured.

## 5. Verification and regression requirements

Actually executed:

- Full read-only CSV scan of the 42 affected Bitget archives and intersection-ID comparisons: 4,037,370 rows; 37 shared boundary buckets; no missing IDs or timestamp regressions; zero shared IDs at intersections.
- Temporary production-loader admission probe: one diagnostic test passed; exact reported requested bytes reproduced; no source decoding or execution in that probe.
- `cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::resources::safety_tests -- --nocapture`: **12 passed**.
- `cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::append_tests -- --nocapture`: **11 passed**. These are small synthetic production-executor checks, not real archive normalization.

Existing tests already confirm source-size-independent estimates, sequential stream sharing, impossible retained admission rejection, bounded metric growth, resource lease lifetimes, shared-boundary rejection for source-local IDs, missing-ID rejection and explicit identity contracts. Passing them does not demonstrate that the complete production workload fits the configured budget. Full integration suite, full batch, depth real-data execution and performance benchmarks were not run. No production correction was applied, so no new behavior is claimed verified.

Needed before proposed corrections ship:

- Synthetic Bitget-like ZIP chunks with second-resolution shared boundaries: missing/unreliable/unestablished contracts must fail; an independently established instrument-scoped contract must preserve distinct IDs, suppress only authoritative duplicates, resolve conflicts deterministically and reject missing IDs. Test disjoint timestamps with reused IDs as well; do not optimize away global identity checks. Repeat with reversed source priorities and assert documented tie/ownership behavior.
- A profile/loader regression proving the approved Bitget reset-domain policy is fingerprinted and cannot be mixed with a contradictory contract. Archive checks are evidence, not the policy authority.
- A nonexecuting complete prepared-batch admission regression using production runtime values, checking component totals and source-count/worker independence. Keep source-size invariance with sparse/dummy metadata rather than decoding real data. Do not bake allocator-capacity-sensitive exact totals into a portable unconditional unit test.
- Many synthetic depth sources at the level limit: streaming boundary writer/reader/manifest output must match independently expected boundaries/offsets and stay within a fixed accounted memory ceiling. Reject malformed, truncated, duplicate-task and extra trailing entries without committing.
- Retained-report spill and aggregation: increasing task count must not accumulate full metric maps in memory; exact counters/windows/provenance must match an independent oracle. Test quota exhaustion, disk errors, finalization failure and restart/cleanup behavior without losing previously definitive results.
- Repeat output comparison across worker counts, scratch codecs and source priorities where appropriate. Exercise memory and scratch exhaustion before allocations/mutations, including the finalization phase. Preserve persistent real-data test outputs under isolated test destinations if later authorized.

## 6. Changes and remaining work

Only this report, the evidence JSON and retained diagnostic source files were added under `.agents/reports/`. The temporary Rust instrumentation was removed after verification. No source archives, runtime budgets, profiles, source-selection documents or production implementation behavior were changed. Existing user modifications remain intact.

No unrelated Forge refactor is required. If prepared batches are produced externally, future Forge integration must preserve the resolved explicit identity contract/profile fingerprint and should consume structured admission component diagnostics rather than infer working set from archive byte sizes. Architectural memory corrections above require a separate reviewed implementation. Bitget's historical ID/reset authority remains unresolved and is explicitly required before enabling that feed's overlapping chunks.

## Appendix: observed trade archives and overlaps

Timestamps below are source Unix milliseconds (canonical nanoseconds are these values multiplied by 1,000,000). Full absolute paths are in the companion JSON.

| Task | ZIP basename | Rows | Actual minimum ms | Actual maximum ms |
|---|---|---:|---:|---:|
| 13 | `BTCUSDT-BTCUSDT_20261027_001.zip` | 100,000 | 1793030400000 | 1793037823000 |
| 14 | `BTCUSDT-BTCUSDT_20261027_002.zip` | 100,000 | 1793037823000 | 1793058335000 |
| 15 | `BTCUSDT-BTCUSDT_20261027_003.zip` | 100,000 | 1793058335000 | 1793071186000 |
| 16 | `BTCUSDT-BTCUSDT_20261027_004.zip` | 100,000 | 1793071186000 | 1793088871000 |
| 17 | `BTCUSDT-BTCUSDT_20261027_005.zip` | 100,000 | 1793088871000 | 1793104760000 |
| 18 | `BTCUSDT-BTCUSDT_20261027_006.zip` | 100,000 | 1793104760000 | 1793113097000 |
| 19 | `BTCUSDT-BTCUSDT_20261027_007.zip` | 45,516 | 1793113097000 | 1793116799000 |
| 20 | `BTCUSDT-BTCUSDT_20261028_001.zip` | 100,000 | 1793116800000 | 1793136706000 |
| 21 | `BTCUSDT-BTCUSDT_20261028_002.zip` | 100,000 | 1793136706000 | 1793153512000 |
| 22 | `BTCUSDT-BTCUSDT_20261028_003.zip` | 100,000 | 1793153512000 | 1793176582000 |
| 23 | `BTCUSDT-BTCUSDT_20261028_004.zip` | 100,000 | 1793176582000 | 1793193163000 |
| 24 | `BTCUSDT-BTCUSDT_20261028_005.zip` | 85,538 | 1793193164000 | 1793203199000 |
| 25 | `BTCUSDT-BTCUSDT_20261029_001.zip` | 100,000 | 1793203200000 | 1793212424000 |
| 26 | `BTCUSDT-BTCUSDT_20261029_002.zip` | 100,000 | 1793212424000 | 1793217090000 |
| 27 | `BTCUSDT-BTCUSDT_20261029_003.zip` | 100,000 | 1793217090000 | 1793232748000 |
| 28 | `BTCUSDT-BTCUSDT_20261029_004.zip` | 100,000 | 1793232748000 | 1793240101000 |
| 29 | `BTCUSDT-BTCUSDT_20261029_005.zip` | 100,000 | 1793240101000 | 1793254319000 |
| 30 | `BTCUSDT-BTCUSDT_20261029_006.zip` | 100,000 | 1793254319000 | 1793273619000 |
| 31 | `BTCUSDT-BTCUSDT_20261029_007.zip` | 100,000 | 1793273619000 | 1793283256000 |
| 32 | `BTCUSDT-BTCUSDT_20261029_008.zip` | 100,000 | 1793283256000 | 1793285451000 |
| 33 | `BTCUSDT-BTCUSDT_20261029_009.zip` | 95,829 | 1793285451000 | 1793289599000 |
| 34 | `BTCUSDT-BTCUSDT_20261030_001.zip` | 100,000 | 1793289600000 | 1793296043000 |
| 35 | `BTCUSDT-BTCUSDT_20261030_002.zip` | 100,000 | 1793296043000 | 1793305028000 |
| 36 | `BTCUSDT-BTCUSDT_20261030_003.zip` | 100,000 | 1793305028000 | 1793306609000 |
| 37 | `BTCUSDT-BTCUSDT_20261030_004.zip` | 100,000 | 1793306609000 | 1793307032000 |
| 38 | `BTCUSDT-BTCUSDT_20261030_005.zip` | 100,000 | 1793307032000 | 1793307981000 |
| 39 | `BTCUSDT-BTCUSDT_20261030_006.zip` | 100,000 | 1793307981000 | 1793310755000 |
| 40 | `BTCUSDT-BTCUSDT_20261030_007.zip` | 100,000 | 1793310755000 | 1793313575000 |
| 41 | `BTCUSDT-BTCUSDT_20261030_008.zip` | 100,000 | 1793313575000 | 1793317724000 |
| 42 | `BTCUSDT-BTCUSDT_20261030_009.zip` | 100,000 | 1793317724000 | 1793319505000 |
| 43 | `BTCUSDT-BTCUSDT_20261030_010.zip` | 100,000 | 1793319505000 | 1793320101000 |
| 44 | `BTCUSDT-BTCUSDT_20261030_011.zip` | 100,000 | 1793320101000 | 1793322026000 |
| 45 | `BTCUSDT-BTCUSDT_20261030_012.zip` | 100,000 | 1793322026000 | 1793325042000 |
| 46 | `BTCUSDT-BTCUSDT_20261030_013.zip` | 100,000 | 1793325042000 | 1793329624000 |
| 47 | `BTCUSDT-BTCUSDT_20261030_014.zip` | 100,000 | 1793329624000 | 1793336588000 |
| 48 | `BTCUSDT-BTCUSDT_20261030_015.zip` | 100,000 | 1793336588000 | 1793346682000 |
| 49 | `BTCUSDT-BTCUSDT_20261030_016.zip` | 100,000 | 1793346682000 | 1793356690000 |
| 50 | `BTCUSDT-BTCUSDT_20261030_017.zip` | 100,000 | 1793356690000 | 1793366954000 |
| 51 | `BTCUSDT-BTCUSDT_20261030_018.zip` | 100,000 | 1793366954000 | 1793368740000 |
| 52 | `BTCUSDT-BTCUSDT_20261030_019.zip` | 100,000 | 1793368740000 | 1793372044000 |
| 53 | `BTCUSDT-BTCUSDT_20261030_020.zip` | 100,000 | 1793372044000 | 1793375466000 |
| 54 | `BTCUSDT-BTCUSDT_20261030_021.zip` | 10,487 | 1793375466000 | 1793375999000 |

All 37 intersecting pairs follow. Shared-ID count is zero for every pair.

| Tasks | Shared timestamp ms | IDs in first / second bucket |
|---|---:|---:|
| 13 / 14 | 1793037823000 | 7 / 35 |
| 14 / 15 | 1793058335000 | 24 / 34 |
| 15 / 16 | 1793071186000 | 120 / 70 |
| 16 / 17 | 1793088871000 | 122 / 11 |
| 17 / 18 | 1793104760000 | 15 / 15 |
| 18 / 19 | 1793113097000 | 5 / 25 |
| 20 / 21 | 1793136706000 | 52 / 1322 |
| 21 / 22 | 1793153512000 | 40 / 24 |
| 22 / 23 | 1793176582000 | 1 / 70 |
| 25 / 26 | 1793212424000 | 57 / 19 |
| 26 / 27 | 1793217090000 | 2 / 32 |
| 27 / 28 | 1793232748000 | 18 / 28 |
| 28 / 29 | 1793240101000 | 39 / 54 |
| 29 / 30 | 1793254319000 | 11 / 5 |
| 30 / 31 | 1793273619000 | 4 / 16 |
| 31 / 32 | 1793283256000 | 25 / 62 |
| 32 / 33 | 1793285451000 | 147 / 9 |
| 34 / 35 | 1793296043000 | 14 / 3 |
| 35 / 36 | 1793305028000 | 5 / 4 |
| 36 / 37 | 1793306609000 | 18 / 316 |
| 37 / 38 | 1793307032000 | 125 / 11 |
| 38 / 39 | 1793307981000 | 27 / 122 |
| 39 / 40 | 1793310755000 | 4 / 22 |
| 40 / 41 | 1793313575000 | 12 / 35 |
| 41 / 42 | 1793317724000 | 5 / 40 |
| 42 / 43 | 1793319505000 | 167 / 19 |
| 43 / 44 | 1793320101000 | 56 / 25 |
| 44 / 45 | 1793322026000 | 14 / 10 |
| 45 / 46 | 1793325042000 | 62 / 66 |
| 46 / 47 | 1793329624000 | 5 / 12 |
| 47 / 48 | 1793336588000 | 16 / 12 |
| 48 / 49 | 1793346682000 | 64 / 33 |
| 49 / 50 | 1793356690000 | 41 / 10 |
| 50 / 51 | 1793366954000 | 35 / 3 |
| 51 / 52 | 1793368740000 | 45 / 9 |
| 52 / 53 | 1793372044000 | 59 / 66 |
| 53 / 54 | 1793375466000 | 27 / 62 |
