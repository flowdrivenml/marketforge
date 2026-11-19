# Historical runtime and policies

Runtime schema 1 is loaded from `data/.config/historical/general.toml`. The authoritative defaults and validators are in `engine/src/shared/historical/config.rs`. Runtime applies to an Engine invocation independently of the selection recipe.

| Field | Requirement or default |
| --- | --- |
| `schema_version` | Required integer 1. |
| `resources`, `integrity_policy` | Required tables. |
| `merge_fan_in` | Optional integer, default 8. |
| `diagnostics.mode` | `disabled` (default), `basic`, or `detailed`. |
| `diagnostics.interval_ms` | Unsigned integer in milliseconds, default 1000. |
| `merge.partitions_per_worker` | Positive integer, default 4. |
| `merge.compression_level` | Zstandard integer level 1–22, default 3. |

### Resource configuration

| Parameter | Meaning | Effect |
|---|---|---|
| `resources.workers` | Worker count | Processing concurrency |
| `resources.memory_budget_bytes` | Maximum accounted memory | Global memory admission |
| `resources.scratch_budget_bytes` | Scratch storage allowance | Reconciliation and report spilling |
| `resources.scratch_path` | Temporary processing location | Scratch-file destination |
| `resources.parquet.row_group_target_bytes` | Target row-group size | Parquet batching |
| `resources.parquet.file_target_bytes` | Target Parquet file size | File rotation |
| `resources.normalization_limits` | Normalization safety limits | Per-source and per-operation bounds |

### Memory accounting

MarketForge reserves resources before admitting work.

The memory estimate includes:

- Active source decoding.
- Input parsing.
- Order-book reconstruction.
- Parquet writer memory.
- Reconciliation workspaces.
- Source metadata.
- Integrity observations.
- Failure-report retention.
- Finalization requirements.

The refactored engine spills completed detailed integrity observations rather than retaining all observations in memory.

Memory admission is based on estimated reservations, not directly measured process RSS.

A successful admission does not guarantee that the operating system will never encounter memory pressure.

---

## Normalization safety limits

The supplied code references these members of `NormalizationLimits`.

| Parameter | Purpose |
|---|---|
| `record_bytes` | Maximum permitted source-record size |
| `depth_levels` | Order-book level limit |
| `metric_entries` | Maximum detailed integrity observation entries |
| `reconciliation_bytes` | Reconciliation memory quota |
| `open_files` | Open-file budget |
| `workbook_bytes` | Workbook memory allowance |
| `workbook_xml_bytes` | Workbook XML allowance |
| `workbook_rows` | Workbook row allowance |

These limits protect processing against oversized records, unexpectedly large books, excessive report cardinality, and resource exhaustion.

### Limit variations

**Higher limits**

May permit larger records, deeper books, or more extensive integrity observations.

However, they increase potential memory requirements and may cause admission to reject more concurrent work.

**Lower limits**

Reduce worst-case resource exposure but may reject otherwise valid source data.

### Important distinction

A resource-limit failure is not necessarily a data-integrity failure.

For example:

```text
resource limit: memory bytes
```

means the requested reservation exceeded the configured allowance.

A malformed record or invalid book is a separate category of failure.

---

## Integrity policy

Integrity rules determine how the engine responds to source-data problems.

### Global policy switch

```text
integrity_policy.enabled
```

The policy supports an enabled/disabled state.

When disabled, violations can still be recorded and the resulting integrity status can be degraded.

Disabling the policy is not equivalent to proving the data valid, and it does not disable independent structural reconciliation safeguards.

### Integrity categories

The engine tracks seven categories.

| Category | Meaning |
|---|---|
| `parse_failure` | Source record cannot be parsed |
| `invalid_record` | Parsed record violates validity requirements |
| `sequence_gap` | Missing or discontinuous sequence |
| `timestamp_regression` | Timestamp moves backward |
| `missing_snapshot` | Required book initialization is missing |
| `invalid_book` | Order-book state violates validity requirements |
| `transformation_failure` | Normalization transformation fails |

### Rule parameters

Each category has a corresponding integrity rule.

| Parameter | Purpose |
|---|---|
| `action` | How the engine responds to a violation |
| `max_count` | Maximum permitted absolute violations |
| `max_rate` | Maximum violation fraction |
| `minimum_samples` | Minimum observations before rate enforcement |
| `windows.hourly` | Optional hourly threshold |
| `windows.daily` | Optional daily threshold |

### Actions

The supplied implementation demonstrates:

| Action | Behavior |
|---|---|
| `Fail` | Violation is immediately fatal |
| `Degrade` | Violation may be tolerated subject to configured thresholds |


### Rate thresholds

Rates are fractions, not percentages.

Examples:

| Value | Meaning |
|---|---|
| `0` | Zero tolerated violation rate |
| `0.0001` | 0.01% |
| `0.001` | 0.1% |
| `0.01` | 1% |
| `0.1` | 10% |
| `1` | 100% |

The implementation validates that rate thresholds are finite and between zero and one.

It compares violations against eligible observations using decimal arithmetic.

An observed rate equal to the threshold is permitted; exceeding it is not.

### Minimum sample count

A rate threshold is not evaluated until the observation count reaches `minimum_samples`.

This prevents rate checks from making decisions on insufficient samples.

An absolute `max_count` limit can still fail earlier.

### Policy scopes

The final evaluator supports:

- Global violation counts.
- Global violation rates.
- Per-file violation rates.
- Per-stream violation rates.
- Daily violation rates.
- Hourly violation rates.

The implementation also tracks observations that cannot be attributed to trustworthy event-time windows.

If a time-window policy requires attribution but eligible observations lack trustworthy timestamps, that can itself cause a policy failure.

### Immediate versus final evaluation

**During source processing**, the engine enforces immediately fatal categories.

**During final evaluation**, it checks counts, rates, time windows, and whole-job integrity conditions.

This separation prevents premature rejection based on incomplete rate denominators.

---

## Processing policies

These schema-1 runtime extensions default to strict; old configurations need no migration. Tables do not activate permissive processing. Positive tolerances and loss allowances must be explicit. Processing, integrity acceptance, reconstruction and publication are separate checks: enabled integrity `action="fail"` rejects any occurrence even with a nonzero count threshold. `degrade` degrades on occurrence and fails above any applicable overall/file/count/rate/hourly/daily threshold. Sample floors defer rate checks, not finite count checks. Disabling integrity evaluation bypasses its policy decisions, never archive validation, processing bounds, reconstruction, resource limits or publication checks.

### Reconciliation settings

| Table/key | Default | Meaning |
|---|---|---|
| `reconciliation.trades.overlap_policy` | `strict` | `strict` or `prefer_source` |
| `reconciliation.depth.overlap_policy` | `strict` | `strict`, `snapshot_recovery` or `prefer_source` |
| `reconciliation.{trades,depth}.prefer_source.selection` | `earlier` | Only `earlier` or `later`; compare actual accepted source coverage starts |
| `reconciliation.depth.prefer_source.switch_boundary` | `snapshot` | `snapshot` or independently proven `source_boundary` |
| `reconciliation.{trades,depth}.prefer_source.on_unique_data_loss` | `fail` | `fail` rejects any exclusion; `mark_degraded` explicitly authorizes bounded exclusion |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_events` | `0` | Job-wide logical event allowance for the relevant content type |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_rate` | absent | Optional finite fraction in [0,1], enforced job-wide |
| `reconciliation.{trades,depth}.prefer_source.minimum_samples` | `0` | Rate-check sample floor; count allowance still applies below it |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_interval_ms` | absent | Maximum contiguous excluded span within each source/stream |

`mark_degraded` requires a positive finite `max_excluded_events`; rate/sample-floor settings alone never authorize unbounded losses. Milliseconds must fit checked signed canonical nanoseconds. Invalid rates, enums, unsupported combinations, unknown keys and overflow fail configuration loading. The default preference table has no effect under strict policy.

Verified equivalence runs before loss decisions. A reliable trade identity plus matching complete contents proves a duplicate; equal timestamps/price/size/side do not. Conflicting contents under a reliable identity fail strict processing. Preference can resolve the conflict only with loss authorization, counting the losing observation conservatively as unverified. Distinct reliable IDs survive overlapping/equal-time boundaries, including nonconsecutive arbitrary-length numeric IDs. Missing or reset-ambiguous identities do not acquire global scope from their shape. Native-domain ambiguity under strict processing fails with the required identity evidence described in the diagnostic.

Preference ranks actual validated coverage starts. Equal starts use canonical physical path plus member identity as a stable reproducibility tie-break, with no quality claim. The ownership sweep covers nested, chained and three-or-more sources; legacy task priorities are not preference authority. Reliable nonconflicting trades are unioned. Unknown identities use explicit conflict-interval ownership and conservative exclusion accounting. Task/worker completion order does not choose a winner.

Depth events are atomic messages/grouped snapshots. Independent level-diff vectors are never spliced without a matching reconstruction state. Source prefixes are reconstructed from scratch spools; selected histories are applied to a separate book and complete state is checked after every event. Identical sequenced boundary snapshots compare decoded evidence, including complete snapshot contents and timestamps; JSON object key order is irrelevant. Canonical-changing terminal snapshots can be represented by the next full initialization. Deltas sharing snapshot IDs remain distinct.

Overlap `snapshot_recovery` retains unique supplied observations in independently initialized source histories and records unproven continuity. It does not exclude unique events or claim one continuous market history. `prefer_source` can exclude observations within explicit limits. Snapshot switches wait for actual authoritative snapshots. `source_boundary` requires matching complete book state and compatible native sequence; unsequenced feeds cannot invent that evidence. Returning to an earlier source requires another valid transition. Unsafe transitions, missing initialization or failed source reconstruction prevent publication even with generous loss settings.

### Timestamp settings

| Table/key | Default | Meaning |
|---|---|---|
| `timestamp_regression.trades.policy` | `strict` | `strict`, `preserve`, `reorder`, `discard` |
| `timestamp_regression.trades.max_regression_ms` | `0` | High-water regression bound for permissive handling; strict never accepts because of tolerance |
| `timestamp_regression.trades.max_reorder_window_ms` | `0` | Positive reorder bound, at least `max_regression_ms` |
| `timestamp_regression.trades.max_discarded_events` | `0` | Job-wide discard allowance |
| `timestamp_regression.trades.max_discarded_rate` | absent | Optional finite [0,1] job-wide discard rate |
| `timestamp_regression.trades.minimum_samples` | `0` | Rate floor, independent of the finite count allowance |
| `timestamp_regression.depth.policy` | `strict` | `strict`, `preserve`, `snapshot_recovery` |
| `timestamp_regression.depth.max_regression_ms` | `0` | Checked high-water bound |
| `timestamp_regression.depth.require_sequence_continuity` | `true` | Applicable native validation remains mandatory; false is unsupported |
| `timestamp_regression.depth.require_recovery_snapshot` | `true` | Authoritative initialization remains mandatory; false is unsupported |
| `timestamp_regression.depth.max_discarded_events` | `0` | Positive allowance required for lossy recovery; enforced job-wide |

Equal timestamps are never regressions. Raw-source evidence compares adjacent accepted observations within a concrete source/native reset domain. Processing uses the relevant high-water timestamp so cumulative backward steps cannot evade a bound. Validated native snapshot resets start a new domain; independent sources are not accidentally compared as one native sequence. Raw anomalies survive deduplication; logical survivor ordering violations are reported separately. Arithmetic is checked at nanosecond precision. Original event/system timestamps are never clamped, shifted or rewritten.

Trade `preserve` retains original native source order, timestamps and original record ordinals. `reorder` chronologically sorts independent executions deterministically, after identity reconciliation, using exact numeric IDs for equal-time ties and source positions as final ties. It uses admitted bounded external runs/scratch, not an unbounded time-window buffer; resource exhaustion fails. Both regression and reorder bounds must be positive. `discard` needs positive regression/count allowances, excludes only eligible regressing survivors, and cannot mask parsing/identity/overlap failures. Duplicate-related raw regressions can resolve after verified deduplication, while a fatal raw integrity rule still fails the job.

Depth `preserve` applies valid native order and checks the book without timestamp sorting. Snapshot-only profiles (including Gate.io inverse perpetual snapshot) use independent authoritative snapshots, without invented sequence fields. `snapshot_recovery` invalidates publication of affected dependent deltas and resumes at an actual validated snapshot with full initialization. Every omitted logical observation is charged conservatively. A regressing authoritative snapshot can recover immediately with zero omission; incremental streams without a subsequent snapshot fail. A snapshot restores state, not omitted history. Native sequence and book validation always remain mandatory where applicable.

### Accounting, quality and output compatibility

The rate denominator is accepted logical source observations minus verified duplicate observations; it includes excluded/unverified observations. It excludes parse/normalization rejections and never counts expanded level rows as separate events. Count/rate bounds apply job-wide per content type; stream/source totals remain in task metrics. Contiguous loss spans continue across reasons and duplicates until an observation is actually retained. Unknown uniqueness is reported as unverified exclusion and charged in full. Known-unique and unverified exclusions both degrade completeness; verified duplicate suppression does not. Exceeding any applicable bound prevents manifest publication.

`source.processing` reports raw/logical regressions, logical input/published events, original/expanded/staged rows, known/unverified exclusions by reason, maximum contiguous spans, recovery/source-switch counts and up to eight bounded decision samples with original source ordinal and selected task when available. Task/source/member/identity contracts remain in the same report; no per-event report inventory is retained. Physical source reads, integrity-rejected rows, normalized rows, suppression, unpublished survivors and committed output remain distinct. On failure committed counters become zero; staged rows and unpublished output remain evidence. Cleanup/report failures append context without replacing the primary failure.

Activated policies use **manifest 5**, preserving canonical Parquet row schemas. Its `processing_quality` records effective policies, actual physical timestamp ordering, reconstruction validity, supplied-observation preservation, continuity, inherited-metadata assertion provenance and unknown external historical coverage. Native-order depth uses event indexes and initialization segments; never replay its price-level rows through timestamp sorting. Depth boundary envelope 3 (`segmented_sources`) permits repeated source IDs for ownership returns; envelope 2 remains unique-source only. Streaming boundary readers validate the full envelope, including suffixes, with bounded decoded state. Trades additionally stream `sources.jsonl`, mapping every canonical row to task/original record ordinal, validated against row ownership before publication.

The managed timestamp-first merger accepts compatible v3 selections and supports some v4/v5 trade layouts when the request pins primary files. A selected trade layout must be timestamp-first with contiguous source ownership and no repeated source spans. v4/v5 depth, native-order data and interleaved trade spans remain outside the managed merge contract. See [supported merge layouts](../workflows/merge.md#supported-layouts). A committed publication does not establish complete exchange history.


## Scratch storage

`scratch_format` accepts `json` (default), `typed_trades`, or `typed_events`. Typed events also support depth scratch records. `scratch_codec` accepts `zlib` (default) and `uncompressed`. Scratch encodings are internal working files. Publication readers use the committed Parquet and manifest formats.
