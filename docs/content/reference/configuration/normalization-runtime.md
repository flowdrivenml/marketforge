# Normalization runtime configuration

## Overview

The Rust Engine reads this configuration to admit processing work, bound intermediate buffers, size Parquet output, collect diagnostics, and decide whether observed anomalies or excluded source events permit publication.

| Configuration | Responsibility |
| --- | --- |
| [Normalization recipe](normalization.md) | Selects registered inputs and an interval. |
| [Normalization profile](../../workflows/normalization/author-profile.md) | Describes how to interpret the source format and its record operations. |
| Normalization runtime | Controls execution resources, diagnostics, and policies for processing and publication. |

## Quick start

```bash
mf normalize config show | cpout
mf normalize config validate | cpout
mf normalize config schema | cpout
```

These commands use the locally installed managed normalization Engine. Schema-based operations need a build with the runtime schema capability. `validate` checks the complete TOML through the production Rust runtime loader. The schema explorer describes the current Engine build and shows whether settings are effective in the current configuration. See [what validation checks](#11-validation-and-safe-modification).

### Inspect individual settings

```bash
mf normalize config get resources.workers | cpout
mf normalize config show --section resources | cpout
mf normalize config show --all | cpout
mf normalize config schema --field reconciliation.depth.overlap_policy | cpout
```

Use a dotted TOML leaf path with `get`, `set`, `unset`, or `schema --field`. Use a canonical table name with `show --section`, including nested sections such as `resources.parquet`, `integrity_policy`, and `reconciliation.depth`. The compact overview groups related settings for readability. Detailed views separate the configured value, the Engine-resolved value, and the default. An inactive value can still be present in the file and must still pass the loader's checks.

For automation, use global `mf --json normalize config …`. Output contains native numbers and booleans and a versioned result object.

For a read-only report on older managed builds that expose runtime settings but lack the schema interface, use `mf engine runtime normalize | cpout`. See [Engine runtime commands](../cli/engine.md) and the [normalization CLI syntax](../cli/normalize.md#engine-runtime-configuration).

## Configuration file and resolution

Forge resolves `data/.config/engine/normalize.toml` under the workspace root, independent of the configured data root. Direct Engine invocations default to that path and accept `--config PATH` and `--project-root ROOT`. Relative configuration and scratch paths resolve from the project root, including when the TOML lives elsewhere. The root must exist and resolve to a directory. A missing current file is an error. If the legacy workspace file `data/.config/historical/general.toml` exists instead, the current managed path reports that migration is required.

| Evidence | Meaning |
| --- | --- |
| Configured settings | The values explicitly written to TOML. |
| Effective settings | Rust resolves omitted fields and converts the scratch path to a project-root-based path. Pipeline runs recursively overlay `[normalize]` in `pipeline.toml` after validating the base and validate the combined result. The overlay cannot set the operation's schema version. |
| Execution-pinned settings | Persisted Forge preparation retains the runtime TOML and its SHA-256 beside the batch. Execution with matching preparation evidence loads the retained copy and checks its fingerprint. Direct batch submission without that evidence uses the current runtime. Managed execution retains its own runtime copy and fingerprint. Pipeline resume uses the pinned stage files. |
| Publication evidence | The normalized publication keeps the effective Rust processing configuration in `processing-runtime.json` with its SHA-256 companion. |

Editing the current file affects later resolutions. In-flight Engines use settings already loaded, and existing prepared and publication evidence keeps its original configuration. See [pipeline inheritance](../runtime.md#pipeline-runtime), the [prepared batch reference](../data-formats/prepared-normalization.md), and [normalized publications](../data-formats/publication-normalized.md).

## Runtime configuration overview

The field tables give **Rust defaults on omission**, rather than the workspace's explicitly configured values. `schema_version`, the four main resource fields, both Parquet targets, and each integrity rule's `action` are required. Optional tables can be omitted unless they contain required fields. A dash in a default column means there is no default or no optional threshold. The loader rejects unknown fields at every supported level. Rust integer types below are unsigned unless stated otherwise. Byte values use binary units.

Performance settings change resource use and layout. Reconciliation, timestamp processing and integrity rules can change event selection or publication eligibility. Treat those choices independently.

## 1. Processing and performance

| Field | Rust type · default | Behavior and constraints |
| --- | --- | --- |
| <a id="schema-version"></a>`schema_version` | `u32` · required | Exactly `1`. An unsupported value fails load. This field versions the runtime document independently of recipes, profiles and batches. |
| <a id="merge-fan-in"></a>`merge_fan_in` | `usize` · `8` | Inclusive range `2–64`. Requested maximum number of input runs merged at a time in normalization scratch sorting/reconciliation. Effective fan-in depends on memory, admitted handles and available runs. Higher values can reduce passes while increasing simultaneous buffers and handles. |
| <a id="scratch-format"></a>`scratch_format` | `ScratchFormat` · `json` | `json`, `typed_trades`, or `typed_events`. Selects encoding on scratch paths that use the reconciler's spools. See the format details below. |
| <a id="scratch-codec"></a>`scratch_codec` | `ScratchCodec` · `zlib` | `zlib` uses fast zlib compression on participating scratch records. `uncompressed` stores raw encoded payloads with the spool framing. Final Parquet encoding is independent. |

`json` is the generic spool representation. `typed_trades` enables typed trade payloads. `typed_events` also enables typed depth payloads on the shared-envelope spool path. Generic records such as routing or metadata still use JSON. The setting does not select a family-discovery or decoding strategy and does not require all records in an execution to have the same payload encoding. Both typed modes are accepted by Rust where the corresponding writer path is used. Compare retained scratch and timing measurements for the workload before changing either setting.

Forge-generated jobs declare `event_timestamp_ns` as the primary ordering key and `stream_rank` as the tie-break. That declaration belongs to the [prepared job contract](../data-formats/prepared-normalization.md#job-ordering). Runtime `[ordering]` is an unknown table.

## 2. Resource management

| Field | Rust type · default | Behavior and constraints |
| --- | --- | --- |
| <a id="resources-workers"></a>`resources.workers` | `usize` · required | Positive ceiling on worker concurrency. The scoped scheduler spawns at most that many worker threads, capped by the number of work units. Memory and handle admission can leave workers idle. Other process threads, such as diagnostics, are outside this count. |
| <a id="resources-memory-budget-bytes"></a>`resources.memory_budget_bytes` | `u64` · required | Positive session-wide admission budget in bytes. Reservations include retained evidence, active units, buffers and finalization estimates. The budget is not an OS RSS cap. |
| <a id="resources-scratch-path"></a>`resources.scratch_path` | `PathBuf` · required | Nonempty path string. An existing path must be a directory. The Engine creates scratch directories as needed during work, so the process needs write access and space on that filesystem. |
| <a id="resources-scratch-budget-bytes"></a>`resources.scratch_budget_bytes` | `u64` · required | Positive byte budget shared through session resource accounting for participating scratch files. Spool writes charge storage as they grow and release it when removed. This is not a quota for the entire filesystem or the final publication. |

An execution unit can represent one source task, reconciled work for a stream, or routed family work. Increasing workers increases potential overlap of decoders, writers, evidence buffers and file handles. An individual unit plus required retained state must fit even when no other worker is active. A clean shutdown drops owned scratch workspaces. A hard kill or failed filesystem cleanup can leave files. Reserve filesystem capacity separately from the Engine budget.

The workspace's configured `68719476736` memory bytes are **64 GiB**, and `137438953472` scratch bytes are **128 GiB**. These are examples of explicit settings, not defaults.

## 3. Normalization safety limits

All eight `resources.normalization_limits` leaves are optional `u64` values. Each must be greater than zero and at most `usize::MAX / 128` for the Engine's platform. Limits are checked at the identified subsystem boundary. Exceeding a limit produces a resource failure rather than permission to skip excess input through an integrity rule.

| Field under `resources.normalization_limits` | Rust default | Enforced accounting |
| --- | --- | --- |
| <a id="resources-normalization-limits-workbook-bytes"></a>`workbook_bytes` | 8 MiB (8388608 bytes) | Bounds the selected XLSX byte stream before workbook decoding. For an externally compressed source or selected member it limits the decoded member's XLSX container bytes, before expanding the workbook's own XML. |
| <a id="resources-normalization-limits-workbook-xml-bytes"></a>`workbook_xml_bytes` | 16 MiB (16777216 bytes) | Bounds each expanded XML member the reader loads. Also bounds accumulated worksheet cell text in the applicable worksheet decode. |
| <a id="resources-normalization-limits-workbook-rows"></a>`workbook_rows` | 25000 | Bounds row materialization on its worksheet parsing and decoded-row paths. Shared strings have a separate count ceiling of three times this setting. This is not an archive-wide count of market events. |
| <a id="resources-normalization-limits-record-bytes"></a>`record_bytes` | 1 MiB (1048576 bytes) | Bounds CSV encoded record payload, including quoting and embedded newlines but excluding the terminating record boundary. JSONL charges line bytes including the terminator before trimming. XLSX applies it to cell text. It also bounds selected metadata strings and sizes internal estimates. |
| <a id="resources-normalization-limits-depth-levels"></a>`depth_levels` | 25000 | Bounds reconstructed book and pending snapshot level counts, with bids and asks counted together at each applicable check. Also feeds depth workspace estimates. |
| <a id="resources-normalization-limits-metric-entries"></a>`metric_entries` | 4096 | Caps per-task file plus hourly and daily observation entries. Separate guards reuse the same numeric ceiling for inventories such as archive members, family or routing entries, output parts, row groups and depth segments. This can fail processing, independently of the diagnostics mode. |
| <a id="resources-normalization-limits-reconciliation-bytes"></a>`reconciliation_bytes` | 64 MiB (67108864 bytes) | Resident reconciliation/routing quota per applicable execution unit. Covers accounted in-memory state such as merge records, ownership indexes, and buffered events. The unit's quota is included in the session memory estimate. Spool storage is accounted separately. |
| <a id="resources-normalization-limits-open-files"></a>`open_files` | 64 | Session ceiling for participating handles with eight reserved for logging, metadata, provenance and finalization. Admission and spool fan-in account for active handles. The OS file-descriptor limit still applies independently. |

The managed value `reconciliation_bytes = 8589934592` is **8 GiB**. The comment has been corrected to 8 GiB. The numeric value is unchanged.

## 4. Parquet output

Both `resources.parquet` fields are required positive `u64` byte targets. Rust enforces `row_group_target_bytes <= file_target_bytes`; equality is allowed.

| Field | Effect |
| --- | --- |
| <a id="resources-parquet-row-group-target-bytes"></a>`resources.parquet.row_group_target_bytes` | Controls flushing buffered rows as their estimated footprint reaches the target. It contributes to each active writer's memory estimate. Depth also uses it with a 1 MiB floor in checks on certain metadata buffers. |
| <a id="resources-parquet-file-target-bytes"></a>`resources.parquet.file_target_bytes` | Controls rotation after accumulated output reaches the target at writer boundaries. Compression, row-group granularity, metadata and footers affect actual file size. |

These targets are approximate sizing controls. Output size is distinct from decoded input, scratch size and in-memory row size. Writers currently select Parquet Zstandard level 3. Smaller row groups can reduce buffering and change scan granularity, while smaller parts can increase file and metadata counts. See [publication layout and sizing](../data-formats/publication-normalized.md#parquet-file-sizing).

## 5. Diagnostics

| Field | Rust type · default | Behavior |
| --- | --- | --- |
| <a id="diagnostics-mode"></a>`diagnostics.mode` | `DiagnosticsMode` · `disabled` | `disabled` skips the optional performance collector. Essential results, integrity counters and publication evidence still exist. `basic` collects progress, phase, resource and process snapshots. `detailed` adds current RSS on Linux if readable. |
| <a id="diagnostics-interval-ms"></a>`diagnostics.interval_ms` | `u64` · `1000` | Inclusive range `100–60000` ms, validated in every mode. The collector wakes on a timer or on coalesced progress notifications. Heartbeats can appear during CPU or I/O waits. This controls reporting, not worker scheduling. |

When enabled and able to open its output, the collector emits diagnostic lines to stderr and writes `effective-config.json`, `live.jsonl` and a final `performance.json` under a `performance-…` directory beside the staging output. The TUI uses the progress snapshots directly. When snapshots are already enabled, the separate collector skips startup. One bounded wake channel coalesces progress; reporting still costs serialization and I/O. The modes do not change integrity thresholds or event processing policies.

## 6. Integrity policies

The integrity system records an eligible check and whether it failed. Its evaluation can be clean, degraded or failed. A failed evaluation blocks that job's publication. A degraded result can still be committed if independent processing and resource checks succeed. Forge verification and [primary-file registration](../cli/normalize.md#registration-and-recovery) are subsequent operations.

<a id="integrity-policy-enabled"></a>

`integrity_policy.enabled` is a Rust `bool`, optional and `true` by default. Setting it to `false` skips immediate rule enforcement and final threshold evaluation. Counting continues, and recorded violations still mark output degraded. Source checks, independent reconciliation/timestamp policy and resource guards continue. All seven rule tables and actions are required and validated even when evaluation is disabled. Legacy `profile` is an alias for `enabled`. Both keys can read legacy strings `standard` and `strict` as `true`. Use a boolean with the CLI.

### Shared rule fields

Apply these fields under each `integrity_policy.<rule>`.

| Leaf | Rust type · default | Effect and validation |
| --- | --- | --- |
| <a id="integrity-rule-action"></a><a id="integrity-policy-parse-failure-action"></a><a id="integrity-policy-invalid-record-action"></a><a id="integrity-policy-sequence-gap-action"></a><a id="integrity-policy-timestamp-regression-action"></a><a id="integrity-policy-missing-snapshot-action"></a><a id="integrity-policy-invalid-book-action"></a><a id="integrity-policy-transformation-failure-action"></a>`action` | `IntegrityAction` · required | `fail` rejects any occurrence, regardless of configured count/rate allowances. `degrade` marks an occurrence degraded and fails if any configured threshold is exceeded. Rust requires at least one count, rate or window threshold for `degrade`. |
| <a id="integrity-rule-max-count"></a><a id="integrity-policy-parse-failure-max-count"></a><a id="integrity-policy-invalid-record-max-count"></a><a id="integrity-policy-sequence-gap-max-count"></a><a id="integrity-policy-timestamp-regression-max-count"></a><a id="integrity-policy-missing-snapshot-max-count"></a><a id="integrity-policy-invalid-book-max-count"></a><a id="integrity-policy-transformation-failure-max-count"></a>`max_count` | `Option<u64>` · absent | Absolute violation count across the job. Checked with strict `>`; zero fails on the first occurrence. The immediate evaluator can check count where supplied. Scheduled workers defer aggregate thresholds to final evaluation. |
| <a id="integrity-rule-max-rate"></a><a id="integrity-policy-parse-failure-max-rate"></a><a id="integrity-policy-invalid-record-max-rate"></a><a id="integrity-policy-sequence-gap-max-rate"></a><a id="integrity-policy-timestamp-regression-max-rate"></a><a id="integrity-policy-missing-snapshot-max-rate"></a><a id="integrity-policy-invalid-book-max-rate"></a><a id="integrity-policy-transformation-failure-max-rate"></a>`max_rate` | `Option<f64>` · absent | Finite fraction in `[0, 1]`. A `degrade` rule checks this separately globally, per source-file scope, and per stream. The denominator is eligible checks of that category within the scope. Rust uses decimal arithmetic at evaluation, so unsupported decimal precision fails evaluation. |
| <a id="integrity-rule-minimum-samples"></a><a id="integrity-policy-parse-failure-minimum-samples"></a><a id="integrity-policy-invalid-record-minimum-samples"></a><a id="integrity-policy-sequence-gap-minimum-samples"></a><a id="integrity-policy-timestamp-regression-minimum-samples"></a><a id="integrity-policy-missing-snapshot-minimum-samples"></a><a id="integrity-policy-invalid-book-minimum-samples"></a><a id="integrity-policy-transformation-failure-minimum-samples"></a>`minimum_samples` | `u64` · `0` | Checks a rate only once the scope has at least this many eligible observations and a nonzero denominator. It never delays a count or fail action. |
| <a id="integrity-rule-window-max-rate"></a><a id="integrity-policy-parse-failure-windows-hourly-max-rate"></a><a id="integrity-policy-parse-failure-windows-daily-max-rate"></a><a id="integrity-policy-invalid-record-windows-hourly-max-rate"></a><a id="integrity-policy-invalid-record-windows-daily-max-rate"></a><a id="integrity-policy-sequence-gap-windows-hourly-max-rate"></a><a id="integrity-policy-sequence-gap-windows-daily-max-rate"></a><a id="integrity-policy-timestamp-regression-windows-hourly-max-rate"></a><a id="integrity-policy-timestamp-regression-windows-daily-max-rate"></a><a id="integrity-policy-missing-snapshot-windows-hourly-max-rate"></a><a id="integrity-policy-missing-snapshot-windows-daily-max-rate"></a><a id="integrity-policy-invalid-book-windows-hourly-max-rate"></a><a id="integrity-policy-invalid-book-windows-daily-max-rate"></a><a id="integrity-policy-transformation-failure-windows-hourly-max-rate"></a><a id="integrity-policy-transformation-failure-windows-daily-max-rate"></a>`windows.hourly.max_rate`, `windows.daily.max_rate` | `f64` · required for a configured window | Finite fraction in `[0, 1]`. Each configured window rate is evaluated independently against category observations within its source scope and time bucket. |
| <a id="integrity-rule-window-minimum-samples"></a><a id="integrity-policy-parse-failure-windows-hourly-minimum-samples"></a><a id="integrity-policy-parse-failure-windows-daily-minimum-samples"></a><a id="integrity-policy-invalid-record-windows-hourly-minimum-samples"></a><a id="integrity-policy-invalid-record-windows-daily-minimum-samples"></a><a id="integrity-policy-sequence-gap-windows-hourly-minimum-samples"></a><a id="integrity-policy-sequence-gap-windows-daily-minimum-samples"></a><a id="integrity-policy-timestamp-regression-windows-hourly-minimum-samples"></a><a id="integrity-policy-timestamp-regression-windows-daily-minimum-samples"></a><a id="integrity-policy-missing-snapshot-windows-hourly-minimum-samples"></a><a id="integrity-policy-missing-snapshot-windows-daily-minimum-samples"></a><a id="integrity-policy-invalid-book-windows-hourly-minimum-samples"></a><a id="integrity-policy-invalid-book-windows-daily-minimum-samples"></a><a id="integrity-policy-transformation-failure-windows-hourly-minimum-samples"></a><a id="integrity-policy-transformation-failure-windows-daily-minimum-samples"></a>`windows.hourly.minimum_samples`, `windows.daily.minimum_samples` | `u64` · required for a configured window | Sample floor for that category, source scope and window. Zero is valid, and an empty bucket has no rate. |

The Engine compares violations using strict `>` against eligible observations times the fraction. A rate of `0.01` is 1%. Hourly and daily buckets use fixed UTC epoch-aligned event-time intervals, including for timestamps before the epoch. Counts use observations present in each bucket, without estimating missing samples or requiring calendar completeness. Eligible observations without a trustworthy timestamp fail verification of any configured window for that category, even when the unattributed observations did not violate the rule. Window thresholds provide independent checks and never cancel a failure.

### Rule categories

All seven use the shared action, count, rate and window contract above. Each eligible check counts once in its category for the applicable unit. A record may reach multiple checks. Source diagnostics carry contextual samples, and evaluation evidence identifies the category, threshold, observation counts and available scope.

| Required rule table | What counts |
| --- | --- |
| <a id="integrity-policy-parse-failure"></a>`integrity_policy.parse_failure` | A physical decoder attempt, such as a CSV record, JSONL message or XLSX row. Both trades and depth can fail while reading or parsing a source record. Unparseable timestamps may prevent window attribution. Typed resource-limit errors remain resource errors. |
| <a id="integrity-policy-invalid-record"></a>`integrity_policy.invalid_record` | An applicable record or event validation unit for trades or depth. Examples include invalid input field values, inconsistent sequence metadata or invalid price levels. A later failure in a different category does not erase an earlier successful check. |
| <a id="integrity-policy-sequence-gap"></a>`integrity_policy.sequence_gap` | A sequenced incremental depth observation reaching continuity checks on an initialized book. Native sequence policy determines valid continuity and can require prior/end linkage. Unsequenced snapshots do not create synthetic sequence evidence. |
| <a id="integrity-policy-timestamp-regression"></a>`integrity_policy.timestamp_regression` | An applicable comparison against the previous accepted timestamp in source order for trades or depth. A lower timestamp is an integrity observation. Separately, temporal processing uses its own high-water checks, which can still fail. |
| <a id="integrity-policy-missing-snapshot"></a>`integrity_policy.missing_snapshot` | An incremental depth event reaching synchronization checks without initialization. The rule can govern whether the anomaly is tolerated, but it cannot initialize a book or satisfy a recovery boundary. |
| <a id="integrity-policy-invalid-book"></a>`integrity_policy.invalid_book` | Reserved for explicit book-invariant observations. The current normalization paths attribute ordinary level validation to `invalid_record`; they do not emit a general `invalid_book` result for every bad or valid book. |
| <a id="integrity-policy-transformation-failure"></a>`integrity_policy.transformation_failure` | One attempted quantity-transformation batch for a trade, depth message or assembled snapshot. For depth, it is not counted once per emitted level. Missing required conversion terms or failed quantity arithmetic can produce it. |

## 7. Trade reconciliation

<a id="reconciliation-trades-overlap-policy"></a>

`reconciliation.trades.overlap_policy` is an optional `TradeOverlap` enum. Its default is `strict`.

- **`strict`** keeps distinct comparable trades and suppresses duplicates only when native identity and canonical equivalence support that decision. Conflicting contents under a reliable native identity, or overlapping sources without established comparable identity/reset domains, can fail. Equal timestamps alone are insufficient evidence of duplication.
- **`prefer_source`** enables source precedence for ambiguous overlap or supported conflicts. It can charge a non-equivalent or unverified observation as excluded. Distinct reliable identities can still be retained. An ordered native-ID run has additional proof requirements and cannot use this option to discard conflicting records or make missing IDs valid.

Overlap ranges use each source's minimum and maximum accepted event timestamps. The selected source wins on supported conflicts, using accepted start time and stable identity tie-breaks. For example, if source A begins at 09:00 and source B at 09:30, `earlier` gives A precedence in their overlap and `later` gives B precedence. This is an illustration of resolution where preference applies, not a rule to crop all records from the other source.

### Source preference and loss limits

The table applies to both `reconciliation.trades.prefer_source` and `reconciliation.depth.prefer_source`. Both tables are optional. Their source-selection behavior activates only with `overlap_policy = "prefer_source"`. Rust validates both tables even when inactive.

| Leaf | Rust type · default | Effect and constraint |
| --- | --- | --- |
| <a id="reconciliation-trades-prefer-source-selection"></a><a id="reconciliation-depth-prefer-source-selection"></a>`selection` | `Selection` · `earlier` | `earlier` or `later` by minimum accepted event timestamp. Stable source identity and source order break ties. |
| <a id="reconciliation-trades-prefer-source-on-unique-data-loss"></a><a id="reconciliation-depth-prefer-source-on-unique-data-loss"></a>`on_unique_data_loss` | `LossAction` · `fail` | `fail` rejects the first known-unique or unverified exclusion. `mark_degraded` permits only bounded exclusions and requires a positive event cap. |
| <a id="reconciliation-trades-prefer-source-max-excluded-events"></a><a id="reconciliation-depth-prefer-source-max-excluded-events"></a>`max_excluded_events` | `u64` · `0` | Logical event cap applied to source and job exclusion totals. Strict `>` fails. Verified duplicates do not consume this cap. |
| <a id="reconciliation-trades-prefer-source-max-excluded-rate"></a><a id="reconciliation-depth-prefer-source-max-excluded-rate"></a>`max_excluded_rate` | `Option<f64>` · absent | Finite fraction in `[0,1]` representable by Rust Decimal. Applies job-wide to overlap exclusions divided by accepted logical source observations minus verified duplicates. A rate strictly above the threshold fails. |
| <a id="reconciliation-trades-prefer-source-minimum-samples"></a><a id="reconciliation-depth-prefer-source-minimum-samples"></a>`minimum_samples` | `u64` · `0` | The optional rate applies at or above this denominator, provided it is nonzero. The event cap still applies below the floor. |
| <a id="reconciliation-trades-prefer-source-max-excluded-interval-ms"></a><a id="reconciliation-depth-prefer-source-max-excluded-interval-ms"></a>`max_excluded_interval_ms` | `Option<u64>` · absent | Maximum timestamp span from minimum to maximum event time within a contiguous run of excluded observations, tracked in native source order. A retained observation resets the run. Zero allows a zero-duration span only. The converted value must fit signed 64-bit nanoseconds. |

Rate limits measure logical observations, independently of the number of canonical rows they would emit. The Engine records overlap and timestamp exclusions, known-unique versus unverified exclusions, and excluded canonical rows separately. A run of exclusions may include both causes and is tracked accordingly. A degraded publication records a permitted loss or other processing-quality signal. This evidence does not establish continuous market history.

## 8. Order-book reconciliation

<a id="reconciliation-depth"></a>
<a id="reconciliation-depth-overlap-policy"></a>

`reconciliation.depth.overlap_policy` is an optional `DepthOverlap` enum and defaults to `strict`. A book is stateful. A snapshot initializes it, and incremental events update that state under the source's sequence contract.

- **`strict`** rejects overlapping independent histories unless a supported boundary equivalence is proven. For example, matching authoritative snapshots at a shared boundary can be suppressed as one proven duplicate.
- **`snapshot_recovery`** uses authoritative initialization snapshots and retained boundary evidence to publish valid reconstruction segments. It does not authorize intentional overlap exclusions of unique supplied observations. The Engine marks continuity unproven when an independent initialization cannot be linked by matching state and applicable native sequence evidence.
- **`prefer_source`** assigns overlap ownership using the accepted event ranges and [shared preference settings](#source-preference-and-loss-limits). An authorized exclusion is charged in logical observations even when it represents many levels or emits zero canonical rows. The resulting segments do not imply continuity across a source change.

<a id="reconciliation-depth-prefer-source-switch-boundary"></a>

`reconciliation.depth.prefer_source.switch_boundary` is an optional `SwitchBoundary` enum, default `snapshot`. `snapshot` waits for a snapshot to switch after an existing source. Excluded observations during the wait remain subject to loss limits. `source_boundary` allows a switch only with matching reconstruction state and native sequence evidence, either at the incoming boundary or via a matching authoritative snapshot. A new history still needs authoritative initialization.

Overlap recovery and timestamp recovery address different conditions. Source processing validates sequence, synchronization and book inputs. The [integrity rules](#6-integrity-policies) can fail the job on those observations, and the recovery algorithms still enforce their own proof and budget requirements.

## 9. Timestamp regression

The Engine records raw timestamp anomalies in source order. Processing limits compare against a source-local accepted high-water mark, and independent integrity rules can fail on the raw anomaly first. Millisecond bounds are optional `u64` values which must fit signed 64-bit nanoseconds after multiplication by 1000000. These controls do not change source timestamps.

### Trades

<a id="timestamp-regression-trades"></a>

| Field under `timestamp_regression.trades` | Rust type · default | Effect and constraint |
| --- | --- | --- |
| <a id="timestamp-regression-trades-policy"></a>`policy` | `TradeTimestamp` · `strict` | `strict` rejects unresolved regressions. `preserve` retains native order for tolerated regressions. `reorder` allows bounded disorder and uses chronological output sorting. `discard` excludes regressing surviving observations within the configured bounds. |
| <a id="timestamp-regression-trades-max-regression-ms"></a>`max_regression_ms` | `u64` · `0` | High-water tolerance for non-strict processing. Must be positive for `reorder` or `discard`. A larger regression fails even when a discard cap remains. |
| <a id="timestamp-regression-trades-max-reorder-window-ms"></a>`max_reorder_window_ms` | `u64` · `0` | High-water window for `reorder`. Must be positive and at least `max_regression_ms` in that mode. A larger window cannot relax the regression cap. |
| <a id="timestamp-regression-trades-max-discarded-events"></a>`max_discarded_events` | `u64` · `0` | Cap on timestamp-discarded logical events. Must be positive for `discard`. Checked per source and job with strict `>`. |
| <a id="timestamp-regression-trades-max-discarded-rate"></a>`max_discarded_rate` | `Option<f64>` · absent | Optional additional job-wide cap for discard. Finite fraction in `[0,1]` representable by Rust Decimal. Uses accepted logical observations minus verified duplicates as the denominator. |
| <a id="timestamp-regression-trades-minimum-samples"></a>`minimum_samples` | `u64` · `0` | Sample floor for the optional discard rate. A nonzero denominator is required. The event count cap applies independently. |

On the general reconciliation path the Engine resolves identity, verified duplicates and overlap before applying timestamp processing to surviving records in concrete source order. Ordered-run identity checks still apply, and raw regressions remain in the evidence even if reconciliation removes the offending record.

### Depth

<a id="timestamp-regression-depth"></a>

| Field under `timestamp_regression.depth` | Rust type · default | Effect and constraint |
| --- | --- | --- |
| <a id="timestamp-regression-depth-policy"></a>`policy` | `DepthTimestamp` · `strict` | `strict` rejects unresolved regressions. `preserve` tolerates bounded regressions in native processing order. `snapshot_recovery` can suppress updates during a regression recovery and resume at an authoritative snapshot, emitting a new initialization. |
| <a id="timestamp-regression-depth-max-regression-ms"></a>`max_regression_ms` | `u64` · `0` | Bounds non-strict high-water regression checks, including `preserve` and recovery checks. A genuine snapshot sequence reset can reset the comparison state. |
| <a id="timestamp-regression-depth-require-sequence-continuity"></a>`require_sequence_continuity` | `bool` · `true` | Must be true. Applicable native continuity checks remain enabled. Unsequenced snapshot sources need no synthetic sequence. |
| <a id="timestamp-regression-depth-require-recovery-snapshot"></a>`require_recovery_snapshot` | `bool` · `true` | Must be true. Recovery needs verified initialization evidence. |
| <a id="timestamp-regression-depth-max-discarded-events"></a>`max_discarded_events` | `u64` · `0` | Timestamp-recovery cap on excluded logical events at source and job scope. The first exclusion exceeds zero. No depth discard-rate field exists. |

Stateful depth events are not reordered by event timestamp to repair continuity. The profile and decoder define whether sequencing is applicable, and an Engine-supported sortable workbook path can sort independent snapshots before stateful processing. A 2 ms tolerance alone establishes nothing about book reconstruction.

## 10. Cross-setting dependencies

| Kind | Relationship |
| --- | --- |
| Loader constraint | Both Parquet targets are positive, with row group at most file target. All limits and configured rates are validated even when inactive. |
| Admission | Workers, writer targets, safety limits and per-unit reconciliation allowances compete for session memory. The handle limit reserves eight session handles. Fan-in is reduced on participating sort paths and can be constrained further by memory and handle budgets. |
| Execution accounting | Scratch encoding can change scratch bytes and codec work. Resident reconciliation state is charged to memory, while spool bytes charge scratch. Full disk and OS descriptor limits can still stop work. |
| Integrity evaluation | A fail action rejects any violation, while degrade needs a threshold and uses independent count, rate and window checks. Disabling evaluation leaves detection and resource checks in effect. |
| Source reconciliation | Preference settings apply to source ownership under `prefer_source`. Count bounds cannot be replaced by sample floors. Depth source changes need the selected initialization or proof boundary. |
| Temporal processing | Reordering requires positive bounds and a window at least as large as the trade regression limit. Discard also requires a positive event cap. Depth requires applicable sequence validation and verified initialization regardless of timestamp policy. |

## 11. Validation and safe modification

Runtime validation checks UTF-8 TOML syntax, the schema version, recognized fields, types and enum spellings, rule and resource bounds, and cross-field constraints. It also resolves paths and rejects an existing scratch path that is not a directory. It does not run admission for a particular workload, test available RAM or free space, or guarantee that scratch is writable.

The direct loader can validate a complete candidate in a separate file. Replace `PATH_TO_CANDIDATE.toml` with your draft's path.

```bash
.bin/marketforge-normalize --runtime-settings PATH_TO_CANDIDATE.toml --project-root .
```

If Forge reports that the managed Engine is absent, invalid or too old for runtime-schema inspection, use [`mf engine build normalize`](../cli/engine.md). A successful config validation depends on the Rust loader actually running.

### Preview and apply

```bash
mf normalize config set resources.workers 12 | cpout
mf normalize config set resources.workers 12 --apply | cpout
mf normalize config set resources.memory_budget_bytes 48GiB | cpout
mf normalize config unset integrity_policy.parse_failure.max_count | cpout
```

`set` and `unset` preview by default. `--apply` rechecks the source file and validates the complete candidate again with Rust before atomic replacement. Forge checks the original file identity, contents and parent directory before saving. The edit round-trips TOML so unrelated settings and comments are preserved. `unset` removes only optional leaf settings. An omitted setting uses its Rust default when it has one. Otherwise the setting is absent.

Enter byte quantities as whole bytes or use case-sensitive binary units `KiB`, `MiB`, `GiB`, or `TiB`. A fractional unit is accepted only when it converts to a whole number of bytes. Enter booleans as `true` or `false` and enum values by their exact spelling.

Integrity-critical changes require `--acknowledge-risk` with `--apply`. The Rust authoring schema classifies field risk. The preview lists the affected settings and required acknowledgement. Resource-sensitive and processing-semantic changes are flagged in the preview. Schema identity is immutable through these commands. Coordinated changes such as adding an integrity window with both required members belong in the editor.

### Edit the complete configuration

```bash
mf normalize config edit
```

The command opens a private draft in the configured editor, with Neovim as the default. It requires a terminal. Review the changes and type `apply` to save. Integrity-critical edits require typing `apply and acknowledge risk`. A validation failure or declined confirmation keeps the draft and displays its path. The managed file must be a regular, single-link file, and the management commands refuse symlink targets and directories.

## 12. Configuration examples

### A. Complete strict starting point

This document supplies all required fields and uses Rust defaults for omitted settings. Resource values are explicit starting values. Workload admission still depends on the selected work.

```toml
schema_version = 1

[resources]
workers = 2
memory_budget_bytes = 8589934592
scratch_path = "data/.work/normalize"
scratch_budget_bytes = 17179869184

[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 536870912

[integrity_policy.parse_failure]
action = "fail"

[integrity_policy.invalid_record]
action = "fail"

[integrity_policy.sequence_gap]
action = "fail"

[integrity_policy.timestamp_regression]
action = "fail"

[integrity_policy.missing_snapshot]
action = "fail"

[integrity_policy.invalid_book]
action = "fail"

[integrity_policy.transformation_failure]
action = "fail"
```

The examples below are replacements or additions to the relevant table in example A. Keep a single declaration of each TOML table.

### B. Smaller resource targets

This sets 4 GiB of memory, 8 GiB of scratch, and 32 MiB/256 MiB Parquet targets. It reduces potential concurrency and writer buffering. A single large execution unit can still exceed admission.

```toml
[resources]
workers = 1
memory_budget_bytes = 4294967296
scratch_path = "data/.work/normalize"
scratch_budget_bytes = 8589934592

[resources.parquet]
row_group_target_bytes = 33554432
file_target_bytes = 268435456
```

### C. Detailed diagnostics

Adds work for snapshot serialization, logging and available RSS sampling.

```toml
[diagnostics]
mode = "detailed"
interval_ms = 2000
```

### D. Source preference with bounded trade loss

Earlier observed starts take precedence where the policy applies. Up to 10 charged exclusions can yield a degraded publication if other checks succeed. The eleventh exceeds the cap.

```toml
[reconciliation.trades]
overlap_policy = "prefer_source"

[reconciliation.trades.prefer_source]
selection = "earlier"
on_unique_data_loss = "mark_degraded"
max_excluded_events = 10
```

### E. Count-bounded integrity degradation

Replace the `invalid_record` rule. An occurrence degrades the job, and the third violation exceeds the count threshold. The rule does not turn invalid source records into valid output.

```toml
[integrity_policy.invalid_record]
action = "degrade"
max_count = 2
```

### F. Preserve bounded depth timestamp regressions

This changes both the processing policy and the independent timestamp integrity rule from example A. Replace the integrity rule table and add the depth timestamp table. Qualifying regressions can be retained in native order, subject to other sequence and reconstruction checks. Any fourth eligible integrity violation exceeds the count cap.

```toml
[integrity_policy.timestamp_regression]
action = "degrade"
max_count = 3

[timestamp_regression.depth]
policy = "preserve"
max_regression_ms = 2
```

## 13. Troubleshooting

| Problem | What to check |
| --- | --- |
| Unknown field or unsupported schema | Check the exact TOML path and enum spelling. Use `schema_version = 1` with the current loader. Ordering belongs to the job contract. |
| Budget rejected or memory admission fails | Check positive values and look at retained admission estimates for the largest unit, writer memory, retained evidence, and handle headroom. |
| Parquet targets invalid | Use positive targets with row group no larger than file. |
| Scratch path unavailable | Resolve it from the project root and check directory permissions and free space. |
| Degrade rule rejected | Supply at least one count, rate or window threshold. A configured window needs both fields. |
| Reconciliation appears inactive | Check the chosen overlap policy, source identity proof and whether the paths contain applicable overlap. |
| Timestamp policy rejected | Check policy-specific positive bounds, the reorder-window relationship, and the required true depth flags. |
| Rust Engine unavailable | Install or rebuild with `mf engine build normalize`. |
| Configuration changed concurrently | Inspect the current file again and build a fresh candidate. |
| Setting appears to have no effect | Compare current effective settings with the preparation and execution copies and the publication's `processing-runtime.json`. Check activation and the evidence in [dataset verification](../../workflows/inspection/datasets.md#verifying-datasets). |

## 14. Complete parameter index

The linked tables are the authoritative field entries. Shared leaves are defined once and apply to each named parent.

| Section | Leaves |
| --- | ---: |
| [Schema, fan-in and scratch](#1-processing-and-performance) | 4 |
| [Resource budgets and path](#2-resource-management) | 4 |
| [Normalization safety limits](#3-normalization-safety-limits) | 8 |
| [Parquet output](#4-parquet-output) | 2 |
| [Diagnostics](#5-diagnostics) | 2 |
| [Integrity switch](#integrity-policy-enabled) and [seven rules](#rule-categories), each with [eight leaves](#shared-rule-fields) | 57 |
| [Trade](#7-trade-reconciliation) and [depth](#8-order-book-reconciliation) reconciliation, including [shared preference leaves](#source-preference-and-loss-limits) | 15 |
| [Trade and depth timestamp regression](#9-timestamp-regression) | 11 |
| **Total current authoring schema** | **103** |

Compatibility for retained `general.toml` is separate. The Rust loader accepts legacy `merge.partitions_per_worker` (`usize`, default 4) and `merge.compression_level` (`i32`, default 3) only under that filename. Normalization discards these values. See [merge runtime](../runtime.md#merge-and-conversion-runtime) for the operation that uses them.

Rust remains the configuration authority. See the [runtime loader and schema](https://github.com/flowdrivenml/marketforge/tree/main/engine/src/shared/historical), [normalization execution](https://github.com/flowdrivenml/marketforge/tree/main/engine/src/historical/normalize), and [Forge runtime resolution](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/engine/runtime.py).

## Settings Explorer field targets

These entries use the runtime schema’s wording and constraints. The sections above explain how settings interact in a processing run.

<a id="setting-schema_version"></a>

**`schema_version`**

Normalization runtime schema identity.

Type `integer` · required in the base runtime file · read-only schema identity. Must match the Engine runtime schema version. Risk `ordinary`.

<a id="setting-merge_fan_in"></a>

**`merge_fan_in`**

Requested merge inputs per pass. Admission can reduce the fan-in to fit memory and file handles.

Type `integer` · default `8`. 2 to 64 inclusive. Risk `resource-sensitive`.

<a id="setting-scratch_format"></a>

**`scratch_format`**

Internal scratch serialization. Typed variants use binary trades; typed_events also uses shared-envelope depth where supported by the path. Other records use JSON.

Type `enum` · default `"json"` · choices `json`, `typed_trades`, `typed_events`. Choices supported by this Engine build. Risk `resource-sensitive`.

<a id="setting-scratch_codec"></a>

**`scratch_codec`**

Compression for temporary scratch records. Final Parquet encoding is independent.

Type `enum` · default `"zlib"` · choices `zlib`, `uncompressed`. Choices supported by this Engine build. Risk `resource-sensitive`.

<a id="setting-diagnostics.mode"></a>

**`diagnostics.mode`**

Controls the detail of Engine progress diagnostics and retained performance evidence.

Type `enum` · default `"disabled"` · choices `disabled`, `basic`, `detailed`. Risk `ordinary`.

<a id="setting-diagnostics.interval_ms"></a>

**`diagnostics.interval_ms`**

Timer-driven diagnostic heartbeat interval.

Type `milliseconds` · units milliseconds · default `1000`. 100 to 60000 inclusive. Risk `ordinary`. Active when `diagnostics.mode` = `basic` or `detailed`.

<a id="setting-resources.workers"></a>

**`resources.workers`**

Requested worker concurrency. Increasing it may increase memory, scratch, and handle demand.

Type `integer` · required in the base runtime file. Greater than zero. Risk `resource-sensitive`.

<a id="setting-resources.memory_budget_bytes"></a>

**`resources.memory_budget_bytes`**

Memory admission budget for Engine work.

Type `bytes` · units bytes · required in the base runtime file. Greater than zero. Risk `resource-sensitive`.

<a id="setting-resources.scratch_budget_bytes"></a>

**`resources.scratch_budget_bytes`**

Scratch-space admission budget for spools and other temporary files.

Type `bytes` · units bytes · required in the base runtime file. Greater than zero. Risk `resource-sensitive`.

<a id="setting-resources.scratch_path"></a>

**`resources.scratch_path`**

Temporary workspace path. Relative paths resolve from the project root.

Type `string` · required in the base runtime file. Nonempty; if it exists it must be a directory. Risk `resource-sensitive`.

<a id="setting-resources.parquet.row_group_target_bytes"></a>

**`resources.parquet.row_group_target_bytes`**

Target allocation for one Parquet row group.

Type `bytes` · units bytes · required in the base runtime file. Positive and no greater than resources.parquet.file_target_bytes. Risk `resource-sensitive`.

<a id="setting-resources.parquet.file_target_bytes"></a>

**`resources.parquet.file_target_bytes`**

Target used when splitting output Parquet files.

Type `bytes` · units bytes · required in the base runtime file. Positive and at least resources.parquet.row_group_target_bytes. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.workbook_bytes"></a>

**`resources.normalization_limits.workbook_bytes`**

Selected XLSX source or archive member bytes.

Type `bytes` · units bytes · default `8388608`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.workbook_xml_bytes"></a>

**`resources.normalization_limits.workbook_xml_bytes`**

Expanded XML member and worksheet cell text bound.

Type `bytes` · units bytes · default `16777216`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.workbook_rows"></a>

**`resources.normalization_limits.workbook_rows`**

Materialized/decoded worksheet rows; also bounds shared strings at three times the row limit.

Type `integer` · default `25000`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.record_bytes"></a>

**`resources.normalization_limits.record_bytes`**

Maximum CSV payload, JSONL line, or XLSX cell text bytes.

Type `bytes` · units bytes · default `1048576`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.depth_levels"></a>

**`resources.normalization_limits.depth_levels`**

Bound on reconstructed book and pending snapshot levels.

Type `integer` · default `25000`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.metric_entries"></a>

**`resources.normalization_limits.metric_entries`**

Bounds retained metrics and inventories, including time buckets, files, row groups, and segments.

Type `integer` · default `4096`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.open_files"></a>

**`resources.normalization_limits.open_files`**

Participating normalization file handles, including session headroom.

Type `integer` · default `64`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-resources.normalization_limits.reconciliation_bytes"></a>

**`resources.normalization_limits.reconciliation_bytes`**

Resident routing and reconciliation buffers per execution unit. Disk spools use the scratch budget.

Type `bytes` · units bytes · default `67108864`. Positive and at most usize::MAX / 128 for this Engine build. Risk `resource-sensitive`.

<a id="setting-integrity_policy.enabled"></a>

**`integrity_policy.enabled`**

Enables integrity acceptance evaluation. Legacy v1 standard and strict strings both enable it. Use a boolean when editing.

Type `boolean` · default `true`. Risk `integrity-critical`.

<a id="setting-integrity_policy.parse_failure.action"></a>

**`integrity_policy.parse_failure.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.parse_failure.max_count"></a>

**`integrity_policy.parse_failure.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`.

<a id="setting-integrity_policy.parse_failure.max_rate"></a>

**`integrity_policy.parse_failure.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`.

<a id="setting-integrity_policy.parse_failure.minimum_samples"></a>

**`integrity_policy.parse_failure.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`, `integrity_policy.parse_failure.max_rate` must be configured.

<a id="setting-integrity_policy.parse_failure.windows.hourly.max_rate"></a>

**`integrity_policy.parse_failure.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`, `integrity_policy.parse_failure.windows.hourly` must be configured.

<a id="setting-integrity_policy.parse_failure.windows.hourly.minimum_samples"></a>

**`integrity_policy.parse_failure.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`, `integrity_policy.parse_failure.windows.hourly` must be configured.

<a id="setting-integrity_policy.parse_failure.windows.daily.max_rate"></a>

**`integrity_policy.parse_failure.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`, `integrity_policy.parse_failure.windows.daily` must be configured.

<a id="setting-integrity_policy.parse_failure.windows.daily.minimum_samples"></a>

**`integrity_policy.parse_failure.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.parse_failure.action` = `degrade`, `integrity_policy.parse_failure.windows.daily` must be configured.

<a id="setting-integrity_policy.invalid_record.action"></a>

**`integrity_policy.invalid_record.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.invalid_record.max_count"></a>

**`integrity_policy.invalid_record.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`.

<a id="setting-integrity_policy.invalid_record.max_rate"></a>

**`integrity_policy.invalid_record.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`.

<a id="setting-integrity_policy.invalid_record.minimum_samples"></a>

**`integrity_policy.invalid_record.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`, `integrity_policy.invalid_record.max_rate` must be configured.

<a id="setting-integrity_policy.invalid_record.windows.hourly.max_rate"></a>

**`integrity_policy.invalid_record.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`, `integrity_policy.invalid_record.windows.hourly` must be configured.

<a id="setting-integrity_policy.invalid_record.windows.hourly.minimum_samples"></a>

**`integrity_policy.invalid_record.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`, `integrity_policy.invalid_record.windows.hourly` must be configured.

<a id="setting-integrity_policy.invalid_record.windows.daily.max_rate"></a>

**`integrity_policy.invalid_record.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`, `integrity_policy.invalid_record.windows.daily` must be configured.

<a id="setting-integrity_policy.invalid_record.windows.daily.minimum_samples"></a>

**`integrity_policy.invalid_record.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_record.action` = `degrade`, `integrity_policy.invalid_record.windows.daily` must be configured.

<a id="setting-integrity_policy.sequence_gap.action"></a>

**`integrity_policy.sequence_gap.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.sequence_gap.max_count"></a>

**`integrity_policy.sequence_gap.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`.

<a id="setting-integrity_policy.sequence_gap.max_rate"></a>

**`integrity_policy.sequence_gap.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`.

<a id="setting-integrity_policy.sequence_gap.minimum_samples"></a>

**`integrity_policy.sequence_gap.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`, `integrity_policy.sequence_gap.max_rate` must be configured.

<a id="setting-integrity_policy.sequence_gap.windows.hourly.max_rate"></a>

**`integrity_policy.sequence_gap.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`, `integrity_policy.sequence_gap.windows.hourly` must be configured.

<a id="setting-integrity_policy.sequence_gap.windows.hourly.minimum_samples"></a>

**`integrity_policy.sequence_gap.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`, `integrity_policy.sequence_gap.windows.hourly` must be configured.

<a id="setting-integrity_policy.sequence_gap.windows.daily.max_rate"></a>

**`integrity_policy.sequence_gap.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`, `integrity_policy.sequence_gap.windows.daily` must be configured.

<a id="setting-integrity_policy.sequence_gap.windows.daily.minimum_samples"></a>

**`integrity_policy.sequence_gap.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.sequence_gap.action` = `degrade`, `integrity_policy.sequence_gap.windows.daily` must be configured.

<a id="setting-integrity_policy.timestamp_regression.action"></a>

**`integrity_policy.timestamp_regression.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.timestamp_regression.max_count"></a>

**`integrity_policy.timestamp_regression.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`.

<a id="setting-integrity_policy.timestamp_regression.max_rate"></a>

**`integrity_policy.timestamp_regression.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`.

<a id="setting-integrity_policy.timestamp_regression.minimum_samples"></a>

**`integrity_policy.timestamp_regression.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`, `integrity_policy.timestamp_regression.max_rate` must be configured.

<a id="setting-integrity_policy.timestamp_regression.windows.hourly.max_rate"></a>

**`integrity_policy.timestamp_regression.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`, `integrity_policy.timestamp_regression.windows.hourly` must be configured.

<a id="setting-integrity_policy.timestamp_regression.windows.hourly.minimum_samples"></a>

**`integrity_policy.timestamp_regression.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`, `integrity_policy.timestamp_regression.windows.hourly` must be configured.

<a id="setting-integrity_policy.timestamp_regression.windows.daily.max_rate"></a>

**`integrity_policy.timestamp_regression.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`, `integrity_policy.timestamp_regression.windows.daily` must be configured.

<a id="setting-integrity_policy.timestamp_regression.windows.daily.minimum_samples"></a>

**`integrity_policy.timestamp_regression.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.timestamp_regression.action` = `degrade`, `integrity_policy.timestamp_regression.windows.daily` must be configured.

<a id="setting-integrity_policy.missing_snapshot.action"></a>

**`integrity_policy.missing_snapshot.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.missing_snapshot.max_count"></a>

**`integrity_policy.missing_snapshot.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`.

<a id="setting-integrity_policy.missing_snapshot.max_rate"></a>

**`integrity_policy.missing_snapshot.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`.

<a id="setting-integrity_policy.missing_snapshot.minimum_samples"></a>

**`integrity_policy.missing_snapshot.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`, `integrity_policy.missing_snapshot.max_rate` must be configured.

<a id="setting-integrity_policy.missing_snapshot.windows.hourly.max_rate"></a>

**`integrity_policy.missing_snapshot.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`, `integrity_policy.missing_snapshot.windows.hourly` must be configured.

<a id="setting-integrity_policy.missing_snapshot.windows.hourly.minimum_samples"></a>

**`integrity_policy.missing_snapshot.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`, `integrity_policy.missing_snapshot.windows.hourly` must be configured.

<a id="setting-integrity_policy.missing_snapshot.windows.daily.max_rate"></a>

**`integrity_policy.missing_snapshot.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`, `integrity_policy.missing_snapshot.windows.daily` must be configured.

<a id="setting-integrity_policy.missing_snapshot.windows.daily.minimum_samples"></a>

**`integrity_policy.missing_snapshot.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.missing_snapshot.action` = `degrade`, `integrity_policy.missing_snapshot.windows.daily` must be configured.

<a id="setting-integrity_policy.invalid_book.action"></a>

**`integrity_policy.invalid_book.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.invalid_book.max_count"></a>

**`integrity_policy.invalid_book.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`.

<a id="setting-integrity_policy.invalid_book.max_rate"></a>

**`integrity_policy.invalid_book.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`.

<a id="setting-integrity_policy.invalid_book.minimum_samples"></a>

**`integrity_policy.invalid_book.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`, `integrity_policy.invalid_book.max_rate` must be configured.

<a id="setting-integrity_policy.invalid_book.windows.hourly.max_rate"></a>

**`integrity_policy.invalid_book.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`, `integrity_policy.invalid_book.windows.hourly` must be configured.

<a id="setting-integrity_policy.invalid_book.windows.hourly.minimum_samples"></a>

**`integrity_policy.invalid_book.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`, `integrity_policy.invalid_book.windows.hourly` must be configured.

<a id="setting-integrity_policy.invalid_book.windows.daily.max_rate"></a>

**`integrity_policy.invalid_book.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`, `integrity_policy.invalid_book.windows.daily` must be configured.

<a id="setting-integrity_policy.invalid_book.windows.daily.minimum_samples"></a>

**`integrity_policy.invalid_book.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.invalid_book.action` = `degrade`, `integrity_policy.invalid_book.windows.daily` must be configured.

<a id="setting-integrity_policy.transformation_failure.action"></a>

**`integrity_policy.transformation_failure.action`**

Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.

Type `enum` · required in the base runtime file · choices `degrade`, `fail`. Degrade requires a count, rate, hourly, or daily threshold. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`.

<a id="setting-integrity_policy.transformation_failure.max_count"></a>

**`integrity_policy.transformation_failure.max_count`**

Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.

Type `integer` · optional, with no default value reported. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`.

<a id="setting-integrity_policy.transformation_failure.max_rate"></a>

**`integrity_policy.transformation_failure.max_rate`**

Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`.

<a id="setting-integrity_policy.transformation_failure.minimum_samples"></a>

**`integrity_policy.transformation_failure.minimum_samples`**

Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`, `integrity_policy.transformation_failure.max_rate` must be configured.

<a id="setting-integrity_policy.transformation_failure.windows.hourly.max_rate"></a>

**`integrity_policy.transformation_failure.windows.hourly.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`, `integrity_policy.transformation_failure.windows.hourly` must be configured.

<a id="setting-integrity_policy.transformation_failure.windows.hourly.minimum_samples"></a>

**`integrity_policy.transformation_failure.windows.hourly.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`, `integrity_policy.transformation_failure.windows.hourly` must be configured.

<a id="setting-integrity_policy.transformation_failure.windows.daily.max_rate"></a>

**`integrity_policy.transformation_failure.windows.daily.max_rate`**

Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.

Type `rate` · required in the base runtime file. Finite fraction between 0 and 1 inclusive; required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`, `integrity_policy.transformation_failure.windows.daily` must be configured.

<a id="setting-integrity_policy.transformation_failure.windows.daily.minimum_samples"></a>

**`integrity_policy.transformation_failure.windows.daily.minimum_samples`**

Eligible sample floor for the corresponding UTC rate window.

Type `integer` · required in the base runtime file. Required when this window is present. Risk `integrity-critical`. Active when `integrity_policy.enabled` = `true`, `integrity_policy.transformation_failure.action` = `degrade`, `integrity_policy.transformation_failure.windows.daily` must be configured.

<a id="setting-reconciliation.trades.overlap_policy"></a>

**`reconciliation.trades.overlap_policy`**

Controls overlap handling. Strict checks agreement; prefer_source selects precedence; depth snapshot_recovery uses verified recovery boundaries.

Type `enum` · default `"strict"` · choices `strict`, `prefer_source`. Risk `integrity-critical`.

<a id="setting-reconciliation.trades.prefer_source.selection"></a>

**`reconciliation.trades.prefer_source.selection`**

Selects the earlier or later source for overlap precedence.

Type `enum` · default `"earlier"` · choices `earlier`, `later`. Risk `processing-semantic`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`.

<a id="setting-reconciliation.trades.prefer_source.on_unique_data_loss"></a>

**`reconciliation.trades.prefer_source.on_unique_data_loss`**

Reject unique loss or mark bounded loss degraded.

Type `enum` · default `"fail"` · choices `fail`, `mark_degraded`. mark_degraded requires a positive max_excluded_events even when preferences are inactive. Risk `integrity-critical`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`.

<a id="setting-reconciliation.trades.prefer_source.max_excluded_events"></a>

**`reconciliation.trades.prefer_source.max_excluded_events`**

Finite cap on excluded unique events. Zero provides no allowance.

Type `integer` · default `0`. Positive when on_unique_data_loss is mark_degraded. Risk `integrity-critical`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`, `reconciliation.trades.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-reconciliation.trades.prefer_source.max_excluded_rate"></a>

**`reconciliation.trades.prefer_source.max_excluded_rate`**

Optional additional rate cap on excluded unique events.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive, representable as a Rust Decimal. Risk `integrity-critical`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`, `reconciliation.trades.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-reconciliation.trades.prefer_source.minimum_samples"></a>

**`reconciliation.trades.prefer_source.minimum_samples`**

Eligible sample floor for the overlap exclusion rate. The event cap still applies.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`, `reconciliation.trades.prefer_source.on_unique_data_loss` = `mark_degraded`, `reconciliation.trades.prefer_source.max_excluded_rate` must be configured.

<a id="setting-reconciliation.trades.prefer_source.max_excluded_interval_ms"></a>

**`reconciliation.trades.prefer_source.max_excluded_interval_ms`**

Optional maximum time span of exclusions.

Type `milliseconds` · units milliseconds · optional, with no default value reported. Must fit in signed nanoseconds after multiplication by 1,000,000. Risk `integrity-critical`. Active when `reconciliation.trades.overlap_policy` = `prefer_source`, `reconciliation.trades.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-reconciliation.depth.overlap_policy"></a>

**`reconciliation.depth.overlap_policy`**

Controls overlap handling. Strict checks agreement; prefer_source selects precedence; depth snapshot_recovery uses verified recovery boundaries.

Type `enum` · default `"strict"` · choices `strict`, `snapshot_recovery`, `prefer_source`. Risk `integrity-critical`.

<a id="setting-reconciliation.depth.prefer_source.selection"></a>

**`reconciliation.depth.prefer_source.selection`**

Selects the earlier or later source for overlap precedence.

Type `enum` · default `"earlier"` · choices `earlier`, `later`. Risk `processing-semantic`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`.

<a id="setting-reconciliation.depth.prefer_source.switch_boundary"></a>

**`reconciliation.depth.prefer_source.switch_boundary`**

Chooses snapshot or verified source boundary for depth source switches.

Type `enum` · default `"snapshot"` · choices `snapshot`, `source_boundary`. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`.

<a id="setting-reconciliation.depth.prefer_source.on_unique_data_loss"></a>

**`reconciliation.depth.prefer_source.on_unique_data_loss`**

Reject unique loss or mark bounded loss degraded.

Type `enum` · default `"fail"` · choices `fail`, `mark_degraded`. mark_degraded requires a positive max_excluded_events even when preferences are inactive. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`.

<a id="setting-reconciliation.depth.prefer_source.max_excluded_events"></a>

**`reconciliation.depth.prefer_source.max_excluded_events`**

Finite cap on excluded unique events. Zero provides no allowance.

Type `integer` · default `0`. Positive when on_unique_data_loss is mark_degraded. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`, `reconciliation.depth.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-reconciliation.depth.prefer_source.max_excluded_rate"></a>

**`reconciliation.depth.prefer_source.max_excluded_rate`**

Optional additional rate cap on excluded unique events.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive, representable as a Rust Decimal. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`, `reconciliation.depth.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-reconciliation.depth.prefer_source.minimum_samples"></a>

**`reconciliation.depth.prefer_source.minimum_samples`**

Eligible sample floor for the overlap exclusion rate. The event cap still applies.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`, `reconciliation.depth.prefer_source.on_unique_data_loss` = `mark_degraded`, `reconciliation.depth.prefer_source.max_excluded_rate` must be configured.

<a id="setting-reconciliation.depth.prefer_source.max_excluded_interval_ms"></a>

**`reconciliation.depth.prefer_source.max_excluded_interval_ms`**

Optional maximum time span of exclusions.

Type `milliseconds` · units milliseconds · optional, with no default value reported. Must fit in signed nanoseconds after multiplication by 1,000,000. Risk `integrity-critical`. Active when `reconciliation.depth.overlap_policy` = `prefer_source`, `reconciliation.depth.prefer_source.on_unique_data_loss` = `mark_degraded`.

<a id="setting-timestamp_regression.trades.policy"></a>

**`timestamp_regression.trades.policy`**

Strict rejects regressions, preserve retains native order, reorder uses a bounded window, and discard drops bounded events.

Type `enum` · default `"strict"` · choices `strict`, `preserve`, `reorder`, `discard`. Reorder and discard require explicit positive bounds. Risk `integrity-critical`.

<a id="setting-timestamp_regression.trades.max_regression_ms"></a>

**`timestamp_regression.trades.max_regression_ms`**

Largest accepted trade regression under a non-strict policy.

Type `milliseconds` · units milliseconds · default `0`. Must fit signed nanoseconds; positive for reorder or discard. Risk `integrity-critical`. Active when `timestamp_regression.trades.policy` = `preserve` or `reorder` or `discard`.

<a id="setting-timestamp_regression.trades.max_reorder_window_ms"></a>

**`timestamp_regression.trades.max_reorder_window_ms`**

High-water window for trade reordering.

Type `milliseconds` · units milliseconds · default `0`. Must fit signed nanoseconds; positive and at least max_regression_ms for reorder. Risk `integrity-critical`. Active when `timestamp_regression.trades.policy` = `reorder`.

<a id="setting-timestamp_regression.trades.max_discarded_events"></a>

**`timestamp_regression.trades.max_discarded_events`**

Maximum discarded trade events.

Type `integer` · default `0`. Positive for discard. Risk `integrity-critical`. Active when `timestamp_regression.trades.policy` = `discard`.

<a id="setting-timestamp_regression.trades.max_discarded_rate"></a>

**`timestamp_regression.trades.max_discarded_rate`**

Additional rate cap on discarded trade events.

Type `rate` · optional, with no default value reported. Finite fraction between 0 and 1 inclusive, representable as a Rust Decimal. Risk `integrity-critical`. Active when `timestamp_regression.trades.policy` = `discard`.

<a id="setting-timestamp_regression.trades.minimum_samples"></a>

**`timestamp_regression.trades.minimum_samples`**

Eligible sample floor for the trade discard rate. The event cap still applies.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `timestamp_regression.trades.policy` = `discard`, `timestamp_regression.trades.max_discarded_rate` must be configured.

<a id="setting-timestamp_regression.depth.policy"></a>

**`timestamp_regression.depth.policy`**

Strict rejects regressions, preserve retains verified native sequence, and snapshot_recovery resumes at a verified snapshot.

Type `enum` · default `"strict"` · choices `strict`, `preserve`, `snapshot_recovery`. Risk `integrity-critical`.

<a id="setting-timestamp_regression.depth.max_regression_ms"></a>

**`timestamp_regression.depth.max_regression_ms`**

High-water regression tolerance under depth preserve or timestamp snapshot recovery.

Type `milliseconds` · units milliseconds · default `0`. Must fit in signed nanoseconds after multiplication by 1,000,000. Risk `integrity-critical`. Active when `timestamp_regression.depth.policy` = `preserve` or `snapshot_recovery`.

<a id="setting-timestamp_regression.depth.require_sequence_continuity"></a>

**`timestamp_regression.depth.require_sequence_continuity`**

Keep applicable depth sequence validation enabled. Unsequenced feeds do not require a synthetic sequence.

Type `boolean` · default `true`. Must be true. Risk `integrity-critical`.

<a id="setting-timestamp_regression.depth.require_recovery_snapshot"></a>

**`timestamp_regression.depth.require_recovery_snapshot`**

Keep verified snapshot initialization enabled for depth recovery.

Type `boolean` · default `true`. Must be true. Risk `integrity-critical`.

<a id="setting-timestamp_regression.depth.max_discarded_events"></a>

**`timestamp_regression.depth.max_discarded_events`**

Finite allowance for observations excluded during depth timestamp snapshot recovery.

Type `integer` · default `0`. Risk `integrity-critical`. Active when `timestamp_regression.depth.policy` = `snapshot_recovery`.
