# Engine runtime settings and policies

Use the [normalization runtime reference](../configuration/normalization-runtime.md) for the normalization TOML, Rust defaults and policy semantics. This page retains the shared entry point for [merge and conversion settings](#merge-and-conversion-runtime) and [pipeline inheritance](#pipeline-runtime).

## Normalization runtime

<!-- Previous normalization anchors lead to the corresponding sections in the dedicated reference. -->

<a id="runtime-schema"></a><a id="runtime-schema-version"></a>
<a id="why-does-the-runtime-have-its-own-version"></a><a id="are-unknown-fields-allowed"></a>
<a id="why-is-strict-validation-useful"></a><a id="main-runtime-sections"></a>
<a id="important-default-behavior"></a><a id="complete-strict-runtime-example"></a>
<a id="what-does-this-configuration-establish"></a><a id="important-notes"></a>
<a id="does-strict-mean-that-every-source-is-historically-complete"></a><a id="does-this-configuration-enable-every-engine-feature"></a>

- [Schema and validation](../configuration/normalization-runtime.md#runtime-configuration-overview)

<a id="resource-management"></a><a id="why-resource-budgets-matter"></a>
<a id="worker-count"></a><a id="what-does-it-control"></a>
<a id="does-more-workers-always-mean-faster-processing"></a><a id="what-happens-if-workers-is-zero"></a>
<a id="how-should-i-choose-the-value"></a><a id="memory-budget"></a>
<a id="what-does-the-budget-limit"></a><a id="what-is-included-in-memory-accounting"></a>
<a id="what-is-memory-admission"></a><a id="does-memory-admission-measure-process-rss"></a>
<a id="does-successful-admission-guarantee-no-out-of-memory-condition"></a><a id="why-not-set-an-extremely-large-budget"></a>
<a id="scratch-storage"></a><a id="scratch-path"></a>
<a id="scratch-budget"></a><a id="why-use-scratch-storage"></a>
<a id="is-scratch-the-same-as-published-output"></a><a id="can-i-use-another-disk"></a>
<a id="what-if-the-path-already-exists-as-a-file"></a><a id="does-the-loader-create-the-directory-automatically"></a>
<a id="can-scratch-exhaustion-cause-processing-failure"></a><a id="parquet-row-group-target"></a>
<a id="what-is-a-parquet-row-group"></a><a id="why-does-row-group-size-matter"></a>
<a id="does-a-row-group-represent-one-historical-interval"></a><a id="does-changing-row-group-size-change-market-events"></a>
<a id="parquet-file-target"></a><a id="why-configure-file-size"></a>
<a id="is-the-target-an-exact-file-size-guarantee"></a><a id="why-does-file-rotation-matter"></a>
<a id="does-the-target-change-the-publication-identity"></a><a id="resource-validation"></a>
<a id="merge-and-conversion-alignment"></a><a id="why-require-whole-mib-values"></a>
<a id="example"></a><a id="does-every-engine-operation-use-identical-resource-validation"></a>

- [Resource management and Parquet](../configuration/normalization-runtime.md#2-resource-management)

<a id="processing-policies"></a>

- [Processing and performance](../configuration/normalization-runtime.md#1-processing-and-performance)

<a id="normalization-safety-limits"></a><a id="why-are-safety-limits-separate-from-budgets"></a>
<a id="why-are-they-useful"></a><a id="default-normalization-limits"></a>
<a id="configure-explicit-limits"></a><a id="are-these-fields-required"></a>
<a id="can-i-set-a-limit-to-zero"></a><a id="record-size-limit"></a>
<a id="what-does-it-protect"></a><a id="why-might-a-record-be-large"></a>
<a id="what-happens-when-the-limit-is-exceeded"></a><a id="does-increasing-the-limit-make-the-record-valid"></a>
<a id="depth-level-limit"></a><a id="what-does-it-control_1"></a>
<a id="why-is-this-useful"></a><a id="does-this-setting-truncate-the-output"></a>
<a id="can-i-increase-it"></a><a id="workbook-limits"></a>
<a id="why-are-xlsx-limits-necessary"></a><a id="what-do-the-limits-control"></a>
<a id="does-increasing-these-limits-enable-arbitrary-xlsx-formats"></a><a id="detailed-metrics-and-reconciliation-limits"></a>
<a id="metric-entries"></a><a id="reconciliation-bytes"></a>
<a id="why-bound-diagnostic-information"></a><a id="does-a-small-metric-limit-suppress-every-integrity-violation"></a>

- [Normalization safety limits](../configuration/normalization-runtime.md#3-normalization-safety-limits)

<a id="diagnostic-collection"></a><a id="what-are-runtime-diagnostics"></a>
<a id="diagnostic-modes"></a><a id="does-detailed-mode-change-normalized-data"></a>
<a id="does-disabled-mean-no-errors-are-reported"></a><a id="diagnostic-sampling-interval"></a>
<a id="accepted-range"></a><a id="why-configure-an-interval"></a>
<a id="does-a-smaller-interval-always-improve-debugging"></a>

- [Diagnostics](../configuration/normalization-runtime.md#5-diagnostics)

<a id="integrity-evaluation"></a><a id="why-does-marketforge-need-integrity-policies"></a>
<a id="what-does-an-integrity-policy-decide"></a><a id="what-does-it-not-decide"></a>
<a id="global-integrity-switch"></a><a id="what-happens-when-enabled"></a>
<a id="what-if-disabled"></a><a id="important-limitation"></a>
<a id="does-disabled-mean-the-dataset-is-clean"></a><a id="should-i-disable-it-to-make-a-failed-job-run"></a>
<a id="integrity-categories"></a><a id="why-distinguish-categories"></a>
<a id="parse-failures"></a><a id="why-can-this-happen"></a>
<a id="does-accepting-a-parse-failure-recover-the-record"></a><a id="what-should-i-investigate"></a>
<a id="invalid-records"></a><a id="why-is-this-different-from-a-parse-failure"></a>
<a id="can-invalid-records-be-counted"></a><a id="does-an-invalid-record-always-require-immediate-failure"></a>
<a id="sequence-gaps"></a><a id="why-is-this-important-for-depth"></a>
<a id="does-every-source-require-consecutive-sequences"></a><a id="can-integrity-configuration-invent-sequence-continuity"></a>
<a id="timestamp-regressions"></a><a id="why-is-this-important"></a>
<a id="are-equal-timestamps-regressions"></a><a id="can-timestamp-policy-change-the-outcome"></a>
<a id="missing-snapshots"></a><a id="what-happens-without-the-snapshot"></a>
<a id="can-integrity-policy-ignore-initialization-requirements"></a><a id="why"></a>
<a id="invalid-book-state"></a><a id="why-is-this-different-from-missing-snapshots"></a>
<a id="can-permissive-loss-settings-allow-an-invalid-book-to-be-published"></a><a id="transformation-failures"></a>
<a id="possible-causes"></a><a id="why-investigate-the-profile"></a>
<a id="integrity-rules-and-thresholds"></a><a id="integrity-actions"></a>
<a id="fail"></a><a id="degrade"></a>
<a id="important-fail-does-not-become-permissive-with-a-count"></a><a id="when-do-thresholds-matter"></a>
<a id="integrity-rule-parameters"></a><a id="example-rule"></a>
<a id="why-have-multiple-limits"></a><a id="does-every-rule-need-all-parameters"></a>
<a id="maximum-count"></a><a id="example_1"></a>
<a id="is-the-threshold-inclusive"></a><a id="does-max-count-apply-to-arbitrary-event-loss"></a>
<a id="maximum-rate"></a><a id="examples"></a>
<a id="why-use-a-rate"></a><a id="are-rates-rounded-to-percentages-before-comparison"></a>
<a id="what-if-the-observed-rate-equals-the-limit"></a><a id="minimum-samples"></a>
<a id="why-is-this-useful_1"></a><a id="does-minimum-samples-disable-absolute-limits"></a>
<a id="example_2"></a><a id="hourly-and-daily-integrity-windows"></a>
<a id="why-use-time-windows"></a><a id="hourly-rule"></a>
<a id="daily-rule"></a><a id="what-if-a-record-has-no-trustworthy-timestamp"></a>
<a id="why-not-assign-an-arbitrary-time"></a><a id="immediate-vs-final-integrity-evaluation"></a>
<a id="during-source-processing"></a><a id="during-final-evaluation"></a>
<a id="why-separate-these-stages"></a><a id="does-delayed-rate-evaluation-mean-violations-are-ignored"></a>

- [Integrity evaluation](../configuration/normalization-runtime.md#6-integrity-policies)

<a id="source-reconciliation"></a><a id="why-do-historical-archives-overlap"></a>
<a id="why-can-this-happen_1"></a><a id="why-is-reconciliation-necessary"></a>
<a id="default-reconciliation-behavior"></a><a id="why-separate-trades-and-depth"></a>
<a id="does-adding-reconciliation-enable-permissive-behavior"></a><a id="trade-reconciliation-policies"></a>
<a id="strict"></a><a id="prefer-source"></a>
<a id="example_3"></a><a id="why-not"></a>
<a id="reliable-trade-identity"></a><a id="why-is-identity-scope-important"></a>
<a id="can-equal-timestamps-prove-duplicates"></a><a id="what-proves-equivalence"></a>
<a id="what-if-the-contents-conflict"></a><a id="source-local-and-ambiguous-ids"></a>
<a id="why-not-deduplicate-them-automatically"></a><a id="does-a-large-numeric-id-imply-global-uniqueness"></a>
<a id="what-about-nonconsecutive-ids"></a><a id="what-if-the-native-reset-domain-is-ambiguous"></a>
<a id="why-is-this-necessary"></a><a id="verified-duplicate-suppression"></a>
<a id="example_4"></a><a id="why-does-this-matter-for-completeness"></a>
<a id="how-is-it-reported"></a><a id="source-preference"></a>
<a id="how-source-preference-works"></a><a id="example_5"></a>
<a id="what-does-earlier-mean"></a><a id="what-does-later-mean"></a>
<a id="does-filename-order-determine-priority"></a><a id="what-if-starts-are-equal"></a>
<a id="source-preference-is-not-task-priority"></a><a id="why-distinguish-them"></a>
<a id="what-determines-ownership"></a><a id="does-processing-concurrency-choose-the-winner"></a>
<a id="what-about-complex-overlaps"></a><a id="trade-union-and-conflict-ownership"></a>
<a id="what-if-one-source-contains-unknown-identities"></a><a id="what-happens-to-excluded-observations"></a>
<a id="why-is-this-important_1"></a>

- [Trade reconciliation](../configuration/normalization-runtime.md#7-trade-reconciliation)

<a id="explicit-event-loss-authorization"></a><a id="why-event-loss-must-be-explicit"></a>
<a id="default-behavior"></a><a id="meaning"></a>
<a id="why-is-this-strict"></a><a id="permit-bounded-degraded-loss"></a>
<a id="example_6"></a><a id="what-does-this-authorize"></a>
<a id="does-it-guarantee-that-five-events-will-be-excluded"></a><a id="does-it-permit-unlimited-exclusion-below-the-rate-threshold"></a>
<a id="does-the-resulting-publication-remain-clean"></a><a id="source-preference-loss-parameters"></a>
<a id="required-permission"></a><a id="why-require-an-explicit-positive-count"></a>
<a id="what-if-max_excluded_rate-is-omitted"></a><a id="what-if-minimum-samples-is-large"></a>
<a id="what-about-contiguous-intervals"></a><a id="why-loss-accounting-uses-logical-events"></a>
<a id="why-distinguish-source-observations-from-rows"></a><a id="what-does-marketforge-count"></a>
<a id="what-about-parse-failures"></a>

- [Explicit event loss](../configuration/normalization-runtime.md#source-preference-and-loss-limits)

<a id="depth-reconciliation"></a><a id="why-depth-reconciliation-is-different"></a>
<a id="why-is-overlapping-depth-difficult"></a><a id="what-could-go-wrong"></a>
<a id="why-not-merge-individual-level-changes"></a><a id="depth-overlap-policies"></a>
<a id="strict_1"></a><a id="snapshot-recovery"></a>
<a id="prefer-source_1"></a><a id="example_7"></a>
<a id="important-distinction"></a><a id="depth-snapshot-recovery"></a>
<a id="what-does-snapshot-recovery-do"></a><a id="does-this-prove-one-continuous-history"></a>
<a id="does-snapshot-recovery-discard-unique-observations"></a><a id="does-it-repair-missing-exchange-events"></a>
<a id="why-is-this-useful_2"></a><a id="depth-source-preference"></a>
<a id="why-is-switch-boundary-necessary"></a><a id="default-switch-boundary"></a>
<a id="alternative"></a><a id="snapshot-switch-boundaries"></a>
<a id="why-use-snapshots"></a><a id="does-any-snapshot-qualify"></a>
<a id="what-if-no-suitable-snapshot-exists"></a><a id="source-boundary-switching"></a>
<a id="what-must-be-established"></a><a id="why-is-this-necessary_1"></a>
<a id="what-about-unsequenced-feeds"></a><a id="can-source-a-be-resumed-after-switching-to-source-b"></a>
<a id="reconstruction-validation"></a><a id="source-reconstruction"></a>
<a id="selected-history"></a><a id="validation"></a>
<a id="why-use-separate-reconstruction"></a><a id="what-happens-when-reconstruction-fails"></a>
<a id="does-a-generous-loss-allowance-override-book-validity"></a><a id="boundary-snapshots-and-duplicate-evidence"></a>
<a id="what-does-marketforge-compare"></a><a id="does-json-property-order-matter"></a>
<a id="what-about-deltas-sharing-snapshot-ids"></a><a id="can-a-terminal-snapshot-be-represented-by-the-next-initialization"></a>

- [Order-book reconciliation](../configuration/normalization-runtime.md#8-order-book-reconciliation)

<a id="timestamp-regression-policies"></a><a id="why-have-separate-timestamp-policies"></a>
<a id="trades"></a><a id="depth"></a>
<a id="why-separate-the-policies"></a><a id="timestamp-configuration-structure"></a>
<a id="trade-policies"></a><a id="depth-policies"></a>
<a id="why-are-the-lists-different"></a><a id="what-counts-as-a-regression"></a>
<a id="example_8"></a><a id="why-is-this-important_2"></a>
<a id="equal-timestamps"></a><a id="source-domains"></a>
<a id="snapshot-resets"></a><a id="timestamp-precision"></a>
<a id="trade-timestamp-handling"></a><a id="strict-trade-timestamp-policy"></a>
<a id="what-does-strict-mean"></a><a id="does-setting-a-positive-bound-make-strict-permissive"></a>
<a id="preserve-trade-source-order"></a><a id="what-does-preserve-do"></a>
<a id="does-it-sort-trades-chronologically"></a><a id="does-it-change-timestamps"></a>
<a id="does-the-regression-bound-still-matter"></a><a id="why-use-preserve"></a>
<a id="reorder-independent-trades"></a><a id="what-does-it-do"></a>
<a id="does-it-modify-event-timestamps"></a><a id="how-are-equal-time-trades-ordered"></a>
<a id="does-it-use-floating-point-trade-ids"></a><a id="is-reordering-in-memory-unbounded"></a>
<a id="what-happens-if-scratch-is-insufficient"></a><a id="reorder-bounds"></a>
<a id="why-require-both"></a><a id="example_9"></a>
<a id="does-a-large-window-guarantee-success"></a><a id="discard-regressing-trades"></a>
<a id="why-is-this-a-lossy-policy"></a><a id="what-is-required"></a>
<a id="does-a-rate-alone-authorize-discards"></a><a id="does-discard-remove-arbitrary-bad-records"></a>
<a id="what-about-verified-duplicates"></a><a id="does-discarding-affect-completeness"></a>
<a id="trade-timestamp-parameter-reference"></a><a id="important-rules"></a>
<a id="depth-timestamp-handling"></a><a id="strict-depth-timestamp-policy"></a>
<a id="why-is-strict-the-default"></a><a id="preserve-depth-native-order"></a>
<a id="what-does-preserve-do_1"></a><a id="what-else-remains-required"></a>
<a id="does-preserving-source-order-guarantee-continuous-market-history"></a><a id="what-about-snapshot-only-feeds"></a>
<a id="depth-snapshot-recovery_1"></a><a id="why-is-recovery-necessary"></a>
<a id="example_10"></a><a id="what-happens-to-the-affected-deltas"></a>
<a id="does-snapshot-b-restore-the-missing-events"></a><a id="what-happens-to-omitted-observations"></a>
<a id="recovery-without-event-loss"></a><a id="why-does-this-matter"></a>
<a id="does-the-snapshot-prove-all-earlier-data-was-complete"></a><a id="recovery-requiring-event-loss"></a>
<a id="what-is-required_1"></a><a id="what-if-no-subsequent-snapshot-exists"></a>
<a id="why-not-continue-applying-updates"></a><a id="mandatory-depth-safeguards"></a>
<a id="can-i-disable-sequence-continuity"></a><a id="can-i-disable-recovery-snapshots"></a>
<a id="why_1"></a><a id="does-this-mean-every-source-must-have-a-sequence-field"></a>
<a id="depth-timestamp-parameter-reference"></a><a id="important-rules_1"></a>

- [Timestamp regression](../configuration/normalization-runtime.md#9-timestamp-regression)

<a id="loss-accounting-and-completeness"></a><a id="why-event-accounting-matters"></a>
<a id="why-distinguish-events-and-rows"></a><a id="loss-denominator"></a>
<a id="what-does-it-exclude"></a><a id="why-does-this-matter_1"></a>
<a id="does-verified-deduplication-degrade-completeness"></a><a id="what-does-degrade-completeness"></a>
<a id="job-wide-vs-stream-wide-limits"></a><a id="example_11"></a>
<a id="why_2"></a><a id="are-stream-metrics-still-available"></a>
<a id="contiguous-loss-intervals"></a><a id="example_12"></a>
<a id="why-track-spans"></a><a id="do-different-loss-reasons-start-separate-spans"></a>
<a id="why_3"></a><a id="what-does-the-interval-limit-control"></a>

- [Loss accounting](../configuration/normalization-runtime.md#source-preference-and-loss-limits)

<a id="runtime-evidence-in-publications"></a>

- [Runtime evidence](../configuration/normalization-runtime.md#configuration-file-and-resolution)

<a id="scratch-representation"></a><a id="why-does-marketforge-have-scratch-formats"></a>
<a id="why-use-separate-scratch-formats"></a><a id="scratch-format"></a>
<a id="why-use-typed-representations"></a><a id="does-typed_events-change-canonical-publication-schemas"></a>
<a id="does-every-scratch-format-support-every-source"></a><a id="scratch-codec"></a>
<a id="zlib"></a><a id="uncompressed"></a>
<a id="why-provide-both"></a><a id="does-changing-the-codec-change-normalized-values"></a>

- [Scratch representation](../configuration/normalization-runtime.md#1-processing-and-performance)

<a id="merge-runtime-settings"></a><a id="merge-fan-in"></a>
<a id="normalization-merge-fan-in"></a><a id="what-does-fan-in-control"></a>
<a id="why-limit-fan-in"></a><a id="does-increasing-fan-in-always-improve-performance"></a>
<a id="is-merge-fan-in-the-same-as-stream-rank"></a>

- [Normalization merge fan-in](../configuration/normalization-runtime.md#merge-fan-in)

<a id="parquet-compression"></a><a id="does-a-higher-number-always-improve-compression"></a>
<a id="does-compression-change-market-data-meaning"></a><a id="is-this-the-same-as-scratch-codec"></a>

## Merge and conversion runtime

The [merge runtime reference](../configuration/merge-runtime.md) documents `merge.toml`, including the CLI, loader defaults, partitioning, and runtime evidence.

Conversion preparation reads `data/.config/engine/convert.toml`. Its schema-1 loader shares the I/O resource types and Parquet settings. The conversion document has no partitioning table.

| Conversion field | Loader requirements |
| --- | --- |
| `schema_version` | Required integer 1. |
| `resources.workers` | Required positive integer. |
| `resources.memory_limit_mb` | Required positive binary MiB, convertible to unsigned 64-bit bytes. |
| `resources.max_open_files` | Positive integer, default 64. |
| `parquet.row_group_target_mb` | Required positive binary MiB, no larger than the file target. |
| `parquet.file_target_mb` | Required binary MiB, at least the row-group target and convertible to unsigned 64-bit bytes. |
| `parquet.compression_level` | Zstandard level 1–22, default 3. |

Conversion clamps recipe resource requests to the runtime ceilings and transfers the resulting values and Parquet settings into the prepared job. The direct converter can lower the prepared worker count with `--workers`. See the [conversion workflow](../workflows/conversion.md) for input and execution requirements.

## Pipeline runtime

`data/.config/engine/pipeline.toml` has a required integer
`schema_version = 1`. It accepts optional `[normalize]` and `[merge]` tables
with the same nested fields as their operation documents, excluding the
operation schema version. For example:

```toml
schema_version = 1

[normalize.resources]
workers = 2

[merge.parquet]
compression_level = 5
```

Each operation TOML is independently validated, then each override is applied
recursively to its own stage. Unspecified fields inherit the operation file
and its Rust defaults. Rust validates the resulting TOML before processing.
Acquire uses Forge HTTP settings and the workflow request. The pipeline executes
acquire, normalize and merge.

For a new run, Forge retains the effective TOMLs, their checksums, the source
TOML hashes, compiled settings, profile source files and executable hashes.
Resume uses the retained files. A prior pipeline snapshot with a shared
`general.toml` remains readable through its retained report and executables.

```bash
mf engine runtime normalize
mf engine runtime merge
mf engine runtime convert
mf --json engine runtime pipeline
```

Runtime inspection requires the matching managed Engine builds and does not open
a database connection. The pipeline workflow recipe supplies the interval,
requests, ranking name and merge ordering. [Pipeline recipe](../configuration/pipeline.md)
