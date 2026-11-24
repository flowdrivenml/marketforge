# Pipeline runtime overrides

The workspace’s `data/.config/engine/pipeline.toml` can contain `[normalize]` and `[merge]` overrides. Each absent override inherits the corresponding base runtime setting. An explicitly set value is retained even if it currently equals the base value. The composition service validates the resulting stage runtime with its managed Engine. Conversion uses its separately resolved runtime and has no override table in this file.

```toml
schema_version = 1

[merge.resources]
workers = 4
```

Pipeline preparation retains composed configuration with the prepared work. Workspace edits affect subsequent preparation. To see what an earlier run used, inspect its retained [prepared jobs and evidence](../data-formats/prepared.md).

<a id="setting-schema_version"></a>

**`schema_version`** is a required, immutable integer with value `1`. It identifies the pipeline override format.

## Supported overrides

Every override entry below links to the base field’s purpose, type, defaults, constraints, units, dependencies and risk. In Pipeline Runtime, that field is always optional and inherits when absent.

### Normalize

<a id="setting-normalize.merge_fan_in"></a>

- [`normalize.merge_fan_in`](normalization-runtime.md#setting-merge_fan_in)

<a id="setting-normalize.scratch_format"></a>

- [`normalize.scratch_format`](normalization-runtime.md#setting-scratch_format)

<a id="setting-normalize.scratch_codec"></a>

- [`normalize.scratch_codec`](normalization-runtime.md#setting-scratch_codec)

<a id="setting-normalize.diagnostics.mode"></a>

- [`normalize.diagnostics.mode`](normalization-runtime.md#setting-diagnostics.mode)

<a id="setting-normalize.diagnostics.interval_ms"></a>

- [`normalize.diagnostics.interval_ms`](normalization-runtime.md#setting-diagnostics.interval_ms)

<a id="setting-normalize.resources.workers"></a>

- [`normalize.resources.workers`](normalization-runtime.md#setting-resources.workers)

<a id="setting-normalize.resources.memory_budget_bytes"></a>

- [`normalize.resources.memory_budget_bytes`](normalization-runtime.md#setting-resources.memory_budget_bytes)

<a id="setting-normalize.resources.scratch_budget_bytes"></a>

- [`normalize.resources.scratch_budget_bytes`](normalization-runtime.md#setting-resources.scratch_budget_bytes)

<a id="setting-normalize.resources.scratch_path"></a>

- [`normalize.resources.scratch_path`](normalization-runtime.md#setting-resources.scratch_path)

<a id="setting-normalize.resources.parquet.row_group_target_bytes"></a>

- [`normalize.resources.parquet.row_group_target_bytes`](normalization-runtime.md#setting-resources.parquet.row_group_target_bytes)

<a id="setting-normalize.resources.parquet.file_target_bytes"></a>

- [`normalize.resources.parquet.file_target_bytes`](normalization-runtime.md#setting-resources.parquet.file_target_bytes)

<a id="setting-normalize.resources.normalization_limits.workbook_bytes"></a>

- [`normalize.resources.normalization_limits.workbook_bytes`](normalization-runtime.md#setting-resources.normalization_limits.workbook_bytes)

<a id="setting-normalize.resources.normalization_limits.workbook_xml_bytes"></a>

- [`normalize.resources.normalization_limits.workbook_xml_bytes`](normalization-runtime.md#setting-resources.normalization_limits.workbook_xml_bytes)

<a id="setting-normalize.resources.normalization_limits.workbook_rows"></a>

- [`normalize.resources.normalization_limits.workbook_rows`](normalization-runtime.md#setting-resources.normalization_limits.workbook_rows)

<a id="setting-normalize.resources.normalization_limits.record_bytes"></a>

- [`normalize.resources.normalization_limits.record_bytes`](normalization-runtime.md#setting-resources.normalization_limits.record_bytes)

<a id="setting-normalize.resources.normalization_limits.depth_levels"></a>

- [`normalize.resources.normalization_limits.depth_levels`](normalization-runtime.md#setting-resources.normalization_limits.depth_levels)

<a id="setting-normalize.resources.normalization_limits.metric_entries"></a>

- [`normalize.resources.normalization_limits.metric_entries`](normalization-runtime.md#setting-resources.normalization_limits.metric_entries)

<a id="setting-normalize.resources.normalization_limits.open_files"></a>

- [`normalize.resources.normalization_limits.open_files`](normalization-runtime.md#setting-resources.normalization_limits.open_files)

<a id="setting-normalize.resources.normalization_limits.reconciliation_bytes"></a>

- [`normalize.resources.normalization_limits.reconciliation_bytes`](normalization-runtime.md#setting-resources.normalization_limits.reconciliation_bytes)

<a id="setting-normalize.integrity_policy.enabled"></a>

- [`normalize.integrity_policy.enabled`](normalization-runtime.md#setting-integrity_policy.enabled)

<a id="setting-normalize.integrity_policy.parse_failure.action"></a>

- [`normalize.integrity_policy.parse_failure.action`](normalization-runtime.md#setting-integrity_policy.parse_failure.action)

<a id="setting-normalize.integrity_policy.parse_failure.max_count"></a>

- [`normalize.integrity_policy.parse_failure.max_count`](normalization-runtime.md#setting-integrity_policy.parse_failure.max_count)

<a id="setting-normalize.integrity_policy.parse_failure.max_rate"></a>

- [`normalize.integrity_policy.parse_failure.max_rate`](normalization-runtime.md#setting-integrity_policy.parse_failure.max_rate)

<a id="setting-normalize.integrity_policy.parse_failure.minimum_samples"></a>

- [`normalize.integrity_policy.parse_failure.minimum_samples`](normalization-runtime.md#setting-integrity_policy.parse_failure.minimum_samples)

<a id="setting-normalize.integrity_policy.parse_failure.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.parse_failure.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.parse_failure.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.parse_failure.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.parse_failure.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.parse_failure.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.parse_failure.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.parse_failure.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.parse_failure.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.parse_failure.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.parse_failure.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.parse_failure.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_record.action"></a>

- [`normalize.integrity_policy.invalid_record.action`](normalization-runtime.md#setting-integrity_policy.invalid_record.action)

<a id="setting-normalize.integrity_policy.invalid_record.max_count"></a>

- [`normalize.integrity_policy.invalid_record.max_count`](normalization-runtime.md#setting-integrity_policy.invalid_record.max_count)

<a id="setting-normalize.integrity_policy.invalid_record.max_rate"></a>

- [`normalize.integrity_policy.invalid_record.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_record.max_rate)

<a id="setting-normalize.integrity_policy.invalid_record.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_record.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_record.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_record.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.invalid_record.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_record.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.invalid_record.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_record.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_record.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_record.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.invalid_record.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_record.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.invalid_record.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_record.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_record.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.sequence_gap.action"></a>

- [`normalize.integrity_policy.sequence_gap.action`](normalization-runtime.md#setting-integrity_policy.sequence_gap.action)

<a id="setting-normalize.integrity_policy.sequence_gap.max_count"></a>

- [`normalize.integrity_policy.sequence_gap.max_count`](normalization-runtime.md#setting-integrity_policy.sequence_gap.max_count)

<a id="setting-normalize.integrity_policy.sequence_gap.max_rate"></a>

- [`normalize.integrity_policy.sequence_gap.max_rate`](normalization-runtime.md#setting-integrity_policy.sequence_gap.max_rate)

<a id="setting-normalize.integrity_policy.sequence_gap.minimum_samples"></a>

- [`normalize.integrity_policy.sequence_gap.minimum_samples`](normalization-runtime.md#setting-integrity_policy.sequence_gap.minimum_samples)

<a id="setting-normalize.integrity_policy.sequence_gap.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.sequence_gap.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.sequence_gap.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.sequence_gap.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.sequence_gap.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.sequence_gap.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.sequence_gap.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.sequence_gap.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.sequence_gap.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.sequence_gap.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.sequence_gap.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.sequence_gap.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.timestamp_regression.action"></a>

- [`normalize.integrity_policy.timestamp_regression.action`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.action)

<a id="setting-normalize.integrity_policy.timestamp_regression.max_count"></a>

- [`normalize.integrity_policy.timestamp_regression.max_count`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.max_count)

<a id="setting-normalize.integrity_policy.timestamp_regression.max_rate"></a>

- [`normalize.integrity_policy.timestamp_regression.max_rate`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.max_rate)

<a id="setting-normalize.integrity_policy.timestamp_regression.minimum_samples"></a>

- [`normalize.integrity_policy.timestamp_regression.minimum_samples`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.minimum_samples)

<a id="setting-normalize.integrity_policy.timestamp_regression.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.timestamp_regression.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.timestamp_regression.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.timestamp_regression.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.timestamp_regression.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.timestamp_regression.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.timestamp_regression.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.timestamp_regression.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.timestamp_regression.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.missing_snapshot.action"></a>

- [`normalize.integrity_policy.missing_snapshot.action`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.action)

<a id="setting-normalize.integrity_policy.missing_snapshot.max_count"></a>

- [`normalize.integrity_policy.missing_snapshot.max_count`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.max_count)

<a id="setting-normalize.integrity_policy.missing_snapshot.max_rate"></a>

- [`normalize.integrity_policy.missing_snapshot.max_rate`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.max_rate)

<a id="setting-normalize.integrity_policy.missing_snapshot.minimum_samples"></a>

- [`normalize.integrity_policy.missing_snapshot.minimum_samples`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.minimum_samples)

<a id="setting-normalize.integrity_policy.missing_snapshot.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.missing_snapshot.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.missing_snapshot.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.missing_snapshot.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.missing_snapshot.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.missing_snapshot.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.missing_snapshot.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.missing_snapshot.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.missing_snapshot.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_book.action"></a>

- [`normalize.integrity_policy.invalid_book.action`](normalization-runtime.md#setting-integrity_policy.invalid_book.action)

<a id="setting-normalize.integrity_policy.invalid_book.max_count"></a>

- [`normalize.integrity_policy.invalid_book.max_count`](normalization-runtime.md#setting-integrity_policy.invalid_book.max_count)

<a id="setting-normalize.integrity_policy.invalid_book.max_rate"></a>

- [`normalize.integrity_policy.invalid_book.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_book.max_rate)

<a id="setting-normalize.integrity_policy.invalid_book.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_book.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_book.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_book.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.invalid_book.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_book.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.invalid_book.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_book.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_book.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.invalid_book.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.invalid_book.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.invalid_book.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.invalid_book.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.invalid_book.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.invalid_book.windows.daily.minimum_samples)

<a id="setting-normalize.integrity_policy.transformation_failure.action"></a>

- [`normalize.integrity_policy.transformation_failure.action`](normalization-runtime.md#setting-integrity_policy.transformation_failure.action)

<a id="setting-normalize.integrity_policy.transformation_failure.max_count"></a>

- [`normalize.integrity_policy.transformation_failure.max_count`](normalization-runtime.md#setting-integrity_policy.transformation_failure.max_count)

<a id="setting-normalize.integrity_policy.transformation_failure.max_rate"></a>

- [`normalize.integrity_policy.transformation_failure.max_rate`](normalization-runtime.md#setting-integrity_policy.transformation_failure.max_rate)

<a id="setting-normalize.integrity_policy.transformation_failure.minimum_samples"></a>

- [`normalize.integrity_policy.transformation_failure.minimum_samples`](normalization-runtime.md#setting-integrity_policy.transformation_failure.minimum_samples)

<a id="setting-normalize.integrity_policy.transformation_failure.windows.hourly.max_rate"></a>

- [`normalize.integrity_policy.transformation_failure.windows.hourly.max_rate`](normalization-runtime.md#setting-integrity_policy.transformation_failure.windows.hourly.max_rate)

<a id="setting-normalize.integrity_policy.transformation_failure.windows.hourly.minimum_samples"></a>

- [`normalize.integrity_policy.transformation_failure.windows.hourly.minimum_samples`](normalization-runtime.md#setting-integrity_policy.transformation_failure.windows.hourly.minimum_samples)

<a id="setting-normalize.integrity_policy.transformation_failure.windows.daily.max_rate"></a>

- [`normalize.integrity_policy.transformation_failure.windows.daily.max_rate`](normalization-runtime.md#setting-integrity_policy.transformation_failure.windows.daily.max_rate)

<a id="setting-normalize.integrity_policy.transformation_failure.windows.daily.minimum_samples"></a>

- [`normalize.integrity_policy.transformation_failure.windows.daily.minimum_samples`](normalization-runtime.md#setting-integrity_policy.transformation_failure.windows.daily.minimum_samples)

<a id="setting-normalize.reconciliation.trades.overlap_policy"></a>

- [`normalize.reconciliation.trades.overlap_policy`](normalization-runtime.md#setting-reconciliation.trades.overlap_policy)

<a id="setting-normalize.reconciliation.trades.prefer_source.selection"></a>

- [`normalize.reconciliation.trades.prefer_source.selection`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.selection)

<a id="setting-normalize.reconciliation.trades.prefer_source.on_unique_data_loss"></a>

- [`normalize.reconciliation.trades.prefer_source.on_unique_data_loss`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.on_unique_data_loss)

<a id="setting-normalize.reconciliation.trades.prefer_source.max_excluded_events"></a>

- [`normalize.reconciliation.trades.prefer_source.max_excluded_events`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.max_excluded_events)

<a id="setting-normalize.reconciliation.trades.prefer_source.max_excluded_rate"></a>

- [`normalize.reconciliation.trades.prefer_source.max_excluded_rate`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.max_excluded_rate)

<a id="setting-normalize.reconciliation.trades.prefer_source.minimum_samples"></a>

- [`normalize.reconciliation.trades.prefer_source.minimum_samples`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.minimum_samples)

<a id="setting-normalize.reconciliation.trades.prefer_source.max_excluded_interval_ms"></a>

- [`normalize.reconciliation.trades.prefer_source.max_excluded_interval_ms`](normalization-runtime.md#setting-reconciliation.trades.prefer_source.max_excluded_interval_ms)

<a id="setting-normalize.reconciliation.depth.overlap_policy"></a>

- [`normalize.reconciliation.depth.overlap_policy`](normalization-runtime.md#setting-reconciliation.depth.overlap_policy)

<a id="setting-normalize.reconciliation.depth.prefer_source.selection"></a>

- [`normalize.reconciliation.depth.prefer_source.selection`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.selection)

<a id="setting-normalize.reconciliation.depth.prefer_source.switch_boundary"></a>

- [`normalize.reconciliation.depth.prefer_source.switch_boundary`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.switch_boundary)

<a id="setting-normalize.reconciliation.depth.prefer_source.on_unique_data_loss"></a>

- [`normalize.reconciliation.depth.prefer_source.on_unique_data_loss`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.on_unique_data_loss)

<a id="setting-normalize.reconciliation.depth.prefer_source.max_excluded_events"></a>

- [`normalize.reconciliation.depth.prefer_source.max_excluded_events`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.max_excluded_events)

<a id="setting-normalize.reconciliation.depth.prefer_source.max_excluded_rate"></a>

- [`normalize.reconciliation.depth.prefer_source.max_excluded_rate`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.max_excluded_rate)

<a id="setting-normalize.reconciliation.depth.prefer_source.minimum_samples"></a>

- [`normalize.reconciliation.depth.prefer_source.minimum_samples`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.minimum_samples)

<a id="setting-normalize.reconciliation.depth.prefer_source.max_excluded_interval_ms"></a>

- [`normalize.reconciliation.depth.prefer_source.max_excluded_interval_ms`](normalization-runtime.md#setting-reconciliation.depth.prefer_source.max_excluded_interval_ms)

<a id="setting-normalize.timestamp_regression.trades.policy"></a>

- [`normalize.timestamp_regression.trades.policy`](normalization-runtime.md#setting-timestamp_regression.trades.policy)

<a id="setting-normalize.timestamp_regression.trades.max_regression_ms"></a>

- [`normalize.timestamp_regression.trades.max_regression_ms`](normalization-runtime.md#setting-timestamp_regression.trades.max_regression_ms)

<a id="setting-normalize.timestamp_regression.trades.max_reorder_window_ms"></a>

- [`normalize.timestamp_regression.trades.max_reorder_window_ms`](normalization-runtime.md#setting-timestamp_regression.trades.max_reorder_window_ms)

<a id="setting-normalize.timestamp_regression.trades.max_discarded_events"></a>

- [`normalize.timestamp_regression.trades.max_discarded_events`](normalization-runtime.md#setting-timestamp_regression.trades.max_discarded_events)

<a id="setting-normalize.timestamp_regression.trades.max_discarded_rate"></a>

- [`normalize.timestamp_regression.trades.max_discarded_rate`](normalization-runtime.md#setting-timestamp_regression.trades.max_discarded_rate)

<a id="setting-normalize.timestamp_regression.trades.minimum_samples"></a>

- [`normalize.timestamp_regression.trades.minimum_samples`](normalization-runtime.md#setting-timestamp_regression.trades.minimum_samples)

<a id="setting-normalize.timestamp_regression.depth.policy"></a>

- [`normalize.timestamp_regression.depth.policy`](normalization-runtime.md#setting-timestamp_regression.depth.policy)

<a id="setting-normalize.timestamp_regression.depth.max_regression_ms"></a>

- [`normalize.timestamp_regression.depth.max_regression_ms`](normalization-runtime.md#setting-timestamp_regression.depth.max_regression_ms)

<a id="setting-normalize.timestamp_regression.depth.require_sequence_continuity"></a>

- [`normalize.timestamp_regression.depth.require_sequence_continuity`](normalization-runtime.md#setting-timestamp_regression.depth.require_sequence_continuity)

<a id="setting-normalize.timestamp_regression.depth.require_recovery_snapshot"></a>

- [`normalize.timestamp_regression.depth.require_recovery_snapshot`](normalization-runtime.md#setting-timestamp_regression.depth.require_recovery_snapshot)

<a id="setting-normalize.timestamp_regression.depth.max_discarded_events"></a>

- [`normalize.timestamp_regression.depth.max_discarded_events`](normalization-runtime.md#setting-timestamp_regression.depth.max_discarded_events)

### Merge

<a id="setting-merge.resources.workers"></a>

- [`merge.resources.workers`](merge-runtime.md#setting-resources.workers)

<a id="setting-merge.resources.memory_limit_mb"></a>

- [`merge.resources.memory_limit_mb`](merge-runtime.md#setting-resources.memory_limit_mb)

<a id="setting-merge.resources.max_open_files"></a>

- [`merge.resources.max_open_files`](merge-runtime.md#setting-resources.max_open_files)

<a id="setting-merge.parquet.row_group_target_mb"></a>

- [`merge.parquet.row_group_target_mb`](merge-runtime.md#setting-parquet.row_group_target_mb)

<a id="setting-merge.parquet.file_target_mb"></a>

- [`merge.parquet.file_target_mb`](merge-runtime.md#setting-parquet.file_target_mb)

<a id="setting-merge.parquet.compression_level"></a>

- [`merge.parquet.compression_level`](merge-runtime.md#setting-parquet.compression_level)

<a id="setting-merge.partitioning.partitions_per_worker"></a>

- [`merge.partitioning.partitions_per_worker`](merge-runtime.md#setting-partitioning.partitions_per_worker)
