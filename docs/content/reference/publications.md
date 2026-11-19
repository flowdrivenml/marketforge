# Publication formats and evidence

## Parquet layout

### Configurable targets

| Parameter | Meaning |
|---|---|
| `row_group_target_bytes` | Desired size of a Parquet row group |
| `file_target_bytes` | Desired size of each Parquet part |

These are resource settings, not an independent prepared-task field.

### Physical output structure

Canonical output is organized by logical stream.

```text
streams/
  <stream-fingerprint>/
    trades/
      part-000000.parquet
      sources.jsonl
```

For depth:

```text
streams/
  <stream-fingerprint>/
    depth/
      part-000000.parquet
      events.parquet
      segments.json
      boundary.json
```

The exact files present depend on the output type and whether execution reached successful finalization.

### Depth-specific auxiliary files

| File | Purpose |
|---|---|
| `events.parquet` | Source-event index and canonical-row offsets |
| `segments.json` | Reconstruction segments |
| `boundary.json` | Source boundary and book-state evidence |


### File-size targets

The configured file target is not necessarily a hard upper bound on every physical output file.

Actual file sizes depend on writer buffering, row-group boundaries, encoding, and rotation behavior.

A file larger than the nominal target should be investigated against the writer's actual implementation before being classified as incorrect.

---

## Dataset publication

A normalization job uses distinct staging and final dataset destinations.

### Staging

Contains intermediate output during execution.

Staged Parquet files are not authoritative completed datasets.

### Published dataset

A dataset is successfully published only after completing the required validation and finalization steps.

### Dataset manifest

The manifest records:

- Protocol version.
- Job ID.
- Dataset ID.
- Content type.
- Canonical row count.
- Parquet file count.
- Timestamp boundaries.
- Source-task inventory.
- Stream metadata.
- Auxiliary files.
- Integrity status.
- Integrity evaluation.
- Processing metrics.

The normalization manifest validator accepts protocol versions 3 through 5. Version 3 is the base format, version 4 records explicit source ownership ranges, and version 5 can represent repeated source spans. Merge applies additional layout rules.

### Publication rule

A failed integrity evaluation cannot produce a committed dataset manifest.

A processing failure may still leave useful staging artifacts and diagnostic evidence.

---

## Output integrity metrics

Processing counters distinguish different forms of work and failure.

### Source counters

| Counter | Meaning |
|---|---|
| `records_read` | Physical source records examined |
| `records_matched` | Records matched to a selected instrument |
| `records_skipped_instrument` | Records excluded by instrument selection |
| `records_rejected` | Rejected records |
| `records_rejected_unmatched` | Rejections without an established instrument match |
| `records_processed` | Successfully processed source records |
| `records_unprocessed` | Matched records not processed |
| `records_unrouted` | Records not routed to a concrete instrument |

### Canonical output counters

| Counter | Meaning |
|---|---|
| `events_normalized` | Canonical rows generated |
| `events_written` | Canonical rows recorded as written |
| `rows_suppressed` | Rows suppressed by reconciliation |
| `rows_unpublished` | Normalized rows not published |

The engine validates the relationship:

```text
events_normalized
    = events_written
    + rows_suppressed
    + rows_unpublished
```

The counters deliberately distinguish source parsing failures from reconciliation suppression.

### Task counters

```text
tasks_completed
tasks_failed
```

These count task outcomes and are distinct from the dashboard's broader task-state classifications.

---


Use [dataset inspection](../workflows/datasets.md) to read the exact Arrow schema and preview rows. Verify the shared manifest and retain every referenced auxiliary artifact with its primary files.

## Canonical columns

| Applies to | Columns and physical types |
| --- | --- |
| Trade and depth | Required `event_timestamp_ns` (int64), `exchange`, `symbol`, `stream_id`, `side` (strings), `instrument_id` (int64), `price` (decimal256, precision 76 and scale 28). Nullable `system_timestamp_ns` (int64) and `quantity_base`, `quantity_quote`, `quantity_contracts` (decimal256, 76/28). |
| Trade | Nullable `trade_id` (string), `sequence` (uint64), `is_rpi` (boolean), and `trade_iv`, `mark_iv`, `index_price`, `mark_price` (decimal256, 76/28). |
| Depth | Nullable `order_count` (uint64). Updates with zero quantity remove the keyed side and price from the corresponding book state. |

Timestamps are Unix nanoseconds. Check the selected file's actual schema, especially for older publications. A depth index row carries `event_ordinal`, `canonical_row_offset`, `canonical_row_count`, event and system timestamps, sequence start/end, event boundary and source operation. The row span is half-open in the stream's logical row space and can be empty. Read primary parts in manifest order. Use segments and boundaries to distinguish initialization and independent histories. See the [Bybit depth inspection example](../getting-started/quickstart.md#compare-with-the-checked-result).
