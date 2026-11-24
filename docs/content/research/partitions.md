# Inspect partitions and row groups

A logical stream identifies a market data sequence. A physical partition groups one or more primary files, each of which can contain several Parquet row groups. The registry can identify a publication or a primary file. A partition locator only has meaning within the dataset handle that listed it.

## Physical locators

| Reference | Scope |
| --- | --- |
| `partition:N` | Manifest partition identifier for physical merge output, or the recorded partition directory for converted output |
| `file:N` | One-based manifest primary-file position when the publication has no stable physical partition identifier, including normalized output |
| Row-group index | Zero-based index in one specific file inside the selected partition, whose file number is one-based |

Re-read locators after changing the selected publication or file. Row groups describe physical storage and carry no separate registry identity. The inventory reads Parquet footers and manifest metadata. Missing observed statistics appear as unknown. File size, rows, and any declared row-group counts are checked against available evidence when opening a registered output.

```python
parts = merged.partitions()
if parts:
    part = merged.partition(parts[0].reference)
    print(part.info(), part.schema())
    print(part.row_group(0).head(5, columns=["event_timestamp_ns"]))
```

Use `part.row_groups()` for per-file group rows and recorded compressed column-chunk size. Use `part.batches()` for exact sequential Arrow access, or `part.scan(columns=[...])` for lazy Polars over compatible fields. An explicitly selected row group supports `head` and `batches`. Use `part.select_row_groups([0, 2], file=1)` to read several groups from one file. Pass a list of locators to `Dataset.query(partition=...)` to read several partitions in manifest order.

## CLI and Forge workspace

```bash
mf datasets partitions --publication-id PUBLICATION_ID --testing
mf datasets partitions --publication-id PUBLICATION_ID --partition PARTITION --row-groups --testing
mf datasets schema --publication-id PUBLICATION_ID --partition PARTITION --testing
mf datasets head --publication-id PUBLICATION_ID --partition PARTITION --row-group 0 --data-type trades --limit 20 --testing
```

Replace the publication ID and locator with values from the preceding commands. For a converted output, substitute `--output-ref OUTPUT_REF` for `--publication-id`. A registered primary-file ID can also be passed positionally. In the Forge workspace, use **Physical partitions** on a publication or indexed conversion, then select a row to open its schema, groups, or preview controls.
