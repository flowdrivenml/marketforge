# Inspect registered datasets

Each primary data file in a verified normalization or merge manifest has its own PostgreSQL dataset ID. An engine job can commit one publication containing several primary files. The publication retains its engine ID, namespace, root, shared manifest, provenance and inventory. Indexes, source maps, checkpoints and other auxiliary artifacts remain dependencies.

The files stay in their original paths under the publication root. Normalization and merge register the files after the whole publication verifies. A failed integrity check blocks the publication. Normalization's partial-success policy applies to independently committed jobs.

## Browse and inspect

```bash
mf datasets list --testing
mf datasets list --testing --wide --publication ENGINE_PUBLICATION_ID
mf datasets list --testing --precise
mf --json datasets list --testing --operation normalize --data-type trades --exchange xnas
mf datasets show FILE_ID --testing
mf datasets streams FILE_ID --testing
mf datasets lineage FILE_ID --testing
mf datasets coverage --testing --instrument AAPL
mf datasets verify FILE_ID --testing
mf datasets schema FILE_ID --testing
mf datasets head FILE_ID --testing --limit 10 --column event_timestamp_ns
mf datasets refresh-summary FILE_ID --testing
```

List and coverage paginate file rows using `--limit` and `--offset`. Filters include operation, content type, status, publication, exchange, symbol or canonical instrument ID, instrument type and market category. Combined identity and time filters match the same stored stream. Use integer nanoseconds or timezone-qualified ISO timestamps for `--start` and `--end`. Stored observed endpoints are inclusive. Query end is exclusive. Observed envelopes do not establish continuous coverage or depth reconstruction availability.


### Terminal views

List groups files by publication, then exchange and canonical instrument and market. Each file keeps its database ID. Files with multiple memberships appear once in a group that names the represented instruments. The default shows compact part names. Use `--wide` to display manifest-relative paths. Wide terminals show compact tables. The display switches to stacked records when space is limited.

Timestamps are UTC and display to whole seconds by default. The year appears in the group heading when all endpoints fall in one year. Use `--precise` for nine fractional digits. Equal displayed seconds can represent different nanosecond endpoints. Global `--json` keeps the existing per-file response and exact nanosecond integers, regardless of the terminal display options.

Coverage summarizes canonical instruments and content types on the current filtered file page. A multi-membership file can contribute to several instrument groups and is counted once within each group. The earliest and latest observed timestamps form an envelope. Continuity within that envelope is unknown. Both views show the next offset when another file page is available.

### Tokyo Night colors

The CLI uses the existing MarketForge theme. In `data/.config/marketforge.toml`, set `presentation.color` to `"auto"` for terminal capability detection or to `"never"` for a plain view. Auto honors `NO_COLOR`. If the terminal advertises color capabilities, `"always"` enables color on interactive output. Redirected dataset output and JSON are plain. `presentation.mode = "silent"` suppresses progress displays and still allows requested inspection results.

Browsing uses versioned summaries from PostgreSQL. Schema and head read only the selected Parquet file and use the Arrow research dependency. Verify checks the file and shared manifest fingerprints and reports that scope. Refresh derives a summary from stored publication evidence without hashing data files. Normalization derives memberships and raw contributions from manifest ownership ranges where available. Merge verification scans only rank and timestamp columns in bounded Arrow batches to record represented streams. Selected merge inputs are shown as publication-scope lineage. A represented stream proves contribution from its input file when only one primary file was selected from that input publication. Attribution among several selected files from the same publication remains publication-scoped.

## Migrate existing registrations

```bash
mf database migrate --testing
mf datasets migrate-registration --all --testing --dry-run
mf datasets migrate-registration --all --testing
```

Preview and apply verify the committed publication against its registered fingerprints. The operation reads existing file bytes to check their SHA-256s and reuses the registered raw links. It leaves the published artifacts in place. Legacy normalization aggregate IDs retain their historical references and report replacement file IDs through `show`. Repeating apply reuses the same IDs.

A registration-only command accepts the publication directory, its manifest, or its execution intent to register the publication. Pass a specific primary file path to register just that file.

```bash
mf normalize register /path/to/publication/streams/STREAM/trades/part-000000.parquet --testing
mf merge register /path/to/publication/timeline/partition-000000/part-000000.parquet --testing
```

See [Merge registered normalized files](merge.md) for supported inputs and depth dependencies.
