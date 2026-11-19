# Datasets CLI

## Browse and inspect

```bash
mf datasets list --testing
mf datasets list --testing --wide --publication ENGINE_PUBLICATION_ID
mf datasets list --testing --precise
mf datasets find --exchange okx --instrument-type future --market-category linear --data-type trades --testing
mf --json datasets list --testing --operation normalize --data-type trades --exchange xnas
mf datasets show FILE_ID --testing
mf datasets streams FILE_ID --testing
mf datasets lineage FILE_ID --testing
mf datasets lineage FILE_ID --testing --publication --limit 10
mf datasets coverage --testing --instrument AAPL
mf datasets verify FILE_ID --testing
mf datasets schema FILE_ID --testing
mf datasets head FILE_ID --testing --limit 10 --column event_timestamp_ns
mf datasets head FILE_ID --testing --wide --precise --limit 5
mf datasets refresh-summary FILE_ID --testing
```

List, find and coverage paginate file rows using `--limit` and `--offset`. Filters include operation, content type, status, publication, exchange, symbol or canonical instrument ID, instrument type and market category. Combined identity and time filters match the same stored stream. Use integer nanoseconds or timezone-qualified ISO timestamps for `--start` and `--end`. Stored observed endpoints are inclusive. Query end is exclusive. Observed envelopes do not establish continuous coverage or depth reconstruction availability.



The default file-page limit is 50 and accepts 1–1000. Offset defaults to 0. `--operation` accepts `normalize` or `merge`. `--data-type` accepts `trade` (`trades`), `l2` (`depth` or `order_book_l2`), and `trade_l2` (`combined`). Use the stored status, venue, instrument type and category tokens with `--status`, `--exchange`, `--instrument-type`, and `--market-category`. `--publication` matches the engine publication ID and `--instrument` matches an exact symbol case-insensitively or a canonical instrument ID.

`head` defaults to 10 rows, accepts 1–1000, and takes repeatable `--column NAME` values. The compact terminal preview selects columns for the recorded content type. Use `--wide` to display all columns or select specific columns with `--column`. Preview preserves stored row order. It displays timestamps to whole seconds by default and `--precise` shows nanoseconds. Global JSON returns all stored columns by default, with integer nanosecond fields and exact decimals serialized as strings. Schema and preview require PyArrow from the research dependencies. `refresh-summary` updates derived metadata in the testing registry. The other inspection actions are read-only. With global JSON, `coverage` returns the original paginated file data and membership envelopes.

### Find and inspect a file

Use `find` to search registered market identities. Each result gives the PostgreSQL file ID, the file's memberships, its content type and its observed interval. Replace `FILE_ID` in the detail examples with an ID from the results. A file with several memberships appears once and shows all known memberships.

`show` summarizes the selected file and its publication. `streams` reports stored file-level stream identities and observed envelopes. `lineage` lists proven file contributions separately from the selections made for the whole publication. Use `--publication` to expand the shared selection and `--checksums` to display retained raw-input checksums. The terminal limit is 25 rows per lineage section, configurable from 1–1000. Use `mf --json datasets lineage FILE_ID --testing` for the complete underlying provenance.


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

Preview and apply verify the committed publication against its registered fingerprints. The operation reads existing file bytes to check their SHA-256s and reuses the registered raw links. It leaves the published artifacts in place. Legacy normalization aggregate IDs retain their historical references and report replacement file IDs through `show`. Repeating apply reuses the same IDs. Both calls require `--all --testing`; `--dry-run` prints an inventory and conflicts without applying the registration change. Legacy merge aggregates are currently reported as conflicts. The migration command exits 1 if any aggregate has a conflict; earlier independent conversions can remain.

A registration-only command accepts the publication directory, its manifest, or its execution intent to register the publication. Pass a specific primary file path to register just that file.

```bash
mf normalize register /path/to/publication/streams/STREAM/trades/part-000000.parquet --testing
mf merge register /path/to/publication/timeline/partition-000000/part-000000.parquet --testing
```

See [Merge registered normalized files](../../workflows/merge.md) for supported inputs and depth dependencies.
