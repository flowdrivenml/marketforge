# Datasets CLI reference

Browse normalized and merged registry publications and converted outputs from the durable work index. Start with [Explore historical datasets](../../workflows/inspection/datasets.md). Raw acquisitions use [Storage](storage.md). Production is the default. Select exactly one of `--testing` and `--production` when specifying an environment, and use the same [registry identity](../../concepts/identity.md#registry-identity) for related IDs.

## Browsing commands

| Command | Terminal default | Explicit views |
| --- | --- | --- |
| `mf datasets list` | One row per evidenced timeline segment | `timelines`, `publications`, `files` |
| `mf datasets find` | One row per matching publication or indexed output | `publications`, `files` |
| `mf datasets coverage` | One row per grouped observed extent | `coverage`, `files` |

```bash
mf datasets list --testing --view timelines --sort start
mf datasets find --testing --search BTCUSDT --view publications
mf datasets coverage --testing --operation merge --data-type depth
```

All three commands read retained summaries. Filtering and grouping precede paging. `--limit` defaults to 50 and accepts 1–1,000. `--offset` is nonnegative and defaults to zero. The page unit is the selected view. Publication rows summarize whole outputs, including bounded membership previews, primary-file counts, recorded size and recorded state or integrity. A membership filter can match only part of an output. File rows show registered primaries or numbered converted parts. Their recorded row counts describe stored rows. See the [publication formats](../data-formats/publications.md) for partition and physical-row definitions.

### Supported filters

| Option | Meaning |
| --- | --- |
| `--operation` | Exact processing stage. Accepts `normalize`, `merge`, `convert`. |
| `--exchange`, `--instrument` | Case-insensitive exact exchange or symbol in explicit views. Instrument also accepts an exact canonical instrument ID. |
| `--instrument-type`, `--market-category` | Exact stored category values, such as `perpetual` and `linear`. |
| `--data-type` | `trade` or `trades`, `l2`, `depth` or `order_book_l2`, `trade_l2` or `combined`. Trade and depth filters can match memberships inside a combined file. In publication view, `combined` can match separate primaries if both kinds satisfy the filters. |
| `--status` | Exact recorded state. Ordinary explicit views select complete, active outputs by default. Superseded history remains accessible through dedicated inspection. |
| `--start`, `--end` | Overlap with a half-open query interval. Use signed 64-bit integer Unix nanoseconds or timezone-qualified ISO 8601 timestamps. Explicit views test membership bounds when present. |
| `--publication-id` | Exact registry publication ID. In timeline view selects segments containing that publication and retains their other qualifying contributors. |
| `--engine-publication-id`, `--publication` | Exact Engine publication ID. Both preserve the Engine ID domain. Timeline selection retains the other qualifying contributors. |
| `--output-ref`, `--target-quote` | Exact indexed conversion work key, or target quote normalized to uppercase. |
| `--search` | Case-insensitive substring match over retained symbol, exchange, stream and supported reference fields. |

The four identity selectors `--publication-id`, `--engine-publication-id`, `--publication`, and `--output-ref` are mutually exclusive. Timelines, publications and files default to observed start descending. In all three views, `time` and `observed` alias `start`. Use `--sort end --order desc` for latest observed end, or `--sort start --order asc` for earliest observed start. Publication bounds describe all registered files even when a filter selects only part of the membership. [Fields, defaults and aliases](sorting.md).

For `list` and `find`, `--wide` shows file paths in file view. In timeline view, `--wide` prints exact inspection commands. `--precise` retains nanosecond display precision. These flags change terminal presentation.

## Understanding dataset coverage

A timeline is a navigation grouping. The identity includes stage, exchange, instrument namespace and canonical ID, market, data kind, stream, and retained representation. Merge uses a resolvable upstream namespace when available. Unresolved namespaces are isolated by publication. Conversion also separates target quote and work-key variant. A publication can contribute to several identities, so the number of segments and the distinct output count describe different units.

| Interval evidence | How to interpret it |
| --- | --- |
| Requested interval | The processing window, with an inclusive start and exclusive end |
| Nominal interval | A source or archive declaration. It can describe expected extent. |
| Observed extent | Inclusive first and last recorded event timestamps for a member |
| Evidence-backed coverage | Requires independent evidence of continuity or completeness |
| Known gap | Requires evidence that an interval was missing. Event silence alone leaves continuity unknown. |

Within one identity, the timeline joins overlapping observed spans, including spans that share the same endpoint. Disjoint spans remain separate, even for a one-nanosecond separation. Membership with unusable bounds is grouped separately when identity is known. There is no inferred gap tolerance. The implementation groups retained observations without upgrading them to proven continuous coverage. For conversion, the observation derives from source publication membership constrained by the converted output’s bounds.

Coverage aggregates observed endpoints and counts distinct supporting registry files and outputs. It can report a single extent across disjoint spans. Use timelines to inspect the separate spans and their contributors. Neither view attributes whole mixed-file row or byte counts to every instrument.

```bash
mf datasets timeline show TIMELINE_REFERENCE --testing --precise
```

Use the versioned reference from `list --wide` or explicit timeline JSON. It identifies the selected registry, segment and contributing evidence. Pagination and sorting preserve its identity. It can resolve the original contributors after later outputs extend a timeline. Changed or no longer selectable evidence can require a fresh reference. The command returns contributing publications and their recorded bounds. See [Dataset identifiers](../../concepts/identity.md#inspection-identifiers).

## File inspection commands

| Command | Reference and options | Reads |
| --- | --- | --- |
| `show` | `FILE_ID`, `--publication-id PUBLICATION_ID`, or `--output-ref OUTPUT_REF`. Optional `--wide`. | Registered metadata or indexed conversion manifest summary |
| `streams` | Same reference choices as `show` | Recorded memberships for a file or publication, or source membership for conversion |
| `lineage` | `FILE_ID` or `--output-ref OUTPUT_REF`. Registered terminal output supports `--publication`, `--checksums`, `--limit N` (default 25 per section). | Proven contributions and publication selection, or the selected conversion source |
| `verify` | `FILE_ID` or `--output-ref OUTPUT_REF`, optionally with `--part N` for conversion | Selected physical files and retained fingerprints |
| `schema` | `FILE_ID` or `--output-ref OUTPUT_REF --part N` | Parquet column types and nullability |
| `head` | Same reference choices as `schema`, plus `--limit N`, repeatable `--column NAME`, `--wide`, `--precise` | Bounded stored rows |

A positional identifier always identifies a registry primary file. A registered publication’s inspection includes a bounded file list. Page the full inventory with `list --view files --publication-id`. For conversion, copy the full lowercase 64-character work key. Part numbers are one-based positions in the recorded output inventory, and the key and part together identify a file.

```bash
mf datasets show --publication-id PUBLICATION_ID --testing
mf datasets streams --publication-id PUBLICATION_ID --testing
mf datasets lineage FILE_ID --testing --publication --checksums
mf datasets schema FILE_ID --testing
mf datasets head FILE_ID --testing --limit 5 --column event_timestamp_ns --precise
mf datasets verify FILE_ID --testing
```

Head defaults to 10 rows and accepts 1–1,000. The terminal uses a bounded column projection unless `--wide` or explicit columns are supplied. JSON defaults to all stored columns unless columns are selected. Exact decimal values in JSON are strings and timestamp fields remain integer nanoseconds. Preserve those integers in downstream code. Converted previews retain original and target schema fields, such as `price` and `price_target`.

### File-level contributions vs. publication-level selection

Registered lineage distinguishes direct raw archive and upstream primary-file links from the full publication selection. In terminal mode, `--publication` expands that selection and `--checksums` shows recorded raw input fingerprints. JSON includes both levels without the display limit. Conversion lineage reports the selected source publication. To trace reused output or identify the producing logical run, compare the publication identifiers and paths against retained run evidence. [Run-to-output workflow](../../workflows/inspection/runs.md#from-a-merged-publication-to-its-source)

### Verification vs. data integrity

Registered verification checks the selected primary file and its shared manifest against their fingerprints. It leaves the remaining files outside the check. There is no registry publication-wide selector for this command. Indexed conversion verification checks the retained fingerprint inventory, or the selected part plus manifest when `--part` is present. Checks are read-only and can fail on missing, changed or unsafe paths. Recorded completion, recorded processing integrity, fresh fingerprint verification and historical completeness are separate properties.

```bash
mf datasets show --output-ref OUTPUT_REF --testing
mf datasets schema --output-ref OUTPUT_REF --part 1 --testing
mf datasets verify --output-ref OUTPUT_REF --part 1 --testing
```

## Registered metadata maintenance

| Command | Syntax and effect |
| --- | --- |
| `deduplicate` | `mf datasets deduplicate --testing` saves a preview. Accepts `--operation normalize\|merge\|convert\|all`, `--apply`, `--plan PLAN`, `--force`, and `--accept-unverified-legacy-references` subject to the [cleanup rules](../../maintenance/publications.md#deduplicate-equivalent-publications). |
| `delete` | `mf datasets delete --testing --publication-id PUBLICATION_ID` saves a preview. Accepts repeatable `--publication-id`, `--output-ref`, and `--retain-publication-id`, and `--cascade` during preview. Apply takes `--apply --plan PLAN` and the conditional legacy-risk acknowledgement. [Testing-only deletion](../../maintenance/publications.md#delete-explicit-testing-outputs) |
| `supersession` | `mf datasets supersession PUBLICATION_ID --testing` reads a retirement or invalidation record. Use `--output-ref OUTPUT_REF` instead of the positional ID for a deleted testing conversion. |
| `migrate-registration` | `mf datasets migrate-registration --testing --all --dry-run` previews migration. Omitting `--dry-run` applies it. |
| `refresh-summary` | `mf datasets refresh-summary FILE_ID --testing` rewrites the stored inspection summary from retained publication evidence. |

### Migrate existing registrations

Migration reads and validates retained publication evidence to expand eligible legacy aggregate records into primary-file registrations. It retains the migration relationship. Inspect a migrated legacy ID with `show` to obtain replacement file IDs. Summary refresh modifies the selected registry row and leaves the physical file unchanged. Use the [maintenance reference](../../maintenance/publications.md#migrate-or-refresh-registrations) for recovery and preview effects. These maintenance commands currently print JSON even in normal terminal mode.

<a id="json-dataset-inspection"></a>
<a id="json-output"></a>

## JSON results

```bash
mf --json datasets list --testing --limit 10
mf --json datasets list --testing --view timelines --limit 10
```

JSON `list`, `find` and `coverage` without an explicit view retain the registered-file page with `datasets`, `limit`, `offset`, and `interval_semantics`. Specify a view for conversion, newer reference filters, text search. Legacy pages keep the historical file filtering behavior and accept file sort fields.

Explicit views return schema version 1 with `view`, `environment`, `total`, `limit`, `offset`, `has_more`, and the corresponding array `timelines`, `publications`, `datasets`, or `coverage`. Timelines include full contributor references plus `timeline_counts`, `matching_publications` and `stage_publications`. Other views expose stage counts in `stages`. `conversion_index` distinguishes an available, unavailable or out-of-filter index. Unreadable indexed manifests are reported separately. Timeline details return the selected `timeline` and environment. File inspection returns action-specific objects. Availability and supersession can change the response. [Global JSON and errors](index.md#global-options)

| Action | Principal JSON fields |
| --- | --- |
| `show FILE_ID` | `file`, `publication`. Migrated legacy IDs can return `replacement_files`. |
| `show --publication-id` | `publication` and a paged `files` result |
| `show --output-ref` | `output` with indexed result and part inventory |
| `streams` | `scope` and `streams`, with the selected identity or its retained metadata |
| `lineage FILE_ID` | `id`, `file_contributions`, `publication_selection` |
| Converted lineage | `file`, `publication_selection` |
| `schema`, `head` | File identity or output reference and part, plus `columns` or `rows` |
| Registered verify | `id`, `status`, `scope`, `file_sha256`, `entire_publication_verified` (false) |
| Converted verify | `output_ref`, `part`, `status`, `scope`, `artifacts_checked` |

## Tokyo Night colors

Dataset browsing uses the shared theme. Redirected dataset output is plain text. See [application presentation settings](../configuration/application.md) for terminal color configuration and [Python dataset inspection](../../research/python-api.md) for analytical use.

## Physical partitions and row groups

```text
mf datasets partitions (FILE_ID | --publication-id ID | --output-ref REF)
                       [--partition LOCATOR] [--row-groups] [--limit N] [--offset N]
                       [--testing | --production]
```

Use `--row-groups` with an exact `--partition`. `schema` and `head` accept the same identities plus `--partition LOCATOR`, `--row-group INDEX`, and `--partition-file NUMBER` (default 1). Head also accepts `--data-type {trades,depth}`, `--instrument-id ID`, `--stream-id STREAM`, `--start TIME`, `--end TIME`, and repeatable `--column NAME`. Bounds are inclusive start, exclusive end in UTC event time. Existing converted `--part` previews retain their prior behavior.

The inventory reads manifests and Parquet footers. JSON returns the exact references, primary paths and counts. For scope and numbering, see [Partitions and row groups](../../research/partitions.md).
