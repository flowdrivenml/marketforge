# Collection sorting

Use `--sort FIELD` to choose the field and `--order asc` or `--order desc` to choose its direction. With only `--order`, Forge changes the default field’s direction. With only `--sort`, Forge uses that field’s direction from the table. Help lists the fields for each view. An unsupported field fails before a registry query.

Forge filters and groups records before ordering and paging. Missing primary sort values go last in either direction. Ties use ascending row identity, including numeric IDs where applicable. A timeline uses its full identity and segment, and converted outputs use their output reference and part number where applicable. These rules apply to terminal and JSON results.

| Command or view | A row represents | Ascending fields | Descending fields | Default | `time` |
| --- | --- | --- | --- | --- | --- |
| `datasets list --view timelines` | Timeline segment | `id`, `stage`, `exchange`, `instrument` | `start`, `end`, `publications` | `start desc` | Observed segment start |
| `datasets list/find --view publications` | Registered publication or indexed conversion output | `id`, `stage` | `start`, `end`, `size`, `files` | `start desc` | Earliest observed timestamp across the publication |
| `datasets list/find/coverage --view files` | Primary file or indexed conversion part | `id`, `stage` | `start`, `end`, `size` | `start desc` | First observed timestamp of the file |
| `datasets coverage --view coverage` | Observed coverage group | `identity`, `stage`, `exchange`, `instrument` | `start`, `end`, `files`, `publications` | `identity asc` | First observed timestamp in the group |
| `datasets streams` | Retained stream membership | `name`, `rank` | `start`, `end` | `name asc` | First observed timestamp for the stream |
| `storage list` | Registered local archive | `id`, `relative_path`, `status` | `start`, `end`, `bytes_written` | `start desc` | Nominal archive start |
| `storage coverage` (also bare `storage`) | Nominal archive scope | `identity` | `start`, `end`, `size`, `files` | `start desc` | Earliest nominal start of the matching archives |
| `runs list` | Indexed Forge run | `id`, `operation`, `action`, `status`, `recipe` | `start`, `end`, `duration` | `start desc` | Recorded run start |
| `acquisition list`, `normalize list`, `merge list`, `convert list`, `pipeline list` | Managed recipe | `name` | — | `name asc` | — |
| `normalize profiles list` | Installed profile | `name` (full profile ID) | — | `name asc` | — |
| `acquisition sources` | Supported source combination | `identity`, `exchange`, `instrument_type`, `market_category`, `data_type`, `scope` | — | `identity asc` | — |
| `markets list` | Stored market | `identity`, `symbol`, `exchange` | `observed`, `measured`, `fetched`, `turnover` | `identity asc` | Source observation time, if retained |
| `markets rank list` | Published ranking snapshot | `id`, `policy`, `currency` | `created`, `ranked`, `universe` | `created desc` | Snapshot creation |
| `markets rank show` | Stored ranking entry | `rank`, `symbol`, `reference` | `observed` | `rank asc` | Retained source observation |
| `catalog exchange list` | Exchange | `code`, `name`, `id` | — | `code asc` | — |
| `catalog instrument list` | Instrument identity | `identity`, `symbol`, `exchange`, `type`, `status`, `id` | — | `identity asc` | — |
| `catalog specs list/show` | Historical specification evidence | `id`, `kind`, `identity` (list only) | `observed` | `identity asc` in list, `id asc` in show | Retained source observation |

## Time, aliases and groups

- Dataset `time` and `observed` alias `start`, and `end` selects observed end. Observed bounds describe retained timestamps. A membership filter selects a publication while its sort and displayed bounds still describe the whole publication. Timeline `id` uses stage and instrument identity. Converted publications can have observed bounds, but indexed conversion file parts lack per-file timestamps and sort last for time. Legacy JSON without an explicit view retains the registered file-page shape and uses file sort fields.
- Storage `start_at` and `end_at` alias `start` and `end`. Archive `size` aliases `bytes_written`; coverage accepts the reverse alias. Run `started_at` and `finished_at` alias `start` and `end`.
- Markets use exchange, market category, instrument type and symbol for identity. `freshness` aliases `measured`, the recorded metric window end. `fetched` uses the fetch timestamp. `turnover` sorts within ascending native denomination groups.
- Ranking volume entries retain base-asset groups. Both directions sort within each group. Exclusions stay separate and ranks retain their stored values. Ranking conversion evidence retains its evidence ID order.
- Current catalogue specifications retain instrument identity order. Sorting options on `catalog specs` apply to historical evidence.
- Run attempts, logs, workflow stages, recipe requests, schema fields, causal or physical row previews, and maintenance plans retain their authoritative sequence. Browsing does not reorder the evidence or change processing.

```bash
mf datasets list --testing --operation merge --view publications --sort time --order desc
mf datasets list --testing --operation merge --view publications --sort start --order asc
mf runs list --testing --order asc
mf markets rank show volume --testing --sort rank --order desc
```
