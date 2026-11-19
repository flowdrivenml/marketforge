# Normalization recipes

A normalization recipe selects registered raw files. Runtime, profiles and the prepared Engine batch have independent schemas. Use a path with a directory component or a saved name under `data/.config/historical/normalization/`.

| Field | Type and requirement |
| --- | --- |
| `version` | Optional integer, defaults to 1. Only 1 is supported. |
| `interval.start`, `interval.end` | Required timezone-aware TOML datetimes. Start precedes end. Selects whole overlapping raw inputs. |
| `requests` | Required nonempty array. Each request selects a venue, instrument type, category, content type and native scope as in [acquisition](acquisition.md#schema). |
| `requests.source_key` | Optional nonblank registered source key with no whitespace. Required for equity, which uses `equity` for both type and category. A source key requires concrete instrument scope. |
| `ranking_policy` | Optional policy name, defaults to `turnover`. Letters, digits, underscores and hyphens. Deterministic identity order is available when ranking positions are absent. |

Prepare checks raw availability, nominal coverage, historical identity and specification evidence. The selected specification must cover the processing input, including any part extending outside the requested interval. See [normalize registered data](../workflows/normalize.md).

## Prepared Batch JSON Parameters

See the [prepared batch reference](../reference/prepared.md).

## Runtime Resource Parameters

See [runtime settings](../reference/runtime.md).

## Processing policies

See [integrity and reconciliation settings](../reference/runtime.md).

## Automatically enriched historical inputs

Use [historical metadata inference](markets.md#historical-metadata-inference) where that policy is appropriate, or supply [historical declarations](../workflows/catalogue/specifications.md). A current observation on its own does not establish historical validity.

## Legacy explicit input recipes

Existing preparation files can select registered IDs directly. Supply positive unsigned 64-bit `job_id` and `dataset_id`, `content_type` as `trades` or `depth`, and a nonempty `[[inputs]]` array. Each input requires a unique positive unsigned 64-bit `task_id` and positive signed 64-bit `archive_id` and `instrument_id`. Optional legacy fields are `format_profile`, `archive_member`, `source_compression` (`plain`, `gzip`, `zip`, `tar_gzip`) and unsigned 32-bit `stream_rank`. An archive/member pair may occur once. Optional `[output]` requires both `staging_path` and `dataset_path`. Use fresh sibling paths for atomic publication. Prefer scoped recipes for new workflows.
