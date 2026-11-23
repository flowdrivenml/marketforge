# Conversion recipes

The editable version-1 Forge recipe selects whole committed merge publications and historical reference trades. Use `mf convert new` to generate a source binding. [Workflow](../workflows/conversion.md) · [Standalone Engine job](../reference/prepared-conversion.md)

| Path | Type and requirement | Default and meaning |
| --- | --- | --- |
| `version` | Required integer 1 | Recipe schema |
| `source.kind` | Required `merge`, `normalize`, `acquisition`, or `publication` | Type of source binding |
| `source.reference`, `source.path` | Required nonblank strings for recipe sources | Named or explicit upstream reference and resolved path, verified on load |
| `source.sha256` | Required 64 lowercase hex characters for recipe sources | Upstream TOML fingerprint |
| `interval.start_ns`, `interval.end_ns` | Required signed 64-bit Unix ns, start before end | Requested half-open bounds used to check selection scope |
| `selection.file_dataset_ids`, `selection.publication_ids` | Optional nonempty unique positive signed-64-bit integer lists. Mutually exclusive | Explicit file IDs or registry publication IDs |
| `selection.expected_engine_publication_ids` | Optional nonempty unique list of canonical positive uint64 decimal strings | Exact Engine IDs expected after resolution |
| `selection.whole_publication` | Boolean | `false`. Set true to approve an overlapping wider publication |
| `reference_stream_ids` | List of distinct nonblank strings | Empty list. Explicit trade stream IDs |
| `reference_publication_id` | Optional positive signed-64-bit registry publication ID | External trade-only merge, requires nonempty reference stream selection |
| `conversion.target_quote` | Required uppercase ASCII alphanumeric currency string | Target quote unit |
| `conversion.price_source` | `last_trade` only | `last_trade` |
| `conversion.max_staleness_ns` | Required positive uint64 in ns | No model default. The `new` template sets 60 seconds |
| `conversion.max_hops` | Integer 1–4 | `3` |
| `conversion.path_policy` | `all` or `primary` | `all`. Planning and execution constraints remain mode-specific |
| `conversion.instrument_kinds` | Optional nonempty unique list of `spot`, `perpetual`, `future`, `option`, `equity` | Omitted selects all kinds |
| `resources.workers`, `resources.memory_limit_mb`, `resources.max_open_files` | Positive uint64 settings in workers, MiB and files | `1`, `1024`, `64`, capped by the conversion runtime during preparation |

## Direct publication binding

Use `mf convert new NAME --from-publication MERGED_PUBLICATION_ID --target-quote USDT --testing` to generate the declaration. The selected merge can be used independently of the editable TOML that produced it.

| Publication source field | Meaning |
| --- | --- |
| `source.kind` | `publication` |
| `source.registry_publication_id` | Positive signed 64-bit ID of the exact registered merge |
| `source.registry_id`, `source.environment` | Registry UUID and `testing` or `production` binding |
| `source.engine_publication_id` | Canonical positive uint64 decimal string |
| `source.manifest_sha256`, `source.processing_job_sha256`, `source.publication_checksums_sha256` | Lowercase SHA-256 fingerprints of the committed manifest, merge job and publication checksum index |
| `source.interval.start_ns`, `source.interval.end_ns` | Exact signed 64-bit nanosecond bounds from the committed half-open range |

A direct recipe requires `selection.whole_publication = true` and its top-level `interval` must equal the committed range. Omit `selection.file_dataset_ids`, `selection.publication_ids`, and `selection.expected_engine_publication_ids` for this source. Reference selection remains independent. Configure explicit trade streams when the conversion needs them. A target quote equal to the historical quote can use the existing identity route.

List, show, and local recipe validation can inspect this declaration offline. Prepare in the bound environment to recheck eligibility and metadata fingerprints, then verify the full publication, normalized parents, namespace, stream membership, and historical currencies. Prepared submissions retain the publication declaration alongside the existing job and input evidence. Preparation keeps the whole publication, including any state reconstruction; the interval does not crop rows.

Absence of an optional field means omission in TOML. The Engine gets the resolved bindings, resources and writer configuration in a generated job. The Forge collection and execution intent are [separate artifacts](../reference/prepared-forge.md). [Recipe model](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/conversion/recipes.py)
