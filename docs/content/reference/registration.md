# Managed registration recipes

Use this version-1 TOML contract to bind existing files to catalogue and profile evidence. Use [the registration guide](../workflows/acquisition/external-data.md) to execute it. The exact validation source is `forge/historical/registration/models.py`.

| Path | Type and requirement |
| --- | --- |
| `registration.version`, `registration.name` | Required integer 1 and nonempty string. |
| `source.key`, `source.provider`, `source.product`, `source.provenance` | Required nonempty strings. Keep the key stable for the same source identity. |
| `source.root` | Required nonempty path string. Relative paths resolve from the project root. |
| `source.kind` | `recording` (default), `purchased`, or `other`. |
| `coverage.start`, `coverage.end` | Required timezone-aware TOML datetimes. Start is inclusive and strictly before exclusive end. Applies to all inputs. |
| `inputs` | Required nonempty array of tables. |
| `inputs.instrument` | Required concrete catalogue reference. |
| `inputs.profile`, `inputs.content` | Required profile ID matching the instrument's exchange, instrument type and category, and content `trades` or `depth`. |
| `inputs.files`, `inputs.patterns` | String lists, each defaulting to empty. Supply at least one selection, relative to the source root. |
| `inputs.max_files` | Integer 1–10000, defaults to 1000. Bounds discovery. |
| `inputs.specification` | Optional 64-character lowercase hexadecimal fingerprint of applicable specification evidence. |
| `inputs.native_identity` | Optional table. Requires nonempty `namespace`, `publisher_field`, `instrument_field`, and `provenance` strings plus nonnegative integer `publisher_id` and `instrument_id`. Use different publisher and instrument fields. |

See the [synthetic registration recipe](../examples/catalogue/registration.toml) and the [Databento native identity example](../examples/bring-your-own-data/databento/registration.toml).
