# Normalization recipes

For Engine processing settings, see [Normalization runtime configuration](normalization-runtime.md). Source field interpretation belongs to [normalization profiles](../../workflows/normalization/author-profile.md).

A recipe selects registered raw inputs. Managed recipes live in `data/.config/workflows/data_engineering/normalization/` and take their names from their TOML filenames. Both recipe shapes use version `1`. The [normalization guide](../../workflows/normalization/index.md) demonstrates source selection. [Prepared batches](../data-formats/prepared-normalization.md) have their own Engine contract.

## Editable recipe

Source selection is the usual recipe shape. Forge matches complete registered archives against each request and selects whole overlapping inputs from the configured raw root. Source selection lets Forge derive the job and Engine dataset IDs and output paths.

| Field | Accepted values and constraints |
| --- | --- |
| `version` | Integer `1`, the default. |
| `interval.start`, `interval.end` | Required timezone-aware timestamps. Start must precede end. The common interval includes start and excludes end. |
| `requests` | Required array with at least one `[[requests]]` entry. |
| `requests[].exchange` | Required registered execution venue code matching `^[a-z][a-z0-9_]*$`. |
| `requests[].instrument_type` | Required. `spot`, `perpetual`, `future`, `option`, or `equity`. |
| `requests[].market_category` | Required. `spot`, `linear`, `inverse`, `option`, or `equity`. Equity must be used in both market and instrument fields together. |
| `requests[].data_type` | Required. `trades` or `order_book_l2`. The Engine calls the latter `depth`. |
| `requests[].scope.kind` | Required. `instrument`, `family`, or `base_coin`. Match the scope recorded for the archive. |
| `requests[].scope.identifier` | Required nonblank native scope identifier, such as Bybit `BTCUSDT` or OKX `BTC-USD` for an inverse futures family. |
| `requests[].source_options.feed` | Gate.io L2 only. `incremental` (default) or `snapshot`. |
| `requests[].source_key` | Optional nonempty key without whitespace. Required for equity. Requires `instrument` scope and selects the named external source. |
| `ranking_policy` | Policy name matching `^[a-zA-Z0-9_-]+$`. Defaults to `turnover`. |

Use the [complete Bybit TOML example](../../workflows/normalization/index.md#2-create-a-normalization-recipe) for a concrete instrument, or the [synthetic equity recipe](../../examples/catalogue/normalization.toml) for an external source with both feeds.

<a id="why-must-source-key-selections-use-instrument-scope"></a>
<a id="why-enforce-this"></a>
<a id="does-source-key-replace-the-exchange"></a>
<a id="complete-external-equity-recipe"></a>
<a id="what-does-this-request"></a>
<a id="what-are-the-prerequisites"></a>
<a id="does-the-recipe-create-these-prerequisites"></a>

### Source key requirements

Use the actual execution venue as `exchange` and the registered recording collection as `source_key`. For example, the [synthetic recording walkthrough](../../examples/catalogue/index.md) registers `synthetic-equity` for AAPL under `example_exchange`. Preparation requires a ready historical binding and verifies the registered profile fingerprint against the installed interpretation.

### Family-scoped archives

A family request matches registered family archives. Forge supplies available instrument and specification authority, and the Engine resolves concrete archive bindings. Missing or conflicting evidence can block preparation. See [family archives](../data-formats/prepared-normalization.md#family-archives).

<a id="are-stored-positions-copied-directly-into-engine-ranks"></a>
<a id="what-happens-when-an-instrument-is-not-ranked"></a>
<a id="what-if-no-published-ranking-exists"></a>
<a id="why-is-determinism-important"></a>
<a id="does-stream-rank-change-event-timestamps"></a>
<a id="does-a-higher-market-ranking-mean-higher-event-priority"></a>

### How stream ranks are assigned

The recipe selects archives. The ranking policy supplies ordering evidence from an existing published turnover snapshot, when available. Forge assigns dense ranks to the selected streams, with stable stream identity ordering for ties and unranked streams. Without a snapshot, it uses deterministic stream identity ordering. Preparation uses retained rankings and does not refresh provider statistics. See [ranking snapshot resolution](../data-formats/prepared-normalization.md#ranking-snapshot-resolution) and the [ranking policy reference](markets.md#workflow-priorities).

<a id="why-does-this-block-preparation"></a>
<a id="does-nominal-coverage-prove-every-event-exists"></a>
<a id="two-different-questions"></a>

### Nominal coverage checks

Preparation checks recorded archive intervals against the request and reports gaps, which can block the plan. Whole-file selection can extend past the requested interval. Nominal intervals and unknown evidence are distinct from observed [event coverage](../../concepts/time.md).

<a id="legacy-request-structure"></a>
<a id="example"></a>
<a id="does-this-example-guarantee-successful-preparation"></a>
<a id="are-these-ids-allocated-automatically"></a>
<a id="why-is-this-risky"></a>

## Legacy file-ID preparation

This shape selects explicit local archive and catalogue instrument IDs and prepares one content type. Use IDs from the intended registry and raw root. The example records IDs from a testing preview of the Bybit trades recipe. Use the corresponding local archive and instrument IDs from your registry, and choose unused Engine job and dataset IDs for your workspace.

```toml
version = 1
job_id = 4876983777293105620
dataset_id = 3120756497191505481
content_type = "trades"

[[inputs]]
task_id = 1
archive_id = 1834
instrument_id = 1999
```

<a id="top-level-fields"></a>
<a id="input-fields"></a>
<a id="accepted-compression-values"></a>
<a id="why-are-task-ids-unique"></a>
<a id="can-the-same-archive-appear-twice"></a>
<a id="do-legacy-profile-fields-override-the-modern-profile-catalogue"></a>

### Legacy request parameters

| Field | Accepted values and constraints |
| --- | --- |
| `version`, `ranking_policy` | Same defaults and constraints as source selection. |
| `job_id`, `dataset_id` | Required positive unsigned 64-bit Engine IDs. Preparation checks collisions against known registered and prepared artifacts. |
| `content_type` | Required. `trades` or `depth`. |
| `inputs` | Required array with at least one entry. Each archive/member pair may appear once. |
| `inputs[].task_id` | Required unique positive unsigned 64-bit integer. Forge assigns task IDs anew in the generated order. |
| `inputs[].archive_id`, `inputs[].instrument_id` | Required positive signed 64-bit IDs. The instrument must agree with resolved archive identity. |
| `inputs[].format_profile` | Optional nonblank historical profile field. |
| `inputs[].archive_member` | Optional nonempty historical member field. |
| `inputs[].source_compression` | Optional historical field. `plain`, `gzip`, `zip`, or `tar_gzip`. |
| `inputs[].stream_rank` | Optional historical field. Unsigned 32-bit integer. |
| `output` | Optional explicit output paths. If present, both paths are required. |

The optional historical input fields remain readable for compatibility. The current planner resolves the archive as a whole, derives ranks, and uses the installed profile catalogue and Engine content resolution. These fields do not provide overrides for those decisions.

<a id="what-is-the-staging-path"></a>
<a id="what-is-the-dataset-path"></a>
<a id="why-must-paths-be-separate"></a>
<a id="why-must-they-share-a-parent"></a>
<a id="can-i-overwrite-an-existing-dataset"></a>
<a id="do-i-need-explicit-output-paths-in-new-recipes"></a>

### Legacy output paths

`output.staging_path` and `output.dataset_path` are nonblank absolute or workspace-relative paths. They must be distinct, absent, and share a parent for atomic publication. By default, the publication goes to `datasets/normalized/<dataset_id>/` under the configured data root, with sibling staging path `.<dataset_id>.staging.<job_id>`.

## Validation rules

Unknown fields are rejected for both shapes. A recipe with `inputs` or `job_id` is read as explicit-input TOML. Structural validation checks fields, types, and local constraints. Preparation separately checks registered inputs, paths and checksums, instrument identity, historical specifications, and planned outputs. Persisted preparation uses the production Rust batch parser for executable plans. See [preparation results](../data-formats/prepared-normalization.md#preparation-status) and the [CLI reference](../cli/normalize.md).
