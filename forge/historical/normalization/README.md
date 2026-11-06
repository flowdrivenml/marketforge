# Historical normalization preparation

Forge selects downloaded archives and supplies explicit source identities and
instrument specifications. It never opens containers, chooses format profiles or
archive members, runs the engine, creates normalized output directories, or registers
jobs/datasets. Archive/task planning uses the guarded testing connection in a repeatable-read,
read-only transaction. Normal preparation may first collect statistics and publish
a missing ranking through the existing ranking update service. Normalization
job/dataset registration remains deferred.

```sh
mf normalize new engine_test --from-acquisition engine_test
mf normalize validate engine_test
mf normalize prepare engine_test --testing --dry-run
mf normalize prepare engine_test --testing
mf normalize prepare another_name --testing --from-acquisition engine_test
# Machine-readable diagnostics and artifact paths (global option):
mf --json normalize prepare engine_test --testing
```

`new` creates an acquisition-shaped TOML in
`data/.config/historical/normalization/`, opens the configured editor, and validates
it. Acquisition seeding preserves scope and source options. Existing recipes and
legacy requests are not overwritten or shadowed.

Recipes retain `version`, `[interval]`, and `[[requests]]`. Preparation selects
completed local archives by exchange, market category, instrument type, scope,
data type, feed and half-open interval overlap. Dates select whole archives.
There is one task per local archive and distinct requested source stream;
overlapping requests do not duplicate tasks. Multiple archive members never cause
Python to expand a task. Trades and depth remain separate embedded jobs in one
batch; `order_book_l2` becomes output content type `depth`.

## Forge task representation, batch schema 3

See [the batch example](../../../docs/historical/normalize-batch-v3.example.json),
which includes a concrete inverse perpetual and a family-scoped futures input.

Concrete tasks explicitly contain `exchange`, `instrument_type`, `market_category`,
native `symbol`, `base_asset`, `quote_asset`, and numerical `instrument`
specifications. The identity is matched exactly against the registered catalogue.
Assets come from metadata, never symbol splitting. `instrument` contains decimal
strings for `tick_size` and applicable `contract_value`, plus `contract_value_asset`.
Contract values are null for spot and where the registered quantity unit does not
use contracts and no contract multiplier is registered. Required contract-unit
specifications must be available. Nested `instrument_kind`/`contract_kind` are
absent; top-level classifications are authoritative.

Family/base-coin tasks contain an explicit `scope` with `kind` and `identifier`,
`symbol: null` and `instrument: null`. Family identifiers are never presented as
contract symbols. Assets are supplied only when matching catalogue metadata
unambiguously agrees; unavailable or conflicting assets are null with report
warnings. No contract-specific specifications are borrowed from a member.

All tasks omit `instrument_id`, `format_profile`, `profile_fingerprint`,
`archive_member`, and `source_compression`, including null placeholders. Internal
instrument IDs remain in report provenance for concrete selections and ranking.
The engine owns format-profile discovery, compression/container handling, member
selection, interpretation, and archive-content-to-instrument mapping. Neither
missing profiles nor missing family-member mappings block Forge preparation.
The report explicitly labels archive-content resolution as delegated, not validated.

Source feeds are explicit as `source_options: {"feed": "snapshot"}` or
`{"feed": "incremental"}` when registered, in addition to distinguishing stream IDs.
`task_id`, `stream_id`, and `stream_rank` remain operational identifiers.

## Ranking and publication

The latest published snapshot of `ranking_policy` (default `turnover`) is pinned
for preparation using the same exact policy-name resolver as Markets inspection.
Partial and older publications remain reusable without a freshness cutoff. Concrete
ranked streams come first by source position; ties use stream identity ordering.
Unranked and unresolved family streams follow by deterministic identity ordering.
Ranks are dense `1..N` within each job and all archives of a stream share its rank.
The published ranking is never modified. If no snapshot exists, normal preparation refreshes statistics once and builds
only the required policy through `refresh_and_publish`, then pins that publication.
A collection/build/policy/database failure is an actionable error, never a silent
alphabetical fallback. Grouped volume policies do not provide comparable priorities
across assets and are rejected.

Set the optional policy before TOML sections, for example:

```toml
ranking_policy = "turnover"
```

Dry-run never collects or writes rankings. With no snapshot it reports
`would_create`, marks ranking pending, and continues archive planning with a
provisional deterministic order (`order_final = false`). Those provisional task
ranks are not published or claimed as final. With an existing snapshot, dry-run
loads its entries and computes final planned ranks.

A family rank describes the requested source stream. The engine must define how
that rank applies when expanding a family into multiple concrete streams. Python
has not resolved or ranked those individual contracts.

Publication stages both files privately and exposes them by one directory rename:

- `data/runs/<run_id>/normalize-batch.json`: Forge schema 3, batch kind, status,
  run identity/paths, ranking snapshot/policy, requested interval and all jobs.
- `data/runs/<run_id>/prepare-report.json`: report schema 1, status, associated
  batch path, summaries and counts, request outcomes, provenance, exclusions,
  metadata warnings, coverage gaps, ranking fallback, and engine compatibility.
  Task definitions are not duplicated here.

Missing files, size mismatches, missing/ambiguous concrete catalogue identities,
conflicting metadata, missing required specifications and nominal coverage gaps
produce precise report diagnostics and partial status. Successful tasks survive;
the CLI returns nonzero for partial preparation. Output paths/content type remain
inside each job. IDs use the existing collision-checked, unreserved testing-ID
logic. No staging or dataset output directory is created.

Dry-run uses the same archive planning and available snapshot priorities, returning
diagnostics and planned paths without publishing either artifact. Write failures publish neither artifact.
Existing preparation runs and published rankings are never rewritten.
Storage dependency inspection continues to find input paths in schema 3 batches,
earlier batch shapes, legacy job files and saved input selections.

## Rust compatibility and legacy configuration

Forge batch schema 3 is separate from Rust normalization protocol 3. Neither the
batch nor an embedded job is accepted by the current Rust loader. It still requires
job-level `streams`, profile/container fields, instrument IDs and the older nested
instrument shape, and rejects task-level `stream_rank` and explicit scope tasks.
New jobs carry no `protocol_version`. Reports always set `engine_ready: false`
and explain compatibility, separately recording binary availability. There is no
adapter, invocation suggestion or engine execution.

Jobs preserve `operation`, job/dataset IDs, `ordering`, and `output` destinations.
Ordering remains required by the current Rust job contract: `[ordering]` in
`historical/normalize.toml` is only a commented future setting. Runtime resources
and integrity policy remain in that TOML and are not duplicated in preparation.
The unchanged Rust wire contract and its examples are documented separately in
[normalization-runtime.md](../../../docs/historical/normalization-runtime.md).

Existing `normalization_jobs/NAME.toml` manual configurations remain readable,
including their old profile/member/compression/rank fields. New preparation ignores
those engine-owned selections and resolves ranks through the same ranking policy;
multiple legacy member selections of one archive collapse to one task. Explicit
job/dataset IDs and output overrides remain supported and collision-checked.
New manual preparations also publish the current batch/report pair in a run
directory. Existing `.jobs/normalize/JOB.json` artifacts remain readable by storage
and collision inspection; they are not rewritten.

## Bitget historical spot pair identities

Archive selection continues to use the recipe's historical identifier, such as
`BTC/USDT`. Exact catalogue-symbol lookup always runs first. Only an exact miss
for an instrument-scoped Bitget spot/spot archive enables the `BASE/QUOTE` fallback.
It requires exactly two nonempty asset components and matches catalogue assets
case-insensitively, following the existing repository asset-filter semantics.
The fallback accepts exactly one match, never strips global punctuation, and
never applies to derivatives, other exchanges, or unresolved family scopes.
Missing matches report the original identifier and parsed assets; ambiguous
matches report candidate identities. An ambiguous exact lookup remains blocked.

Tasks use the matched catalogue's native symbol, registered asset spelling and
ordinary instrument specifications, while retaining the original storage path.
Ranking uses the matched instrument ID. Each selected archive's report provenance
contains `identity_resolution`, including `archive_identifier`, resolution `method`
(`exact_symbol`, `bitget_spot_asset_pair`, or `requested_scope`), and the resolved
symbol for concrete instruments. Pair fallback also records `parsed_assets`.
Recipes, archive registrations and downloaded bytes are not changed.


## Ranking decisions and deletion coordination

The report's `ranking_resolution` records `reused`, `created`, or `would_create`,
policy, pinned UUID, creation time, partial-universe flag, matched/unmatched stream
counts and automatic-creation outcome. A failed creation returns a structured CLI
`ranking_resolution` with `decision = "failed"` and the actual collection/build
errors; no batch/report pair is published for that failure.

Existing workspace ownership remains nonblocking. Database-wide advisory ownership
coordinates preparations, ranking updates/builds, inspection and deletion across
workspaces sharing the testing database. Normal preparation acquires exclusive
ownership, rechecks availability, and holds it through collection (if needed),
entry loading and atomic artifact publication. Dry-run and inspection use shared
ownership. Archive planning still runs in its own read-only repeatable-read
transaction. An eligible deletion cannot run during entry loading or publication;
subsequent deletion sees a published batch's concrete snapshot reference and blocks.
A competing preparation rechecks after obtaining ownership and reuses the first
preparation's publication. No second refresh/build implementation is introduced.
