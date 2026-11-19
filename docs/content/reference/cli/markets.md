# Markets CLI reference

Use `marketforge` or `mf`; put global `--json`, `--project-root PATH`, and `--config PATH` **before** `markets`. `--config` selects application settings, not a ranking policy. All registry operations below require an explicitly configured, identity-verified `marketforge_test` database; see [prerequisites](../../workflows/acquisition/index.md#prerequisites). `--testing` is a real destination guard, not a dry run.

## Synchronization and statistics refresh

```text
marketforge markets sync --testing [--exchange EXCHANGE ...] [--market MARKET ...] [--instrument SYMBOL] [--silent]
marketforge markets refresh --testing [--exchange EXCHANGE ...] [--market MARKET ...] [--instrument SYMBOL] [--silent]
```

`sync` retrieves metadata and writes catalogue/membership records. `refresh` retrieves statistics, writes retained observations and publishes successful scopes' latest values. A current successfully completed metadata sync for the matching selection is required before refresh. Metadata is not implicitly synchronized.

| Option | Default / allowed values |
| --- | --- |
| `--testing` | Required on every sync/refresh/list/show. No production fallback. |
| `--exchange` | Repeatable: binance, bybit, okx, gateio, bitget. Omitted means all supported providers. |
| `--market` | Repeatable: spot, linear, inverse. Omitted means supported categories for selected providers. |
| `--instrument` | Exact native symbol, optional; does not change market/type identity. |
| `--silent` | Suppress collection progress; final result/failures remain. JSON also suppresses progress. |

With no scope filters, 14 current-market scopes are collected: all five exchanges × spot/linear/inverse except Bitget inverse. Options are absent. Linear/inverse include supported dated-future endpoints, with Gate inverse perpetual only. Unsupported-only filter selection errors. See the [metadata support matrix](../../workflows/markets/instrument-discovery.md#synchronize-the-catalogue).

```bash
marketforge markets sync --exchange bybit --market spot --instrument BTCUSDT --testing
marketforge --json markets refresh --exchange bybit --market spot --instrument BTCUSDT --testing
```

These are network/registry-writing commands. Independent scope failures continue and make the result partial. Output includes collection status, `refresh_id` (also used for the metadata-sync identifier), the `metadata` flag, and per-source states/counts/errors, with type counts where relevant. Human progress is on stderr; JSON is one stdout object. Failure can leave retained observations but no newly published latest cache for that scope.

The parser also accepts `--limit` and `--sort` on sync/refresh, but they are hidden and do **not** limit or sort network collection. Collection has no asset/glob/type selector; use the documented scope and exact-symbol filters.

## Stored market inspection

```text
marketforge markets list --testing [--exchange EXCHANGE ...] [--market MARKET ...] [--instrument SYMBOL]
    [--base-asset ASSET] [--quote-asset ASSET] [--match GLOB] [--sort identity|turnover|freshness] [--limit N] [--silent]
marketforge markets show REFERENCE --testing [--exchange EXCHANGE ...] [--market MARKET ...] [--instrument SYMBOL] [--silent]
```

Both read stored data only, without HTTP or registry mutation. `REFERENCE` is required for show: use the exact returned `exchange.market.symbol`, or `exchange.market.instrument_type.symbol` when type-qualified. Ambiguous/missing references fail.

| Option | List behavior |
| --- | --- |
| `--instrument` | Exact native symbol; combines with other filters. |
| `--base-asset`, `--quote-asset` | Exact case-insensitive instrument assets. Quote is unrelated to a ranking comparison currency. |
| `--match` | Case-insensitive full-native-symbol glob, `*`/`?` only; shell-quote it. `%`/`_` are literal. |
| `--sort` | `identity` default; `turnover` groups by native denomination then descending amount; `freshness` orders measured/window-end time descending. |
| `--limit` | 50 default, 1–10000. Applies to both human and JSON lists. Results include `truncated`. |

```bash
marketforge markets list --testing --exchange okx --market linear --sort turnover --limit 30
marketforge markets list --testing --base-asset ETH --quote-asset USDT --match 'ETH*'
marketforge --json markets show bybit.spot.BTCUSDT --testing
```

No `--instrument-type` exists on `markets list`; asset/glob filters exist only on list, not show/sync/refresh. Show accepts hidden `--limit` and `--sort` through the shared parser. `--limit` is validated in the same 1–10000 range but detail uses an internal two-row lookup to detect ambiguity. `--sort` is forwarded to that lookup and does not bypass ambiguity checks.

JSON returns `instruments`, `truncated`, and a semantics note. Rows expose reference/type/assets/status/specifications, available native quantities/prices, observation/source/fetch/window times, interpretation evidence and staleness. Human details group those fields. Decimal quantities serialize as strings; missing is null. The one-hour browse staleness rule is based on fetch time and does not certify ranking comparability. See [statistics semantics](../../workflows/markets/market-statistics.md).

## Ranking command hierarchy

Policies resolve under `data/.config/markets/rankings/NAME.toml`; names optionally include `.toml`, not paths. Except update's standard-only selector, use safe managed names. See [full TOML contracts](../../configuration/markets.md).

| Syntax | Purpose, prerequisites and effects |
| --- | --- |
| `markets rank new NAME` | Writes a version-2 automatic turnover template exclusively; no editor, database or network. It is not a volume template even if named volume. |
| `markets rank validate NAME` | Reads and validates policy, units/conversion controls and manual provenance structure; no database/network or proof of input freshness. |
| `markets rank build NAME --testing` | Reads retained observations and writes one immutable published snapshot. No network/implicit refresh; failure rolls back this build. Returns snapshot UUID. |
| `markets rank list --testing` | Reads published snapshots newest first; output `rankings` and `truncated`. |
| `markets rank show ID_OR_NAME --testing` | Reads published snapshot by UUID, or latest published exact policy name ordered by creation time then UUID. Name lookup does not filter by age/partial coverage. |
| `markets rank update [turnover|volume] --testing` | One global statistics refresh, then independent publication of named standard policy or both. Requires existing policy files and matching metadata syncs; writes and uses network. Custom names not accepted. |

Every ranking subcommand accepts `--silent` and `--limit N` (50 default, 1–10000). Limit affects snapshot listing and human show; it is accepted but does not narrow build/update evaluation or file operations. On rankings, `--silent` suppresses progress for update; final results still print. File commands `new`/`validate` do not accept `--testing`; database commands require it.

```bash
marketforge markets rank new activity
marketforge markets rank validate activity
marketforge markets rank build activity --testing
marketforge markets rank list --testing --limit 10
marketforge markets rank show activity --testing --limit 30
```

For a volume policy, write the [volume TOML](../../configuration/markets.md#complete-base-asset-volume-policy) and validate it; no additional metric flag exists on build.

## Ranking inspection options

```text
marketforge markets rank show ID_OR_NAME --testing
    [--instrument-type spot|perpetual|future] [--base-asset ASSET] [--quote-asset ASSET]
    [--match GLOB] [--limit N] [--all] [--excluded] [--conversion REFERENCE_OR_POSITION] [--silent]
```

Filters combine with AND before human limits. No `--exchange`, `--market`, or `--instrument` is registered on rank show. Use returned references for evidence selection; universe filters belong in the policy.

- `--instrument-type` includes both linear/inverse entries of that type.
- Asset/glob filters have the same semantics as stored markets list. They preserve pinned positions and volume `group_rank`.
- `--limit` caps human included/excluded rows, not JSON entries.
- `--all` removes human row limits; `--excluded` additionally shows individual exclusions rather than only grouped reasons.
- `--conversion` inspects one matching included entry's native amount/path/rate/source timestamps or volume derivation evidence. It accepts a full stored reference or unique storage position. A filtered-out/unavailable entry errors. JSON already includes complete evidence for matching entries.

```bash
marketforge markets rank show activity --testing --excluded --all
marketforge markets rank show activity --testing --instrument-type perpetual --quote-asset USDT
marketforge markets rank show activity --testing --conversion 1
marketforge --json markets rank show activity --testing --limit 1
```

The last command still returns all matching entries; its limit is human-only. To pin inspection, replace the policy name with a returned snapshot UUID. Output includes snapshot metadata, included/excluded entries, conversion evidence and metadata provenance. No refresh, rebuild, revaluation or renumbering occurs during inspection. Old entries can require explicitly identified current-catalogue fallback for missing metadata.

## Standard updates

```bash
marketforge markets rank update turnover --testing
marketforge markets rank update volume --testing
marketforge markets rank update --testing
```

Requires `turnover.toml`/`volume.toml` as selected; missing files are errors, not auto-created presets. Refresh has no exchange/market options on update and runs globally. Each policy build is pinned to that refresh and reports a publication ID or failure. A successful publication can coexist with a nonzero overall partial result. Existing published snapshots are unchanged.

## Snapshot deletion

```text
marketforge markets rank delete ID_OR_NAME --testing [--apply] [--silent] [--limit N]
marketforge markets rank delete --all --testing [--apply] [--silent] [--limit N]
```

Exactly one selector is required. A name selects all published snapshots for that policy; UUID selects exactly one snapshot, including a draft; `--all` selects all snapshots including drafts. Without `--apply`, it is a nondeleting preview. With apply, selection is re-resolved and eligible snapshots plus owned entries/conversion evidence are deleted transactionally; no saved preview file or interactive prompt is required. New publications after a name/`--all` preview can be included in a later apply.

Concrete job/dataset dependencies block their referenced UUIDs. A mixed set can delete eligible snapshots while returning blocked ones and exit 1. Already absent UUIDs/empty selections are explicit. Policy TOML, catalogue, observations, acquisition records/files and datasets are not deleted. Ordinary published snapshot mutation remains guarded. Use global JSON for exact UUIDs, eligibility and blockers before deliberate application.

## Exit behavior and common errors

| Code | Meaning |
| --- | --- |
| 0 | Completed operation/inspection or successful preview. Inspect coverage and age separately. |
| 1 | Partial sync/refresh, incomplete standard update, or blocked snapshot deletion. Successful independent writes can remain. |
| 2 | CLI parse, configuration, guarded database, missing/ambiguous identity, strict build rejection, filesystem/setup or ownership error handled by the CLI. |
| 130 | Handled cancellation/interruption. Hard termination cannot promise finalization. |

Global JSON errors handled by the CLI are on stderr; collection partials are structured stdout results with nonzero exit. Unsupported scope, missing current metadata membership, stale source windows, no eligible conversion, or an empty comparable universe should be resolved from their explicit diagnostics. `rank validate` cannot establish that observations/prices exist. See [ranking troubleshooting](../../workflows/markets/rankings.md#troubleshooting-and-maintenance).
