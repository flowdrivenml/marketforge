# Markets and ranking commands

Use the [command-selection guide](../../workflows/markets/index.md) for a workflow and the [policy reference](../configuration/markets.md) for TOML. Put global options before `markets`, as in `mf --json --project-root . markets list --testing`. Global `--config` selects application settings. Ranking TOMLs live separately under `data/.config/markets/rankings/`.

## Command reference

The syntax below follows `mf markets`. Every command supports `--help`. Database commands accept mutually exclusive `--testing` and `--production`, with production as the default. The examples explicitly select testing.

| Syntax | Effect and result | Example after `mf markets` |
| --- | --- | --- |
| `sync [COLLECTION_FILTERS] [--silent] [ENV]` | Fetch public instrument metadata. Store catalogue revisions, scope membership and source outcomes. Existing absent instruments remain stored. | `sync --exchange bybit --market spot --instrument BTCUSDT --testing` |
| `refresh [COLLECTION_FILTERS] [--silent] [ENV]` | Fetch ticker statistics for compatible synchronized membership. Append observations and publish the latest view for successful scopes. | `refresh --exchange bybit --market spot --instrument BTCUSDT --testing` |
| `list [COLLECTION_FILTERS] [IDENTITY_FILTERS] [--sort MODE] [--limit N] [ENV]` | Browse stored instruments and current observations. Read-only and offline from venues. | `list --testing --base-asset ETH --match 'ETH*' --limit 20` |
| `show REFERENCE [COLLECTION_FILTERS] [ENV]` | Read a single stored Markets identity, its specifications and latest observation. | `show bybit.spot.BTCUSDT --testing` |
| `rank new NAME` | Exclusively create a version-2 turnover policy. No registry or network access. Open the result in your editor separately. | `rank new activity` |
| `rank validate NAME` | Parse and validate local policy settings and provenance fields. No collection or snapshot write. | `rank validate activity` |
| `rank build NAME [ENV]` | Evaluate a saved policy against retained observations and write a snapshot. Returns its UUID. No exchange request. | `rank build activity --testing` |
| `rank list [--limit N] [ENV]` | List published snapshots by creation time descending, then UUID ascending. Policies without snapshots do not appear. | `rank list --testing --limit 10` |
| `rank show NAME_OR_UUID [IDENTITY_FILTERS] [--instrument-type TYPE] [--conversion ENTRY] [--limit N] [--all] [--excluded] [ENV]` | Read a published snapshot and its retained evidence. Name selects the latest publication for that policy. | `rank show activity --testing --excluded` |
| `rank update [turnover\|volume] [--silent] [ENV]` | Refresh the supported statistics scopes once and independently publish the chosen standard policy or both. | `rank update turnover --testing` |
| `rank delete (NAME_OR_UUID\|--all) [--apply] [ENV]` | Preview or delete selected snapshots and their owned evidence. See [maintenance](#snapshot-deletion). | `rank delete activity --testing` |

Managed names use letters, digits, underscores and hyphens and start with a letter or digit. `new`, `validate`, `build`, and named `show` accept an optional `.toml` suffix. `update` accepts only the two literal standard names, without suffixes. `delete` treats a non-UUID as an exact stored policy name, including any suffix supplied. A name is never an explicit file path.

The parser also accepts `--silent` outside collection and standard update, where it has no current effect. Ranking leaves all accept `--limit`, but only list and human show use it. Market sync, refresh and show accept hidden `--limit` and `--sort`; collection ignores both and show uses at most two rows internally to check ambiguity. Sorting has no practical effect on a unique show result.

<a id="supported-exchanges"></a><a id="supported-market-categories"></a>

## Market command options

| Option | Commands | Match |
| --- | --- | --- |
| `--exchange CODE` | Sync, refresh, list, show | Repeatable alternatives from `binance`, `bybit`, `okx`, `gateio`, `bitget`. Lowercase parser choices. |
| `--market CATEGORY` | Sync, refresh, list, show | Repeatable alternatives from `spot`, `linear`, `inverse`. Lowercase parser choices. |
| `--instrument SYMBOL` | Sync, refresh, list, show | Exact case-sensitive native symbol. One value. |
| `--base-asset ASSET`, `--quote-asset ASSET` | Market list, rank show | Exact case-insensitive asset match. A quote-asset filter uses instrument identity. The ranking comparison currency remains the policy's currency. |
| `--match PATTERN` | Market list, rank show | Case-insensitive whole native symbol glob. `*` matches zero or more characters and `?` one. Other characters, including SQL `%`, `_`, brackets and `!`, are literal. Quote patterns in the shell. |
| `--instrument-type TYPE` | Rank show | One of `spot`, `perpetual`, `future`. Market list exposes type in results and has no type filter. |

Different filter categories intersect. With collection filters omitted, sync/refresh visit all 14 supported combinations. A supplied symbol filters matching metadata or statistics within each selected scope. See the [collector matrix](../../workflows/markets/instrument-discovery.md#synchronize-the-catalogue).

Market `show` accepts `exchange.market.symbol` or `exchange.market.instrument_type.symbol`. Use the qualified form for collisions and copy native punctuation exactly. The separate [manual catalogue](catalog.md) uses `exchange.instrument_type.market_category.symbol`. Market list defaults to ascending identity. `--sort time` uses the retained source observation timestamp. `--sort freshness` uses the stored metric window end. `--sort turnover` orders within native denominations. `--order asc|desc` changes the direction of the selected field. The stale label uses a separate fetch-time test. [Sorting fields and ranking groups](sorting.md). Filters precede the limit. Market list and rank list default to 50 rows with bounds 1–10000 and return `truncated` in JSON. These commands expose no offset or cursor.

## Ranking inspection

`rank show` retains the published score, position and group rank. Identity filters select a view of included and excluded entries. The snapshot header remains publication-wide, while displayed entry and exclusion summaries use matching rows. `--limit` caps human included/excluded tables separately. `--excluded` shows individual exclusions in addition to grouped reasons. `--all` removes human row caps. JSON returns all matching entries, including exclusions and evidence, regardless of these three options. The separate `conversions` array contains the snapshot-wide conversion records.

`--conversion ENTRY` accepts the stored full instrument reference or unique storage `position` in the matching view. A grouped-volume `group_rank` can repeat across assets and is unsuitable as a numeric selector. An excluded entry can be selected by reference. The option validates the selection even in JSON. It adds human evidence detail without slicing JSON to the chosen entry.

```bash
mf markets rank show SNAPSHOT_UUID --testing --base-asset ETH --match 'ETH*' --excluded | cpout
mf markets rank show SNAPSHOT_UUID --testing --conversion 1 | cpout
mf --json markets rank show SNAPSHOT_UUID --testing | cpout
```

## Snapshot deletion

| Selector | Selected records |
| --- | --- |
| Exact snapshot UUID | One stored snapshot, if present |
| Exact policy name | All published snapshots for the name |
| `--all` | All stored snapshots in the selected registry |

Preview is default. Review exact UUIDs, blockers, and eligible counts. Apply re-resolves the selector and dependencies under a maintenance lock and deletes eligible snapshots transactionally. The CLI has no saved plan or additional prompt. Use an exact UUID to keep a later apply scoped to the reviewed snapshot. Both database environments are supported.

```bash
mf markets rank delete SNAPSHOT_UUID --testing | cpout
mf markets rank delete SNAPSHOT_UUID --testing --apply
```

Deletion removes the snapshots and their owned entries and conversion evidence. Policy TOMLs, catalogue instruments, observations, and historical data remain. Concrete references in managed jobs, run evidence, registered datasets or historical jobs can block individual snapshots. The service inspects known JSON references and skips unreadable local evidence. A mixed apply can remove eligible rows and return 1 because others were blocked. An absent exact UUID reports `already_absent`; an unmatched name gives zero matches.

## Results and exit codes

| Command family | Human result | Global JSON |
| --- | --- | --- |
| Sync / refresh | Per-scope counts, timings, errors and final status | Collection record ID, scope results, version and status |
| Market list / show | Instrument rows or individual fields with units and freshness | `instruments` and `truncated`; missing fields remain null |
| Rank new / validate / build | Created path, policy summary or snapshot UUID | `configuration`, validated `policy`, or `snapshot_id` respectively |
| Rank list / show | Snapshot inventory or ranked rows and evidence | `rankings` plus truncation, or snapshot header and matching `entries` |
| Rank update | One refresh outcome and separate publication outcomes | Refresh, failed scopes, and per-policy snapshot/error |
| Rank delete | Selection, eligibility, blockers and applied results | Per-snapshot results and counts, including exact IDs |

JSON suppresses live progress and carries the result on stdout. Decimal amounts can be serialized as strings to preserve precision. Preflight or handled command errors use stderr under the [shared CLI rules](index.md). A successful operation returns 0, including a deliberately permitted partial-universe build. Collection with failed scopes and ranking update with a failed/partial refresh or policy return 1. Deletion with blockers returns 1 even when it also removes eligible snapshots. Cancellation returns 130. Argument or handled preflight failures return 2. A failed scope or nonzero update can leave successful independent work intact.
