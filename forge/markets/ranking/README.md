# Comparable rankings with stored spot reference prices

Rankings can value native monetary turnover in one comparison currency. This is
**aggregate turnover valued at reference prices**, not a reconstruction of FX at
each execution time. Spot, perpetual and dated-future instruments can all be
ranked together. Only spot observations supply conversion prices.

## A valid policy

Create a named policy with `marketforge markets rank new automatic`. Names resolve
under `data/.config/markets/rankings/`. Existing files are never overwritten.
The generated version-2 template enables automatic conversion and remains strict
about incomplete universes. Here is an example that explicitly permits reported
exclusions:

```toml
version = 2
metric = "turnover_24h"
comparison_currency = "USDT"
exchanges = ["bybit"]
markets = ["spot", "linear", "inverse"]
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude"
allow_partial_universe = true
manual_preferences = []

[conversion]
automatic = true
valuation = "reference_price_turnover"
max_price_age_seconds = 3600
max_leg_skew_seconds = 300
max_hops = 2
bridge_assets = [] # any connecting spot asset; e.g. ["BTC", "ETH"] restricts bridges
prefer_same_exchange = true
allow_cross_exchange = false
allow_mixed_exchanges = false
source_exchanges = ["bybit", "binance", "okx", "gateio", "bitget"]
equivalent_currencies = []
# Optional, explicit assumption: equivalent_currencies = ["USD"] means USD=USDT.
# It does NOT include USDC, USDE, USD1, or any other asset.

# Optional override; use its actual observation time, not a copied stale example.
# [conversion.manual_rates.EUR]
# rate = "1.08"
# as_of = 2026-11-05T00:00:00Z
# source = "explicit user reference quote"
```

`max_price_age_seconds` may be omitted to reuse `max_age_seconds`.
`max_leg_skew_seconds` may be omitted to reuse `max_window_skew_seconds`.
Automatic source timestamps must also be consistent with the native turnover
window end within `max_window_skew_seconds`. Future market timestamps beyond
that existing clock tolerance are rejected. Manual timestamps must not be in the
future. Both fetch time and source/effective observation time must be fresh.
When source time is absent, the stored window end is used and identified as such;
fetch time is retained separately. No timestamp is invented.

An empty `bridge_assets = []` permits any intermediate asset connecting the source
and comparison currencies on the same exchange in stored spot observations. Only
neighbor intersections are searched, never arbitrary paths; unrelated pairs are
not loaded. A nonempty list is an ordered allowlist of explicit currency codes,
with at most 16 entries. Omitted settings retain the BTC/ETH default. Only one
or two spot instruments are supported. No unbounded search, repeated nodes, or
cyclic routes are used. Source exchange names form an allowlist in preference
order. `allow_cross_exchange` permits a path on another exchange; it does not
permit mixing exchanges within a path. `prefer_same_exchange=false`, mixed legs,
unsupported path limits, duplicate currencies/venues, invalid codes, unknown
settings, nonpositive/nonfinite rates and invalid timestamps are rejected.

## Precedence and deterministic selection

For each validated native monetary amount and denomination:

1. Identity conversion when it already uses the comparison currency.
2. A manual override for that currency (legacy or new form).
3. An explicit 1:1 equivalence assumption for that currency.
4. Eligible stored spot prices, if automatic conversion is enabled.

A stale manual override fails explicitly; it does not silently fall back to a
market rate. A currency must not appear in both old and new manual-rate forms.
A manual override may supersede an equivalence assumption, as stated by this
precedence. A comparison-currency manual rate other than 1 is rejected. Manual
rates and equivalence are scalar native-to-comparison rules, not synthetic graph
edges to be silently composed into market paths.

Among eligible market paths, selection order is:

1. Same target exchange before any other exchange.
2. One spot instrument before two.
3. Configured source-exchange preference.
4. Newest weakest leg (the oldest source timestamp on the path).
5. Configured bridge preference (nonempty allowlists only).
6. Stable source identity, observation ID and inversion direction.

Consequently, a local bridge beats a remote direct pair. Preferences never use
raw turnover, a final ranking position, an assumed stablecoin peg, or request
completion order. Mixed-exchange paths are never constructed. Fresh eligible
alternatives can replace stale/invalid candidates; an entirely unavailable path
produces an explicit exclusion. If an eligible foreign path exists but permission
is off, the reason is `cross-exchange conversion disallowed`.

A source price P for explicit base A / quote B supplies A→B=P and B→A=1/P. Symbol
parsing is not used for assets. Current active spot metadata and the latest
successfully collected retained observations are read; invalid latest prices are
not hidden by an automatic fallback to older prices. Derivative prices and the
latest-value cache do not supply FX edges. Price observations can be outside the
selected ranking subset. No network call occurs during build or inspection.

## Native monetary interpretation and precision

Missing native turnover is not zero. Zero monetary turnover is valid. Negative
and nonfinite amounts are rejected. Denominations come from the retained monetary
observation, never the derivative symbol or settlement asset. Existing reported
native turnover is not multiplied by contract value again. Contract-count-derived
notional needs verified quote-denominated contract value; a mismatched or unknown
value unit leaves turnover missing. New provider observations identify this check
with `current-metrics-v2-native-money`; no existing observations are rewritten.

Volume-derived estimated turnover remains governed separately by `estimates`.
A reported native turnover valued through a spot FX reference is not relabelled
as volume-derived estimated turnover.

Rates are calculated as exact rational products of Decimal prices, including
inversions. The exact numerator/denominator and original inputs are retained.
Rates and final scores are rounded once to **80 significant Decimal digits**;
terminal formatting alone abbreviates them further. Score calculation uses the
exact ratio rather than multiplying an already rounded displayed rate. Sorting
compares complete stored scores without ambient Decimal-context rounding.

For example, 100000 USDT/BTC and 92000 EUR/BTC give EUR→USDT=25/23. Valuing 92000
EUR gives exactly 100000 USDT. The repeating rate is never replaced by a short
rounded UI value during calculation.

## Storage and publication

Migration `0009_ranking_conversions.sql` adds:

- `ranking.conversions`: one immutable evidence record per distinct conversion in
  a snapshot, with method, rate, exact ratio and full source evidence.
- `ranking.conversion_inputs`: foreign-key references to the original retained
  spot observations, ordered by leg.
- Entry conversion references and frozen native monetary amounts/units.
- Publication completeness checks and guards preventing modification/removal of
  published evidence and referenced price observations.

Existing migration files and published snapshots are unchanged. Native
observations remain in `catalog`; calculated valuations remain in `ranking`.
Published source observations cannot be deleted or updated. Later refreshes append
new observations and can change a new ranking, not an existing pinned ranking.

Builds retain the existing REPEATABLE READ boundary: catalogue membership, native
turnover and spot sources come from one consistent database view. Native inputs
use indexed latest-observation lookups in bounded batches; catalogue and spot
source selection use keyset pages with no arbitrary 10,000-record cutoff. Resolved
rates and evidence are reused within each build. No task/network call is created
per instrument. A strict rejection or persistence failure rolls back the entire
snapshot, entries and evidence. Partial policies publish explicit exclusions;
a snapshot still requires at least one included entry.

Exclusions distinguish missing/invalid native monetary turnover, unavailable paths,
missing/invalid prices, stale/future observations, leg skew, native/FX window skew,
incomplete sources, and disallowed cross-exchange paths. No rate is invented to
force inclusion. Strict failures summarize reasons and suggest a spot refresh.

## CLI and migration from old policies

```sh
marketforge database migrate --testing
marketforge markets sync --testing
marketforge markets refresh --testing
marketforge markets rank new automatic
# Edit automatic.toml to select the intended universe / partial policy.
marketforge markets rank validate automatic
marketforge markets rank build automatic --testing
marketforge markets rank show SNAPSHOT_ID --testing
marketforge markets rank show SNAPSHOT_ID --testing --conversion bybit.spot.BTCEUR
marketforge markets rank show SNAPSHOT_ID --testing --conversion 2
marketforge --json markets rank show SNAPSHOT_ID --testing
```

Refresh prices before a new build when necessary:
`marketforge markets refresh --market spot --testing`. Existing statistics for the
selected derivatives must also satisfy freshness/completeness; no hidden refresh
is performed by ranking commands. Broad strict policies may reject incomplete
universes or unavailable conversions; use explicit reported partial behavior only
when intended.

Tables retain stored rank order and the snapshot comparison currency. Summaries
count methods **per included entry**, not per shared conversion object. Equivalence
assumptions are shown distinctly. `--conversion` inspects native amount, turnover
observation, exact ratio, path, original prices, inversion directions, exchanges,
source observation IDs and timestamps. Human row limits never truncate JSON;
JSON exposes entries and full shared evidence with string Decimal precision.
Older snapshots render as **legacy** when evidence was not retained; no path or
rate is inferred from an old score. The stream-rank resolver reads only the pinned
snapshot and requested concrete streams; it never rebuilds or revalues priorities.

Version-1 policies retain manual-only defaults and their existing keys:

```toml
version = 1
comparison_currency = "USDT"
conversion_as_of = 2026-11-05T00:00:00Z
conversion_source = "explicit manual provenance"
[conversion_rates]
EUR = "1.08"
```

To enable automatic conversion, back up your policy and change `version = 2`, or
keep version 1 and explicitly add `[conversion] automatic = true`. Legacy rates
still take precedence. Remove stale overrides intentionally; do not expect an
automatic rate to bypass them. Unknown fields remain errors. No policy is
rewritten automatically; the existing `default` policy was not modified.

## Verification on 2026-11-05

Deterministic tests cover identity/direct/inverse/bridge arithmetic, missing/zero/
invalid money and prices, age/future/skew rules, source preference, mixed-venue
rejection, manual/equivalence precedence, old policies/snapshots, atomic failure,
source retention, new-price reproduction, native contract interpretation, and
pinned mixed-stream resolution. One real-DB test ranks spot, linear perpetual,
inverse perpetual, linear future and inverse future together, using spot sources
outside the ranking subset and excluding a genuinely missing currency path.
A tempting derivative conversion price is deliberately ignored. Another test
publishes a 10,006-entry universe without truncation. Gate pagination regression
collects 205 distinct records in pages 100/100/5 and keeps Bybit's 1000 limit intact.
Tests use real guarded testing connections with rollback/isolated cleanup, not
exchange networking.

A bounded **live CLI** demonstration refreshed Bybit spot BTCUSDT / BTCEUR and
inverse BTCUSD, validated the new named policy `auto-fx-eur-demo-5192ff6a`, and
published snapshot `719d5d62-64a8-401d-9287-4383fd8de07a` in testing:

- 2 entries included, 1 excluded; identity 1, bridged 1.
- BTCEUR native amount: `1239550.0135397 EUR`.
- Spot observations: `14764` (BTCEUR price `71400.9 EUR/BTC`) and `14763`
  (BTCUSDT price `80459.4 USDT/BTC`).
- Path: EUR→BTC→USDT; exact rate: `268198 / 238003`.
- Stored score:
  `1396809.4290043422166947475451990100965113885119095137456250551463637012978827998 USDT`.
- BTCUSD remained excluded because the available USD conversion observations were
  stale under this policy. Its fresh derivative price was not used as an FX edge.
- The score was independently reproduced from retained database prices and native
  amount, after a further spot refresh. No snapshot rebuild or mutation occurred.

Inspect the retained demonstration:

```sh
marketforge markets rank show 719d5d62-64a8-401d-9287-4383fd8de07a --testing --conversion bybit.spot.BTCEUR
marketforge --json markets rank show 719d5d62-64a8-401d-9287-4383fd8de07a --testing
```

Its policy is a separate new local file; `default` was left unchanged. Its 300-second
price age requires refreshed spot observations for a future rebuild. This live
check is not exhaustive verification of every currency or exchange. It does not
establish historical execution-time FX, expired-market coverage, or market-price
availability. No historical archive download, Rust job, normalization or TUI was
run. Mixed-exchange legs, paths beyond one bridge, and automatic currency pegs are
intentionally unsupported.

Final validation: Ruff lint and formatting passed, strict Pyright reported zero
errors or warnings, and the complete maintained Forge suite passed: **397 tests,
zero skips**. Database integration tests used the guarded testing database.

Empty-bridge follow-up: unrestricted connecting-asset selection is covered by a
SOL bridge unit test and a real-database test using an isolated asset outside the
ranked universe. Ruff lint/format and strict Pyright pass; the full Forge suite
passes **400 tests with zero skips**. No live exchange requests were needed.

## Standard CLI workflow

```sh
marketforge init --testing
marketforge markets rank show turnover --testing --instrument-type perpetual --limit 30
marketforge markets rank update --testing
marketforge markets rank update turnover --testing
marketforge markets rank update volume --testing
marketforge markets rank show volume --testing
```

`init` creates missing application directories, applies pending Python core
migrations using the root's guarded testing connection (environment or
`.secrets/postgres.env`), syncs the supported catalogue and refreshes statistics
once, then attempts each standard policy independently. Existing files and
migration history are preserved. There is no production fallback or database
creation. A repeat invocation safely repeats the collections and publishes new
immutable snapshots when a policy can build.

`rank update` shares the refresh-and-publish service, with one refresh for either
one selected preset or both. `rank build NAME --testing` remains the explicit
lower-level operation on stored observations. The commands acquire workspace
ownership once; the services do not acquire nested locks. Collection retains its
existing bounded workers and shared per-exchange HTTP budgets.

Update/init builds pin turnover inputs, scope certification and spot conversion
prices to the new refresh ID. Failed scopes cannot fall back to older successful
observations. Partial policies may publish exclusions with partial coverage;
strict policies reject them. A failed build rolls back and leaves prior published
snapshots available. Each policy reports its publication ID or failure reason,
and any failed collection/ranking makes the command return nonzero.

`rank show` accepts names or UUIDs. Names resolve published snapshots by
`created_at DESC, id DESC`. Missing names explain the update command to run.
`--instrument-type spot|perpetual|future` filters SQL results before the human
limit, includes both linear/inverse perpetuals, and retains original positions.
JSON includes all matching rows regardless of `--limit`. Snapshot metadata and
conversion evidence remain available; policy name, creation time and partial
coverage are shown in the shared Tokyo Night table.

### Base-asset volume preset

The volume preset explicitly uses `metric = "volume"`, `volume_unit = "base_asset"`
and `group_by = "base_asset"`. Turnover conversion/comparison-currency settings
are rejected, and version 2 does not enable automatic FX for this metric.
The preset's default `estimates = "exclude"` omits reference-price estimates;
an explicit `estimates = "allow"` admits them with retained estimate evidence.

```sh
marketforge markets rank validate volume
marketforge markets rank build volume --testing
marketforge markets rank show volume --testing --base-asset ETH --limit 30
marketforge markets rank update --testing
marketforge init --testing
```

Quantities compare only within their base asset. ETHBTC, ETHUSDT and ETH
perpetuals/futures share an ETH group when their retained observations carry
reliable ETH quantities. BTC has its own rank 1. Groups sort by asset, then by
descending base quantity and instrument reference. The unique `position` is only
deterministic storage order; `group_rank` is the displayed rank. Filters preserve
both fields. `--base-asset` and `--instrument-type` filter before human limits;
JSON retains all matching rows. Grouped snapshots cannot resolve stream priorities
that require one comparable ordering across assets.

Migration 0010 adds base asset, group rank, unit, estimate flag and JSON derivation
evidence to ranking entries, protected by the existing immutable publication
triggers. Snapshot comparison currency is nullable for nonmonetary quantities.
Existing turnover snapshots retain their fields and semantics. `score` stores the
base quantity; monetary `native_turnover` is never used to store base volume.

Scoring uses the same retained observation for quantity, timestamps, source-window
checks and scope certification. Reported base volume matching its retained
instrument base is preferred, including zero. Otherwise derivative contract
counts require the instrument specifications saved with that observation:

- Base-denominated face value: count × base units per contract, without estimation.
- Quote-denominated face value: count × quote face value ÷ positive reference price,
  with a matching quote unit and retained price-time basis. This is an estimate,
  not execution-time base volume.

The observation ID, formula inputs, full saved specification, units, price-time
basis and estimate flag are exposed in `volume_evidence`. The observation and
publication evidence stay pinned when current catalog metadata changes.
`--conversion REFERENCE` (or unique storage position) also prints retained volume
evidence in human output; JSON always includes it. No FX graph is loaded.

Current provider mappings retain base volumes directly for Binance spot/linear
and inverse `baseVolume`, Bybit spot/linear `volume24h` and inverse `turnover24h`,
OKX spot `vol24h` and derivative `volCcy24h`, Gate `base_volume`/`volume_24h_base`,
and eligible Bitget `volume24h`. Contract fallback uses only the contract-count
fields already mapped by the provider and the saved face value/unit (including
OKX's `ctVal × ctMult`). Bybit inverse quote quantity is not treated as contracts.

Inputs lacking scope/unit evidence, contract multipliers/currencies, or a valid
matching reference price are excluded with specific reasons. Bitget reality spot
products have no trusted platform-local base-volume field; unknown Bitget spot
classifications and legacy observations lacking base-volume scope evidence also
remain excluded. Refreshing metadata/statistics can supply current evidence but
does not rewrite a published snapshot or fabricate missing upstream quantities.

## Filtering published snapshots

```sh
marketforge markets rank show turnover --testing --instrument-type perpetual --quote-asset USDT --limit 30
marketforge markets rank show turnover --testing --base-asset ETH --quote-asset USDT
marketforge markets rank show volume --testing --base-asset ETH --match '*BTC' --limit 15
```

Both turnover and volume inspection accept the same asset/symbol filters as
`markets list`. Asset matches are exact and case-insensitive. Native-symbol globs
are case-insensitive, support only `*` and `?`, and should be shell-quoted.
`%` and `_` remain literal. Filters combine with AND and run in SQL before the
human display limit. Snapshot positions and volume group ranks never change.
JSON includes every matching row regardless of that human limit. Name and UUID
lookup use the same inspection path. The ranking comparison currency does not
participate in instrument quote-asset filtering.

Migration 0011 freezes structured native identity and base/quote assets on new
ranking entries at publication. Older entries use structured specifications saved
with their volume evidence or retained observations where available. Volume
base-asset groups already have a dedicated frozen field and retain precedence.
Only fields lacking retained metadata fall back to current catalogue values.
Old `instrument_reference` strings remain pinned identities; their optional type
qualification and potentially dotted native symbols are not decoded into guessed
structured fields.

Each returned entry includes `metadata_sources` for symbol/base/quote, identifying
`publication`, `publication_group`, `retained_volume_observation`,
`retained_observation`, or `current_catalogue`. Human output warns when matching
older entries require current-catalogue fields. New publication metadata and
existing retained observation evidence are immutable; current-catalogue fallback
fields can change as the catalogue changes. Existing published rows are not
rewritten or backfilled by the migration.


## Snapshot maintenance deletion

```sh
mf markets rank delete turnover --testing          # preview every published turnover snapshot
mf markets rank delete volume --testing            # preview every published volume snapshot
mf markets rank delete SNAPSHOT_UUID --testing     # preview exactly one UUID, including drafts
mf markets rank delete --all --testing             # preview every snapshot, including drafts
mf markets rank delete turnover --testing --apply
mf markets rank delete volume --testing --apply
mf markets rank delete SNAPSHOT_UUID --testing --apply
mf markets rank delete --all --testing --apply
```

Exactly one policy/UUID/`--all` selector is required; `--testing` is mandatory.
Preview never deletes. Apply re-resolves and validates its selected set inside the
deletion transaction; a preview does not lock a later target set and no preview
file needs to be copied. UUIDs are explicit in both human and global `--json`
output, with policy, creation time, included/excluded counts, eligibility and
blocking dependencies. Already-absent UUIDs and zero-match policies are explicit.

Migration 0012 adds a UUID-scoped maintenance deletion function. It cleans entries,
conversion inputs, conversion evidence and then the snapshot, restoring its
transaction-local maintenance scope even on error. It never permits ordinary
published INSERT/UPDATE mutations, disables no triggers, and changes no old
migration. Direct ordinary deletion/update guards remain effective. Catalogue
instruments, retained observations, acquisition data/raw files, datasets and policy
TOMLs are preserved.

Managed job/batch references and dataset metadata/publication references block
deletion of the specific referenced UUID. Diagnostic preparation reports alone
are not execution dependencies. Mixed selections delete only eligible snapshots
transactionally, report exact deleted and blocked UUIDs, and return nonzero while
some selected snapshots remain blocked. A published status alone is not a blocker.
Existing workspace ownership and database advisory coordination protect inspection,
updates/builds, normalization preparation and deletion. New snapshots published
after a preview may be selected by a later policy/`--all` apply.

Markets name inspection and normalization now share one lookup: exact
`policy_name`, published status, `created_at DESC, id DESC`. Neither partial-universe
nor age filters apply to reuse. Turnover's policy metric is `turnover_24h`; it must
not be incorrectly filtered to `turnover`. Observation freshness is checked when
building a snapshot, not when reusing its ordering.
