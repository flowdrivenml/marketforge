# Build and inspect market rankings

Rankings publish an immutable view of eligible retained observations under a named TOML policy. They describe measured activity, not trading signals. Changing current statistics or a policy does not rewrite an existing snapshot.

## Choose the comparison

**Turnover** (`metric = "turnover_24h"`) compares monetary activity in one explicit currency. Optional stored spot prices value aggregate native turnover at reference prices; this is not reconstruction of FX at each trade. Derivative last prices are not conversion sources. USD/USDT/USDC equivalence is never automatic.

**Volume** (`metric = "volume"`) compares base-asset quantities **within each base asset**. ETH quantities can share an ETH group across spot, perpetuals and futures; BTC has a different group and its own rank 1. Unique storage `position` is not a global cross-asset score; `group_rank` is the within-asset rank. See the [complete policies](../../reference/configuration/markets.md).

## Build an instrument-scoped turnover snapshot

Create a policy and open it in Neovim.

```bash
mf markets rank new activity
nvim data/.config/markets/rankings/activity.toml
```

Replace the template with the [minimal turnover example](../../reference/configuration/markets.md#complete-examples). It selects the same Bybit BTCUSDT spot instrument as the discovery and refresh guides and compares its native USDT turnover. To compare a broader currency universe, choose the explicit automatic or manual conversion example and make sure its references are available.

```bash
mf markets rank validate activity | cpout
mf markets rank build activity --testing | cpout
mf --json markets rank list --testing | cpout
```

Build reads retained observations and writes a published snapshot. Use fresh observations from the [matching refresh](market-statistics.md#refresh-a-synchronized-selection). Policy validation checks local fields. Build additionally checks catalogue membership, observation evidence and eligibility. A broad policy requires recent full-scope certification unless partial coverage is allowed. An explicit instrument list keeps this example narrow without relaxing data-quality checks.

## Build grouped base volume

Use the [grouped-volume example](../../reference/configuration/markets.md#complete-examples) in a separate policy named `base_activity`. That example selects BTCUSDT and ETHUSDT. Repeat synchronization and refresh for ETHUSDT before building so both groups have observations.

```bash
mf markets sync --exchange bybit --market spot --instrument ETHUSDT --testing | cpout
mf markets refresh --exchange bybit --market spot --instrument ETHUSDT --testing | cpout
mf markets rank new base_activity
nvim data/.config/markets/rankings/base_activity.toml
mf markets rank validate base_activity | cpout
mf markets rank build base_activity --testing | cpout
mf markets rank show base_activity --testing --base-asset ETH | cpout
```

## Inspect a pinned result

```bash
mf markets rank show activity --testing --excluded --all
mf markets rank show activity --testing --instrument-type perpetual --quote-asset USDT
mf --json markets rank show activity --testing
```

A name resolves the latest published snapshot by creation time then UUID, both descending. The JSON inventory includes UUIDs even when the narrow terminal table omits them. Use a returned `SNAPSHOT_UUID` to pin one publication; replace that placeholder in commands. Name resolution applies no age/partial-universe cutoff, so inspect creation time and coverage rather than assuming latest means fresh.

```bash
mf markets rank show SNAPSHOT_UUID --testing --conversion 1
mf markets rank show SNAPSHOT_UUID --testing --base-asset ETH --match 'ETH*'
```

`--conversion` accepts a stored full instrument reference or unique storage position. It exposes native amount, rate, path, timestamps and source observations; volume snapshots expose derivation evidence. Human `show` defaults to at most 50 included rows and grouped exclusion reasons; `--excluded` adds excluded rows and `--all` removes row limits. JSON always returns all matching entries and evidence, regardless of human row limit. Filters preserve original positions/group ranks and do not rebuild the snapshot.

A filtered volume view can begin at a stored group rank greater than 1. In this illustrative excerpt, removing the first ETH row with a filter leaves ranks 2 and 3.

```text
Base asset  Group rank  Instrument
ETH         2           ETH-USDT-SWAP
ETH         3           ETHUSDC
```

## Conversion and volume evidence

Use the [ranking rate reference](../../reference/configuration/markets.md#rate-selection-and-precision) to interpret manual overrides, venue preference, and path freshness. Ranking conversion values aggregate statistics. [Historical conversion](../conversion.md) values dataset events using a separate Engine contract.

`estimates` controls volume-derived turnover estimates such as OKX linear quantity × last; converting reported turnover by FX is a separate valuation convention. Manual preferences can move specified turnover instruments ahead of score order; ties otherwise use score then stable reference. Inspect the policy before treating a rank as pure descending activity.

Volume prefers reliable reported base quantity, including zero. A saved base-denominated contract face value permits exact contract-count conversion; quote-denominated face value divided by a matching positive reference price is estimated. Missing specification/scope evidence causes exclusion. Policies exclude estimates by default. Each result retains its formula, inputs, observation and specification evidence.

New snapshots freeze the policy document and hash, algorithm, observation and conversion evidence links, scores, order, exclusions, and identity metadata. Later edits and refreshes affect future builds. Inspection reads the published evidence. Older snapshots may fall back to retained observation evidence or current catalogue fields; JSON `metadata_sources` and human warnings identify this. Current-catalogue fallback can change for old entries, while published scores/order remain pinned.

## Update standard policies

After a suitable metadata sync and with standard `turnover.toml`/`volume.toml` policies already installed:

```bash
mf markets rank update turnover --testing
mf markets rank update --testing
```

Unlike `build`, `update` performs one global statistics refresh, then publishes the named standard policy or both independently, pinned to that refresh. It accepts only `turnover` and `volume`, not custom names, and does not create missing policy files or synchronize metadata. A partial refresh/build can produce nonzero status even if one snapshot was published. Inspect each result; no automatic rollback of the other successful publication is implied.

## Troubleshooting and maintenance

For missing/stale data, inspect sync membership and scope errors, then intentionally refresh. For missing currency paths, collect suitable spot inputs or configure an explicit justified rule; do not assume a stablecoin peg. For window mismatch, inspect source end times and tolerance: a fresh fetch alone is insufficient. Strict rejection and a partial publication are different outcomes.

`markets rank delete` previews snapshots by policy, UUID or `--all`; adding `--apply` deletes eligible snapshots and owned evidence transactionally after re-resolving the selection. Referenced snapshots remain blocked. This is explicit maintenance, separate from immutability under normal publication. See [deletion syntax and effects](../../reference/cli/markets.md#snapshot-deletion).
