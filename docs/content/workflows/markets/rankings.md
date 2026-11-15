# Build and inspect market rankings

Rankings publish an immutable view of eligible retained observations under a named TOML policy. They describe measured activity, not trading signals. Changing current statistics or a policy does not rewrite an existing snapshot.

## Choose the comparison

**Turnover** (`metric = "turnover_24h"`) compares monetary activity in one explicit currency. Optional stored spot prices value aggregate native turnover at reference prices; this is not reconstruction of FX at each trade. Derivative last prices are not conversion sources. USD/USDT/USDC equivalence is never automatic.

**Volume** (`metric = "volume"`) compares base-asset quantities **within each base asset**. ETH quantities can share an ETH group across spot, perpetuals and futures; BTC has a different group and its own rank 1. Unique storage `position` is not a global cross-asset score; `group_rank` is the within-asset rank. See the [complete policies](../../configuration/markets.md).

## Create and publish a policy

`new` writes a strict version-2 turnover template, without opening an editor:

```bash
marketforge markets rank new activity
```

Edit `data/.config/markets/rankings/activity.toml` manually to select your universe, comparison, and intended partial-coverage behavior. Validate it before publication:

```bash
marketforge markets rank validate activity
marketforge markets rank build activity --testing
marketforge markets rank list --testing
marketforge markets rank show activity --testing --limit 30
```

`build` performs no network refresh; it reads retained observations and **writes** a published snapshot. A successful matching sync and fresh statistics are prerequisites. It evaluates the full selected universe, not the display limit. Explicit instrument references must exist in the selected catalogue; eligible spot conversion prices can be outside the ranking subset but must already be stored.

By default a policy is strict: stale, missing, estimated-disallowed, incompatible-window or unconvertible inputs reject publication. `allow_partial_universe = true` explicitly permits exclusions and partial-refresh inputs; it does not make missing values valid or disable age/unit checks. At least one included entry is still required. Failed builds roll back their publication; older published snapshots remain available.

## Inspect a pinned result

```bash
marketforge markets rank show activity --testing --excluded --all
marketforge markets rank show activity --testing --instrument-type perpetual --quote-asset USDT
marketforge --json markets rank show activity --testing
```

A name resolves the latest published snapshot by creation time then UUID. Use a returned `SNAPSHOT_UUID` to pin one publication; replace that placeholder in commands. Name resolution applies no age/partial-universe cutoff, so inspect creation time and coverage rather than assuming latest means fresh.

```bash
marketforge markets rank show SNAPSHOT_UUID --testing --conversion 1
marketforge markets rank show SNAPSHOT_UUID --testing --base-asset ETH --match 'ETH*'
```

`--conversion` accepts a stored full instrument reference or unique storage position. It exposes native amount, rate, path, timestamps and source observations; volume snapshots expose derivation evidence. Human `show` defaults to 50 included rows and grouped exclusion reasons; `--excluded` adds excluded rows and `--all` removes row limits. JSON always returns all matching entries and evidence, regardless of human row limit. Filters preserve original positions/group ranks and do not rebuild the snapshot.

## Conversion and volume evidence

Turnover conversion precedence is identity, explicit manual rate, explicit equivalence, then eligible stored spot prices. Automatic paths use one pair or one bridge, without mixed-exchange legs. Same-exchange paths take priority; cross-exchange paths require permission. Freshness, future-time, price-leg skew and turnover-window alignment checks apply separately. A stale manual override fails rather than silently falling back to spot prices.

`estimates` controls volume-derived turnover estimates such as OKX linear quantity × last; converting reported turnover by FX is a separate valuation convention. Manual preferences can move specified turnover instruments ahead of score order; ties otherwise use score then stable reference. Inspect the policy before treating a rank as pure descending activity.

Volume prefers reliable reported base quantity, including zero. A saved base-denominated contract face value permits exact contract-count conversion; quote-denominated face value divided by a matching positive reference price is estimated. Missing specification/scope evidence causes exclusion. Policies exclude estimates by default. Each result retains its formula, inputs, observation and specification evidence.

New snapshots freeze identity and asset metadata. Older snapshots may fall back to retained observation evidence or current catalogue fields; JSON `metadata_sources` and human warnings identify this. Current-catalogue fallback can change for old entries, while published scores/order remain pinned.

## Update standard policies

After a suitable metadata sync and with standard `turnover.toml`/`volume.toml` policies already installed:

```bash
marketforge markets rank update turnover --testing
marketforge markets rank update --testing
```

Unlike `build`, `update` performs one global statistics refresh, then publishes the named standard policy or both independently, pinned to that refresh. It accepts only `turnover` and `volume`, not custom names, and does not create missing policy files or synchronize metadata. A partial refresh/build can produce nonzero status even if one snapshot was published. Inspect each result; no automatic rollback of the other successful publication is implied.

## Troubleshooting and maintenance

For missing/stale data, inspect sync membership and scope errors, then intentionally refresh. For missing currency paths, collect suitable spot inputs or configure an explicit justified rule; do not assume a stablecoin peg. For window mismatch, inspect source end times and tolerance: a fresh fetch alone is insufficient. Strict rejection and a partial publication are different outcomes.

`markets rank delete` previews snapshots by policy, UUID or `--all`; adding `--apply` deletes eligible snapshots and owned evidence transactionally after re-resolving the selection. Referenced snapshots remain blocked. This is explicit maintenance, separate from immutability under normal publication. See [deletion syntax and effects](../../reference/cli/markets.md#snapshot-deletion).
