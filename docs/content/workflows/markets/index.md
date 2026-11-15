# Markets and statistics

Markets provides an instrument catalogue and retained current observations. It is separate from historical archive acquisition: a ticker refresh does not download trade/order-book archives, and a current listing is not proof of historical availability.

| Information | Purpose |
| --- | --- |
| Instrument metadata | Native identity, assets, market/type, contract specification, lifecycle and status. |
| Current statistics | Last price, available rolling volumes/turnover, and optional open interest/funding, with units and timing evidence. |
| Historical archives | Source files covering nominal historical intervals, managed by [acquisition](../acquisition/index.md). |
| Ranking snapshots | Immutable selections and ordering computed from retained observations under a named policy. |

Begin with [instrument discovery](instrument-discovery.md), then [statistics interpretation](market-statistics.md) and [rankings](rankings.md). Use the [markets configuration](../../configuration/markets.md) and [CLI reference](../../reference/cli/markets.md) for exact settings and options.

## Prerequisites and effects

Install Forge and configure the [guarded testing database](../acquisition/index.md#prerequisites). All `markets sync`, `refresh`, `list`, and `show` commands require `--testing`; ranking file creation/validation do not. Testing mode is not simulation.

- `sync` retrieves public instrument metadata and writes catalogue/sync records.
- `refresh` retrieves current statistics and appends observations; run a matching successful sync first.
- `list` and `show` browse stored data without network access or registry mutation.
- `rank build` writes a published snapshot using stored observations; it does not refresh.
- `rank update` retrieves statistics and publishes standard policy snapshots.

Use filtered collections initially: omitting exchange/market filters selects all 14 supported current-market scopes. See each command's effects before running it. Examples here are parser/source-verified; no network or database workflow was executed for this documentation assignment.
