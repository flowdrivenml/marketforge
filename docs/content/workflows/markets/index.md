# Markets and statistics

Use Markets to collect current instrument metadata, refresh retained observations, and publish rankings. For historical files and date coverage, use [Acquisition](../acquisition/index.md).

| I want to… | Command |
| --- | --- |
| Collect current instrument metadata | `mf markets sync` |
| Refresh current statistics for synchronized scopes | `mf markets refresh` |
| Browse stored instruments and observations | `mf markets list` |
| Inspect one stored instrument | `mf markets show REFERENCE` |
| Create or check a local policy | `mf markets rank new NAME` or `mf markets rank validate NAME` |
| Publish a snapshot from retained observations | `mf markets rank build NAME` |
| Refresh once and publish standard rankings | `mf markets rank update [turnover or volume]` |
| List published snapshots | `mf markets rank list` |
| Inspect a snapshot and its evidence | `mf markets rank show NAME_OR_UUID` |
| Preview snapshot maintenance | `mf markets rank delete NAME_OR_UUID` |

Database commands default to production. Pass `--testing` for the testing registry. Sync, refresh and standard update make public exchange requests. Build reads retained inputs and writes a snapshot. List and show read stored records.

Start with [instrument discovery](instrument-discovery.md), [statistics refresh](market-statistics.md), or [ranking workflows](rankings.md). See the [CLI reference](../../reference/cli/markets.md) and [TOML reference](../../reference/configuration/markets.md) for exact syntax and defaults.
