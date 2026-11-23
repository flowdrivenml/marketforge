# Using MarketForge

MarketForge turns historical market-data archives and external recordings into canonical trade and order-book datasets. Forge selects and tracks inputs, prepares processing jobs, and records their outcomes. The Engine interprets events and writes publication files that can be inspected with the CLI or Python.

## Start with a first dataset

[Install and prepare a testing workspace](getting-started/installation.md), then follow the [Bybit BTCUSDT trades and depth quick start](getting-started/quickstart.md). You will verify the managed copies, inspect the normalized rows, and check the initial depth snapshot against its recorded boundary.

## Choose a task

| If you need to… | Start here |
| --- | --- |
| Find an instrument, browse current statistics, or inspect a ranking | [Explore markets](workflows/markets/index.md) |
| Download archives or add a recording | [Acquisition](workflows/acquisition/index.md) and [external recordings](workflows/acquisition/external-data.md) |
| Inspect raw availability and manage copies | [Raw storage](workflows/acquisition/managing-acquisitions.md) |
| Process data | [Normalize](workflows/normalize.md), [merge](workflows/merge.md), or [convert](workflows/conversion.md) |
| Run acquisition, normalization, and merge together | [Historical pipeline](workflows/pipeline.md) |
| Read an existing publication | [Inspect datasets](workflows/datasets.md) or [use a notebook](workflows/notebooks.md) |
| Look up exact syntax, fields, or formats | [Reference](reference/index.md) |

Read the [concepts](concepts/index.md) to understand the evidence behind a result. Use the [worked examples](examples/index.md) when you want reusable inputs, or [troubleshooting](workflows/operations.md) when a step fails. Contributors can start in [Development](development/index.md).
