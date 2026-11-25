# MarketForge Roadmap

MarketForge is building local, configuration-driven infrastructure for quantitative research and algorithmic systems. The roadmap tracks completed milestones, current work, and planned development.

## Completed historical milestones

| Milestone | Status |
| --- | --- |
| Market catalogue and archive acquisition | Operational across Binance, Bybit, OKX, Bitget, and Gate.io, with provider-dependent coverage |
| Historical normalization and merge | Operational for supported sources and profiles, with canonical event data, specification evidence, and ordered Parquet outputs |
| Historical pipeline orchestration | Operational from one recipe through archive discovery, managed raw registration, normalization, merge, output verification, and publication registration |
| Optional quote conversion | Operational for supported routes and reference inputs, with verified output indexing |
| Workflow and dataset inspection | CLI and interactive Forge workspace for recipe execution, retained run reports, and publication inspection |
| Research access | Dataset discovery, exact Arrow reads, and lazy Polars queries for compatible fields |

The [historical pipeline guide](https://flowdrivenml.github.io/marketforge/workflows/pipeline/) describes the operational workflow and its prerequisites.

## Current development priorities

1. **Live stream integration and reliability.** Integrate the Engine's ingestion, routing, streaming normalization, order-book integrity, and recovery components across supported providers. Verify lifecycle controls, monitoring, reconnects, and failure recovery.
2. **Historical-to-live bootstrap.** Complete the handoff from historical initialization to live observations. Verify compatible state, event ordering, gap handling, and recovery at that boundary.
3. **Historical coverage and usability.** Expand source and profile coverage, improve external data registration, and refine workflow diagnostics and run recovery.

## Planned development

| Phase | Direction |
| --- | --- |
| Spectra financial feature engineering | Reusable historical and live computations for order flow, liquidity, volume structure, cross-market relationships, and event-driven sampling |
| Spectra optimization | Reproducible parameter search, experiment tracking, and out-of-sample evaluation |
| Research and ML laboratory | Feature and model evaluation, experiment workflows, and backtesting |
| Algorithmic strategy infrastructure | User-defined live strategies, monitoring, simulation, and paper trading, followed by execution and risk integration |

Open technical feedback and feature proposals in [GitHub Issues](https://github.com/flowdrivenml/marketforge/issues).
