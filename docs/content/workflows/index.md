# Workflows

## Acquisition and market selection

Use [Historical data acquisition](acquisition/index.md) to configure discovery, download provider archives, introduce external files, and inspect managed results. Use [Markets and statistics](markets/index.md) to synchronize instrument metadata, interpret current observations, and publish activity rankings. These are separate capabilities: current statistics can inform selection, but do not substitute for historical archives.

## Bring data into a shared foundation

MarketForge has acquisition services for supported sources and tooling to describe external-file imports. Discovery, selection, local storage, format interpretation, and instrument metadata are separate concerns. Importing a file does not establish that its fields or market semantics can be normalized correctly.

## Prepare and process

Forge prepares selections and processing artifacts, with registry-backed operations where required. Rust Engine implements historical normalization, merging, and conversion. The first [reproducible walkthrough](../getting-started/quickstart.md) is a documentation draft; the Forge-to-Engine handoff has not been verified here.

## Research and live operation

Historical preparation supports research, while live ingestion uses shared market-data and processing components. Research-to-live is a direction built on that common foundation, rather than a required sequence of unrelated tools.

**Implementation status not assessed:** end-to-end live operation and complete research workflows. **Planned capability:** broader algorithmic workflows. The presence of live, feature, or lab modules is not a readiness claim.
