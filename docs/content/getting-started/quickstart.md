# Quick Start

## Explore the CLI

After [installing Forge](installation.md), request help:

```bash
marketforge --help
marketforge acquisition --help
marketforge normalize --help
```

These commands are established by the local parser and exit before application context initialization.

## First data workflow

Follow the [Databento AAPL walkthrough](../examples/bring-your-own-data/databento/index.md) to register the supplied JSONL recordings in the testing PostgreSQL catalogue, validate both profiles, build a prepared batch, and publish and inspect canonical Parquet.

The [BYOD guide](../examples/bring-your-own-data/index.md) covers authoring a profile for your own source. For venue archive downloads, see the [historical acquisition workflow](../workflows/acquisition/historical-data.md).
