# Quick Start

## Explore the CLI

After [installing Forge](installation.md), request help:

```bash
marketforge --help
marketforge acquisition --help
marketforge normalize --help
```

These commands are established by the local parser and exit before application context initialization. They were not executed for this documentation assignment.

## First data workflow

> **Runnable walkthrough pending verification.** A small, reproducible dataset example and its database, profile, and output prerequisites still need to be established.

The intended minimum path is to select a supported source or explicitly configured external import, verify its format and instrument metadata, acquire or register inputs, prepare processing artifacts, then process and inspect the resulting dataset.

Current Forge normalization preparation is restricted to its testing database path and does not execute Engine. Forge currently emits a schema-3 batch, while Rust's prepared-batch loader requires schema 4. Forge's preparation code also records older integration limitations. This producer/consumer mismatch must be resolved before documenting a combined invocation. No data job is required to browse this scaffold.

See [Workflows](../workflows/index.md) for the broader context and [Configuration](../configuration/index.md) for the distinction between recipes and runtime settings.
