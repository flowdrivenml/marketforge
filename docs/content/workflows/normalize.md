# Normalize registered data

Use a configured [testing workspace](../getting-started/installation.md) with a verified normalizer, complete managed raw copies, applicable specification evidence and profiles that match the source. Run from the checkout.

1. Select raw inputs in a [normalization recipe](../configuration/normalization.md). For a reusable starting point see the [Bybit selection](../examples/bybit/acquisition.toml).
2. Run `mf normalize validate RECIPE`, then `mf normalize prepare RECIPE --testing`. Replace `RECIPE` with your saved recipe name or the path to its TOML. Review the matched archives, nominal gaps, historical evidence, planned jobs and Engine validation. Use the returned batch path in the next command.
3. Run `mf --json normalize run BATCH --testing`. Replace `BATCH` with that prepared JSON path. A complete result identifies the registered files. Keep the run report to recover registration if committed output exists after a failure.
4. [Inspect the registered datasets](datasets.md) using their returned file IDs.

If selection fails, inspect raw status and resolve missing coverage, profiles or historical evidence. If execution fails, inspect the Engine result and integrity categories. See the [command and recovery reference](../reference/cli/normalize.md).
