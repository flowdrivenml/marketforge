# Browse raw data

Storage is the catalogue of locally managed raw copies. Use it after [historical acquisition](acquisition/historical-data.md) to find raw files by instrument, check their state and assess nominal archive coverage. For canonical or merged Parquet, use [Datasets](inspection/datasets.md).

## Find a copy and check its coverage

Choose the registry that managed the data and use a local archive ID returned by the list command.

```bash
mf storage list --testing --exchange bybit --instrument BTCUSDT --wide
mf storage show LOCAL_ID --testing
mf storage coverage --testing --exchange bybit --instrument BTCUSDT --data-type trades --gaps
```

Review the copy state and path before inspecting the raw file. Coverage uses nominal archive intervals. Use [time and coverage](../concepts/time.md) to interpret gaps and distinguish nominal availability from observed events.

Use the [Storage CLI reference](../reference/cli/storage.md) for filters and verification. If bytes are missing or the acquisition was interrupted, use [acquisition recovery](acquisition/managing-acquisitions.md#recover-partial-or-failed-runs). Once the required raw data is available, continue to [Normalization](normalization/index.md).
