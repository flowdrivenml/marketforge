# Inspect registered datasets

For local Python discovery, previews and schema inspection, see the [Jupyter workflow](notebooks.md).

Run from the checkout with the [testing registry and Arrow inspection tools](../getting-started/installation.md). Start with the file IDs returned by normalization or merge. To discover them later, use `mf datasets find --testing --exchange bybit --instrument BTCUSDT --instrument-type perpetual --market-category linear`.

For each ID, run `mf datasets show ID --testing` to identify the owning publication, `mf datasets schema ID --testing` for physical types, and `mf datasets head ID --testing --limit 5 --precise` for stored records. `mf datasets verify ID --testing` checks the selected file and shared manifest fingerprints. `mf datasets streams ID --testing` reports stream membership and observed bounds. `mf datasets lineage ID --testing --publication` includes the publication's input selections.

An observed envelope gives the earliest and latest timestamps. For depth, inspect the publication's indexed observations and reconstruction boundaries before treating rows as a usable book. Preserve the shared manifest and auxiliary files. To merge, first check layout, stream namespace and required parts in the [merge guide](merge.md#supported-layouts).

## Migrate existing registrations

See [registry migration and recovery](../reference/cli/datasets.md#migrate-existing-registrations).

## Tokyo Night colors

See [terminal presentation settings](../reference/cli/datasets.md#tokyo-night-colors).

## Inspect from Python

Use a primary file path returned by normalization, or its path shown by `datasets show`. With the lab dependencies installed, open the physical file through the checkout's dataset API:

```python
from forge.datasets.discovery import open_dataset

dataset = open_dataset(path)
print(dataset.schema())
print(dataset.head(5, columns=["event_timestamp_ns", "price"]))
```

Substitute the returned primary path for `path`. Use `batches(columns=..., start=..., end=...)` for bounded Arrow iteration with optional half-open timestamp filtering. To interpret depth observations, use the publication's event index and boundary metadata. [Bybit example](../getting-started/quickstart.md#compare-with-the-checked-result)
