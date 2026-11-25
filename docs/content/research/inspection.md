# Dataset inspection

Start with a `Dataset` from [discovery](discovery.md). These calls expose identity, schema, and evidence before decoding a preview:

```python
from forge.lab.inspection import inspect

display(dataset.discovery_fields())
display(dataset.info())
display(dataset.schema())
display(dataset.streams())
display(dataset.lineage())

report = inspect(dataset)
display(report.columns())
display(report.timestamps())
display(report.row_groups()[:3])
```

`info()` describes the handle's file scope, observed extent, and metadata evidence. `schema()` returns the Arrow schema. `lineage()` reads the recorded publication manifest, which can identify input publications or source tasks. Footer row counts and observed envelopes describe stored output, and do not establish uninterrupted source coverage. For actual rows, use `head()` or `batches()`. For a bounded column profile, use `report.profile(["event_timestamp_ns"], input_rows=1000)`.

## Normalized records

Normalized files hold canonical events for the stream that the normalizer published. A merged publication combines input streams in a shared timeline. Find the available normalized files by event kind and choose an exact reference.

```python
normalized = catalog.find(operation="normalize", exchange="bybit", symbol="BTCUSDT")
display(normalized.find(data_type="trades").summary())
display(normalized.find(data_type="depth").summary())
dataset = normalized.get(input("Exact normalized file reference: ").strip())
display(dataset.schema())
display(dataset.head(
    5, columns=["event_timestamp_ns", "symbol"],
    start="2026-10-27T00:00:00Z", end="2026-10-28T00:00:00Z",
    input_rows=100_000,
))
```

Use a time interval present in the selected file. A depth preview can show updates without enough preceding state to reconstruct the book. See [depth semantics](../concepts/depth.md).

## Converted valuations

Conversion is optional and the Bybit Quick Start ends at merge. If you already have a converted publication, inspect its target quote and compare its projected schema with the original merge:

```python
converted = catalog.find(operation="convert", target_quote="USDT")
display(converted.summary())
output = market.open(input("Converted publication directory: ").strip())
display(output.info()["target_quote"])
display(output.lineage())
display(output.schema())
display(output.query(data_type="trades").head(5, input_rows=100_000))
```

Read the manifest's valuation policy and source evidence when interpreting target values, reference routes, and unavailable valuations. A filesystem open uses a committed path. Registry lookup of an indexed conversion uses the complete output reference. The [conversion guide](../workflows/conversion.md#6-inspect-the-result) covers verification and retained receipts.
