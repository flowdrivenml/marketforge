# Explore historical datasets

Start here when MarketForge has already processed historical market data. A run records an operation, its attempts and its outcome. A publication is an output of that work, with a manifest and one or more primary data files. Use [execution history](runs.md) to investigate the processing and this guide to inspect the output.

## Find an instrument and period

```bash
mf datasets list --testing
mf datasets list --testing --exchange bybit --instrument BTCUSDT --sort start
```

These examples use the testing registry. Select the environment that produced the data. Production is the default, and the same numeric ID can identify different objects in the two registries.

The default terminal view groups members of available outputs into historical timeline segments. Check the exchange and market alongside the symbol. Bybit spot BTCUSDT and Bybit linear perpetual BTCUSDT are different instruments. Market labels include Spot, Perpetual · Linear, Perpetual · Inverse, Future · Linear, and Equity.

| Stage | What you are browsing |
| --- | --- |
| [Normalized](normalize.md) | Source records transformed into canonical market-data representations |
| [Merged](merge.md) | Selected normalized streams combined into an ordered [merge publication](../reference/publication-merge.md) |
| [Converted](conversion.md) | Merged data valued in a target quote using reference observations. Browse it through the conversion index |

A timeline identifies trades or depth (L2) separately. A single publication can contain Trades + Depth and contribute to several timelines across data kinds, instruments or exchanges. Timeline rows point to existing outputs. They share those outputs with other timeline rows.

## Open a timeline and publication

Compare the recorded UTC bounds with the period you need. Separate spans can reflect event silence or limitations in the retained evidence. Recorded first and last events establish observed extent. Check the [coverage reference](../reference/cli/datasets.md#understanding-dataset-coverage) before treating a span as uninterrupted data.

```bash
mf datasets list --testing --exchange bybit --instrument BTCUSDT --limit 5 --wide
mf datasets timeline show TIMELINE_REFERENCE --testing
mf datasets show --publication-id PUBLICATION_ID --testing
mf datasets streams --publication-id PUBLICATION_ID --testing
```

Copy the timeline reference from the command printed by `--wide`. Pick a registry publication ID from the timeline detail. Publication inspection shows the output’s recorded scope, size and files. Streams identifies the logical streams within that scope. Multiple streams can share an instrument, and a combined file can contain several streams.

Use `mf datasets list --testing --view publications` to browse whole outputs or the file view to find the individual primaries. A converted output has an indexed work reference. Use `mf datasets show --output-ref OUTPUT_REF --testing` with the exact key printed for it. Its numbered parts are local to that output. [Identifier reference](../concepts/identity.md#inspection-identifiers)

<a id="inspect-lineage-and-verify-files"></a>

## Inspect a primary file

Choose a real file ID from publication inspection or the file view. Use the same ID for each command below.

```bash
mf datasets list --testing --view files --publication-id PUBLICATION_ID
mf datasets show FILE_ID --testing
mf datasets schema FILE_ID --testing
mf datasets head FILE_ID --testing --limit 5 --column event_timestamp_ns --precise
mf datasets lineage FILE_ID --testing --publication
```

Schema reports stored Arrow types and nullable fields. Head previews stored rows from the selected Parquet file. Repeat `--column` to choose more fields after inspecting the schema. A physical file count can differ from the number of partitions. Stored rows can also include reconstruction records depending on the publication format. [Publication formats](../reference/publications.md)

Lineage separates sources proven to contribute to the selected file from inputs selected at publication scope. For a merged file, follow the returned parent file IDs to their normalized publications. To find the producing run, match the output IDs and paths against [retained run reports](runs.md#from-a-merged-publication-to-its-source).

## Verifying datasets

Use `mf datasets verify FILE_ID --testing` to check the selected primary file and shared manifest against their registered fingerprints. The recorded processing integrity and the result of this fresh check describe different evidence. To analyze the data, continue with [notebooks](notebooks.md) or [Python dataset inspection](../reference/python-datasets.md).

<a id="migrate-existing-registrations"></a>
<a id="recover-missing-file-registrations"></a>

For older IDs, missing registrations or retired outputs, consult [run and dataset maintenance](../reference/history-maintenance.md).
