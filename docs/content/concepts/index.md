# Concepts

## From source to publication

```text
provider archive or recording
  → managed raw copy and checksum
  → prepared jobs with profiles and instrument evidence
  → committed Parquet, manifest and indexes
  → PostgreSQL publication and file registrations
```

The provider distributes bytes, the venue defines the trading context, and a product identifies the feed. A native symbol belongs to a venue, type and category. A source-specific native ID may also be scoped to a publisher. The canonical ID belongs to its recorded namespace. For example, the Bybit linear perpetual and Bybit spot instrument both use the native symbol BTCUSDT. [Catalogue declarations](../workflows/catalogue/index.md) define their separate identities.

A historical specification declares economic interpretation over a validity interval, including quantity denomination, tick size or contract value where applicable. An observed price grid alone cannot establish those rules. See [historical specifications](../workflows/catalogue/specifications.md).

## Interpretation and execution

A profile describes how to decode and interpret a source. A recipe expresses which inputs to select, and runtime settings govern resources and policy. Forge resolves the recipe to a prepared job with concrete inputs and output destinations. Engine validates and interprets that contract. [Prepared contracts](../reference/prepared.md) · [Historical runtime](../reference/runtime.md)

Event time describes the source event; receive or system time, when provided, describes receipt or recording. Source order and native sequence can encode dependencies independent of timestamp order. Some sequence domains reset at snapshots. A monotonic sequence is evidence about order, and only documented sequence semantics and appropriate checks support a continuity claim.

A book snapshot initializes the published scope, such as a bounded top-of-book window. Updates can change levels or remove them. Reconstruct using the recorded observation order, event-to-row spans and boundary state. A no-op observation may have no level rows. Bounded coverage and resets limit what can be reconstructed. [Publication formats](../reference/publications.md)

## Outcome, identities and lineage

Execution describes whether jobs ran successfully. Integrity describes source and processing evidence and may be degraded even for committed output. Publication is the filesystem commit; registration is the later PostgreSQL transaction. A registration failure can leave verifiable output available for recovery.

A publication has an Engine ID and namespace, while PostgreSQL assigns a publication record ID and a file dataset ID for each primary file. File-level lineage records proven contributions. Publication-level input selections can cover more inputs than any one file. [Inspect datasets](../workflows/datasets.md)
