# Publication formats

A publication is a directory with a manifest and its committed file inventory. A registered file ID identifies a primary member in PostgreSQL. See [execution and lineage](../concepts/evidence.md) for that boundary.

| Operation | Format | Processing and inspection |
| --- | --- | --- |
| Normalization | [Canonical rows, indexes, manifests and counters](publication-normalized.md) | [Normalize](../workflows/normalize.md) · [Inspect files](../workflows/datasets.md) |
| Merge | [Physical timelines and partition evidence](publication-merge.md) | [Merge](../workflows/merge.md) |
| Conversion | [Preserved source and target values](publication-converted.md) | [Convert](../workflows/conversion.md) |

## Canonical columns

The base schemas are specified under [normalized publications](publication-normalized.md). Merged and converted files extend or nest those fields. Exact decimal fields use Decimal256 precision 76 and scale 28. Time fields use signed Unix nanoseconds.

## Source reconciliation

Read [ordering and reconciliation](../concepts/ordering.md) for interpretation and [historical policies](runtime.md) for configuration.
