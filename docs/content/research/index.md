# Research & Analysis

Discover historical datasets, inspect their recorded identity and provenance, then read only the files and market events needed for a question. The research API provides exact Arrow access and lazy Polars queries over immutable Parquet. The [historical data model](../concepts/data-model.md) explains how normalized, merged, and converted outputs relate to their source recordings.

1. Set up the research dependencies and open a dataset in [Jupyter](notebooks.md).
2. [Search by market or identifier](discovery.md), then open the exact publication or file.
3. Inspect [partitions and row groups](partitions.md) before reading a large publication.
4. Build a [bounded trade or depth query](querying.md). Use the [Python API](python-api.md) to check selectors and return types.

For registry verification, run reports, and deletion, use the [operational Dataset workflow](../workflows/inspection/datasets.md).
