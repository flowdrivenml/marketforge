# Jupyter notebooks

Launch JupyterLab from the [research environment](getting-started.md). Download a notebook, save it inside your MarketForge checkout, and open it with that environment's kernel. Each notebook uses existing data and asks you to select the exact publication or file to read.

| Notebook | Existing input | What it demonstrates |
| --- | --- | --- |
| [Normalized trades and depth](../examples/notebooks/normalized.ipynb) | A normalized trade or L2 depth file | Find files by event kind, inspect the schema, preview projected time-filtered rows |
| [Merged trades and depth](../examples/notebooks/merged.ipynb) | A combined merge such as the Bybit Quick Start output | Inspect logical streams, filter events, preview one partition, and use a projected lazy scan |
| [Converted valuations](../examples/notebooks/converted.ipynb) | A committed converted publication | Find outputs by quote currency, inspect valuation evidence, and preview exact values |

If you prefer an example with tiny synthetic files, use the existing [dataset inspection notebook](../examples/notebooks/dataset_inspection.ipynb) and save the adjacent [fixture builder](../examples/notebooks/dataset_fixtures.py) in the same directory. That notebook creates temporary fixtures. The three notebooks above read existing MarketForge datasets.

For integer nanosecond bounds, Decimal256, and projected scans, see [Arrow and Polars](arrow-polars.md). For the available selectors and reader options, see the [Python API reference](python-api.md).
