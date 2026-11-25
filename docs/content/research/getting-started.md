# Getting started with Python

Start with an existing publication, such as the merge from the [Quick Start](../getting-started/quickstart.md). From the checkout, install the locked research dependencies and start JupyterLab:

```bash
uv sync --project forge --locked --no-default-groups --group lab
uv run --project forge --locked --no-default-groups --group lab jupyter lab
```

This creates or updates the uv environment under `forge/.venv` by default. The standard installer has its own runtime environment. In JupyterLab choose the Python 3 kernel from the lab you just launched. If you connect from an existing Jupyter server, register the research kernel first and select **MarketForge Research**:

```bash
uv run --project forge --locked --no-default-groups --group lab python -m ipykernel install --user --name marketforge-research --display-name "MarketForge Research"
```

In a notebook under the checkout, Forge finds the project root by searching upward. To work elsewhere, set `MARKETFORGE_PROJECT_ROOT` or pass `MarketData(root=...)`. The default data root is `data` relative to the checkout. If your Forge workspace uses another storage root, pass it explicitly as `data_root`. A relative data root or path passed to `open()` resolves against the project root.

```python
from forge.datasets import MarketData

market = MarketData()
matches = market.discover(metadata_only=True).find(
    operation="merge", exchange="bybit", symbol="BTCUSDT"
)
display(matches.summary())
```

Match the listing to the publication from your run. If needed, open **Data → Merged** in Forge and inspect the corresponding publication's manifest path. Use the directory containing that manifest.

```python
publication = market.open(input("Merged publication directory: ").strip())
display(publication.info())
display(publication.query(data_type="trades").head(5, input_rows=100_000))
```

The preview reads at most the requested input prefix and may return fewer than five matching rows. Continue with [Jupyter examples](notebooks.md), [dataset discovery](discovery.md), or [stream selection](streams.md).
