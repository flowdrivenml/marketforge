# Discover and open datasets

## Search local files

Filesystem discovery walks the selected roots and returns one `Dataset` per primary file. It reads manifests and Parquet footers without connecting to PostgreSQL. The default can also read projected provenance columns for supported causal merges. Use `metadata_only=True` for a lighter search, and open the chosen publication with the default settings if you need more stream evidence.

```python
from forge.datasets import MarketData

market = MarketData()
catalog = market.discover(scope="committed", metadata_only=True)
tests = market.discover(scope="tests", metadata_only=True)
all_files = market.discover(scope="all", metadata_only=True)
display(catalog.summary())
```

The testing scope covers historical test fixture trees. It does not select the testing PostgreSQL registry. Staging is excluded unless you explicitly pass `include_staging=True`. Check `catalog.diagnostics` for unreadable files, conflicting metadata, and unknown identity.

```python
matches = catalog.find(
    exchange=["bybit", "okx"],
    symbol="BTCUSDT",
    instrument_type="perpetual",
    market_category="linear",
    data_type="trades",
    operation="normalize",
    start="2026-10-27T00:00:00Z",
    end="2026-10-30T00:00:00Z",
    sort="time",
    order="desc",
)
if not matches:
    print("No matching normalized trade files.")
elif len(matches) == 1:
    dataset = matches.one()
else:
    display(matches.summary())
    dataset = matches.get(input("Exact file reference from the table: ").strip())
```

Alternatives within a filter match with OR. Different filters match with AND. Market conditions must match the same membership, and successive `find()` calls preserve that restriction. Symbol matching is exact and case-sensitive by default. Use `symbol_match="contains"` for substring searches. Time filters match observed envelopes against `[start, end)`. Use `interval="envelope_contained"` to require the complete known envelope inside the bounds. Neither mode establishes continuity.

`one()` and `get()` raise `ValueError` without exactly one result. Use the summary's reference and reference type when several files match. The returned handle is already open. To open a filesystem path directly or group the files in a publication, use:

```python
primary_file = market.open(input("Primary Parquet path: ").strip())
publication = market.open(input("Directory containing manifest.json: ").strip())
```

The TUI publication inspection shows the manifest location. Use the parent directory for the complete publication. To read one logical market sequence within it, [select a stream](streams.md).

## Browse a registry

The asynchronous registry methods require access to the selected PostgreSQL registry. `find_registered()` lists publications and indexed conversions using registry metadata. A file within a publication has a separate registry primary-file dataset ID. In Jupyter use top-level `await`. A synchronous script can call a coroutine through `asyncio.run()`.

```pycon
>>> page = await market.find_registered(
...     testing=False, operation="merge", exchange="bybit",
...     instrument="BTCUSDT", limit=10,
... )
>>> display(page["publications"])
>>> publication_id = int(input("Registry publication ID from your run: "))
>>> publication = await market.open_registered(publication_id=publication_id)
```

For a registered primary file, pass `file_id=int(...)` using the ID from publication inspection. For an indexed conversion, pass `output_ref=...` with the full work reference from the pipeline or registry entry. Use exactly one identity and pass `testing=True` consistently when browsing and opening testing records.

| Reference | Meaning | How to open |
| --- | --- | --- |
| Filesystem path | Publication directory or primary Parquet file | `open(path)` |
| Engine publication ID | Identifier recorded by the Engine in output evidence | Locate the corresponding path or registry item |
| Registry publication ID | Registered normalized or merged publication | `open_registered(publication_id=...)` |
| Registry primary-file ID | One registered primary file | `open_registered(file_id=...)` |
| Converted output reference | Indexed conversion work reference | `open_registered(output_ref=...)` |
| Logical stream ID | A stream within an open dataset | `dataset.select(stream_id=...)` |

See [inspection identifiers](../concepts/identity.md#inspection-identifiers) and the [Python API reference](python-api.md#marketdata) for filter and error details.
