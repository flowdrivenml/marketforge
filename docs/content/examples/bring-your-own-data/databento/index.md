# Databento AAPL Trades and MBP-10

This recording covers AAPL on Nasdaq (`xnas`) from 14:30 to 14:40 UTC on October 28, 2024. The registry stores Databento as provider and XNAS.ITCH as product. AAPL is a USD equity with quantities in shares. Publisher 2 and native instrument 38 identify the raw observations. PostgreSQL supplies the canonical instrument ID. [XNAS.ITCH dataset documentation](https://databento.com/docs/datasets/xnas-itch)

- `AAPL_trades.jsonl` contains 1,977 executions. The [trade profile](xnas.equity.equity.trades.toml) maps `A` to sell, `B` to buy, and `N` to unknown. It preserves execution prices including sub-cent values and both event and receive timestamps to nanoseconds. [Databento Trades](https://databento.com/docs/schemas/trades)
- `AAPL_orderbook_L2.jsonl` contains 39,605 MBP-10 messages. The [depth profile](xnas.equity.equity.depth.toml) maps both sides and order counts from `levels[]`. Each message describes the supplied top ten levels per side. `flags & 128` marks the last record of an event for the instrument. Preserve intermediate observations in source order. The raw action and flags remain available in the managed source file. [Databento MBP-10](https://databento.com/docs/schemas/mbp-10) · [Databento flags and sides](https://databento.com/docs/conventions/common-fields-enums-types)
- The two `.dbn.zst` files contain the original Zstandard-compressed DBN binary records. Use the supplied JSONL exports for this workflow. Native DBN decoding is unsupported. [Databento Binary Encoding](https://databento.com/docs/conventions/databento-binary-encoding)

## Obtain the exports

Use existing exports or request `trades` and `mbp-10` for `AAPL` from `XNAS.ITCH`, with start `2024-10-28T14:30:00Z` and exclusive end `2024-10-28T14:40:00Z`. The checkout's `research/marketforge_databento_equities.ipynb` estimates cost, calls the Historical API, and exports the records with the Databento SDK's `to_json` method. Review the estimate before running the download cell. Place the resulting `AAPL_trades.jsonl` and `AAPL_orderbook_L2.jsonl` under `.recordings/` in the checkout. The site does not distribute the licensed recordings.

## Inspect and register

Run from the project root with Forge installed and a [configured testing database](../../../getting-started/installation.md#postgresql-credentials). The registration recipe selects the original JSONL filenames under `.recordings`. The [specification](specs.toml) illustrates the declaration schema. Replace its illustrative economic parameters and provenance with evidence applicable to the full recording interval before registration. The observed price grid alone does not establish historical venue rules.

```bash
examples="$PWD/docs/content/examples/bring-your-own-data/databento"
wc -l .recordings/AAPL_{trades,orderbook_L2}.jsonl
head -n 1 .recordings/AAPL_trades.jsonl | python -m json.tool
head -n 1 .recordings/AAPL_orderbook_L2.jsonl | python -m json.tool

mf database migrate --testing
mf engine build normalize
mf catalog exchange add --from "$examples/exchange.toml" --testing
mf catalog instrument add --from "$examples/instrument.toml" --testing
mf catalog specs add --from verified-aapl-specs.toml --testing
for kind in trades depth; do
    profile="xnas.equity.equity.$kind"
    mf normalize profiles validate --file "$examples/$profile.toml"
    mf normalize profiles add "$profile" --from "$examples/$profile.toml" --testing
done
mf acquisition register plan "$examples/registration.toml" --testing
mf acquisition register run "$examples/registration.toml" --testing
```

## Prepare and publish

The [normalization recipe](normalization.toml) selects the registered source and requests one job per canonical data type. Only the standalone trade file feeds the trade job. The MBP-10 trade messages contribute book observations to the depth job. Their event time, native instrument ID, sequence, price, size, and side match the standalone trades as a multiset.

```bash
mf --json normalize run "$examples/normalization.toml" --testing > byod-result.json
```

## Inspect the output

The run report maps each primary Parquet file to its PostgreSQL ID under `catalogue_dataset_ids`. Inspect file-local row counts, timestamps and membership with:

```bash
mf datasets list --testing --exchange xnas --instrument AAPL
mf datasets show FILE_ID --testing
mf datasets head FILE_ID --testing --limit 10
```

The depth publication retains its indexed observation and boundary metadata in the shared manifest. The observed envelope alone does not establish sequence continuity.

To rerun the full verification against every raw trade and reconstructed depth state, including the overlap check, use the test below. It uses disposable schemas in the testing database and retains isolated artifacts under `data/tests/historical/normalize/`.

```bash
MARKETFORGE_FORGE_INTEGRATION=1 python -m pytest \
  forge/tests/integration/forge_workflow/test_workflow.py -k databento
```
