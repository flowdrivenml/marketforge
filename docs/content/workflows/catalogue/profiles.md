# Trades and L2 depth profiles

Create a complete standalone schema-2 TOML file for each source interpretation. The canonical name is `exchange.instrument_type.market_category.trades` or `exchange.instrument_type.market_category.depth`. Use the exact ID as the filename. Format codes use the uppercase venue prefix with hyphens for underscores, followed by `-T1` or `-B1` for the first format.

These Rust-validated examples describe only the supplied synthetic [trade CSV](../../examples/catalogue/synthetic-trades.csv) and [depth JSONL](../../examples/catalogue/synthetic-depth.jsonl). The example files have explicit side values and independent depth snapshots.

- [Complete trades profile](../../examples/catalogue/example_exchange.equity.equity.trades.toml)
- [Complete L2 depth profile](../../examples/catalogue/example_exchange.equity.equity.depth.toml)

```bash
examples=docs/content/examples/catalogue
trade=example_exchange.equity.equity.trades
depth=example_exchange.equity.equity.depth
mf acquisition profiles validate --file "$examples/$trade.toml"
mf acquisition profiles validate --file "$examples/$depth.toml"
mf acquisition profiles add "$trade" --from "$examples/$trade.toml" --testing
mf acquisition profiles add "$depth" --from "$examples/$depth.toml" --testing
mf acquisition profiles list --exchange example_exchange
mf acquisition profiles inspect "$depth"
```

Validation stages the submitted definition beside installed profiles and asks Rust to compile the catalogue. Installation checks the venue in PostgreSQL, retains the Rust fingerprint, and rejects a changed interpretation under the same ID. A canonical ID currently selects one physical format. Gate.io depth has existing explicit feed variants. A second arbitrary source layout under the same logical ID requires an engine contract extension.

Registered equity venues can run through the managed normalization workflow. The [Databento reference profiles](../../examples/bring-your-own-data/databento/index.md) show nested JSONL timestamps, explicit unknown trade sides, and bounded depth observations.
