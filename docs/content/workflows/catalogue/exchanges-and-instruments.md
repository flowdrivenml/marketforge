# Exchanges and instruments

Declare the execution venue in [exchange.toml](../../examples/catalogue/exchange.toml). The exchange code is the stable key and uses lowercase letters, digits, and underscores. Exact re-registration returns the same ID. A different name under the same code is rejected.

```toml
code = "example_exchange"
name = "Example Exchange"
```

```bash
mf catalog exchange validate --file docs/content/examples/catalogue/exchange.toml
mf catalog exchange add --from docs/content/examples/catalogue/exchange.toml --testing
mf catalog exchange list --testing
mf catalog exchange show example_exchange --testing
```

The [instrument declaration](../../examples/catalogue/instrument.toml) uses an exchange code, a market category, an instrument type, and the native symbol. For an equity, both category fields are `equity`. The optional settlement asset belongs to the execution instrument if applicable. PostgreSQL returns the catalogue ID and the historical authority ID. Conflicting properties under the same instrument identity are rejected.

```toml
exchange = "example_exchange"
symbol = "AAPL"
instrument_type = "equity"
market_category = "equity"
base_asset = "AAPL"
quote_asset = "USD"
status = "active"
```

```bash
mf catalog instrument validate --file docs/content/examples/catalogue/instrument.toml
mf catalog instrument add --from docs/content/examples/catalogue/instrument.toml --testing
mf catalog instrument list --testing
mf --json catalog instrument show example_exchange.equity.equity.AAPL --testing
```
