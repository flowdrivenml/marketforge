# Create a trade profile

Work from the checkout with the [managed normalizer and guarded registry](../../getting-started/installation.md). Use the fictional [synthetic bundle](../../examples/catalogue/index.md) as a baseline.

1. Inspect the [raw CSV](../../examples/catalogue/synthetic-trades.csv). It has no header. Its four cells hold event time, price, quantity and side.

   ```bash
   mf acquisition profiles sample --file docs/content/examples/catalogue/synthetic-trades.csv --no-header
   ```
2. Open the [trade profile](../../examples/catalogue/example_exchange.equity.equity.trades.toml). Declare the container and CSV layout in `source`. Use the field positions to map the timestamp as an integer in Unix nanoseconds, price and quantity as decimal values, and side as text.
3. Map the price directly and quantity in base units. The profile maps case-insensitive `B` to buy and `S` to sell. This sample has no native trade ID. Avoid claiming uniqueness by inventing an ID or using time as one. For another source, design any identity or filtering rules against its documented semantics. [Trade mappings](../../reference/profiles.md#required-trade-mappings)
4. Run the [synthetic bundle commands](../../examples/catalogue/index.md) to validate and install the profile, register the sample and normalize it.
5. Preview the returned trade file. Check the timestamp 1730125800000000000, buy side, 233.62 price and 10 base units against the CSV.

If the preview differs, inspect raw field positions, timestamp encoding, side transformation and quantity convention before processing more data. [Troubleshoot profiles](profile-troubleshooting.md)
