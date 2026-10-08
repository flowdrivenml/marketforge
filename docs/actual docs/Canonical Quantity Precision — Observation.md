During real-archive testing across 18 trade configurations, MarketForge's quantity calculations were consistent with the expected instrument economics.

For inverse contracts, exchange-provided base quantities sometimes differed slightly from MarketForge's calculated values due to rounding or truncation.

Example — Binance inverse futures:

```text
Raw contracts       = 12
Contract value      = 100 USD
Price               = 79800.5

Exchange base_qty   = 0.01503749
MarketForge base    = 0.0150374997650390661712645911
```

MarketForge calculates:

\[
Q_{\text{base}} = \frac{12 \times 100}{79800.5}
\]

**Observation:** MarketForge retains greater numerical precision than the exchange-provided quantity, which is limited to eight decimal places in this example.

However, greater precision does not necessarily imply greater accuracy. MarketForge calculates a more precise theoretical base-equivalent quantity using contract metadata and execution price, while exchanges may apply their own rounding or truncation conventions.

**Policy:**
- Preserve native quantities without unnecessary rounding.
- Derive base and quote equivalents using decimal arithmetic and instrument metadata.
- Retain available computational precision, subject to `rust_decimal` limitations.
- Validate exchange-provided quantities using precision-aware tolerances rather than exact equality.
- Distinguish numerical precision from economic accuracy.

**Conclusion:** MarketForge provides higher-precision calculated quantities for downstream quantitative research without claiming to recover information beyond the exchange's underlying market data.