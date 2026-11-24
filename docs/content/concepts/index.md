# MarketForge concepts

MarketForge turns source recordings into inspectable historical publications. Each stage carries evidence forward about the bytes it used, how it interpreted them, and what it committed. Start with the objects and their identities, then follow the example through the processing stages. Read the operation guides when you are ready to run commands.

| Question | Start here |
| --- | --- |
| What is stored, processed, and published? | [Data model](data-model.md) |
| Which identifier do I need? | [Identity](identity.md) |
| What belongs in a recipe, profile, runtime file, or job? | [Configuration](configuration.md) |
| What does the requested interval actually select? | [Time and coverage](time.md) |
| How do trade and book observations become rows? | [Trades and depth](depth.md) |
| Which event order can a reader rely on? | [Ordering](ordering.md) |
| How do source records become canonical streams? | [Normalization](normalization.md) |
| How do compatible streams share a timeline? | [Merge](merge.md) |
| How are historical prices valued in another quote currency? | [Conversion](conversion.md) |
| What did validation verify, and what evidence survives? | [Evidence](evidence.md) |
| What happened to a run or its output? | [Execution](execution.md) |
| What does a recurring term mean? | [Glossary](glossary.md) |

## Follow one example

The running example selects Bybit BTCUSDT **linear perpetual** trades and depth for `[2026-10-27T00:00:00Z, 2026-10-28T00:00:00Z)`. It assumes suitable archives, applicable evidence, and supported profiles are available. The observations and prices used to illustrate behavior are hypothetical. The BTCUSDT example ends with identity valuation in USDT. A separate multi-instrument example demonstrates conversion through reference trades.

For procedures, use [Acquisition](../workflows/acquisition/index.md), [Normalization](../workflows/normalization/index.md), [Merge](../workflows/merge.md), and [Conversion](../workflows/conversion.md). For exact schemas, see [prepared requests](../reference/data-formats/prepared.md) and [publication formats](../reference/data-formats/publications.md).
