# Catalogue declarations

Catalogue commands accept one TOML table per declaration. The file describes a venue, a canonical instrument, or its specifications. The registry assigns numeric identities and retains evidence independently of the declaration file. See the [registration procedure](../workflows/catalogue/index.md) and [CLI syntax](../reference/cli/catalog.md).

## Venue and instrument

| Declaration | Path | Type and requirement | Rules |
| --- | --- | --- | --- |
| Exchange | `code` | Required string | Lowercase token beginning with a letter, followed by lowercase letters, digits or underscores. Maximum 32 characters. `external` is reserved. |
| Exchange | `name` | Required string | Nonempty printable display name without surrounding whitespace. |
| Instrument | `exchange` | Required string | Registered execution venue code. |
| Instrument | `symbol` | Required string | Nonempty, no whitespace. `UNSPECIFIED` is rejected. |
| Instrument | `market_category` | Required enum | `spot`, `linear`, `inverse`, `equity`, `option`. |
| Instrument | `instrument_type` | Required enum | `spot`, `perpetual`, `future`, `equity`, `option`. Spot, equity and option must agree with the category. |
| Instrument | `base_asset`, `quote_asset` | Required asset tokens | Declare currency or asset identity explicitly. |
| Instrument | `settlement_asset` | Optional asset token | Omitted when unknown or inapplicable. |
| Instrument | `status` | Optional token | Defaults to `active`. |

## Specifications

The canonical `instrument` key has the form `exchange.instrument_type.market_category.symbol`. Use the identity already registered in the target catalogue. The specification declaration requires `provenance` and positive `tick_size`. Asset fields, when supplied, are assertions checked against the registered identity.

| Path | Type and requirement | Rules and meaning |
| --- | --- | --- |
| `instrument` | Required string | Canonical instrument reference. |
| `provenance` | Required nonblank string | Evidence description. |
| `as_of`, `fetched_at` | Conditionally required timezone-aware timestamps | Retrieval time. Supply at least one for current observations. If both are supplied, they must agree. An omitted alias inherits the other. |
| `valid_from`, `valid_to` | Optional timezone-aware timestamps | Supply both for historical applicability. Start precedes end. Dates can be TOML offset datetimes or datetime strings. |
| `quantity_type` | Required enum | `base`, `quote`, `contracts`. |
| `contract_value`, `contract_value_asset` | Conditionally required decimal string and denomination | Required for contracts. Positive finite size and explicit denomination must be paired. Spot and equity forbid contract sizing. |
| `tick_size`, `qty_step` | Exact decimal. Tick required, step optional | Finite and positive. Tick is the price increment. Step uses the declared quantity unit. |
| `min_qty`, `max_qty` | Optional exact decimals | Finite, nonnegative, and ordered when both are given. Units follow `quantity_type`. |
| `min_notional`, `max_notional` | Optional exact decimals | Finite and nonnegative. When denominations agree, the minimum cannot exceed the maximum. |
| `min_notional_asset`, `max_notional_asset` | Optional strings | Denominations for applicable notional bounds. |
| `base_asset`, `quote_asset`, `settlement_asset` | Optional asset tokens | Asset assertions for the registered instrument. |

Use TOML integers or quoted decimals for exact decimal fields. TOML floating-point values are rejected. The [current and historical examples](../examples/catalogue/index.md) use synthetic values. Collection and inference rules are documented in [historical metadata policies](markets.md#historical-metadata-inference).
