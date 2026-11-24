# Historical quote conversion

Conversion values a supported merged timeline in a target quote currency. It retains the original source event and instrument identity, including native price, quantity and provenance. It adds valuation status, target quote, rate and evidence as applicable. A trade adds target price and target-quote quantity when defined. Depth keeps the source changes and attaches target price and applicable quote quantity to valued levels. Native timestamps, stream membership, order and reconstruction markers travel with the source event. A target price expresses quote value in the requested currency. A target quote quantity values the original quote-denominated amount when present. Base amounts, contract counts and contract denominations keep their source meanings.

The running Bybit BTCUSDT example already has USDT quote. A selected observation valued in USDT uses identity rate 1 without an FX leg. This example illustrates identity valuation alone.

| Source layout | Target values added by the Engine |
| --- | --- |
| Trades | `price_target` and `quantity_quote_target` |
| Depth | `changes_target` alongside the original level changes |
| Combined | `trade_target` or `depth_target` for the corresponding event kind |

The result carries `conversion_status` and, for applicable outcomes, `conversion_rate`, `conversion_path`, `conversion_references` and `conversion_failure`. References identify the observed trade and direction for each chosen leg. A missing native quote amount remains unavailable as a target quote amount. Target decimal fields use Decimal256 with scale 28, and arithmetic follows the Engine’s checked rounding rules.

## Separate example with reference instruments

These hypothetical prices illustrate units. A rate multiplies an amount in the starting currency to obtain an amount in the destination currency.

| Route | Reference price and calculation |
| --- | --- |
| USDT → USDT | Rate 1, so 100 USDT remains 100 USDT |
| BTC → USDT | BTCUSDT at 60,000 USDT per BTC gives 0.01 BTC × 60,000 = 600 USDT |
| USDC → BTC | BTCUSDC at 62,500 USDC per BTC gives 625 USDC ÷ 62,500 = 0.01 BTC |
| USDC → BTC → USDT | Combine the two legs. 625 USDC becomes 0.01 BTC and then 600 USDT |

Each eligible trade supplies a price for its bound reference stream. This relationship uses real configured trades at eligible times. USDC and USDT carry separate identities.

## How references become eligible

Forge resolves configured trade stream IDs and pins the publication and instrument bindings. A combined input may supply its own selected trade references. Combined inputs can use a distinct trade-only merge for external references. Depth-only preparation requires a distinct trade-only external publication used by at least one non-identity route. Trade-only conversion uses internal references and rejects selected streams with no planned route. Combined can retain events without a route as unavailable. Internal combined trade identities take precedence over conflicting external identities. A depth stream for the same instrument supplies book observations. Trade references supply prices.

The Engine uses the last eligible trade under the reference key and ordering contract. Internal reference state uses already encountered observations with a strictly earlier key, ordered by event timestamp, stream rank, task, and event ordinal. An earlier key can share the target timestamp. A current trade is valued before being added to reference state. The external cursor advances only through strictly earlier timestamps. Freshness measures nonnegative event-time age in nanoseconds. An observation at the configured age limit remains eligible.

Causal input can regress in event time. The regression-aware state selects an eligible predecessor rather than borrowing a future-time observation already seen. Supported checkpoints retain reference state across physical boundaries. Route reachability only establishes a graph path. A particular event can lack an eligible or fresh observation on one of its legs. Reference updates count changes in price state, independently of output events.

## Policy and output outcomes

| Control or outcome | Meaning |
| --- | --- |
| Price source and hops | `last_trade`. The Engine accepts a maximum of 1–4 legs, with default 3 |
| Path policy | `primary` keeps the first planned route. `all` considers planned alternatives in order and uses the first successful valuation |
| Instrument kinds | An optional selection limits which source observations are valued |
| Identity / converted | Identity covers matching quote currency. Converted records a successful non-identity valuation and its route evidence |
| Unavailable | The selected conversion cannot be valued, for example because a required reference is missing, future, stale, or fails arithmetic. Valuation fields remain null where unavailable |
| Skipped | In combined output, a source observation retained outside the selected valuation policy |

The combined reader explicitly emits the skipped status for an excluded instrument kind. The single-kind trade and depth readers can instead report no-path unavailability for an excluded stream.

Conversion preserves the approved full source-publication scope, including supported causal context. A verified commit can contain unavailable or skipped valuations. Forge indexes eligible converted outputs for lookup and reuse. Formal catalogue registration is unavailable for this operation.

Historical event valuation and [current-market ranking valuation](../reference/configuration/markets.md#rate-selection-and-precision) have separate inputs, time semantics and policy schemas. [Conversion guide](../workflows/conversion.md) · [Prepared job contract](../reference/data-formats/prepared-conversion.md) · [Converted publication schema](../reference/data-formats/publication-converted.md)

## Execution and reuse

Forge resolves whole source and reference publications before handing standalone jobs to the managed Engine. A persisted preparation can run Rust input validation and record planned routes. Execution determines the valuation for each source event, then validates staged output and publishes it with an atomic no-replace commit on Linux. Forge verifies the committed output and associates it with a durable work reference. Registry-backed indexing records that reference separately from catalogue registration.

Forge processes jobs in order and stops on a blocked, interrupted, or failed job. Later jobs appear as not attempted. A job may finish Engine execution and still fail Forge verification or indexing. Output validation verifies source preservation and layout-specific conversion constraints. A verified result can include unavailable valuations.

| Disposition | Physical work |
| --- | --- |
| Newly executed | Forge claims the work, retains submission evidence, and invokes the converter. |
| Reused | Forge verifies matching completed output and saves a receipt pointing to the canonical result. The Engine skips physical conversion. |
| Recovered | Forge verifies a previously committed result against the original intent, pinned job and executable, then completes the original index entry. The requesting run can reuse that entry. |

The work key includes the registry, executable checksum, and resolved job content, including input paths and fingerprints, reference selection, policy, embedded metadata, resources and Parquet settings. The generated job ID and output destination are excluded. Changing an effective job field or the binary can change the key. Forge also checks live input eligibility and verifies the retained output before reuse. A pathname alone establishes neither equivalence nor integrity.

[Prepared jobs](../reference/data-formats/prepared-conversion.md) · [Converted output and counters](../reference/data-formats/publication-converted.md) · [Recovery](../maintenance/conversion.md#workspace-ownership-and-interrupted-publication)
