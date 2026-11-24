# Identity and provenance

An identity is meaningful within its scope. Bybit BTCUSDT spot and Bybit BTCUSDT linear perpetual are separate instruments even though the displayed native symbols match. A vendor that distributes the recording can differ from the venue represented by the data.

## Inspection identifiers

| Identifier | Identifies and scope | Where to find it or use it |
| --- | --- | --- |
| Venue code | Execution venue, such as `bybit` | Catalogue, recipe selections and stored instrument context |
| Provider or source key | Registered distribution source or product scope | Acquisition and raw provenance reports; source selectors |
| Native symbol | Venue- and product-specific text such as `BTCUSDT` | Provider records and symbol filters; combine it with the venue, market and type for identity |
| Source-native instrument identity | Provider product, publisher or native instrument ID used to resolve the recording | Acquired/imported evidence and profile bindings |
| Canonical instrument ID | Instrument in a namespace | Canonical rows, prepared jobs, historical authority and stream membership |
| Catalog instrument ID | Catalogue row in a selected registry | Catalog detail. The historical instrument ID is a separate field |
| Instrument reference | A command-specific textual instrument identity | Catalog and Markets display and selectors described below |
| Family or archive scope | A provider grouping that can cover multiple concrete instruments | Discovery and historical family evidence. Normalize resolves actual membership rather than treating a family as a tradable instrument |
| Source object key / raw local-copy ID | Remote object within a source context / registry record of a managed copy | Acquisition and Storage; explicit normalization input selectors use registered inputs |
| Stream ID | Logical instrument/content stream in the resolved processing context | Prepared jobs and publication membership; merge rank overrides use the exact stream ID |
| Stream rank | Ordering or identity binding within a job | Prepared inputs, diagnostics and canonical membership |
| Engine job ID | Engine submission identity | Per-job documents and diagnostics |
| Engine publication ID | Engine-side committed publication identity | Manifests and Engine provenance; dataset search can filter on the Engine publication ID. Merge derives the output ID from the job ID. Normalization has separate job and publication fields |
| Registry publication ID | Registered publication in the selected registry | `datasets show --publication-id PUBLICATION_ID`; publication selection in merge and conversion recipes |
| Registered primary-file ID | Registered physical primary in the selected registry | Positional file ID in dataset inspection; explicit file selectors in recipes |
| Forge run ID | Logical orchestration operation | `runs show RUN_ID`, `runs report RUN_ID`, and `runs logs RUN_ID` |
| Attempt UUID and number | One attempt within a Forge run | Run detail and artifact pointers; the run commands take a run ID |
| Registry identity | Verified database UUID and environment | Retained submissions and recovery checks |
| Timeline reference | Versioned grouping of retained contributors in one registry | `datasets timeline show TIMELINE_REFERENCE` |
| Converted output reference | Durable work key for one indexed converted publication | `datasets show --output-ref OUTPUT_REF`; one-based part numbers identify files within it |

The labels above identify different objects even if two fields contain the same integer. Copy identities from the selected registry's output. The uppercase placeholders stand for your own IDs.

## Instrument references and economic fields

Catalog uses `exchange.instrument_type.market_category.symbol`, for example `bybit.perpetual.linear.BTCUSDT`. Markets commonly displays `exchange.market.symbol`. For a collision, copy its qualified `exchange.market.instrument_type.symbol` reference. Keep dots, hyphens, and other punctuation in the native symbol intact.

| Field | Interpretation |
| --- | --- |
| Instrument type | Product form such as spot, perpetual, dated future, option, or equity |
| Market category | Economic grouping, including linear or inverse contracts. This classifies the instrument; account margin settings and leverage are separate |
| Base and quote assets | The asset whose amount is priced and the asset that denominates the price, where applicable |
| Settlement asset | Asset used for settlement where defined |
| Contract value and denomination | Economic amount per contract and the asset in which it is stated. Read together with quantity type and applicable specification |

## Historical applicability

Current catalogue metadata is useful for discovery. A historical record needs an applicable specification with provenance. If a contract's lot size changed at midnight, the preparation needs evidence covering the appropriate interval. Inferred applicability records a policy assumption with its source and scope. Verified historical specifications carry a separate evidence classification.

Current collection, historical acquisition, profile support, and downstream Engine compatibility are separate capabilities. Consult their respective references for the product you need. [Catalogue and specifications](../workflows/catalogue/specifications.md) · [Markets discovery](../workflows/markets/instrument-discovery.md)

## Registry identity

Testing and production are separately guarded registries. File, publication, and catalogue integers can coincide across them. Retained jobs and recovery evidence can also pin the registry UUID. Use the original registry for exact inspection or recovery. [Run evidence](../reference/data-formats/prepared-forge.md)
