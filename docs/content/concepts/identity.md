# Identity and provenance

The provider distributes bytes. The execution venue supplies the market context. A recording can identify a product, publisher and native instrument ID even when many venues use the same symbol. In MarketForge, Bybit spot BTCUSDT and the Bybit linear perpetual BTCUSDT have different canonical identities. [Catalogue declarations](../configuration/catalogue.md)

| Identifier | Scope and relationships |
| --- | --- |
| Source object key | Stable within the registered source and request scope. A managed archive row identifies the local copy and retained digest |
| Canonical instrument ID | Qualified by instrument namespace. Effective-dated specifications determine quantity and contract interpretation |
| Stream ID | Distinguishes instrument, content and source context. One logical stream can span parts. One combined part can contain many streams |
| Stream rank | Integer tie-breaker or rank binding inside a prepared job and its output. Repeated parts of one stream can share its rank |
| Engine job and publication IDs | Normalization carries separate job and dataset IDs. Physical merge identifies output through the merge job ID. Interpret each field at its wire boundary |
| Registry publication ID | One database registration for the committed Engine publication and namespace |
| Registry file dataset ID | One registered primary file. A publication can have several. Recipe selectors state which type they accept |

Preserve native identity and effective historical evidence alongside canonical fields. A catalogue observation made today can be operationally applicable under an explicit inference policy without independently proving the historical specification. [Historical specifications](../workflows/catalogue/specifications.md) · [Inference policy](../configuration/markets.md#historical-metadata-inference)

## Historical applicability

An instrument identity survives changes in its trading terms. Suppose a contract’s lot size changes at midnight. A current catalogue tick size or quantity step applies to the observation collected today. A retained historical specification has its own provenance and applicability interval, which is what makes it usable for an archived record. A request that crosses a terms change may need separate processing inputs or specifications.

Keep the native provider identifier and recorded interval with the canonical instrument. Use the [specifications guide](../workflows/catalogue/specifications.md) to record applicability or inspect inferred evidence before preparation.

## Registry identity

Numeric catalogue and file IDs belong to one PostgreSQL registry. Two environments can use the same integer for different objects. Use the registry that produced a recorded ID, and preserve the retained UUID for managed execution recovery. [Run evidence](../reference/prepared-forge.md)


## Inspection identifiers

| Copy this value | Where to use it |
| --- | --- |
| Forge run ID | `mf runs show RUN_ID`, `logs` or `report`. A child run has its own run ID. |
| Attempt number and UUID | Find the attempt and artifact pointers in `runs show RUN_ID`. The CLI has no standalone attempt selector. |
| Engine job, execution or publication ID | Correlate prepared jobs, receipts and manifest evidence. `datasets find --engine-publication-id` filters registered publications by their exact Engine publication ID. |
| Registry publication ID | `mf datasets show --publication-id PUBLICATION_ID` or `streams --publication-id PUBLICATION_ID` |
| Registry primary-file ID | Positional `FILE_ID` for file inspection. Obtain it from publication inspection, the file view or an execution report’s registered outputs. |
| Timeline reference | `mf datasets timeline show TIMELINE_REFERENCE`. Copy it from timeline `--wide` or explicit timeline JSON. It describes a grouping in one registry. |
| Indexed conversion output reference | `mf datasets show --output-ref OUTPUT_REF`. The work key can identify a converted output without registry publication or file IDs. Use its one-based part number for file-level reads. |

A run can record a new output, reuse of an existing canonical publication, or a result with no publication. A publication can remain in a historical run report after retirement. Compare exact IDs and retained evidence when tracing the relationship. [Run workflow](../workflows/runs.md) · [Dataset workflow](../workflows/datasets.md)
