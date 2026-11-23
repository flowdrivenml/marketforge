# Extend source integration and processing

Use this guide to place a source change at the correct boundary. The examples follow Bybit BTC perpetual data from provider discovery to canonical trades and depth. Run focused tests in the [local environment](setup.md) and keep a small raw sample with a known outcome when changing interpretation.

## Change acquisition discovery

Use this path when the provider changes its listing, URI, authentication, pagination, feed variants, or archive download behavior. Acquisition owns locating and identifying objects and recording their nominal time bounds. It does not turn provider records into canonical events.

1. Start at `ArchiveRequest`, the internal `ArchiveSource` protocol, and `ExchangeSource.validate` and `discover`. In the Bybit branch, check native scope requirements and the final response URL before parsing a listing. Mirror that dispatch and validation for new provider behavior. Adding a provider also requires checking how the calling acquisition service constructs its source and how the venue or provider authority is registered.
2. Build a `DiscoveredArchive` with the correct source request, source object key, filename and nominal half-open interval. Use the provider's calendar or archive semantics for time bounds. `archive_scope_document` and `archive_scope_key` are part of the storage identity. Feed variants can share a symbol and date while requiring distinct identities.
3. Keep temporary or signed URLs separate from stable object identity. Distinguish a successful empty listing from an error page or incomplete collection. Validate access, redirects, returned scope, and malformed links before handing a candidate to download or cache logic.
4. Check the persistence path through archive registry and managed acquisition storage. Preserve the local archive hash, source object relationship, storage root and relative path. An external file can enter through the registration/import path when its bytes and identity are already known.

Use the sanitized Bybit directory examples as the nearby fixture. For a listing change, verify an exact scope, interval, and object key for a positive case. Add a negative page or link that would silently contaminate scope if accepted. If downloads change, also check retention, status and checksums with the relevant storage tests. Update the [acquisition configuration](../configuration/acquisition.md), [historical acquisition guide](../workflows/acquisition/historical-data.md), and [external data guide](../workflows/acquisition/external-data.md) when their respective contracts change. [Archive source protocol](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/archives/sources/base.py) · [Provider dispatch](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/archives/sources/exchanges.py) · [Archive identity](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/archives/models.py)

## Change a format profile

Use a profile when the representation is already supported and the change can be described with existing field, format, timestamp, quantity and operation mappings. Start with the installed profile for the same venue, instrument type, market category and content type. See the [profile authoring workflow](../workflows/catalogue/profiles.md), its [trade](../workflows/catalogue/profile-trades.md) and [depth](../workflows/catalogue/profile-depth.md) examples, and the [authoritative profile reference](../reference/profiles.md).

1. Identify the archive member and raw field shape from a representative sample. State the exact expected canonical timestamp and quantity first. Specify timestamp units and distinguish source event time from system time when the source provides both. For derivative sizes, identify whether the source quantity represents base, quote, or contracts, and verify that effective instrument specifications support the interpretation.
2. Edit the profile under the identity convention `exchange.instrument_type.market_category.data_type`. The lifecycle in Forge's profile service inspects and validates TOML and manages installation. Rust's `FormatCatalog` loads schema-2 catalogue profiles, validates physical and semantic fields and checks the compiled profile against a task. The task and manifest record provenance that downstream checks can use.
3. Exercise both compiler validation and execution. With a current managed normalizer and installed profiles, run the schema and catalogue check on the Bybit linear perpetual trade profile from the checkout root.

    ```bash
    uv run --project forge --locked mf normalize profiles validate bybit.perpetual.linear.trades
    ```

    For a candidate file, use `validate --file PATH` in place of the installed ID. Forge builds a temporary catalogue containing the candidate and runs the normalizer's production profile validation. This check makes no registry changes.

    A valid profile establishes that the declared format and mappings fit the supported contract. Run a bounded sample to verify the canonical values and any integrity counters. For depth, test one initialization followed by a change and verify the resulting levels and the event index, including an observation that emits no levels if it is relevant.
4. Update the corresponding profile reference only if the contract has changed. Update workflow examples when their supported parameters or expected interpretation change. Keep historical provenance traceable when a same-name profile changes.

[Forge profile lifecycle](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/profiles/service.py) · [Rust profile compiler](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/config/catalog.rs) · [Profile regression target](https://github.com/flowdrivenml/marketforge/blob/main/engine/tests/historical/normalize/profiles.rs)

## Change a reader or Engine semantics

A reader change belongs where bytes become records. Begin with `SourceFormat` classification and `with_source_reader` in the normalization worker, then follow trade or depth worker dispatch to the shared format implementation. For a new physical format, audit both ordinary and family-task paths in addition to profile validation. Archive compression and member selection precede record interpretation. A provider download adapter alone cannot add a record encoding.

For a semantic change, start at the worker's downstream boundary. Trade interpretation and reconciliation have different state from a depth book. Depth distinguishes initialization, changes, observations, segments, fingerprints and level rows. Decide whether a rule affects source interpretation, ordering, reconstruction or the persisted format. Check the compatibility impact at the stage where the rule first changes the meaning of an input or output.

| Example | Expected behavior to write down before coding |
| --- | --- |
| Source CSV timestamp at a sub-millisecond boundary | Exact nanosecond value under the declared timestamp unit, including the null or invalid-field behavior |
| Snapshot then an absolute-size update | Resulting size at the affected level, unchanged levels, and the recorded boundary and canonical row offset |
| New decoder record over its configured limit | Explicit error through the worker path with bounded input handling |
| Causal timeline consumption | Preserved sequence order and reconstruction context even when event timestamps regress |

Use the nearby CSV depth pipeline test or a trade fixture to exercise format dispatch. When changing a codec or column reader, exercise invalid bytes, missing fields or members and relevant resource limits. When changing Engine semantics, assert exact values and independently specified state. Add parallel or batch-boundary equivalence coverage where the changed guarantee spans those boundaries. Check merge and conversion consumers if the change touches chronology, provenance fields, event offsets or depth initialization. [Worker code](https://github.com/flowdrivenml/marketforge/tree/main/engine/src/historical/normalize/worker) · [Family processing](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/normalize/family.rs) · [Depth CSV pipeline tests](https://github.com/flowdrivenml/marketforge/blob/main/engine/tests/historical/normalize/depth/depth_csv_pipeline.rs) · [Runtime and integrity policy](../reference/runtime.md)

## Walkthrough: accept a revised Bybit directory listing

Suppose Bybit changes the markup around an existing BTCUSDT archive link while keeping the URL path and filename convention. The owning boundary is listing discovery, since the archive bytes and profile semantics are unchanged.

1. Open `forge/historical/archives/sources/exchanges.py` in `nvim`. Trace the existing Bybit dispatch from `ExchangeSource.discover` to the directory parser. Add support in that branch so all callers use the same adapter and registry integration.
2. Start with `forge/tests/unit/historical/archives/fixtures/bybit_linear.html`. Keep its `/trading/BTCUSDT` directory and one in-window link such as `BTCUSDT2026-11-02.csv.gz`. Change the markup to the new form. Add an out-of-window link or a neighboring instrument to test isolation.
3. Test via the existing `httpx.MockTransport` harness in `test_source_regressions.py`. Assert the exact filename, unchanged stable object identity, and nominal interval from November 2 at 00:00 UTC to November 3 at 00:00 UTC for the in-window archive. Check separately that a wrong-instrument-only page produces the existing discovery error and does not masquerade as a successful empty listing.
4. Run the fixture and negative cases together:

    ```bash
    uv run --project forge --group dev pytest -q \
      forge/tests/unit/historical/archives/test_source_regressions.py -k bybit
    ```

5. If object identity or interval semantics change, review adapter versioning and the discovery-cache and managed-copy tests before accepting the change. A markup-only change keeps the registration shape. Document any new selector in the [acquisition recipe reference](../configuration/acquisition.md) and any changed operator behavior in the [acquisition guide](../workflows/acquisition/historical-data.md).

The same fixture strategy works for altered pagination or feed variants. Keep the provider boundary assertions about identity and interval separate from profile and Engine assertions about decoded events.
