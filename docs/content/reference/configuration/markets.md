# Ranking and historical metadata policies

Ranking policies live under `data/.config/markets/rankings/`. A policy is a local TOML definition. A [snapshot](../../workflows/markets/rankings.md#inspect-a-pinned-result) is a registry publication with its own UUID. See [historical metadata inference](#historical-metadata-inference) for historical specification policy.

## Ranking policy fields

Keys are optional with the defaults shown unless a metric requires them. The loader rejects unknown keys. `rank validate` checks the document and policy constraints. Build separately verifies observations and membership at publication time.

| TOML path | Type and default | Bounds and meaning |
| --- | --- | --- |
| `version` | Integer, `1` | `1` or `2`. Version 2 defaults to automatic turnover conversion. |
| `metric` | String, `"turnover_24h"` | `turnover_24h` for money, `volume` for grouped 24-hour base quantity. |
| `volume_unit`, `group_by` | Strings, omitted for turnover | Both explicitly `"base_asset"` for volume. |
| `exchanges` | String list, `[]` | Empty selects all supported exchanges. Otherwise use `binance`, `bybit`, `okx`, `gateio`, `bitget`. |
| `markets` | String list, `["spot", "linear", "inverse"]` | Nonempty subset of those categories. Collectible combinations follow the [scope matrix](../../workflows/markets/instrument-discovery.md#synchronize-the-catalogue). |
| `instruments` | String list, `[]` | Empty evaluates the exchange/market universe. Nonempty selects exact Markets references within that scope. Use qualified references for collisions. |
| `max_age_seconds` | Integer seconds, `3600` | 1–604800. Both fetch time and window end must be within this age. |
| `max_window_skew_seconds` | Integer seconds, `300` | 0–86400. Source window-end alignment and future window tolerance. For volume the comparison is within each asset group. Also controls automatic FX alignment with the turnover window. |
| `comparison_currency` | String, `"USDT"` | Turnover score denomination. 1–32 uppercase letters or digits. |
| `estimates` | String, `"exclude"` | `exclude` or `allow`. Controls estimated turnover or estimated base volume. Reference-price FX for native turnover is a separate valuation step. |
| `allow_partial_universe` | Boolean, `false` | Allows publishing an eligible subset and recording exclusions or missing scopes. At least one comparable row must survive. |
| `manual_preferences` | Unique string list, `[]` | Turnover only when nonempty. Promotes included exact references in listed order ahead of ordinary score order. Unknown preferences outside the build input universe fail validation at build. |
| `conversion` | Table, defaults below | Turnover only. |
| `conversion_rates` | Table of currency → decimal rate, `{}` | Legacy manual turnover rates, with one shared observation time and source. Positive finite values. |
| `conversion_as_of` | Aware datetime, omitted | Required with nonempty legacy rates. |
| `conversion_source` | String, omitted | Nonblank provenance required with nonempty legacy rates. |

Volume rejects the presence of `comparison_currency`, `conversion`, `conversion_rates`, `conversion_as_of`, and `conversion_source`, including empty tables or explicitly written defaults. An empty `manual_preferences` is allowed.

## Turnover conversion fields

Ranking FX values a retained aggregate with current-market spot references. [Historical event conversion](../../workflows/conversion.md) uses a separate job, settings and observation contract.

| TOML path | Type and default | Bounds and effect |
| --- | --- | --- |
| `conversion.automatic` | Boolean, false in v1 and true in v2 turnover | Enables stored spot-price paths. |
| `conversion.max_price_age_seconds` | Optional integer seconds, inherits `max_age_seconds` | 1–604800. Applies to manual observations and automatic leg freshness. |
| `conversion.max_leg_skew_seconds` | Optional integer seconds, inherits window skew | 0–86400. Maximum timestamp difference between two spot legs. |
| `conversion.max_hops` | Integer, `2` | Exactly 1 or 2. Two hops allow one bridge. |
| `conversion.bridge_assets` | Unique currency list, `["BTC", "ETH"]` | At most 16. Nonempty is an ordered allowlist. Empty allows any eligible connecting asset. |
| `conversion.prefer_same_exchange` | Boolean, `true` | Only `true` is supported. |
| `conversion.allow_cross_exchange` | Boolean, `false` | Permits a complete path on a different exchange. |
| `conversion.allow_mixed_exchanges` | Boolean, `false` | Only `false` is supported. Both legs must come from one venue. |
| `conversion.source_exchanges` | Unique exchange list | Defaults to Binance, Bybit, OKX, Gate.io, Bitget in that order. 1–5 supported codes, in venue preference order. |
| `conversion.equivalent_currencies` | Unique currency list, `[]` | At most 32. Listed currencies are explicitly assumed equal to the comparison currency. Do not include the comparison currency itself. |
| `conversion.manual_rates` | Table, `{}` | Keyed by the native currency being converted. Each entry requires rate, timestamp and source. |
| `conversion.manual_rates.CURRENCY.rate` | Decimal, required per entry | Positive and finite. Prefer a quoted decimal string to retain the intended digits. Multiply native currency by this rate to get the comparison amount. |
| `conversion.manual_rates.CURRENCY.as_of` | Offset-aware datetime, required per entry | Actual reference observation time. Future or expired overrides fail at build. |
| `conversion.manual_rates.CURRENCY.source` | Nonblank string, required per entry | Provenance of the declared rate. |
| `conversion.valuation` | String, `"reference_price_turnover"` | Only accepted convention. Values an aggregate at retained reference prices. |

Currency identifiers use 1–32 uppercase letters/digits. A manual rate for the comparison currency must be exactly 1. The same currency cannot appear in both current and legacy manual rates. Different currency entries may use different representations.

## Rate selection and precision

Identity wins, followed by a manual override, explicit equivalence, then automatic spot FX if enabled. A stale or future selected manual rate prevents valuation for that row. The resolver does not retry by equivalence or market price. Automatic legs require positive finite prices and complete source evidence. Each leg uses source observation time or window end as a proxy and requires both that time and fetch time to pass freshness. The general window skew bounds future tolerance and distance from the turnover window. Two legs additionally pass the leg skew limit.

Eligible automatic paths sort by same venue, fewer legs, configured venue preference, newest oldest leg, configured bridge preference, then stable reference/observation/direction identity. A same-venue bridge can beat a remote direct pair. Rates are ratios of exact decimal inputs. For example, a retained spot price of 0.80 EUR per USDT gives a EUR→USDT reciprocal of 1.25, so 100 EUR compares as 125 USDT. Rational multiplication precedes the 80-significant-digit decimal score projection. Human display can round further. Inspect exact numerator/denominator, source observation IDs, direction, and timestamps using [snapshot inspection](../cli/markets.md#ranking-inspection). No implicit USD or stablecoin equivalence applies.

## Complete examples

Each example is an independent file. Replace illustrative instrument references with those returned from the same registry. The version-1 example uses one synchronized Bybit spot instrument whose turnover is already in USDT.

```toml
exchanges = ["bybit"]
markets = ["spot"]
instruments = ["bybit.spot.BTCUSDT"]
```

Automatic spot conversion may use an eligible stored reference outside the ranking subset. The reference must already be available on the same exchange for this policy.

```toml
version = 2
comparison_currency = "USDT"
exchanges = ["bybit"]
markets = ["spot"]
instruments = ["bybit.spot.BTCUSDT"]

[conversion]
automatic = true
max_price_age_seconds = 300
max_hops = 2
bridge_assets = ["BTC", "ETH"]
source_exchanges = ["bybit"]
```

The manual example is for a fixed-clock illustration. Its rate and timestamp are sample data. A current build requires an actual observation with current provenance and age within policy.

```toml
version = 2
comparison_currency = "EUR"
exchanges = ["bybit"]
markets = ["spot"]
instruments = ["bybit.spot.BTCUSDT"]

[conversion]
automatic = false

[conversion.manual_rates.USDT]
rate = "0.80"
as_of = 2026-10-27T12:00:00Z
source = "Illustrative fixed-clock fixture"
```

Grouped base volume with explicit symbol selection suits a bounded testing walkthrough.

```toml
version = 2
metric = "volume"
volume_unit = "base_asset"
group_by = "base_asset"
exchanges = ["bybit"]
markets = ["spot"]
instruments = ["bybit.spot.BTCUSDT", "bybit.spot.ETHUSDT"]
```

## Defaults, generated template, and standard files

`rank new` writes version 2, USDT comparison, strict coverage, excluded estimates, 3600-second general and price ages, 300-second window and leg skew, and an empty bridge allowlist. It selects all supported exchanges and categories explicitly, uses same-exchange paths and declares no equivalences. An omitted `bridge_assets` in the loader instead defaults to BTC and ETH.

The checkout's standard `turnover.toml` allows partial coverage and estimates, allows a 3600-second window skew, uses 300-second price age and leg skew, enables automatic paths on the same venue with Bybit first, and explicitly treats USD as USDT. The standard `volume.toml` allows partial coverage, inherits estimate exclusion, uses 3600-second age and window skew, and groups base volume. Local policy files can differ. `rank update` reads the installed files.

## Universe, ordering, and exclusions

Broad policies require recent certification of full scopes. Explicit instrument lists require matching registered references and observations. Missing scope certification can block a strict build before individual evaluation. Assess native units, activity scope, estimated flags, timestamps and available conversion separately. Strict publication rejects exclusions. Partial turnover can evaluate an available observation from an incomplete refresh where other checks allow it. Volume requires certified refresh evidence for each included entry even when partial publication is allowed. Standard update pins all inputs to the new refresh and excludes failed scopes.

Turnover orders listed included preferences first, then descending score and stable reference. Exclusions never receive positions. Volume sorts by base asset, descending quantity and stable reference. `group_rank` restarts for each base asset. Storage `position` remains unique across groups and supports inspection.

Volume prefers reported quantity in the correct base unit, including zero. Contract count × a compatible base-denominated face value gives base quantity. With a quote-denominated face value, divide contract count × quote face value by a positive reference price with the matching quote unit and retained time basis. That result is estimated. Volume checks window duration against 24 hours using the window skew tolerance and aligns window ends within each base group. The retained evidence records derivation, scope and specification. Missing or incompatible specifications exclude the row.

## Workflow priorities

[Normalization recipes](normalization.md) and pipeline preparation look up the latest published snapshot by policy name, including older or partial snapshots. Only turnover entries supply priorities. If no usable positions exist, including when the selected snapshot is grouped volume, preparation falls back to deterministic stream identity ordering. Preparation uses stored rankings and makes no statistics requests. Prepared evidence records the snapshot selection and resulting ranks.

## Historical metadata inference

MarketForge works with historical financial data.

However, exchanges do not always provide a complete historical catalogue of every instrument and its specifications.

For example, you might have an OKX futures archive containing trades from a contract that expired several years ago.

The source file may still be available.

However, the exchange's current API might no longer provide all the corresponding historical specifications.

### Why is this a problem?

To normalize the trades correctly, MarketForge needs to know the contract's economic properties.

For example:

- The instrument identity.
- Contract value.
- Contract denomination.
- Quantity type.
- Base and quote assets.
- Settlement asset.
- Historical applicability.

Without this information, a raw quantity such as:

```text
100 contracts
```

cannot be safely interpreted.

### What if historical specifications are missing?

MarketForge supports a controlled metadata-enrichment policy.

It can use retained catalogue information and compatible reference-contract evidence to establish operationally applicable metadata.

This is different from inventing arbitrary contract specifications.

### Why is the distinction important?

A metadata value inferred from a related instrument may be operationally useful.

However, it is not necessarily independently verified as historically correct.

**MarketForge distinguishes operational inference from verified historical authority.**

---

## How Historical Enrichment Fits into Initialization

The current initialization workflow includes historical metadata enrichment.

Run:

```bash
mf init --testing
```

The full initialization workflow performs several stages.

Conceptually:

```text
Application Setup
        |
        v
Database Migrations
        |
        v
Current Catalogue Collection
        |
        v
Historical Metadata Enrichment
        |
        v
Standard Ranking Publication
```

### What does enrichment do?

The historical enrichment stage can:

- Inspect registered exchange downloads.
- Establish canonical instrument identities.
- Preserve current API observations.
- Resolve available historical specification evidence.
- Examine supported contract-family relationships.
- Append immutable specification evidence.
- Report unresolved metadata.

### Does enrichment download raw archives?

No.

It operates on the relevant existing registered information and managed raw inputs.

It does not replace the historical acquisition workflow.

### Does it copy or re-register raw files?

No.

The enrichment stage does not create a separate raw-data registry or move existing raw files into another storage subsystem.

### What if an exchange metadata scope fails?

Previously retained observations can remain available.

Initialization reports exchange collection failures separately from historical metadata readiness.

One successful metadata stage does not automatically mean every historical archive is ready for normalization.

---

## The Historical Inference Policy

MarketForge currently uses a named historical metadata inference policy:

```text
catalogue-btc-reference-v1
```

This policy defines how supported historical specifications are resolved.

### Why use a named policy?

Inference rules should be explicit and traceable.

For example, a future change to the inference algorithm should not silently redefine the meaning of earlier evidence.

A named policy allows MarketForge to record which interpretation rules were used.

### Evidence precedence

The current policy considers evidence in the following order:

1. Historically verified specifications.
2. Immutable historical declarations.
3. Exact retained catalogue specifications.
4. Compatible BTC futures or options reference evidence.

### Why is precedence important?

Not all metadata sources provide the same authority.

For example, an explicit historical specification supported by exchange documentation should not be silently replaced by a value inferred from another contract.

The policy therefore gives stronger evidence precedence over weaker inference.

### Can inference override conflicting explicit declarations?

No.

Conflicting explicit declarations block preparation.

MarketForge does not resolve those conflicts by silently falling back to inferred values.

### Why?

Because doing so could replace historically established instrument rules with an assumption.

For financial data, such substitutions can materially change the interpreted quantities.

---

## Why Use BTC Reference Contracts?

Some derivative markets contain families of contracts with shared economic conventions.

For example, a futures chain may contain several dated BTC contracts belonging to the same product family.

A compatible contract may provide useful evidence about shared specification fields.

### Example

Imagine an exchange lists several BTC futures:

```text
BTC Contract A
BTC Contract B
BTC Contract C
```

Their expiration dates differ.

However, certain shared properties may be the same.

For example:

```text
Quantity denomination
Contract-value convention
Market category
Settlement asset
```

When the required compatibility conditions are satisfied, MarketForge can use a reference contract to fill missing shared specification information.

### Does this mean every BTC contract is identical?

No.

Even related contracts can differ.

For example, contract values or other economic properties may change across products or historical periods.

The inference policy therefore imposes compatibility requirements.

### What must match?

The reference and target must agree on the applicable known properties, including:

- Exchange.
- Product type.
- Market category.
- Base asset.
- Quote asset.
- Settlement asset.
- Known quantity denomination.
- Known contract-value denomination.

### Can a Binance contract be used for an OKX instrument?

Not under the described same-exchange reference requirement.

The reference must belong to the compatible exchange and product context.

### Does this apply only to BTC?

The current named policy uses compatible BTC reference evidence for supported futures and options cases.

Do not generalize this into unrestricted cross-asset specification inheritance.

---

## What Information Can Be Inherited?

The inference policy is deliberately limited.

It can inherit compatible missing shared specification fields.

It does not blindly replace all metadata.

### Example

Suppose the target contract already has:

```text
Exchange: OKX
Instrument type: Future
Market category: Linear
Base asset: BTC
Quote asset: USDT
Expiration: Known
Contract value: Missing
```

A compatible reference may provide evidence for the missing contract-value information under the current policy.

However, the actual inheritance depends on the available source evidence and compatibility rules.

### What cannot be invented?

MarketForge preserves instrument-specific information.

For example:

- Expiration dates.
- Option strike prices.
- Call or put classification.
- Underlying identity.
- Exact known specifications.
- Other contract-specific values.

### Why preserve these fields?

Two futures contracts may share an economic contract convention while expiring on different dates.

They remain different instruments.

Similarly, two options may share a base asset while having different strikes and expirations.

Inference must not erase those distinctions.

### What if the reference conflicts with known target information?

The inference is not accepted as a silent replacement.

Conflicts must be reported and resolved through the supported evidence system.

---

## Operational Inference vs. Historical Verification

MarketForge distinguishes several kinds of specification evidence.

Two important inference methods are:

```text
catalogue_inferred
reference_contract_inferred
```

### Catalogue-inferred evidence

This records information inferred from retained catalogue observations under the supported policy.

### Reference-contract-inferred evidence

This records information inherited from a compatible reference contract.

### Why are these not historically verified?

Because the available evidence may not independently prove that the same specifications applied throughout the target archive's historical period.

For example, a reference contract observed today may share the target's current product conventions.

However, that does not automatically establish the target's exact historical contract value several years earlier.

### What does operationally accepted mean?

The current project policy allows qualifying inferred evidence to be used operationally when its compatibility and applicability requirements are satisfied.

This makes it possible to prepare some historical processing work that would otherwise lack required metadata.

### Does operational acceptance prove the inference is correct?

No.

The distinction should remain visible.

For example:

| Evidence | Interpretation |
|---|---|
| Historically verified | Supported by applicable historical authority. |
| Historical declaration | Explicitly supplied historical information. |
| Catalogue inferred | Derived from retained catalogue evidence under policy. |
| Reference-contract inferred | Derived from a compatible reference contract under policy. |

The last two are operational inference methods.

They should not be presented as independent historical verification.

---

## Inferred Applicability Intervals

Historical normalization requires specifications applicable to the processed source interval.

When MarketForge creates operational inference evidence, it records the assumed applicability interval.

### What does the interval represent?

The current policy can assign an assumed interval covering the relevant registered inventory.

This makes the inferred specification operationally usable for the selected archive collection under the project's policy.

### Does the interval prove historical validity?

No.

The interval is an explicit applicability assumption.

It is not proof that the exchange historically used the same specifications throughout the entire interval.

### Why distinguish observation time?

The evidence also retains when its source information was actually observed.

For example:

```text
Observation date:
November 2026

Assumed applicability:
Historical archive interval
```

These are different timestamps.

The observation time should not be rewritten to look as though the metadata was collected at the historical archive date.

### Why is this important?

Otherwise, a current specification could appear to be a historical observation.

That would make later research and auditing misleading.

---

## What Evidence Does Inference Preserve?

Historical inference records more than the resulting economic values.

The retained evidence includes information such as:

- Source catalogue identities.
- Source API identifiers.
- Source snapshot hashes.
- Inherited fields.
- Inference policy version.
- Applicability information.
- Deterministic evidence fingerprints.

### Why preserve this information?

Suppose a contract multiplier was inferred from a compatible reference.

Later, you discover a verified historical specification.

You need to know how the earlier value was obtained.

The retained evidence makes that interpretation traceable.

### Why use fingerprints?

Fingerprints identify the relevant source evidence and interpretation.

For example, unchanged recollection of the same evidence can reuse the corresponding immutable revision.

If the source metadata or applicability changes, MarketForge can append a new revision.

### Does updating current metadata overwrite historical evidence?

Not automatically.

The historical evidence store is append-oriented and preserves immutable revisions.

This allows MarketForge to retain the history of the information used for previous processing decisions.

### Does a new revision silently replace an explicit declaration?

No.

Conflicting explicit historical declarations remain authoritative conflicts that must be resolved.

---

## Futures Families and Archive Identity

## Why Are Futures Families Difficult?

Some historical derivative archives contain several concrete contracts.

For example, a dated-futures family may include:

```text
BTC-USD Contract A
BTC-USD Contract B
BTC-USD Contract C
```

Each contract has its own expiration and canonical instrument identity.

However, the archive provider may distribute their records together.

### Why is this difficult?

The archive filename may identify only the family.

For example:

```text
BTC-USD
```

This does not identify every concrete contract represented inside the file.

MarketForge needs additional source-native identity evidence.

### What does family membership mean?

A family membership describes which concrete instruments are associated with a particular source family.

For example:

```text
BTC-USD Family
      |
      +-- BTC Contract A
      +-- BTC Contract B
      +-- BTC Contract C
```

### Does a current family listing prove historical completeness?

No.

A current exchange API may omit expired contracts.

An archive may also contain historical instruments not present in the current catalogue.

Therefore, the current family population is not automatically a complete historical record.

---

## Catalogue vs. Archive-Derived Family Membership

MarketForge retains different sources of family membership evidence.

### Catalogue membership

This comes from the retained instrument catalogue.

For example, current exchange metadata may identify several related contracts.

### Archive-derived membership

This comes from supported identity discovery performed against registered historical archive contents.

### Why retain both?

Because current metadata and historical archives can describe different instrument populations.

For example:

```text
Current catalogue:
Contract B
Contract C

Historical archive:
Contract A
Contract B
```

The example is illustrative.

The current catalogue does not automatically prove that Contract A never existed.

Likewise, discovering Contract A in an archive does not establish that every historical contract has been discovered.

### Does MarketForge merge the two into one complete authority?

It retains the relevant evidence and identity mappings under the supported policy.

However, it does not claim that the resulting family membership is historically exhaustive.

---

## OKX Historical Instrument Discovery

OKX provides an important example of family-based historical data.

### Perpetual archives

For supported OKX perpetual archives, MarketForge uses the relevant source-native instrument identity.

### Dated futures

Dated-futures archive bundles may contain several concrete contracts.

MarketForge can perform bounded identity discovery using the production Rust Engine.

### Why use Rust for discovery?

The archive may contain substantial data.

The Engine has the source-format processing capabilities needed to inspect the supported native instrument identities.

### Current discovery limits

The documented bounds are:

| Limit | Value |
|---|---|
| Maximum bytes inspected per managed copy | 256 MiB |
| Maximum discovery time | 180 seconds |

These limits constrain the discovery operation.

### Does discovery read the entire archive?

Not necessarily.

The byte and time budgets can prevent full inspection.

### Does discovering several contracts prove that no others exist?

No.

A bounded scan can establish observed identities.

It cannot guarantee that every historical contract appears within the inspected portion.

### Why is this still useful?

It can identify concrete instruments represented by a registered family archive without requiring every contract to remain listed in the current exchange API.

This provides additional historical identity evidence for normalization preparation.

---

## Historical Identity Discovery Caching

Archive identity discovery can be expensive.

MarketForge therefore caches supported discovery results.

### What identifies a cached result?

The cache uses information including:

- Managed copy checksum.
- Compiled profile fingerprint.
- Inference policy.

### Why use these values?

A discovery result depends on both the source bytes and how those bytes are interpreted.

For example, changing the source profile may change which native instrument fields are decoded.

Similarly, changing the archive contents may change the observed instrument population.

### When can a cached result be reused?

When the relevant source bytes, compiled interpretation, and policy identity remain compatible with the cached evidence.

### What if the archive changes?

A changed copy checksum requires the discovery result to be reconsidered.

### What if the profile changes?

A different compiled profile fingerprint can require rediscovery.

### What if the inference policy changes?

The discovery evidence must be evaluated under the appropriate policy version.

### Does cached discovery create normalized datasets?

No.

Identity discovery produces metadata evidence.

It does not publish normalized Parquet datasets.

### Does discovery remove expired instruments?

No.

The historical identity infrastructure retains relevant inactive and expired catalogue entries.

Archive-discovered expired identities can receive stable canonical mappings.

---

## Options and Historical Metadata

## What About Options?

Options require additional instrument-specific information.

For example:

- Underlying asset.
- Expiration date.
- Strike price.
- Call or put classification.
- Contract value.
- Settlement convention.

These properties distinguish individual option contracts.

### Why can't MarketForge infer everything from the base asset?

Because one underlying can have many options.

For example:

```text
BTC Call
BTC Put
Different strikes
Different expirations
```

These are separate instruments.

### What can the inference policy preserve?

The historical specification inference mechanism preserves available option identity fields.

It does not fabricate missing strike prices, expirations, or underlying identities.

### Are options fully supported for normalization?

Not currently.

The existing Rust offline catalogue and installed normalization profiles do not provide general options-normalization support.

### Important distinction: acquisition vs. current-market collection

The current `mf markets sync` collection adapters omit options.

However, certain historical acquisition adapters support options archive discovery.

For example, the separate historical acquisition subsystem supports relevant Bybit and OKX options archive selections.

These are different capabilities.

**Being able to obtain an options archive does not mean MarketForge can already normalize its contents.**

### What happens when required options support is missing?

Initialization and preparation report the applicable limitation.

They do not invent missing instrument identity or Engine support.

---

## Initialization and Historical Readiness

## Historical Evidence Storage

The historical metadata system uses immutable evidence records.

The relevant database support was introduced through migration 0014.

### Why retain evidence separately?

MarketForge needs to distinguish:

- Current catalogue observations.
- Explicit historical declarations.
- Inferred historical specifications.
- Family membership evidence.
- Archive-derived identity information.

These records have different meanings and levels of authority.

### Does this create a second raw-data registry?

No.

The evidence tables are not a replacement for the managed raw-data registry.

Raw archives remain tracked through the existing acquisition and storage infrastructure.

### Does this replace current catalogue observations?

No.

Current observations remain useful for market discovery, statistics, and reference metadata.

Historical evidence adds information needed to interpret older registered recordings.

### Why preserve immutable revisions?

Because financial-data processing should remain traceable.

If the source evidence changes, the new interpretation should not silently erase the information used for earlier work.

---
