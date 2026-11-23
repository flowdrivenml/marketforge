# Bring external data

See the [BYOD guide](../../examples/bring-your-own-data/index.md) for the complete workflow and a real recording example.

Register the [venue, instrument, historical specifications, and Rust profiles](../catalogue/index.md) first. Archive recipes refer to those existing entities and record separate source provider and product information.

The complete [synthetic archive example](../../examples/catalogue/registration.toml) registers one trade file and one L2 depth file. Each input gives its canonical instrument, profile ID, content type, and file selection. The declared half-open coverage interval applies to every file in the recipe. Add an optional `specification` fingerprint to an input to pin its applicable historical evidence.

```bash
recipe="$PWD/docs/content/examples/catalogue/registration.toml"
mf acquisition register inspect "$recipe"
mf acquisition register validate "$recipe" --testing
mf acquisition register plan "$recipe" --testing
mf acquisition register run "$recipe" --testing
mf storage list --testing --origin external --exchange example_exchange --instrument-type equity
```

Validation requires the previously registered catalogue identity, one applicable historical declaration covering the interval, and an installed profile whose Rust fingerprint can be verified. Registration rejects mismatched profiles, conflicting source identities, and inconsistent sampled layouts before publishing managed copies. It retains the declaration and profile fingerprints beside the input checksum. The sample is bounded. Complete event validity belongs to normalization.

Repeating registration can reuse an existing managed copy after checksum verification. Provider, product, source root, and source key describe acquisition provenance. Supply the real execution venue separately. Native publisher and instrument identifiers can be recorded as provenance when available and remain scoped to the source.

## Raw-only preservation

Use `mf acquisition import` to preserve local bytes whose interpretation is still incomplete. For example, create a draft with `mf acquisition import new recorder --from ./recordings/trades.csv`, edit its explicit venue and source identity, and run `mf acquisition import run recorder --testing`. The [legacy import configuration](../../configuration/acquisition.md#external-import-configuration) describes its fields. Raw-only import can retain declarations and reuse managed copies. Use `acquisition register` to bind a source to a verified catalogue and profile interpretation.


## Creating Historical Normalization Profiles

Use `mf normalize profiles list` to find an installed interpretation for the raw layout. To author a missing interpretation, run `mf normalize profiles guide trades` or `mf normalize profiles guide depth`. Follow the [profile workflow](../catalogue/profiles.md) and use the [schema reference](../../reference/profiles.md) when choosing field types and transformations. After registration, [prepare normalization](../normalize.md).

For types, defaults and native identity fields, see the [registration recipe reference](../../reference/registration.md).

## Inspect and apply a registration plan

### Native Identity Sampling

When native identity is supplied, registration preflight can check the corresponding fields against bounded source samples.

### Why is this useful?

Suppose a recipe declares:

```toml
publisher_id = 2
instrument_id = 38
```

but the sampled source records contain a different publisher or instrument ID.

This may indicate that the wrong file was selected or the declaration is incorrect.

Registration can detect such mismatches during preflight.

### Does sampling read the entire file?

No.

The supported source-layout check is bounded.

It does not independently verify every record in an arbitrarily large recording.

### Does every source format support native identity sampling?

Not necessarily.

The current bounded sampler supports particular CSV and JSONL identity paths.

For example, the specialized XLSX sample path does not provide the same native identity sampling behavior.

### What happens if the source format is unsupported?

Registration reports the applicable source or sampling incompatibility.

Do not bypass the check by inventing field names.

### Does successful sampling guarantee complete source validity?

No.

A bounded sample can establish useful evidence about selected records.

Complete record processing and integrity evaluation belong to normalization.

---

## Complete Databento Example

### Register AAPL Trades and MBP-10 Recordings

The repository contains a more realistic registration example using Databento XNAS.ITCH data.

The source files are:

```text
.recordings/
├── AAPL_trades.jsonl
└── AAPL_orderbook_L2.jsonl
```

They represent the historical interval:

```text
2024-10-28 14:30:00 UTC
through
2024-10-28 14:40:00 UTC
```

The end is exclusive.

### Source provenance

The recording was supplied by Databento.

The execution venue is Nasdaq.

The instrument is AAPL.

The source-native identifiers include:

```text
publisher_id = 2
instrument_id = 38
```

These values are scoped to the relevant source recording.

### Why is this example important?

It demonstrates the distinction between:

- Market-data provider.
- Execution venue.
- Canonical instrument identity.
- Source-native instrument identity.
- Source-format profile.

All of these are represented separately.

---

### Complete Databento Registration Recipe

Save this as:

```text
data/.config/workflows/data_engineering/import/registration/databento-aapl.toml
```

```toml
[registration]
version = 1
name = "databento-aapl"

[source]
key = "databento-xnas-itch-aapl-20241028"
provider = "Databento"
product = "XNAS.ITCH"
kind = "recording"
root = ".recordings"
provenance = "User-supplied Databento Trades and MBP-10 JSONL exports for 2024-10-28. Publisher ID 2 and native instrument ID 38 are scoped to these source files."

[coverage]
start = 2024-10-28T14:30:00Z
end = 2024-10-28T14:40:00Z

[[inputs]]
instrument = "xnas.equity.equity.AAPL"
profile = "xnas.equity.equity.trades"
content = "trades"
files = ["AAPL_trades.jsonl"]

[inputs.native_identity]
namespace = "databento:XNAS.ITCH:2024-10-28"
publisher_field = "hd.publisher_id"
publisher_id = 2
instrument_field = "hd.instrument_id"
instrument_id = 38
provenance = "Header identity in the supplied XNAS.ITCH exports."

[[inputs]]
instrument = "xnas.equity.equity.AAPL"
profile = "xnas.equity.equity.depth"
content = "depth"
files = ["AAPL_orderbook_L2.jsonl"]

[inputs.native_identity]
namespace = "databento:XNAS.ITCH:2024-10-28"
publisher_field = "hd.publisher_id"
publisher_id = 2
instrument_field = "hd.instrument_id"
instrument_id = 38
provenance = "Header identity in the supplied XNAS.ITCH exports."
```

This is the complete registration recipe supplied with the repository's Databento example.

### What is registered?

| Input | Instrument | Profile | Native identity |
|---|---|---|---|
| Trades | AAPL | `xnas.equity.equity.trades` | Publisher 2, instrument 38 |
| Depth | AAPL | `xnas.equity.equity.depth` | Publisher 2, instrument 38 |

### Why do both inputs use the same native IDs?

Both recordings describe the same source-native instrument under the declared publisher and product namespace.

However, they contain different market-data content.

### Why use separate profiles?

The trade and depth records have different structures and normalization requirements.

The trade profile interprets executions.

The depth profile interprets the corresponding order-book observations.

### Does Databento become the exchange?

No.

The instrument reference remains:

```text
xnas.equity.equity.AAPL
```

The provider is recorded separately:

```toml
provider = "Databento"
```

### Does registration require the original Databento API?

No.

This workflow operates on the supplied local files.

It does not need to download the same files again.

### Important prerequisites

Before executing this recipe, make sure:

- The source files exist.
- XNAS is registered.
- AAPL is registered.
- Applicable historical specifications are available.
- The trade profile is installed.
- The depth profile is installed.
- The profile quantity encodings agree with the specifications.

See the [Databento example ](../../examples/bring-your-own-data/databento/index.md) for the complete setup.

---

## Validation Stages

### Structural Validation vs. Registration Preflight

A registration recipe must satisfy several levels of validation.

These checks are different.

### Structural validation

Checks the TOML declaration itself.

For example:

- Required fields.
- Supported version.
- Input array structure.
- Instrument reference format.
- Profile ID matching.
- File-selection presence.
- Specification fingerprint syntax.
- Native identity field types.

### Source inspection

Checks the declared local files.

For example:

- Source root.
- Selected paths.
- File count.
- Physical file identity.
- Duplicate inputs.

### Registration preflight

Uses the testing registry and installed profiles to check the complete registration binding.

For example:

- Registered instrument identity.
- Explicit historical specification evidence.
- Profile installation.
- Rust semantic profile validation.
- Quantity encoding compatibility.
- Bounded source-layout compatibility.
- Existing source registration conflicts.

### Why separate these checks?

Different errors require different information.

For example, a missing TOML field can be detected locally.

A missing historical specification requires a database lookup.

A profile compilation error requires the Rust normalizer.

A source-layout mismatch requires inspecting the selected file.

Keeping these stages distinct makes failures easier to diagnose.

---
