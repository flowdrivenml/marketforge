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

Follow the [trade and depth profile guides](../catalogue/profiles.md) and use the [schema reference](../../reference/profiles.md) when choosing field types and transformations. After registration, [prepare normalization](../normalize.md).

For types, defaults and native identity fields, see the [registration recipe reference](../../reference/registration.md).
