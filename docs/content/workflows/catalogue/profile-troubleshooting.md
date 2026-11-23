# Troubleshoot normalization profiles

Use the [authoring guide](../../reference/profiles.md) for draft creation, Rust validation, installation, and the sample check. Start here when the compiler rejects a definition or a known raw value differs from the emitted record.

```bash
mf engine status | cpout
mf normalize profiles list --exchange binance --type trades | cpout
mf normalize profiles validate binance.spot.spot.trades | cpout
```

To inspect an installed file in read-only Neovim, run
`mf normalize profiles inspect binance.spot.spot.trades`. To inspect only its text,
use `mf normalize profiles show binance.spot.spot.trades | cpout`.
Use the [CLI reference](../../reference/cli/normalize.md#profile-commands) for the full syntax.

<a id="mapping-and-input-diagnostics"></a>

## Common Profile Identity Problems

### Profile ID has too many components

**Cause:** The ID contains an unsupported variant or version component.

For example:

```text
binance.spot.spot.trades.custom
```

is invalid.

Use exactly:

```text
exchange.instrument_type.market_category.trades|depth
```

### Filename does not match ID

**Cause:** The filename differs from the internal profile identity.

Use the exact required filename:

```text
<profile-id>.toml
```

### Instrument type and market category disagree

**Cause:** The declared combination is invalid.

For example, an equity profile must use the equity market category.

### Format code is rejected

**Cause:** The code does not match the supported exchange prefix, content letter, or positive-number rule.

Use the format code associated with the actual source.

### Installation conflicts with an existing profile

**Cause:** The same profile ID already has a different compiled interpretation.

Inspect the installed definition and resolve the conflict explicitly.

Do not invent another ID component or overwrite the installed file manually.

---

## Common Source Format Problems

### Unsupported record format

**Cause:** The selected format and record layout do not form a supported decoder combination.

For example, arbitrary headered CSV depth input is not automatically available.

Use an existing compatible profile as the starting point.

### Incorrect source container

**Cause:** The profile declares the wrong packaging type.

For example, a ZIP archive is declared as plain CSV.

Inspect the actual source container.

### Header mismatch

**Cause:** The profile expects headers that do not match the source file.

For headered trade CSV, use the actual header field names.

### Invalid positional fields

**Cause:** Headerless CSV positions are duplicated, missing, or noncontiguous.

Use 1-based positions covering every declared physical column.

### Unsupported sorting

**Cause:** The profile declares timestamp-sortable ordering for a source whose runtime path cannot safely sort its stateful events.

Use the actual supported source-ordering contract.

---

## Timestamp and Quantity Problems

### Timestamps are incorrect

**Possible cause:** The profile uses the wrong timestamp encoding.

Check whether the source uses:

```text
seconds
milliseconds
microseconds
nanoseconds
ISO 8601
```

Do not infer the unit solely from the exchange name.

### ISO timestamps fail

**Possible cause:** The source lacks a timezone or the declarations do not satisfy the supported ISO timestamp requirements.

Use the correct source encoding.

### Trade quantities are too large or too small

**Possible cause:** The source denomination is wrong.

For example, a contract count may have been treated as base-asset quantity.

Inspect the actual quantity encoding and applicable instrument specifications.

### Spot profile declares contracts

**Cause:** Spot profiles cannot use contract quantity encoding.

Use the correct supported quantity interpretation.

### Contract value is missing

**Cause:** The source uses contract quantities but the processing job lacks authoritative contract-value information.

Resolve the relevant instrument specifications through the catalogue.

Do not hardcode an invented multiplier into the profile.

---

## Trade Mapping Problems

### Side mapping is reversed

**Cause:** The source-side convention was misunderstood.

Consult the provider's actual trade-side semantics.

### Unmapped side value

**Cause:** The source contains a value not covered by the explicit mapping.

Add the correct supported mapping only after establishing its meaning.

### `casefold` does not accept a value

**Cause:** The source value falls outside the supported buy/sell vocabulary.

Do not assume arbitrary whitespace trimming or custom case normalization.

### Signed quantity has zero side

**Cause:** Zero does not establish positive or negative direction.

Investigate the source semantics rather than assigning an arbitrary side.

### Trade ID scope is invalid

**Cause:** The declaration uses an unsupported reset-domain combination or lacks the required trade ID mapping.

Check the selected native identity contract.

### Source-local IDs were treated as global

**Cause:** The profile claims stronger uniqueness than the source guarantees.

Use a scope supported by actual source evidence.

---

## Depth Mapping Problems

### A record matches no rule

**Cause:** The event filters do not cover the source's actual event type.

Inspect the source values and supported rule mappings.

### A record matches multiple rules

**Cause:** Filters overlap.

Make the rule selections disjoint.

Rule order does not resolve ambiguous matches.

### Snapshot and update are confused

**Cause:** The event classifier maps the source to the wrong target.

Verify the source's snapshot and incremental-update semantics.

### Book levels are incorrect

**Cause:** The profile uses the wrong array element order or object field names.

Inspect `level_format` and `level_fields`.

### Absolute quantities accumulate incorrectly

**Cause:** Absolute updates were treated as relative changes.

For an absolute-level source, the supplied quantity replaces the level quantity.

### Relative updates fail

**Cause:** The source layout is not supported by the current relative-update decoder.

The implemented path is specialized for supported scalar CSV sources.

### Sequence errors appear unexpectedly

**Cause:** The profile declares the wrong sequence policy.

For example, a source may allow jumps while the profile declares consecutive sequencing.

Use the source's documented sequence contract.

### Missing snapshot

**Cause:** Incremental updates cannot be initialized according to the required reconstruction rules.

Check the source archive and snapshot availability.

Do not invent an initial book state.

### Bounded observation is treated as full depth

**Cause:** The source's top-N limitation was ignored.

Preserve the actual bounded depth semantics.

---

## Specialized Format Problems

### XLSX profile fails

**Possible cause:** The workbook does not match the fixed supported layout.

Check:

- One worksheet.
- Headers.
- Required field names.
- Unix-second timestamps.
- Supported ascending ordering.

### Mixed-instrument XLSX fails

**Cause:** The current runtime does not support XLSX family routing.

Use a supported source-specific workflow.

### Gate.io feed variant fails

**Cause:** The selected variant is incomplete or does not match the requested source feed.

Each variant must provide its own complete source, field, and normalization declarations.

### Native instrument lookup fails

**Cause:** The corresponding concrete instrument is unavailable in the prepared job's catalogue.

`lookup = "catalog"` does not fetch missing exchange metadata.

Resolve the historical instrument identity and specifications first.
