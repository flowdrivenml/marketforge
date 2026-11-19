# Validate, install and troubleshoot profiles

## Validating and Installing a Profile

### Inspect the existing catalogue

Run commands from the project root:

```bash
mf acquisition profiles list

mf acquisition profiles list \
  --exchange binance \
  --instrument-type spot \
  --market-category spot \
  --type trades

mf acquisition profiles inspect binance.spot.spot.trades
```

Prefer starting from an existing profile with the same physical layout and event semantics.

`list` and `inspect` check TOML syntax and namespace consistency. They do not perform full Rust semantic validation. [Profile CLI](../../reference/cli/registration.md)

### Build the managed normalizer

Semantic validation uses the managed Rust normalizer. Build it when it is not already installed or needs updating:

```bash
mf engine build normalize
mf engine status
```

The managed build installs the normalizer and its installation metadata. Merely copying an arbitrary executable into `.bin` does not satisfy the managed installation checks. [Profile CLI](../../reference/cli/registration.md)

### Validate a draft

To validate a manually authored file:

```bash
mf acquisition profiles validate \
  --file ./profile-drafts/binance.spot.spot.trades.toml
```

For machine-readable output:

```bash
mf --json acquisition profiles validate \
  --file ./profile-drafts/binance.spot.spot.trades.toml
```

Validation stages a temporary catalogue containing the installed profiles and the candidate, then invokes production Rust semantic compilation.

An installed profile can also be validated by ID:

```bash
mf acquisition profiles validate binance.spot.spot.trades
```

Supply either the installed ID or `--file`, not both.

Leave `unsupported_reason` absent for an executable profile. Profiles with that setting are inventory-only and are rejected by the executable-profile validation workflow. [Profile CLI](../../reference/cli/registration.md)

### Install a validated definition

```bash
mf acquisition profiles add binance.spot.spot.trades \
  --from ./profile-drafts/binance.spot.spot.trades.toml
```

Installation validates frozen candidate bytes and installs an independent copy.

When the ID already exists:

| Candidate interpretation | Result |
| --- | --- |
| Equivalent compiled interpretation | Existing profile is reused. |
| Different compiled interpretation | Installation fails with a conflict. |

A changed same-ID candidate can pass `validate --file` and subsequently fail `add`. Validation substitutes it into a temporary catalogue; installation additionally compares it with the currently installed interpretation.

The current CLI has no `profiles update` or `--force` option. Resolve a same-ID interpretation change deliberately in the catalogue and associated fixtures. [Profile CLI](../../reference/cli/registration.md)

### Verify against actual records

Semantic validation establishes that the profile compiles. It does not establish that the profile correctly describes the source bytes.

Use the normal orchestration workflow to process a small, registered sample with authoritative instrument metadata. Check representative output values and the integrity report.

A useful verification sample should establish:

- Timestamps correspond to known source times after conversion to nanoseconds.
- Prices, quantity units, and contract conversions are correct.
- Trade sides match the intended aggressor-side convention.
- Native trade IDs have the declared scope.
- Snapshots initialize the book correctly.
- Updates set, add, subtract, or delete levels according to the source contract.
- Sequence behavior matches the actual feed.
- Parsing failures, rejected records, and integrity findings are understood.

For a profile covering several archive periods or source layouts, include representative samples from those periods. Passing a single recent file does not establish that an older layout has the same interpretation.

## Common Authoring Problems

| Problem | What to check |
| --- | --- |
| Filename mismatch | The filename must be exactly `<id>.toml`. |
| Invalid namespace | Check all four ID components and the corresponding metadata. |
| Invalid format code | Check the exchange prefix, `T`/`B` letter, and positive numeric suffix. |
| Unknown field reference | Every `field` mapping must reference a declared raw field exactly. |
| Wrong headerless CSV values | Declare all columns with explicit, contiguous, 1-based positions. |
| Incorrect timestamps | Verify the raw field’s `unix_*` encoding against source evidence. |
| Incorrect derivative quantities | Verify `quantity_encoding` and authoritative contract specifications. |
| Conflicting normalization rule | Move the mapping into either the common section or the individual rule; do not duplicate it. |
| Depth record matches multiple rules | Make the event filters disjoint. Reordering rules does not resolve ambiguity. |
| Depth record matches no rule | Check actual filter values, string types, and coverage of the intended record types. |
| Validation succeeds but input processing fails | Check the actual decoder path, packaging, header behavior, and source bytes. |
| Installation rejects a validated candidate | Check whether the installed same-ID profile has a different compiled interpretation. |
