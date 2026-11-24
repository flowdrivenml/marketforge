# Catalogue commands

List exchanges and instruments by identity, or sort historical specification evidence by retained observation time. See [collection sorting](sorting.md).

Validate, register and inspect catalogue declarations. Catalog lists show registered records in tables. Specification inspection displays current fields and historical evidence separately, with assumed intervals labeled as such. Local validation checks the declaration. Registration reports the accepted identity or evidence without assigning a mutation type. Use global `--json` for the original structured result. [Workflow and prerequisites](../../workflows/catalogue/index.md) · [Shared CLI behavior](index.md)


## Synopsis and arguments

### `mf catalog exchange add`

```text
mf catalog exchange add [-h] [--testing | --production] --from SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--from` | Required. |

### `mf catalog exchange list`

```text
mf catalog exchange list [-h] [--testing | --production]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf catalog exchange show`

```text
mf catalog exchange show [-h] [--testing | --production] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `reference` | Required. |

### `mf catalog exchange validate`

```text
mf catalog exchange validate [-h] --file SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--file` | Required. |

### `mf catalog instrument add`

```text
mf catalog instrument add [-h] [--testing | --production] --from SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--from` | Required. |

### `mf catalog instrument list`

```text
mf catalog instrument list [-h] [--testing | --production]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf catalog instrument show`

```text
mf catalog instrument show [-h] [--testing | --production] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `reference` | Required. |

### `mf catalog instrument validate`

```text
mf catalog instrument validate [-h] --file SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--file` | Required. |

### `mf catalog specs add`

```text
mf catalog specs add [-h] [--testing | --production] --from SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--from` | Required. |

### `mf catalog specs list`

```text
mf catalog specs list [-h] [--testing | --production]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf catalog specs show`

```text
mf catalog specs show [-h] [--testing | --production] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `reference` | Required. |

### `mf catalog specs validate`

```text
mf catalog specs validate [-h] --file SOURCE
```

| Argument or option | Parser contract |
| --- | --- |
| `--file` | Required. |

## Catalogue Command Reference

MarketForge provides the same four basic operations for exchanges, instruments, and specifications.

These commands use the `mf catalog` namespace.

### Supported entities

The current manual catalogue CLI supports:

| Entity | What it manages |
|---|---|
| `exchange` | Execution venues. |
| `instrument` | Individual financial instruments. |
| `specs` | Current and historical instrument specifications. |

The general command structure is:

```text
mf catalog ENTITY ACTION
```

Replace `ENTITY` with one of the supported entity names.

### Available actions

| Action | Purpose |
|---|---|
| `validate` | Check a local declaration without writing to the database. |
| `add` | Validate and register a declaration in the testing catalogue. |
| `list` | List registered catalogue records. |
| `show` | Inspect a specific registered entry. |

### Validate

```text
mf catalog ENTITY validate --file PATH
```

For example:

```bash
mf catalog instrument validate \
    --file docs/content/examples/catalogue/instrument.toml
```

Validation reads the local TOML file.

It does not establish that the corresponding records are already present in PostgreSQL.

### Add

```text
mf catalog ENTITY add --from PATH --testing
```

For example:

```bash
mf catalog instrument add \
    --from docs/content/examples/catalogue/instrument.toml \
    --testing
```

Adding a declaration performs database work.

The command checks the relevant existing identities and rejects conflicting information.

### List

```text
mf catalog ENTITY list --testing
```

For example:

```bash
mf catalog instrument list --testing
```

This lists the corresponding registered records.

### Show

```text
mf catalog ENTITY show REFERENCE --testing
```

For example:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

The reference format depends on the entity.

| Entity | Reference |
|---|---|
| Exchange | Registered venue code |
| Instrument | `exchange.instrument_type.market_category.symbol` |
| Specifications | The corresponding instrument reference |

### JSON output

All catalogue actions support the global `--json` option.

For example:

```bash
mf --json catalog instrument list --testing
```

```bash
mf --json catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

The `--json` option must appear before `catalog`.

JSON output is useful when scripting catalogue inspection or examining detailed registration evidence.

### Database behavior

| Command | Requires PostgreSQL | Can write catalogue records |
|---|---|---|
| `validate` | No | No |
| `add` | Yes | Yes |
| `list` | Yes | No |
| `show` | Yes | No |

These commands do not contact exchanges to download historical archives.

Manual catalogue registration also does not require an acquisition adapter, market-statistics refresh, or ranking snapshot.

---

## Repeated Registration and Conflicts

MarketForge is designed to preserve stable instrument identities.

This means repeated submissions should not silently change an existing market definition.

### Registering the same exchange twice

If an exchange declaration matches the existing registered information, MarketForge can reuse it.

A different exchange name under the same code is rejected.

### Registering the same instrument twice

If an instrument declaration matches the existing registered identity and properties, MarketForge can reuse the existing record.

Conflicting instrument properties are rejected.

### Registering the same historical specifications twice

Identical historical evidence can be reused.

Conflicting overlapping validity intervals are rejected.

An adjacent or disjoint historical interval may contain different specifications.

### Why preserve these rules?

Consider historical research performed using a particular contract specification.

If the catalogue silently replaced that specification with newer values, previously processed datasets could become difficult to explain or reproduce.

MarketForge therefore retains historical specification evidence and rejects conflicting declarations instead of silently choosing new values.

The goal is to make historical interpretation traceable.

### Can I change an existing instrument's meaning?

Do not attempt to redefine an existing instrument by submitting incompatible properties under the same identity.

If the original declaration was incorrect, investigate the existing catalogue record and the appropriate supported correction path.

A conflicting `add` operation is not a general-purpose migration or overwrite mechanism.

Similarly, a new historical specification should describe a genuinely applicable period rather than overwrite earlier evidence without justification.

---

## Troubleshooting Catalogue Registration

Most catalogue errors involve invalid declarations, missing dependencies, or conflicting metadata.

### Exchange not found

**Possible cause:** You are trying to register an instrument before registering its exchange.

First inspect the exchange:

```bash
mf catalog exchange show example_exchange --testing
```

If it does not exist, register the appropriate exchange declaration.

Then retry the instrument registration.

### Instrument not found

**Possible cause:** You are registering specifications for an instrument that has not yet been added.

Inspect the instrument:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

If it is absent, register the instrument declaration first.

### Invalid TOML declaration

**Possible cause:** The file is malformed, missing required fields, or contains unsupported values.

Run the relevant validation command:

```bash
mf catalog specs validate \
    --file docs/content/examples/catalogue/specs.toml
```

Review the reported validation error and correct the declaration.

Validation is useful because it does not modify the database.

### Conflicting exchange or instrument definition

**Possible cause:** A catalogue identity already exists with different registered properties.

Inspect the existing record before retrying.

For example:

```bash
mf catalog instrument show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Compare it with the submitted TOML file.

Do not change the symbol or market classification arbitrarily to bypass the conflict.

### Conflicting historical specifications

**Possible cause:** A new declaration overlaps an existing historical interval but contains incompatible values.

Inspect the existing specification evidence:

```bash
mf --json catalog specs show \
    example_exchange.equity.equity.AAPL \
    --testing
```

Review the validity intervals, economic values, and provenance.

If a genuine specification change occurred, declare the correct applicable periods.

Do not use a new observation timestamp as a substitute for historical validity.

### Missing historical specification coverage

**Possible cause:** The registered specifications do not cover the entire period represented by a raw input.

Inspect the relevant historical specification intervals.

Compare them with the declared source coverage.

Register additional applicable evidence where necessary.

Do not extend a validity interval without evidence that the values were actually valid during the additional period.

### Database connection rejected

**Possible cause:** The testing database is not configured, initialized, or recognized as the expected testing environment.

Check:

```bash
mf database status --testing
```

See [Database setup](../../getting-started/installation.md) for the required credentials and identity checks.

### Profile installation fails after catalogue registration

Catalogue registration and profile installation are separate operations.

If a profile fails validation, inspect its format mappings and the Rust compiler diagnostics.

If installation fails because of a missing venue or an existing conflicting profile identity, inspect the corresponding catalogue and profile records.

See [Profile authoring and troubleshooting](../../workflows/normalization/author-profile.md).

---
