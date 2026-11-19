# Manual catalogue CLI

Register canonical execution venues, instruments and economic specifications before [registering raw archives](forge.md#profiles-and-registration). For every action below, `ENTITY` is one of `exchange`, `instrument` or `specs`. The same four actions are available on each entity.

| Invocation | Behavior |
| --- | --- |
| `mf catalog ENTITY validate --file PATH` | Validate the declaration TOML locally. No database, network or mutation. |
| `mf catalog ENTITY add --from PATH --testing` | Validate and write in the testing catalogue. Reuses an exact declaration and rejects conflicts. |
| `mf catalog ENTITY list --testing` | List stored entities. Specifications include current observations and historical evidence. |
| `mf catalog ENTITY show REFERENCE --testing` | Inspect one venue, one instrument, or its specification evidence. |

Use the venue code for an exchange reference. For instruments and specifications the reference is `exchange.instrument_type.market_category.symbol`. PostgreSQL generates database identities. Register the exchange before an instrument, and the instrument before its specifications. Specification declarations preserve decimal precision, provenance and optional temporal validity. A current observation does not establish historical applicability. Review unit and validity examples in the [catalogue workflow](../../workflows/catalogue/index.md) and [specification reference](../../workflows/catalogue/specifications.md).

```bash
mf catalog exchange validate --file docs/content/examples/catalogue/exchange.toml
mf catalog instrument add --from docs/content/examples/catalogue/instrument.toml --testing
mf --json catalog specs show example_exchange.equity.equity.AAPL --testing
```

For the example write and lookup, first follow the catalogue workflow to register the prerequisite declarations. Add creates catalogue records and can add immutable historical evidence. It does not acquire archives or install format profiles. A profile maps recorded source fields into normalization and is managed with [`mf acquisition profiles`](forge.md#profiles-and-registration). All catalogue actions support global `--json`. Successful actions return zero, with validation errors and conflicts reported through the common CLI error path.
