# Instrument specifications

In **Catalogue → Specifications**, choose **New** and use **Find** to
select the instrument by venue, type, market and symbol. Exact decimal
values can be entered directly. The form reveals applicable sizing fields
and marks contract value and denomination as required when quantity is
measured in contracts. Review the validity interval before **Register**.

Use a quoted decimal or an exact TOML integer for economic values. An instrument declaration supplies the default base, quote, and settlement assets. Derivative quantities expressed in contracts require an explicit value and denomination. Historical declarations need provenance and an inclusive start and exclusive end. An observation timestamp can be supplied when known.

[Example historical specification](../../examples/catalogue/specs.toml) for synthetic AAPL data:

```toml
instrument = "example_exchange.equity.equity.AAPL"
quantity_type = "base"
tick_size = "0.01"
qty_step = "1"
valid_from = 2024-10-28T00:00:00Z
valid_to = 2024-10-29T00:00:00Z
provenance = "Synthetic example parameters for the example files only"
```

```bash
mf catalog specs validate --file docs/content/examples/catalogue/specs.toml
mf catalog specs add --from docs/content/examples/catalogue/specs.toml --testing
mf catalog specs list --testing
mf --json catalog specs show example_exchange.equity.equity.AAPL --testing
```

Historical declarations are immutable and retain their evidence IDs and fingerprints. Conflicting overlapping revisions are rejected. An adjacent or disjoint interval can carry a different specification. Exact repeat submissions reuse the existing evidence. Archive registration requires a single applicable declaration that covers the whole interval.

To register a [current observation](../../examples/catalogue/current-specs.toml), omit both validity fields and supply the actual `fetched_at` or `as_of` time. A newer observation advances the current catalogue row. An older one is retained as evidence. Current evidence carries no historical validity and does not qualify an external archive for registration.
