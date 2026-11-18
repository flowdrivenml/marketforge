# Manual catalogue registration

Register the execution venue, its instrument, applicable historical specifications, and each required Rust profile before registering external archives. The order matters because PostgreSQL assigns the exchange and instrument IDs and creates a canonical historical instrument mapping when it inserts the instrument. A profile identifies a venue and market category and can be installed once that venue exists.

Use the testing database and install the managed Rust validator first.

```bash
mf database migrate --testing
mf engine build normalize
```

Catalogue registration requires no API adapter, market statistics, or ranking snapshot. Initialization collects only the venues and scopes that have API adapters. A provider such as Databento distributes data, while the venue is the explicit execution identity. Publisher and provider details belong in source provenance. Source-native instrument IDs are scoped to their publisher and source and are independent of PostgreSQL IDs.

The linked files use an AAPL-shaped equity identity at a fictional venue and two synthetic source records. Their economics are demonstration values. Use verified declarations and source evidence for real recordings.

- [Exchange and instrument](exchanges-and-instruments.md)
- [Current and historical specifications](specifications.md)
- [Trades and L2 depth profiles](profiles.md)
- [Register external archives](../acquisition/external-data.md)
