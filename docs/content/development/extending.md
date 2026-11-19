# Extend MarketForge

## Add a historical adapter

Implement native scope validation and archive identity in `forge/historical/archives/sources/`. Derive UTC nominal bounds from the provider's calendar and distinguish feed variants that can share a filename. Test discovery with controlled HTTP responses and assertions for exact instrument and content type. Verify how local copies register before adding a normalization example.

## Extend profiles and decoders

Start at the Rust catalogue compiler and the shared trade or depth decoder. Keep profile validation and runtime interpretation in agreement. Add an example raw record with independently stated expected canonical values. For depth, check complete reconstructed state, observation offsets and boundaries as well as row counts. Validate the profile through Forge and then process the sample.

## Change a handoff or publication

Review both sides of the [architecture contract table](architecture.md). Update the actual protocol negotiation, parsers, fixtures and compatibility checks together. Exercise the producer, Engine parser, execution and Forge registration. For Parquet or manifest changes, check physical types, ownership ranges, auxiliary inventory, lineage and consumers such as merge. A successful parser test alone establishes structural compatibility.

## Evolve the PostgreSQL schema

Add a migration using the existing registry migration sequence. Applied scripts are checked by name and checksum. Test upgrading a previous registry and applying to a fresh guarded database. Exercise rollback on failure and test how old IDs, references and filesystem artifacts remain accessible. See [database maintenance](../reference/cli/maintenance.md#database).
