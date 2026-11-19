# Acquisition profiles and registration CLI

## Profiles and registration

```bash
mf acquisition profiles list --type trades --exchange bitget
mf acquisition profiles inspect bitget.perpetual.linear.depth
mf acquisition profiles validate bitget.perpetual.linear.depth
mf acquisition profiles validate --file /absolute/profile-id.toml
mf acquisition profiles add bitget.perpetual.linear.depth --from /absolute/bitget.perpetual.linear.depth.toml --testing
mf acquisition register new example --from docs/content/examples/catalogue/registration.toml
mf acquisition register edit example
mf acquisition register inspect example
mf acquisition register validate example --testing
mf acquisition register plan example --testing
mf acquisition register run example --testing
```

`add` is canonical; `register` is an alias for profile publication. Profile IDs have exactly four components: `exchange.instrument_type.market_category.trades|depth`. The filename must be exactly `<id>.toml`. The production Rust catalogue is `data/.config/historical/profiles/{trades,depth}`; schema-2 self-contained profiles compile through the installed normalizer.

Users author all fields. Forge does not generate profiles or infer units, side semantics, sequence domains, resets or snapshot/deletion rules. Discovery/inspection require no database and report TOML/namespace inspection, not engine acceptance. Validation uses production semantic compilation and a staged candidate catalogue. Unsupported inventory-only definitions cannot be installed for processing. Acceptance does not prove arbitrary source files conform to the definition. Different interpretations under the same ID conflict; equivalent compiled interpretations can reuse an installed definition without replacing its bytes. Gate.io feed variants remain selected by the supported explicit source feed; arbitrary competing variants under one ID are unsupported.

Register the [catalogue prerequisites](../../workflows/catalogue/index.md) before validating or running an archive recipe. Registration names resolve under `data/.config/historical/registration/`; explicit paths are accepted. `new --from` copies a recipe without overwriting an existing draft, or creates an incomplete declaration for a directory/file. Source roots relative to the repository resolve independently of the invocation directory. File selectors are relative to that source root; enumeration is deterministic and bounded. A registration freezes selected files; files arriving afterward require a new registration. Duplicate physical inputs fail.

See the [complete synthetic workflow](../../examples/catalogue/index.md) for examples and limitations.

### Profile and archive action options

| Profile command | Behavior and options |
| --- | --- |
| `acquisition profiles list` | List stored profile definitions. Exact optional filters are `--type trades\|depth`, `--exchange VENUE`, `--instrument-type TYPE`, and `--market-category CATEGORY`. |
| `acquisition profiles sample --file PATH` | Read a bounded sample of an original file. Use `--member MEMBER` for a specific ZIP member and `--header` or `--no-header` to specify CSV header evidence. Omit both header flags to leave it unspecified. |
| `acquisition profiles inspect PROFILE_ID` | Return the installed definition and its location without compiling. |
| `acquisition profiles validate PROFILE_ID` or `acquisition profiles validate --file PATH` | Supply exactly one source. Uses the installed normalizer's validation mode, without processing data or querying the registry. |
| `acquisition profiles add PROFILE_ID --from PATH [--testing]` | Validate and install or reuse a definition. `acquisition profiles register PROFILE_ID --from PATH [--testing]` is the same operation. Both use the guarded testing catalogue even if the optional flag is omitted. |

| Archive registration command | Behavior |
| --- | --- |
| `acquisition register new NAME --from PATH` | Create a new recipe or draft. A TOML input is copied. A file or directory creates a starter declaration to complete. |
| `acquisition register edit NAME` | Open in the configured editor, then inspect. Global JSON is rejected. |
| `acquisition register inspect NAME` | Inspect the declared existing local inputs without a database or copy. |
| `acquisition register validate NAME --testing` | Check catalogue bindings, historical applicability, profile and source preflight. |
| `acquisition register plan NAME --testing` | Preflight and write a retained operation plan. No managed raw copies or registration rows are created. |
| `acquisition register run NAME --testing` | Resolve and freeze inputs, verify hashes and publish or reuse managed copies and interpretations. Retry the same declaration after inspecting its copy receipts. |

Commands support global JSON except the interactive archive editor. Use the [complete registration TOML](../../examples/catalogue/registration.toml) as the declaration model. Source/provider provenance is separate from the execution venue. Each input selects root-relative files or patterns, bounded by `max_files` (default 1000, maximum 10000). Optional specification fingerprints pin historical evidence. A native identity can record source-scoped publisher and instrument IDs. See the [recording example](../../examples/bring-your-own-data/databento/index.md).
