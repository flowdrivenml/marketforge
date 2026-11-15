# Bring external data

External imports register a recording, purchased dataset, or other local source and copy selected files into managed raw storage. They preserve bytes and declared identity; they do not infer venue semantics, normalize records, or establish processing readiness.

## Prepare a description

Replace `./recordings/trades.csv` with an existing stable source file:

```bash
marketforge acquisition import new recorder --from ./recordings/trades.csv
```

This creates `data/.config/acquisition/import/recorder.toml`. A directory instead seeds `patterns = ["*"]`; review selection before running. The generated draft contains `symbol = "UNSPECIFIED"`, which deliberately fails validation until you supply real identity. Edit it manually: there is no import `edit` command.

Use the [complete import configuration](../../configuration/acquisition.md#external-import-configuration). Declare the actual venue/instrument represented by the file, not the vendor as the exchange. `source_key` and `source_kind` separately describe external origin. Spot and perpetual identities in spot/linear/inverse categories are supported by this import schema; dated futures and options are not.

## Inspect, validate and plan locally

```bash
marketforge acquisition import inspect recorder
marketforge acquisition import validate recorder
marketforge --json acquisition import plan recorder
```

These read source files without PostgreSQL or network access. Selection must stay within `root`, fit `max_files`, and resolve to stable regular files. Relative `root` paths resolve from the process working directory, not from the TOML file; use the repository-root convention or an intentional absolute path.

Inspection samples at most 20 files and 64 KiB of decoded content per file. It recognizes CSV-like delimited text or JSONL from plain, gzip, and ZIP containers. Multi-member ZIPs require explicit member selection for drafting. Binary/unknown layouts and tar-gzip member sampling are reported unsupported; **raw-byte import can still be possible**. Recognition is not full-file parsing or market-schema validation.

## Optional format draft

For an actual selected input named `trades.csv` with a known header:

```bash
marketforge acquisition import format new recorder --profile recorder-trades --input trades.csv --header
```

`--input` is the exact root-relative input reference in the frozen selection. For a multi-member ZIP, add `--member` with the exact member name. Use `--no-header` only when known; otherwise leave header interpretation unresolved.

> **Current checkout limitation.** The draft helper expects physical templates under `data/.config/historical/formats/trades/` or `formats/depth/`. Those directories are absent in the inspected working tree, so this exposed command cannot currently complete here without separately resolving that catalogue prerequisite. Raw import does not depend on this drafting step. Do not copy unrelated profiles into place to bypass it.

Drafting writes physical/profile TOML and diagnostics under `data/.config/historical/format-drafts/recorder-trades/`, outside the active catalogue. It requires existing Engine physical templates in the workspace. Timestamp/quantity encodings, event/order-book semantics, and mapping rules remain unapproved. It does not activate the profile or update the import recipe. A standalone Engine profile-validation command has not been established by this interface; drafting is not approval for execution.

## Register managed copies

This step writes files, registry records and run artifacts:

```bash
marketforge acquisition import run recorder --testing
```

Unknown instruments require explicit historical specifications. Known catalogue identity can permit raw registration without a historical specification, but does not prove that today's specification matches the recording. Declare historical specifications when needed. A source key is bound to its root and origin kind; reusing it for a different source fails.

Content identity includes source, root-relative object key and SHA-256. Reusing identical bytes with conflicting identity, interval or specifications is rejected. Changed bytes can create a new revision. The importer checks size/file identity/timestamps around hashing and copying; a source changing during the operation fails rather than silently mixing revisions. Existing verified managed copies can be reused. Originals are not edited or moved.

Inspect copies with `marketforge storage list --testing --origin external` and use the returned run report/events for per-file outcomes.

## What registration enables

Registration gives external inputs managed local archive identities, provenance, declared interval/specification records, and storage inspection/recovery. It is not automatic inclusion in every downstream processing workflow. Readiness is recorded as `requires_profile`, `requires_historical_specs`, or `pending_engine_validation`; even the last state is not a verified processing contract. A supplied `format_profile` is a reference, not semantic validation.

See [format/profile boundaries](../../reference/index.md#contracts-and-outputs) for the current reference scope. A complete external-to-processed-dataset walkthrough remains unverified; no such sequence is claimed here.
