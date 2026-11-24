# What a merge combines

Merge packages compatible normalized observations into a common physical timeline. Forge selects registered normalized primary files and pins their publication and input evidence. Rust validates the supported representation, stream membership, interval and ordering dependencies before committing output.

The running example selects eligible BTCUSDT perpetual trade and depth streams. A combined publication can carry both. For wider work, a publication can include several instruments, markets and execution venues under the compatible instrument namespace. Mode follows the selected contents. One partition can hold observations from many streams and can rotate into more than one Parquet file.

## What a row represents

Trade and depth modes have their own physical row layouts. A combined row identifies the event kind and carries the corresponding trade or depth payload. Depth payloads group level changes by observation. Provenance identifies the normalized stream, task, event ordinal and row span. Logical stream identity survives packaging.

The requested interval selects market events. Supported causal depth may include earlier context to initialize the book or replay the selected segment. Select all required depth parts and preserve the causal sequence when reading them. Chronological and causal layouts have different guarantees. See [ordering](ordering.md).

Merge preserves selected observations in a compatible timeline. Aggregating economic quantities, resampling and computing features are separate operations. Unsupported overlaps or missing dependencies can block a merge.

## Choose the intended generation

A single normalized publication can contribute multiple files. Multiple matching generations can still be competing selections. Inspect their provenance and choose the intended compatible registry publication IDs in the recipe when automatic selection is ambiguous. An explicit selection establishes the input choice. Equivalence is a separate verification and reuse decision.

See the [merge guide](../workflows/merge.md), [recipe reference](../reference/configuration/merge.md), [runtime reference](../reference/configuration/merge-runtime.md), and [merged publication contract](../reference/data-formats/publication-merge.md).
