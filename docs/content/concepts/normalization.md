# What normalization does

Normalization turns source-shaped records into canonical trade and depth streams. Its inputs are selected registered raw copies and members, an applicable source profile, instrument specifications, and the prepared job's runtime and ordering evidence.

## From records to canonical observations

1. **Resolve interpretation.** Bind each task to the physical input, profile, instrument namespace and applicable specification. A provider product or family can require multiple concrete streams.
2. **Decode and interpret.** Decode timestamps, preserve exact decimal values within the supported representation, map source fields, and apply the profile's quantity, trade-side, and depth semantics. For a contract count, the applicable contract value and denomination establish what the count represents.
3. **Check and reconcile.** Apply source and state validation, integrity limits, stream reconciliation, and timestamp policy. Accepting a decoded record is a separate decision from decoding it.
4. **Emit and publish.** Emit the canonical trade rows or depth event spans and level rows, plus the evidence needed to read them. A completed publication requires successful final output checks and commit.

In the running example the trade profile interprets Bybit BTCUSDT perpetual executions. The depth profile interprets snapshot and incremental observation semantics for the same instrument. The two content streams retain separate identities and can be selected together for a compatible merge. A selected archive or member can contribute events outside the request because the task processes the selected source input.

## What can affect the result

| Policy or constraint | Effect |
| --- | --- |
| Reconciliation | In supported overlap layouts, source preference, verified duplicates, and recovery boundaries determine what each source contributes. Permitted loss is tracked separately from verified duplicate suppression |
| Timestamp regression | Trade and depth policies can fail, preserve, reorder, discard, or recover in their respective supported modes. Applicable limits and proof requirements still apply, and these policies preserve the retained timestamp values |
| Integrity | A fail rule can reject the job on a violation. A bounded degrade rule can record an accepted degraded result within its limits. Invalid records are still subject to the parsing and acceptance rules of the operation |
| Resource admission and limits | Memory, scratch, file handles and work-unit bounds can block or stop processing |
| Output | Canonical schema, stream membership, source ownership or depth indexes, and recorded integrity distinguish what was emitted, excluded or left unresolved |

A valid commit establishes output under the recorded contract. Its reach is limited by the input and historical evidence. Arbitrary feed faults can remain unrecoverable. See [profiles](../workflows/normalization/author-profile.md), [runtime policies](../reference/configuration/normalization-runtime.md), [prepared batches](../reference/data-formats/prepared-normalization.md), and the [normalization workflow](../workflows/normalization/index.md).
