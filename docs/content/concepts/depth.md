# Trades, depth, and reconstruction

A trade describes an execution with source identity, price, quantity and, when available under the profile, interpreted trade side. A bid is an order to buy. An ask is an order to sell. These are book sides. A trade's side describes the direction defined by its source profile. A raw side token can refer to different participants on different feeds.

Depth describes observations of quoted price levels. A snapshot initializes or resets the book within the declared scope. A bounded or partial observation establishes only the levels and semantics its profile supports. Absolute updates replace a keyed side/price quantity, with zero removing that level under the supported absolute contract. Relative source operations add or subtract according to their source semantics. Applying a relative update requires suitable prior state.

## Replay a small book

This illustrative feed uses absolute changes and consecutive sequence numbers within a segment. Prices are in USDT and quantities are in the resolved base unit. The snapshot at sequence 15 is a new initialization accepted by the feed and policy.

| Observation | Bid state after applying | Ask state after applying |
| --- | --- | --- |
| 10 initializes bid 100 × 2 and ask 101 × 3 | 100 × 2 | 101 × 3 |
| 11 replaces the bid quantity with 5 | 100 × 5 | 101 × 3 |
| 12 sets ask 101 quantity to 0 | 100 × 5 | Empty |
| 14 arrives with 13 missing | Last known bid 100 × 5 | Last known empty side |
| 15 initializes a new segment | 99 × 4 | 102 × 2 |

At 14 the previous book is insufficient for a certified continuation. A strict integrity policy can fail the job on the discontinuity. A supported recovery path needs a suitable initialization and still applies integrity and reconciliation requirements. Missing initialization, invalid quantities, or a source-specific sequence mismatch can prevent valid reconstruction. The next snapshot establishes a new known state rather than recovering the missing update.

A normalized depth event index maps observations to canonical row spans. It can record an accepted observation with an empty span. It also carries the boundaries needed to replay the segment. File rotation alone does not create a new book. In the Bybit example, a merged event may carry several level changes packed together. A causal merge can carry earlier context as reconstruction-only rows.

## Read counts by population

| Count | Population |
| --- | --- |
| Source records read | Records consumed by the source reader |
| Accepted and rejected records | Source records admitted or rejected by parsing, interpretation and processing checks. Also read separate skip, suppression and exclusion counters |
| Market events | Market observations in the output scope. For causal merge the in-range count excludes reconstruction-only events |
| Trade events | Logical executions represented as trades |
| Depth events | Logical book observations such as initializations and updates |
| Depth levels | Individual side/price/quantity entries within depth events |
| Canonical rows | Normalized physical trade or level rows, or their represented source-row count in merged provenance |
| Reconstruction rows | Physical context rows marked reconstruction-only in causal output |
| Physical output rows | Records actually stored in the publication's primary Parquet files |
| Reference updates | Selected trade observations incorporated into conversion price state |

In the first three observations above, one snapshot and two updates describe three depth events and four level rows if each supplied level emits one row. A compatible merged representation can pack those rows into three event rows. A merged physical row can pack those changes into one event. An increase in a counter across stages needs interpretation using these populations and the operation's report. [Normalized depth formats](../reference/data-formats/publication-normalized.md#depth-event-indexes) · [Ordering](ordering.md)
