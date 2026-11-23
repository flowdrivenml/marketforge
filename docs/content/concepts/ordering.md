# Ordering and reconciliation

Choose ordering according to the dependencies of the data. Source observations can arrive in native order with regressing event timestamps. Chronological merge orders by event time and applies the recorded stream rank as a tie-breaker. Causal merge preserves the physical timeline sequence and any depth events needed to reconstruct market rows.

Normalization can reconcile overlapping native archives. Source preference, stream and member identity, duplicate handling, timestamp policies and explicitly permitted event exclusions affect that outcome. Inspect the retained integrity and reconciliation evidence before treating fewer output rows as benign. A row suppressed by policy differs from a rejected source record, a skipped instrument or an accepted observation that emitted zero level rows. [Runtime settings and policy keys](../reference/runtime.md) · [Merge Engine architecture](../development/merge.md)
