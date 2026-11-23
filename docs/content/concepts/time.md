# Time, selection, and coverage

Historical recipes select with timezone-aware intervals. The useful interpretation depends on the next boundary.

| Boundary | Selection and evidence |
| --- | --- |
| Acquisition | Provider archives whose nominal intervals overlap the half-open request. A selected archive is downloaded as a whole |
| Normalization | Selected raw archives or archive members. The output can extend outside the requested window because the worker processes whole inputs |
| Merge | Requested Engine event window in signed Unix nanoseconds. Causal depth can include reconstruction rows outside the market window |
| Conversion | Entire selected merge publication, including its causal context. The recipe interval is a selection constraint, and explicit whole-publication approval can permit a wider overlapping publication |
| Dataset discovery | File or stream metadata filters on stored envelopes. Row filters act separately on a chosen timestamp column |

Event timestamps describe source event time. Nullable system timestamps describe receipt or recording where the source provides it. Native order can encode dependencies even when event timestamps regress. Observed first and last timestamps describe measured extrema, often inclusive. A contiguous sequence of nominal archive intervals or matching extrema alone cannot prove every event was observed. Check source coverage, sequence semantics, integrity observations and applicable reconstruction scope. [Ordering](ordering.md) · [Publication schemas and time fields](../reference/publications.md)
