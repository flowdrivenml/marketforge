**Priority:** Fix python orchestration level Schema. Also update DOCS

### Problem

Gate.io raw depth archives revealed inconsistencies with the existing normalization metadata.

**Confirmed issue — Spot side encoding**

Current mapping:

```json
"values": {
  "0": "bid",
  "1": "ask"
}
```

Actual source values: `1` and `2`.

The correct bid/ask mapping must be verified and updated in the catalog.

### Additional Validation

- Verify CSV column definitions for spot and perpetual markets.
- Validate snapshot grouping (`set` rows grouped by timestamp and sequence).
- Confirm relative update semantics (`make` / `take`).
- Verify signed-size side encoding for perpetuals.
- Validate sequence continuity using `begin_id` and `merged_count`.
- Check hourly archive boundaries and snapshot initialization.
- Investigate unusually small Gate.io linear archives on September 3 at 15:00–16:00 UTC.

### Implementation

Update Gate.io raw-format and normalization specifications in:

`src/marketforge/catalog/specs/raw_formats/`

Then regenerate processing jobs and validate against real archives.

**Note:** Keep schema corrections separate from Rust processor implementation. Do not modify Python orchestration during the current depth-processing development phase.