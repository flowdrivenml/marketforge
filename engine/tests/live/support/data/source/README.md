# Deterministic native trade fixtures

`trades.json` derives each native envelope from the first recorded BTC message in `data/.cache/live/websocket-fixtures/trades/<profile>.json`. Array payloads retain only their first trade. Native prices are explicitly set to `80000` so inverse quantity conversions have finite decimal representations under the production exact-precision policy. Native quantity, side, identity, timestamps, and source envelope are retained.

These are controlled protocol fixtures, not unmodified exchange captures or evidence of live exchange compatibility. The Source integration tests assert that each original quantity representation survives normalization exactly. Depth fixtures are constructed explicitly in the test module with finite-decimal prices for the same reason.
