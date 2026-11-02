# PostgreSQL → Historical TOML audit matrix

Captured from actual PostgreSQL using read-only transactions. Complete original records are preserved in [database-catalog.json](database-catalog.json); capture metadata is in [database-audit-summary.json](database-audit-summary.json). Comparisons use parsed JSON/TOML objects, retaining array/field order and ignoring object-key order. `tar.gz` and `tar_gzip` are the database and Rust spellings of the same archive wrapper.

| Profile | Raw ID | Mapping IDs | Physical schema | Normalization | Final status |
|---|---:|---|---|---|---|
| `binance.spot.spot.binance-t1.v1` | 1 | 1 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `binance.linear.perpetual.binance-t2.v1` | 2 | 2 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `binance.inverse.perpetual.binance-t3.v1` | 3 | 3 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bitget.spot.spot.bitget-t1.v1` | 4 | 4 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bitget.linear.perpetual.bitget-t1.v1` | 5 | 5 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bitget.spot.spot.bitget-b1.v1` | 6 | 6 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bitget.linear.perpetual.bitget-b1.v1` | 7 | 7 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bitget.inverse.perpetual.bitget-b1.v1` | 8 | 8 | EXACT MATCH | EXACT MATCH | UNSUPPORTED |
| `bybit.spot.spot.bybit-t1.v1` | 9 | 9 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.linear.perpetual.bybit-t2.v1` | 10 | 10 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.inverse.perpetual.bybit-t2.v1` | 11 | 11 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.option.option.bybit-t3.v1` | 12 | 12 | EXACT MATCH | EXACT MATCH | UNSUPPORTED |
| `bybit.spot.spot.bybit-b1.v1` | 13 | 13, 14 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.linear.perpetual.bybit-b1.v1` | 14 | 15, 16 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.inverse.perpetual.bybit-b1.v1` | 15 | 17, 18 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `bybit.option.option.bybit-b2.v1` | 16 | 19, 20 | EXACT MATCH | EXACT MATCH | UNSUPPORTED |
| `gateio.spot.spot.gate-t1.v1` | 17 | 21 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `gateio.linear.perpetual.gate-t2.v1` | 18 | 22 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `gateio.inverse.perpetual.gate-t2.v1` | 19 | 23 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `gateio.spot.spot.gate-b1.v1` | 20 | 24, 25 | EXACT MATCH | JUSTIFIED CORRECTION | JUSTIFIED CORRECTION |
| `gateio.linear.perpetual.gate-b2.v1` | 21 | 26, 27 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `gateio.inverse.perpetual.gate-b2.v1` | 22 | 28, 29 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.spot.spot.okx-t1.v1` | 23 | 30 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.linear.perpetual.okx-t1.v1` | 24 | 31 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.inverse.perpetual.okx-t1.v1` | 25 | 32 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.option.option.okx-t1.v1` | 26 | 33 | EXACT MATCH | EXACT MATCH | UNSUPPORTED |
| `okx.spot.spot.okx-b1.v1` | 27 | 34, 35 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.linear.perpetual.okx-b1.v1` | 28 | 36, 37 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.inverse.perpetual.okx-b1.v1` | 29 | 38, 39 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.option.option.okx-b2.v1` | 30 | 40, 41 | EXACT MATCH | EXACT MATCH | UNSUPPORTED |
| `binance.linear.future.binance-t2.v1` | 274 | 291 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `binance.inverse.future.binance-t3.v1` | 275 | 292 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.linear.future.okx-t1.v1` | 334 | 372 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.inverse.future.okx-t1.v1` | 335 | 373 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.linear.future.okx-b1.v1` | 336 | 374, 375 | EXACT MATCH | EXACT MATCH | EXACT MATCH |
| `okx.inverse.future.okx-b1.v1` | 337 | 376, 377 | EXACT MATCH | EXACT MATCH | EXACT MATCH |

All identities resolve uniquely. All 36 physical definitions match, including field order/types/positions, header behavior and archive metadata. The 19 deduplication groups contain equivalent original physical definitions; see [physical-deduplication.json](physical-deduplication.json). All 49 mappings are accounted for: 47 equal and two Gate.io mappings intentionally corrected. No unresolved migration discrepancy remains in this captured inventory. Five profiles still cannot be executed safely.

Per-profile file references, discrepancies, evidence and resolutions are in [audit-matrix.json](audit-matrix.json). Unsupported status does not mean its rules compiled successfully.
