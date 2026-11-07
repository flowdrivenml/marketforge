# Task 06 — Exchange-profile test migration

Status: completed. Commit: recorded in the next progress update.

Implementation: one file per exchange, 33 individually named synthetic production-profile execution cases, exact identity-set coverage checks, structured maintained-workload selection and persistent isolated outputs. No canonical schema changes. Removed normalization slow wrapper; merge/convert wrappers remain. Generic tests remain grouped by responsibility. The shared support target owns accounting/source-ordering cases without duplicate module registration.

Decisions: private September job files cannot identify October source expectations. Retained archive oracles now require their original prepared interval and explicitly fail when absent. They remain compiled and selectable. Generic tests instead resolve explicit synthetic tasks through production batch validation/catalog; synthetic source records and independent arithmetic expectations are authored in Rust. The small synthetic offline namespace is explicit and append-only; it is never used to identify real family records. No new maintained per-exchange job documents. Fixed a Task 02 regression: resolved-job reload must preserve descriptive marketforge_source provenance while comparing executable schema against its production profile.

Coverage inventory: before migration ordinary target124 + slow target33 =157. After migration full feature inventory185, ordinary source-only150. Retained all14 exchange depth oracle bodies in exchange files and other generic/real cases. Replaced five obsolete configuration tests (private job inventory, fixed catalog counts, historical DB snapshot equality, obsolete multiple-container schema, obsolete legacy task matrix) with strict production batch/catalog library validation and33 named source interpretation cases. Replaced two legacy-vs-TOML aggregate execution tests with33 independent per-profile execution oracles. Replaced one private trade-format inventory with exact catalog/case identity coverage. Added33 profile cases +2 coverage checks +1 bounded Binance oracle; net +28. Preserved observed Gate bid/ask orientation, committed configuration checksums and reload through both single-job/session APIs, tenth-write sink failure metrics, depth boundaries/sequences/recovery and Parquet reconstruction assertions. No disconnected retained test modules; deleted trade_formats.rs after replacement.

Files: engine/Cargo.toml; historical normalize entrypoint, exchanges, profiles, generic execution/depth/trade/Parquet/resource adapters and configuration/equivalence tests; support migration/profile_cases/depth_assertions/runtime; shared depth contract tests; config/job.rs; normalization-tests documentation and progress. Source archives, maintained prepared fixture, production runtime and integrity thresholds unchanged.

Verification: source-only historical_normalize150 passed; historical_normalize_support34 passed including bounded Bybit trade/depth representatives; shared depth_processor4, depth_outcome3, depth_spec1 passed; bounded Binance linear perpetual first trade1 passed. Historical check and Clippy lib passed with warnings. Real/full targets compiled and listed (183/185); no full workload executed. Changed-file rustfmt and git diff check passed. Whole-repository fmt still fails on pre-existing live files. Initially four newly restored compatibility tests failed; corrected source-provenance reload and self-contained profile fixture filename, then all150 passed.

Unavailable prerequisite check: explicitly selected retained Bybit inverse September oracle failed with the expected actionable interval diagnostic; this is blocked real-data coverage, not a passing normalization result. Other September oracles were compiled/listed only. No full Gate monthly archive, full corpus, benchmark, throughput measurement or scaling experiment executed. The old large consistency case retains canonical/metric equality assertions but its benchmark-reporting section was removed and its optional correctness concurrency reduced to two workers.

Artifacts: exclusively isolated data/tests/historical/normalize runs, exchange cases under exchanges/<exchange>; guards never delete integration artifacts. Metadata records configuration/resolution scope separately from actual execution. Original source provenance and reviewed record locators retained. Artifact paths remain ignored by Git.

Required future Forge changes: supply schema-3 offline instrument authority and real family member specifications as documented in Task02; no Python changes in Task06. No new Forge contract fields.

Profile matrix: all rows below distinguish synthetic passed execution from real archive evidence. Fixture availability indicates references in the maintained workload, not successful normalization. Current bounded real evidence: Binance linear perpetual trade first row, Bybit spot trade and initialized depth prefix. Real family metadata and exchange/feed coverage gaps remain explicit.

| Profile | Named execution case | Prepared workload | Verified synthetic behaviors | Remaining evidence |
|---|---|---|---|---|
| `bitget.perpetual.linear.depth` | `perpetual_linear_depth_profile` passed (synthetic) | 4 tasks | snapshot and quantity update replay | real archive oracle unverified |
| `bitget.spot.spot.depth` | `spot_spot_depth_profile` passed (synthetic) | 4 tasks | snapshot and quantity update replay | real archive oracle unverified |
| `bybit.future.linear.depth` | `future_linear_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `bybit.perpetual.inverse.depth` | `perpetual_inverse_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `bybit.perpetual.linear.depth` | `perpetual_linear_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `bybit.spot.spot.depth` | `spot_spot_depth_profile` passed (synthetic) | 9 tasks | initial book and deletion replay | real archive oracle unverified |
| `gateio.perpetual.inverse.depth` | `perpetual_inverse_depth_profile` passed (synthetic) | 72 tasks | snapshot object levels | real archive oracle unverified; incremental sortable feed rejected |
| `gateio.perpetual.linear.depth` | `perpetual_linear_depth_profile` passed (synthetic) | 72 tasks | snapshot object levels | real archive oracle unverified; incremental sortable feed rejected |
| `gateio.spot.spot.depth` | `spot_spot_depth_profile` passed (synthetic) | 72 tasks | snapshot and quantity update replay | real archive oracle unverified |
| `okx.future.inverse.depth` | `future_inverse_depth_profile` passed (synthetic) | 0 tasks | initial book and deletion replay | real archive oracle unverified; family specifications unresolved |
| `okx.future.linear.depth` | `future_linear_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified; family specifications unresolved |
| `okx.perpetual.inverse.depth` | `perpetual_inverse_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `okx.perpetual.linear.depth` | `perpetual_linear_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `okx.spot.spot.depth` | `spot_spot_depth_profile` passed (synthetic) | 3 tasks | initial book and deletion replay | real archive oracle unverified |
| `binance.future.inverse.trades` | `future_inverse_trades_profile` passed (synthetic) | 0 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `binance.future.linear.trades` | `future_linear_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `binance.perpetual.inverse.trades` | `perpetual_inverse_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `binance.perpetual.linear.trades` | `perpetual_linear_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `binance.spot.spot.trades` | `spot_spot_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bitget.perpetual.linear.trades` | `perpetual_linear_trades_profile` passed (synthetic) | 42 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bitget.spot.spot.trades` | `spot_spot_trades_profile` passed (synthetic) | 12 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bybit.future.linear.trades` | `future_linear_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bybit.perpetual.inverse.trades` | `perpetual_inverse_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bybit.perpetual.linear.trades` | `perpetual_linear_trades_profile` passed (synthetic) | 3 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `bybit.spot.spot.trades` | `spot_spot_trades_profile` passed (synthetic) | 9 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `gateio.perpetual.inverse.trades` | `perpetual_inverse_trades_profile` passed (synthetic) | 1 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `gateio.perpetual.linear.trades` | `perpetual_linear_trades_profile` passed (synthetic) | 1 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `gateio.spot.spot.trades` | `spot_spot_trades_profile` passed (synthetic) | 1 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `okx.future.inverse.trades` | `future_inverse_trades_profile` passed (synthetic) | 0 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified; family specifications unresolved |
| `okx.future.linear.trades` | `future_linear_trades_profile` passed (synthetic) | 4 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified; family specifications unresolved |
| `okx.perpetual.inverse.trades` | `perpetual_inverse_trades_profile` passed (synthetic) | 4 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `okx.perpetual.linear.trades` | `perpetual_linear_trades_profile` passed (synthetic) | 4 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
| `okx.spot.spot.trades` | `spot_spot_trades_profile` passed (synthetic) | 4 tasks | exact timestamp, price, side, quantities, ID, RPI | real archive oracle unverified |
