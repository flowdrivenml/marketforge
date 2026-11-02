Production benchmarks of MarketForge's Rust engine for physically materializing combined historical tick-level trades and order-book depth, with selective currency conversion into USDT using historical market references.

## Hardware

| Component       | Specification        |
| --------------- | -------------------- |
| CPU             | AMD Ryzen 9 9950X    |
| Cores / Threads | 16 / 32              |
| RAM             | 128 GB               |
| Storage         | NVMe SSD             |
| OS              | Ubuntu Linux         |
| Engine          | Rust (release build) |

## Configuration

| Parameter                | Value                          |
| ------------------------ | ------------------------------ |
| Conversion mode          | Combined (trades + depth)      |
| Target quote currency    | USDT                           |
| Instrument filter        | Spot-only conversion           |
| Reference pricing        | Historical last-trade prices   |
| Benchmark interval       | September 1–4, 2026 (72 hours) |
| Repetitions              | 1 per configuration            |
| Total executions         | 6                              |
| Parquet row-group target | 128 MiB                        |
| Parquet file target      | 512 MiB                        |
| Compression              | ZSTD level 3                   |
| Physical materialization | Enabled                        |
| Physical validation      | Excluded                       |
| Transactional commits    | Excluded                       |

The benchmark measures the production combined-conversion materialization pipeline: instrument resolution, conversion-path planning, historical reference alignment, checkpoint construction, partition planning, parallel conversion, and physical Parquet writing.

Spot instruments are eligible for currency conversion into USDT using historical market references and exact Decimal256 arithmetic.

Perpetual and dated futures instruments are excluded from numerical conversion but remain physically preserved in the combined output.

Raw archive processing, canonical normalization, physical merging, independent numerical validation, and transactional publication are excluded.

## Dataset

| Metric                | Value                                |
| --------------------- | ------------------------------------ |
| Source dataset        | `final-combined`                     |
| Exchanges             | Binance, Bitget, Bybit, Gate.io, OKX |
| Instrument types      | Spot, perpetual, dated futures       |
| Conversion filter     | Spot                                 |
| Reference instrument  | Binance BTCUSDT (spot)               |
| Target quote currency | USDT                                 |
| Input trade events    | 50,106,717                           |
| Input depth events    | 405,581,426                          |
| **Total input events**| **455,688,143**                      |
| Input Parquet files   | 55                                   |
| Input row groups      | 2,269                                |
| Benchmark duration    | 72 hours                             |
| Successful executions | 6                                    |
| Failed executions     | 0                                    |

The dataset contains historical trades and order-book depth across five cryptocurrency exchanges, including spot markets, linear and inverse perpetual contracts, and dated futures.

The benchmark processes the complete combined timeline while applying numerical conversion only to eligible spot instruments.

All configurations process the same **455,688,143 events**, consisting of approximately **50.1 million trades and 405.6 million depth events**.

Event throughput includes identity conversions, numerical conversions, unavailable conversions, and skipped events.

## Performance

### Benchmark Results

| Workers | Execution Time | Throughput             | Speedup   |     |
| ------- | -------------- | ---------------------- | --------- | --- |
| 1       | 897.548 s      | 507,703 events/s       | 1.00×     |     |
| 4       | 382.384 s      | 1,191,702 events/s     | 2.35×     |     |
| **8**   | **323.132 s**  | **1,410,223 events/s** | **2.78×** |     |
| 12      | 328.236 s      | 1,388,296 events/s     | 2.73×     |     |
| 18      | 340.671 s      | 1,337,620 events/s     | 2.63×     |     |
| 24      | 362.803 s      | 1,256,022 events/s     | 2.47×     |     |

Execution times include reference preparation and physical Parquet materialization, but exclude publication validation.

Throughput is calculated using all input events, including those excluded from numerical currency conversion.

Speedup is calculated relative to the single-worker configuration.

### Scaling

Increasing concurrency from 1 to 8 workers improves throughput from approximately **508,000 to 1.41 million events/s**.

The highest observed throughput occurs at **8 workers**, completing the full combined materialization in approximately **323.1 seconds (5.39 minutes)**.

Performance declines beyond 8 workers:

- 12 workers: approximately 1.6% slower than 8.
- 18 workers: approximately 5.4% slower than 8.
- 24 workers: approximately 12.3% slower than 8.

The results indicate diminishing returns from additional concurrency beyond 8 workers.

Potential limiting factors include memory bandwidth, Parquet decoding and encoding, nested depth-array processing, historical reference handling, and partition scheduling overhead.

The combined dataset contains substantially more events than the trade-only and depth-only conversion benchmarks.

However, its higher event throughput does not imply faster numerical depth conversion. The spot-only filter allows perpetual and futures events to bypass numerical conversion while remaining in the materialized output.

Because each configuration was executed once, repeated measurements are required to establish a definitive optimal worker count.

## Validation

All six benchmark configurations completed successfully.

| Check                          | Result   |
| ------------------------------ | -------- |
| Physical materialization       | PASS     |
| Event count consistency        | PASS     |
| Execution receipt verification | PASS     |
| Parquet output inventory       | PASS     |
| Conversion status accounting   | PASS     |
| Skipped-event accounting       | PASS     |
| Independent numerical checks   | Excluded |
| Transactional publication      | Excluded |
| Failed executions              | 0        |

Every execution processed exactly **455,688,143 combined events**.

The conversion engine preserves the original mixed-event timeline while materializing conversion metadata and converted monetary values for eligible instruments.

Historical reference alignment prevents the use of future market observations.

The benchmark measures materialization performance only. Independent numerical validation and transactional publication are available in the production pipeline but were not included in these measurements.

Successful materialization and consistent event counts do not independently establish byte-for-byte equivalence across worker configurations.

## Conclusion

MarketForge physically materialized a combined historical dataset containing **455.7 million trade and order-book depth events** across five cryptocurrency exchanges.

The benchmark applied selective USDT conversion to eligible spot instruments while preserving perpetual and futures events in the combined timeline.

The highest observed throughput was **1,410,223 events/s**, achieved with **8 workers**.

The complete materialization finished in approximately **323.1 seconds (5.39 minutes)**, including historical reference preparation, parallel processing, and physical Parquet writing.

Performance peaks at 8 workers and gradually declines with additional concurrency, indicating diminishing returns from parallel execution for this workload.

Configurations using 4–18 workers achieved approximately **1.19–1.41 million events per second**.

The results establish a production-scale combined materialization throughput of approximately **1.2–1.4 million events per second** on the tested hardware.

These measurements represent mixed-event materialization with selective currency conversion and exclude independent numerical validation and transactional publication.