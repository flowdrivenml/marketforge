Production benchmarks of MarketForge's Rust engine for converting historical tick-level order-book depth into a common quote currency using historical trade references and physical Parquet materialization.

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
| Conversion mode          | Depth-only                     |
| Target quote currency    | USDT                           |
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

The benchmark measures the production depth-conversion materialization pipeline: instrument resolution, conversion-path planning, historical reference alignment, checkpoint construction, partition planning, parallel Decimal256 conversion, and physical Parquet writing.

Historical reference prices are used to convert order-book prices and quote quantities into USDT. The engine supports identity conversion, direct conversion, and exact-rational arithmetic without intermediate rate rounding.

Raw archive processing, canonical normalization, physical merging, independent numerical validation, and transactional publication are excluded.

## Dataset

| Metric                | Value                   |
| --------------------- | ----------------------- |
| Source dataset        | `depth-conversion-test` |
| Exchanges             | Bybit, OKX              |
| Instrument type       | Perpetual               |
| Instrument streams    | 4                       |
| Contract types        | Linear, inverse         |
| Reference dataset     | `final-trades`          |
| Target quote currency | USDT                    |
| Input depth events    | 5,661,998               |
| Input depth levels    | 316,251,694             |
| Input Parquet files   | 64                      |
| Input row groups      | 363                     |
| Benchmark duration    | 72 hours                |
| Successful executions | 6                       |
| Failed executions     | 0                       |

The dataset contains historical order-book depth from four perpetual instruments across Bybit and OKX.

Each depth event contains multiple price levels, averaging approximately **55.9 levels per event**.

All configurations process the same **5,661,998 depth events and 316,251,694 depth levels**.

## Performance

### Benchmark Results

| Workers | Execution Time | Events/s          | Depth Levels/s      | Speedup   |
| ------- | -------------- | ----------------- | ------------------- | --------- |
| 1       | 179.589 s      | 31,528            | 1,760,979           | 1.00×     |
| 4       | 76.999 s       | 73,533            | 4,107,218           | 2.33×     |
| **8**   | **60.631 s**   | **93,384**        | **5,216,007**       | **2.96×** |
| 12      | 64.421 s       | 87,890            | 4,909,139           | 2.79×     |
| 18      | 67.395 s       | 84,012            | 4,692,510           | 2.66×     |
| 32      | 72.920 s       | 77,647            | 4,336,419           | 2.46×     |

Execution times include reference preparation and physical Parquet materialization, but exclude publication validation.

Depth-level throughput represents all processed levels, including identity, converted, and unavailable conversions.

Speedup is calculated relative to the single-worker configuration.

### Scaling

Increasing concurrency from 1 to 8 workers improves throughput from approximately **1.76 million to 5.22 million depth levels/s**.

The highest observed throughput occurs at **8 workers**, completing the entire materialization pipeline in approximately **60.6 seconds**.

Performance declines beyond 8 workers:

- 12 workers: approximately 5.9% slower than 8.
- 18 workers: approximately 11.2% slower than 8.
- 32 workers: approximately 20.3% slower than 8.

The results indicate that additional concurrency does not necessarily improve performance for this workload.

Potential limiting factors include memory bandwidth, Parquet decoding and encoding, Decimal256 arithmetic, historical reference processing, and partition scheduling overhead.

The benchmark processes nested order-book structures containing hundreds of millions of individual values, making depth conversion substantially more computationally intensive than trade conversion.

Because each configuration was executed once, repeated measurements are required to establish a definitive optimal worker count.

## Validation

All six benchmark configurations completed successfully.

| Check                          | Result   |
| ------------------------------ | -------- |
| Physical materialization       | PASS     |
| Event count consistency        | PASS     |
| Depth-level accounting         | PASS     |
| Execution receipt verification | PASS     |
| Parquet output inventory       | PASS     |
| Independent numerical checks   | Excluded |
| Transactional publication      | Excluded |
| Failed executions              | 0        |

Every execution processed exactly **5,661,998 depth events**.

The conversion engine preserves original source columns and event ordering while materializing converted depth prices, quote quantities, and conversion provenance.

Historical reference alignment prevents the use of future market observations.

The benchmark measures materialization performance only. Independent numerical validation and transactional publication are available in the production pipeline but were not included in these measurements.

Successful materialization does not independently establish byte-for-byte equivalence across worker configurations.

## Conclusion

MarketForge physically materialized currency conversions for **316.3 million historical order-book depth levels** across 5.66 million events from four perpetual instruments and two exchanges.

The highest observed throughput was approximately **5.22 million depth levels/s**, achieved with **8 workers**.

The complete materialization finished in approximately **60.6 seconds**, including historical reference preparation, parallel conversion, and physical Parquet writing.

Performance peaks at 8 workers and declines with additional concurrency, suggesting that this workload encounters resource contention or parallel scheduling overhead beyond that point.

The results establish a production-scale physical depth-conversion throughput of approximately **4.1–5.2 million depth levels per second** using 4–12 workers on the tested hardware.

These measurements exclude independent numerical validation and transactional publication.