Production benchmarks of MarketForge's Rust engine for converting historical tick-level trades into a common quote currency using historical market references and physical Parquet materialization.

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
| Conversion mode          | Trade-only                     |
| Target quote currency    | USDT                           |
| Reference pricing        | Historical last-trade prices   |
| Maximum reference age    | 5 seconds                      |
| Benchmark interval       | September 1–4, 2026 (72 hours) |
| Repetitions              | 1 per configuration            |
| Total executions         | 9                              |
| Parquet row-group target | 128 MiB                        |
| Parquet file target      | 512 MiB                        |
| Compression              | ZSTD level 3                   |
| Memory budget            | 60 GiB                         |
| Maximum open files       | 1,024                          |
| Physical materialization | Enabled                        |
| Physical validation      | Enabled                        |
| Transactional commits    | Enabled                        |

The benchmark measures the production conversion pipeline: instrument resolution, conversion-path planning, historical reference alignment, partition planning, parallel currency conversion, physical Parquet writing, numerical validation, and transactional publication.

Historical reference prices are used to convert monetary values into USDT. The benchmark includes multi-hop conversion paths and Decimal256 arithmetic.

Raw archive processing, canonical normalization, and physical merging are excluded.

## Dataset

| Metric                | Value                        |
| --------------------- | ---------------------------- |
| Source dataset        | `conversion-multihop`        |
| Exchange              | Binance                      |
| Instrument type       | Spot                         |
| Instrument streams    | 3                            |
| Trading pairs         | BTCUSDT, ETHBTC, BTCUSDC     |
| Reference instruments | 3 (BTCUSDT, ETHBTC, BTCUSDC) |
| Target quote currency | USDT                         |
| Input trade events    | 12,627,646                   |
| Input Parquet files   | 8                            |
| Input Parquet groups  | 42                           |
| Benchmark duration    | 72 hours                     |
| Successful executions | 9                            |
| Failed executions     | 0                            |
The dataset contains historical spot trades requiring identity, direct, and multi-hop currency conversion into USDT.

All configurations process the same **12,627,646 trade events**.

## Performance

### Benchmark Results

| Workers | Execution Time | Throughput         | Speedup |
| ------- | -------------- | ------------------ | ------- |
| 1       | 26.703 s       | 472,898 events/s   | 1.00×   |
| 2       | 21.507 s       | 587,129 events/s   | 1.24×   |
| 3       | 18.521 s       | 681,806 events/s   | 1.44×   |
| 4       | 16.925 s       | 746,089 events/s   | 1.58×   |
| 8       | 15.193 s       | 831,156 events/s   | 1.76×   |
| 12      | 14.915 s       | 846,663 events/s   | 1.79×   |
| 16      | 14.779 s       | 854,407 events/s   | 1.81×   |
| **24**  | **14.771 s**   | **854,918 events/s** | **1.81×** |
| 32      | 15.122 s       | 835,065 events/s   | 1.77×   |

Execution times include physical materialization, validation, and publication.

Speedup is calculated relative to the single-worker configuration.

### Scaling

Increasing concurrency from 1 to 8 workers substantially improves throughput, from approximately **473,000 to 831,000 events/s**.

Performance gains diminish beyond 8 workers, with the highest observed throughput at **24 workers**.

The difference between 12, 16, and 24 workers is small, indicating that additional concurrency provides limited benefits for this workload.

Potential limiting factors include historical reference preparation, Parquet decoding and encoding, memory bandwidth, and partition scheduling overhead.

The input contains 42 physical Parquet row groups, which constrain available parallelism.

Because each configuration was executed once, small performance differences require repeated measurements to establish a definitive optimum.

## Validation

All nine configurations completed successfully.

| Check                          | Result |
| ------------------------------ | ------ |
| Physical materialization       | PASS   |
| Event count consistency        | PASS   |
| Historical reference alignment | PASS   |
| Physical Parquet validation    | PASS   |
| Numerical conversion checks    | PASS   |
| Manifest verification          | PASS   |
| Transactional publication      | PASS   |
| Failed executions              | 0      |

Every execution produced exactly **12,627,646 trade events**.

The conversion engine preserves source event ordering and original trade data while materializing converted monetary values and conversion provenance.

Historical reference alignment prevents the use of future market observations.

Identical event counts and successful validation do not independently establish byte-for-byte output equivalence across worker configurations.

## Conclusion

MarketForge physically converted **12.6 million historical spot trade events** into USDT using historical market references and multi-hop currency conversion.

The highest observed throughput was **854,918 events/s**, achieved with **24 workers**.

The complete conversion pipeline finished in approximately **14.8 seconds**, including reference preparation, physical Parquet materialization, numerical validation, and transactional publication.

Performance improvements diminish beyond 8–12 workers, with configurations using 12–32 workers achieving approximately **0.84–0.85 million events per second**.

The results establish a production-scale physical trade-conversion throughput of approximately **0.8–0.85 million events per second** on the tested hardware.