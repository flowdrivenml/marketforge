# MarketForge Roadmap

**From Data to Financial Engineering**

MarketForge is an open-source platform being developed to automate the infrastructure behind quantitative research and algorithmic systems.

The long-term objective is a **unified, configuration-driven pipeline** capable of taking users from raw market data through financial feature engineering, optimization, machine learning, backtesting, and eventually live operation.

**All on a single machine.** From a personal laptop to a high-performance workstation, without requiring distributed infrastructure.

This roadmap tracks the platform's development, current capabilities, and future direction. It is organized by functional area rather than fixed release dates.

> [!NOTE]
> MarketForge is under active development. Some components are implemented but not yet fully integrated into user-facing workflows. Completion estimates reflect development progress, not verified production readiness.

---

## Development Overview

| Component | Status | Progress |
|---|---|---|
| Historical Data Acquisition | ✅ Available | Operational |
| Historical Processing | 🚧 Integration | ~90% |
| Live Streaming & Bootstrapping | 🚧 Integration | ~80% |
| Unified Pipeline Orchestration | 🚧 In Development | Ongoing |
| Spectra — Financial Feature Engineering | 📋 Planned | Design |
| Spectra Parameter Optimization | 📋 Planned | Design |
| Research & ML Laboratory | 📋 Planned | Future |
| Algorithmic Strategy Infrastructure | 📋 Planned | Future |
| CLI, TUI & Documentation | 🚧 Ongoing | Continuous |

*Percentages are approximate maintainer estimates of implementation progress, not measured feature completeness or release guarantees.*

---

## 1. Data Infrastructure

**Status: Available / Integration**

The foundation of MarketForge is the acquisition, preparation, and management of large-scale financial datasets.

### Historical Data Acquisition

**✅ Available**

MarketForge currently supports automated historical data discovery and downloading from five cryptocurrency exchanges:

**Binance · Bybit · OKX · Bitget · Gate.io**

These exchanges provide substantial amounts of historical market data free of charge, making them valuable resources for independent quantitative research.

Current capabilities include:

- Automated discovery and acquisition of historical trade ticks and L2 order-book data.
- Access to a combined catalog of more than 10,000 instruments.
- Instrument metadata discovery and market classification.
- Configurable acquisition through CLI commands and TOML files.
- Persistent acquisition records and management of downloaded archives.

Historical availability depends on the provider, instrument, data type, and requested period. Free access does not imply complete or uninterrupted coverage.

### High-Performance Historical Processing

**🚧 Integration — Approximately 90% implemented**

The Rust Engine contains the principal components for preparing heterogeneous market data for research.

These include:

- Normalization into consistent data representations.
- Merging and chronological ordering of compatible data streams.
- Data and quantity conversions, including contract and currency-related interpretation where supported.
- Combining data from different instruments, market categories, and sources.
- Integrity validation, diagnostics, and processing provenance.
- Efficient output generation for downstream analytical workflows.

**Current priority:** Finalize the remaining integration between Python Forge and the Rust Engine, connecting individual processing operations into a reliable, configurable pipeline.

The intended result is a workflow that transforms raw or registered inputs into consistent, research-ready datasets without requiring custom processing scripts.

### External Data Integration

**🚧 In Development**

MarketForge is designed to accommodate data beyond its built-in providers.

The objective is to let users describe compatible external inputs through configuration files, register them, and reuse them throughout downstream workflows.

Source-specific interpretation and validation remain necessary. More substantial extensions, including new live providers or unsupported data types, require code changes.

---

## 2. Live Market Infrastructure

**🚧 Integration — Approximately 80% implemented**

MarketForge's live infrastructure is being developed alongside historical processing, with the aim of sharing data representations, processing components, and integrity guarantees across both environments.

The Rust Engine already contains substantial implementations for:

- Real-time market-data connectivity and ingestion.
- Streaming normalization and processing.
- Order-book state management and synchronization.
- Data integrity monitoring and recovery.
- Stream lifecycle management and operational monitoring.
- Historical initialization and live-state bootstrapping components.

The remaining work is primarily focused on integration, correctness fixes, reliability, and completing the intended operational workflows.

### Historical-to-Live Continuity

**🚧 In Development**

A central objective is enabling algorithms to initialize their state from historical data and subsequently continue processing live observations.

Rather than maintaining completely separate implementations, compatible transformations and data structures should be reusable across historical and live workflows.

This is particularly important for sophisticated financial features that depend on accumulated observations or historical market state.

The complete research-to-live workflow is not yet production-ready.

---

## 3. Spectra — Financial Feature Engineering

**📋 Planned**

**Spectra** is the planned financial feature-engineering layer of MarketForge.

Its purpose is to transform raw market observations into more informative representations of market behavior.

Price alone reveals only part of what happens in financial markets. Understanding market microstructure requires examining liquidity, trading pressure, order-book changes, and interactions between market participants.

Spectra will be designed for integration into **both historical research and live processing pipelines**, allowing compatible features to be developed and evaluated without rebuilding their underlying computations for each environment.

### Planned Research Capabilities

| Area | Examples |
|---|---|
| Order Flow | Aggressive buying/selling, execution imbalance, trading pressure |
| Liquidity | Resting depth, liquidity distribution, additions and withdrawals |
| Market Microstructure | Absorption, order-book imbalance, execution dynamics |
| Order-Book Behavior | Cancellation patterns and signals potentially consistent with spoofing |
| Volume-Based Structure | Volume concentration, liquidity zones, support/resistance hypotheses |
| Cross-Market Analysis | Lead-lag effects, spot/perpetual relationships, cross-instrument signals |
| Event-Based Sampling | Information-driven bars, adaptive sampling, event-triggered observations |
| Statistical Features | Volatility, market regimes, distributional and temporal characteristics |

These are proposed research directions, not claims that the resulting features can reliably identify participant intentions or predict market behavior.

The architecture is intended to remain extensible so that additional mathematical models and feature families can be introduced over time.

### Feature Ideas & Collaboration

Financial engineering offers considerably more possibilities than any fixed collection of indicators.

**Have an interesting idea for a financial feature, market-microstructure model, or unconventional quantitative signal?**

I'd be interested in hearing about it and potentially implementing it in Spectra.

Open a proposal through [GitHub Issues](https://github.com/flowdrivenml/marketforge/issues), or contact me directly by email.

**Email:** *[Contact address to be added]*

---

## 4. Spectra Optimization

**📋 Planned**

Sophisticated financial features often depend on parameters that are difficult to select reliably in advance.

For example, liquidity measurements may depend on observation windows, depth thresholds, decay functions, or other parameters whose effectiveness varies across instruments and market conditions.

MarketForge plans to introduce optimization workflows specifically for Spectra features.

### Planned Capabilities

- Parameter search and optimization for configurable financial features.
- Automated evaluation across instruments, datasets, and time periods.
- Comparison of alternative feature configurations.
- Reproducible optimization results and experiment tracking.
- Integration with downstream model evaluation and backtesting.

The objective is to replace arbitrary parameter selection with systematic experimentation.

Optimization will require appropriate out-of-sample evaluation and safeguards against overfitting.

---

## 5. Unified Research Pipeline

**🚧 Foundation in Development — Full Integration Planned**

The broader direction of MarketForge is to connect its individual capabilities into reusable, configuration-driven workflows.

Instead of managing acquisition, normalization, feature generation, and model development as independent tasks, users should be able to define how these operations work together.

### Target Workflow

```text
                  MARKETFORGE
                       │
                DATA ACQUISITION
                / DATA IMPORT
                       │
                DATA PROCESSING
          Normalize · Merge · Convert
                       │
                DATASET MANAGEMENT
                       │
                      SPECTRA
              Financial Feature Engineering
                       │
                  OPTIMIZATION
                       │
                 RESEARCH LAB
           Train · Validate · Evaluate
                       │
                   BACKTEST
                       │
                RESEARCH RESULTS
                       │
              HISTORICAL → LIVE
```

*Conceptual target architecture. This is not yet an executable end-to-end workflow.*

### Pipeline Orchestration

**📋 Planned**

The unified orchestration layer is intended to support:

- Reusable pipeline definitions through configuration.
- Automatic execution of dependent processing stages.
- Batch execution across multiple instruments, datasets, and configurations.
- Independent or interconnected research pipelines.
- Persistent execution records, results, and diagnostics.
- Reuse of prepared data and intermediate artifacts where appropriate.

**The goal: configure the workflow once, then reuse and extend it across research experiments.**

All of this is intended to operate locally, with resource management appropriate to the available hardware.

---

## 6. Research & Machine Learning Laboratory

**📋 Planned**

The MarketForge research laboratory will extend the unified pipeline into algorithm development, experimentation, and evaluation.

The intention is to support the complete research lifecycle within a reproducible environment.

### Planned Capabilities

**Experimentation**
- Configurable research experiments and reusable study definitions.
- Dataset selection and feature configuration.
- Experiment tracking, comparison, and reproducible outputs.

**Machine Learning**
- Model training and validation.
- Hyperparameter optimization.
- Feature selection and model evaluation.
- Automated experimentation across different datasets and configurations.

**Backtesting**
- Strategy research and historical simulation.
- Evaluation across instruments and market conditions.
- Performance and risk analysis.
- Integration with optimized financial features.

### End-to-End Research Automation

The longer-term objective is to make complete workflows possible through configuration:

**Acquire → Normalize → Engineer Features → Optimize → Train → Test → Backtest**

Users should eventually be able to execute individual stages, complete workflows, or multiple experiments in bulk.

The laboratory will be designed to work with MarketForge's shared infrastructure rather than introducing another disconnected research environment.

---

## 7. Algorithmic Systems

**📋 Planned**

The final direction is to connect validated research workflows with live operational infrastructure.

Planned areas include:

- Reuse of compatible research features in live environments.
- Historical initialization of algorithm state.
- Continuous processing of live observations.
- Integration points for user-defined strategies.
- Strategy monitoring and operational diagnostics.
- Simulation and paper-trading workflows.
- Future execution and risk-management infrastructure.

MarketForge is intended to provide the infrastructure for developing and operating algorithmic systems—not ready-made profitable strategies.

---

## 8. Platform & Developer Experience

**🚧 Continuous Development**

MarketForge is designed primarily for local execution on a single machine.

A laptop should be sufficient for smaller workloads, while powerful workstations can provide additional capacity for demanding historical processing, feature engineering, and research experiments.

The platform does not require a distributed computing cluster.

### User Experience

- CLI-driven operations and TOML-based configuration.
- Reusable configuration profiles and processing definitions.
- Terminal interfaces for interactive management and monitoring.
- Clear diagnostics, progress reporting, and execution history.
- Consistent interaction across historical, research, and live workflows.

### Developer Experience

- Modular Rust and Python architecture.
- Documented extension points for data sources and processing operations.
- Automated testing and integration verification.
- Reproducible benchmarks and performance analysis.
- Zensical documentation covering configuration, workflows, and development.

The objective is to support both researchers who want to use MarketForge without writing infrastructure code and developers who want to extend its capabilities.

---

## Current Priorities

The immediate focus is completing and integrating the existing infrastructure before expanding into the larger research environment.

| Priority | Objective |
|---|---|
| **1** | Finalize historical normalization, merging, conversion, and Forge–Engine integration |
| **2** | Establish a complete acquisition-to-dataset workflow |
| **3** | Complete live streaming and bootstrapping integration, correctness fixes, and reliability verification |
| **4** | Consolidate reusable pipeline orchestration and configuration |
| **5** | Begin Spectra feature-engineering development |
| **6** | Introduce feature optimization and research laboratory capabilities |

Priorities may evolve as technical requirements and implementation progress change.

---

## Development Philosophy

MarketForge is being built around a few principles:

**Research over infrastructure.** Reduce repetitive engineering so users can focus on developing and evaluating quantitative ideas.

**Configuration over custom scripts.** Make supported workflows reusable without requiring application-code modifications for every experiment.

**Historical and live continuity.** Share compatible data representations and processing components wherever practical.

**Local-first computing.** Make sophisticated quantitative research accessible without requiring expensive cloud infrastructure.

**Extensibility by design.** Allow new sources, financial features, processing methods, and research workflows to be introduced as the platform evolves.

---

## Feedback & Collaboration

MarketForge is an independent open-source project under active development.

Technical feedback, feature proposals, research ideas, and contributions are welcome through [GitHub Issues](https://github.com/flowdrivenml/marketforge/issues).

The roadmap describes development intentions rather than guaranteed capabilities or release commitments.

---

*Last reviewed: November 2026*