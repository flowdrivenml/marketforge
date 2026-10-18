# MarketForge Live — Development Progress

## Overview

MarketForge Live is a Rust-based infrastructure service for continuous cryptocurrency market-data acquisition, normalization, integrity validation, recovery, distribution, and persistence.

The implementation builds upon MarketForge's existing historical processing engine, reusing canonical data models, instrument metadata, normalization primitives, and Parquet infrastructure.

## Current Phase: Architecture and Protocol Specification

### Completed

- Defined the overall MarketForge Live architecture and module boundaries.
- Established foundational domain models and subsystem contracts.
- Designed hierarchical configuration with global defaults and per-stream overrides.
- Specified runtime reconfiguration, configuration revisions, and reconciliation.
- Defined stream acquisition deduplication and dependency management.
- Planned Unix-socket publication, PostgreSQL recording, and Parquet-backed retention.
- Designed native metrics, stream health monitoring, and independent Ratatui management.
- Established deterministic testing and fault-tolerance requirements.

### Exchange Protocol Specifications

**Binance Spot**
- WebSocket connectivity and subscription protocol.
- REST endpoints and rate-limit policies.
- Individual trade extraction and normalization.
- L2 depth extraction and snapshot synchronization.

**Binance USDⓈ-M Futures**
- WebSocket connectivity and subscription protocol.
- REST endpoints and rate-limit policies.
- Aggregated trade extraction and normalization.
- L2 depth extraction and sequence-continuity strategy.

Configuration files are maintained under `data/.config/live/`.

### Validation

- TOML syntax and structural checks performed.
- Exchange specifications require integration with the Rust configuration compiler.
- Live protocol fixtures and connection tests remain pending.

## Next Steps

- Complete Binance COIN-M Futures specifications.
- Research and document Bybit, OKX, Bitget, and Gate.io protocols.
- Finalize exchange-specific recovery strategies.
- Implement the Rust configuration compiler and validation framework.
- Begin deterministic transport and stream-registry tests.

## Engineering Principles

- Contract-first, modular Rust development.
- Reuse existing historical processing infrastructure.
- One logical source acquisition per unique stream.
- Explicit data-integrity and recovery guarantees.
- Independent control and data planes.
- Comprehensive deterministic and regression testing.