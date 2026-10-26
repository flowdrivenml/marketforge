//! Compile declarative depth protocol rules into a typed, immutable plan.
//!
//! This module is deliberately not a runtime sequence validator. It neither
//! mutates books nor authorizes publication. Live controllers must implement
//! each strategy's documented transitions, generation fencing and recovery.

use thiserror::Error;

use crate::live::config::{exchange::ExchangeId, format::DepthFormatConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthContinuityFamily {
    BinanceSpotRange,
    BinanceFuturesPrevious,
    BybitV5,
    BitgetV3Previous,
    GateRangeCover,
    OkxV5Previous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthSnapshotSource {
    Rest,
    WebSocket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthBridgeRule {
    /// Binance Spot: the exchange-specific first buffered update joins the snapshot.
    BinanceSpotRangeContainsSnapshot,
    /// Binance Futures: initial buffered update must meet the configured futures bridge.
    BinanceFuturesSnapshotBridge,
    /// Bitget: update.pseq <= snapshot.seq <= update.seq.
    PreviousToCurrentContainsSnapshot,
    /// Gate: update.U <= snapshot.id + 1 <= update.u.
    NextRequiredInRange,
    /// Initial WS snapshot establishes a baseline without a REST bridge.
    WebSocketSnapshotBaseline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledDepthContinuityPlan {
    pub format_id: String,
    pub family: DepthContinuityFamily,
    pub snapshot_source: DepthSnapshotSource,
    pub initial_bridge: DepthBridgeRule,
    pub requires_update_buffer: bool,
    pub accepts_websocket_snapshots: bool,
    pub allows_overlapping_ranges: bool,
    pub allows_equal_sequence_keepalive: bool,
    pub allows_linked_sequence_decrease: bool,
    pub requires_generation_fencing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DepthContinuityCompileError {
    #[error("depth format '{format_id}' has incompatible continuity declaration: {reason}")]
    Invalid { format_id: String, reason: String },
}

pub struct DepthContinuityCompiler;

impl DepthContinuityCompiler {
    pub fn compile(
        config: &DepthFormatConfig,
    ) -> Result<CompiledDepthContinuityPlan, DepthContinuityCompileError> {
        let fail = |reason: String| DepthContinuityCompileError::Invalid {
            format_id: config.format.id.clone(),
            reason,
        };

        config.validate().map_err(|e| fail(e))?;

        let family = match (
            config.format.exchange,
            config.sequence.strategy.as_str(),
            config.synchronization.strategy.as_str(),
        ) {
            (ExchangeId::Binance, "binance_spot_update_range", "binance_spot_diff_depth") => {
                DepthContinuityFamily::BinanceSpotRange
            }
            (
                ExchangeId::Binance,
                "binance_futures_previous_update",
                "binance_futures_diff_depth",
            ) => DepthContinuityFamily::BinanceFuturesPrevious,
            (ExchangeId::Bybit, "bybit_v5_orderbook", "bybit_v5_snapshot_delta") => {
                DepthContinuityFamily::BybitV5
            }
            (ExchangeId::Bitget, "bitget_v3_books", "bitget_v3_books") => {
                DepthContinuityFamily::BitgetV3Previous
            }
            (ExchangeId::Gateio, "gate_spot_update_range", "gate_spot_order_book_update")
            | (ExchangeId::Gateio, "gate_futures_update_range", "gate_futures_order_book_update") => {
                DepthContinuityFamily::GateRangeCover
            }
            (ExchangeId::Okx, "okx_v5_books", "okx_v5_books") => {
                DepthContinuityFamily::OkxV5Previous
            }
            _ => {
                return Err(fail(format!(
                    "unsupported combination: exchange={:?}, sequence.strategy={:?}, synchronization.strategy={:?}",
                    config.format.exchange,
                    config.sequence.strategy,
                    config.synchronization.strategy
                )));
            }
        };

        let snapshot_source = match config.synchronization.snapshot_source.as_str() {
            "rest" => DepthSnapshotSource::Rest,
            "websocket" => DepthSnapshotSource::WebSocket,
            other => return Err(fail(format!("unsupported snapshot_source {other:?}"))),
        };

        // No downgrade to permissive processing. Every profile must explicitly
        // require generation fencing and atomic changes.
        require(
            config,
            config.recovery.require_generation_fencing,
            "recovery.require_generation_fencing=true",
        )?;
        require(
            config,
            config.book.require_valid_generation,
            "book.require_valid_generation=true",
        )?;
        require(
            config,
            config.book.atomic_message_updates,
            "book.atomic_message_updates=true",
        )?;
        require(
            config,
            config.output.atomic_updates,
            "output.atomic_updates=true",
        )?;
        require(
            config,
            config.book.zero_quantity_action == "delete",
            "book.zero_quantity_action=delete",
        )?;
        require(
            config,
            config.integrity.checksum_strategy == "none",
            "integrity.checksum_strategy=none (no checksum algorithm compiled)",
        )?;

        let (source, bridge, buffer, ws_snapshots, overlaps, keepalive, linked_decrease) =
            match family {
                DepthContinuityFamily::BinanceSpotRange => {
                    field(
                        config,
                        config.sequence.first_field.as_deref(),
                        "sequence.first_field",
                    )?;
                    field(
                        config,
                        config.sequence.last_field.as_deref(),
                        "sequence.last_field",
                    )?;
                    require(
                        config,
                        config.sequence.validate_range == Some(true),
                        "sequence.validate_range=true",
                    )?;
                    require(
                        config,
                        config.sequence.require_first_lte_last == Some(true),
                        "sequence.require_first_lte_last=true",
                    )?;
                    require(
                        config,
                        config.sequence.previous_field.is_none(),
                        "no sequence.previous_field",
                    )?;
                    require(
                        config,
                        config.synchronization.initial_bridge_strategy.as_deref()
                            == Some("update_range_contains_snapshot"),
                        "Binance Spot bridge strategy",
                    )?;
                    require(
                        config,
                        config.synchronization.continuity_strategy.as_deref()
                            == Some("binance_spot_range_continuity"),
                        "Binance Spot continuity strategy",
                    )?;
                    (
                        DepthSnapshotSource::Rest,
                        DepthBridgeRule::BinanceSpotRangeContainsSnapshot,
                        true,
                        false,
                        true,
                        false,
                        false,
                    )
                }
                DepthContinuityFamily::BinanceFuturesPrevious => {
                    field(
                        config,
                        config.sequence.first_field.as_deref(),
                        "sequence.first_field",
                    )?;
                    field(
                        config,
                        config.sequence.last_field.as_deref(),
                        "sequence.last_field",
                    )?;
                    field(
                        config,
                        config.sequence.previous_field.as_deref(),
                        "sequence.previous_field",
                    )?;
                    require(
                        config,
                        config.sequence.previous_field_required == Some(true),
                        "sequence.previous_field_required=true",
                    )?;
                    require(
                        config,
                        config.sequence.validate_range == Some(true),
                        "sequence.validate_range=true",
                    )?;
                    require(
                        config,
                        config.sequence.require_first_lte_last == Some(true),
                        "sequence.require_first_lte_last=true",
                    )?;
                    require(
                        config,
                        config.sequence.continuity_strategy.as_deref()
                            == Some("previous_last_update_id"),
                        "Binance Futures continuity strategy",
                    )?;
                    require(
                        config,
                        config.synchronization.initial_bridge_strategy.as_deref()
                            == Some("binance_futures_snapshot_bridge"),
                        "Binance Futures bridge strategy",
                    )?;
                    (
                        DepthSnapshotSource::Rest,
                        DepthBridgeRule::BinanceFuturesSnapshotBridge,
                        true,
                        false,
                        false,
                        false,
                        false,
                    )
                }
                DepthContinuityFamily::BybitV5 => {
                    field(
                        config,
                        config.sequence.update_id_field.as_deref(),
                        "sequence.update_id_field",
                    )?;
                    field(
                        config,
                        config.sequence.cross_sequence_field.as_deref(),
                        "sequence.cross_sequence_field",
                    )?;
                    require(
                        config,
                        config.sequence.require_consecutive_update_id == Some(false),
                        "Bybit update IDs must not be treated as consecutive",
                    )?;
                    require(
                        config,
                        config.sequence.cross_sequence_requires_consecutive == Some(false),
                        "Bybit cross-sequence must not be treated as consecutive",
                    )?;
                    require(
                        config,
                        config.synchronization.require_initial_snapshot == Some(true),
                        "synchronization.require_initial_snapshot=true",
                    )?;
                    require(
                        config,
                        config.synchronization.snapshot_only_depths.contains(&1),
                        "Bybit depth-1 snapshot-only rule",
                    )?;
                    require(
                        config,
                        config.synchronization.reset_update_id == Some(1),
                        "Bybit reset_update_id=1",
                    )?;
                    (
                        DepthSnapshotSource::WebSocket,
                        DepthBridgeRule::WebSocketSnapshotBaseline,
                        false,
                        true,
                        false,
                        false,
                        false,
                    )
                }
                DepthContinuityFamily::BitgetV3Previous => {
                    field(
                        config,
                        config.sequence.sequence_field.as_deref(),
                        "sequence.sequence_field",
                    )?;
                    field(
                        config,
                        config.sequence.previous_sequence_field.as_deref(),
                        "sequence.previous_sequence_field",
                    )?;
                    require(
                        config,
                        config.sequence.continuity_strategy.as_deref()
                            == Some("previous_sequence_matches"),
                        "Bitget previous-sequence strategy",
                    )?;
                    require(
                        config,
                        config.sequence.require_consecutive_integer_ids == Some(false),
                        "Bitget sequences must not be consecutive",
                    )?;
                    require(
                        config,
                        config.sequence.snapshot_bridge.as_ref().is_some_and(|b| {
                            b.enabled && b.strategy == "snapshot_sequence_in_update_range"
                        }),
                        "Bitget snapshot bridge",
                    )?;
                    require(
                        config,
                        config.sequence.reset.as_ref().is_some_and(|r| {
                            r.reset_previous_sequence_id == Some(0)
                                && r.action == "require_new_snapshot"
                        }),
                        "Bitget reset requires new snapshot",
                    )?;
                    require(
                        config,
                        config.sequence.maintenance.as_ref().is_some_and(|m| {
                            m.allow_sequence_decrease && m.require_previous_sequence_match
                        }),
                        "Bitget linked maintenance exception",
                    )?;
                    require(
                        config,
                        config.synchronization.require_initial_snapshot == Some(true),
                        "synchronization.require_initial_snapshot=true",
                    )?;
                    (
                        DepthSnapshotSource::WebSocket,
                        DepthBridgeRule::PreviousToCurrentContainsSnapshot,
                        false,
                        true,
                        false,
                        false,
                        true,
                    )
                }
                DepthContinuityFamily::GateRangeCover => {
                    field(
                        config,
                        config.sequence.first_field.as_deref(),
                        "sequence.first_field",
                    )?;
                    field(
                        config,
                        config.sequence.last_field.as_deref(),
                        "sequence.last_field",
                    )?;
                    require(
                        config,
                        config.sequence.continuity_strategy.as_deref()
                            == Some("range_covers_next_update"),
                        "Gate range-covers-next-update continuity",
                    )?;
                    require(
                        config,
                        config.sequence.validate_range == Some(true),
                        "sequence.validate_range=true",
                    )?;
                    require(
                        config,
                        config.sequence.require_first_lte_last == Some(true),
                        "sequence.require_first_lte_last=true",
                    )?;
                    require(
                        config,
                        config.sequence.incremental.as_ref().is_some_and(|i| {
                            i.allow_overlapping_ranges && i.stale_action == "ignore"
                        }),
                        "Gate overlapping-range/stale policy",
                    )?;
                    require(
                        config,
                        config
                            .synchronization
                            .snapshot_bridge
                            .as_ref()
                            .is_some_and(|b| b.enabled && b.strategy == "next_update_in_range"),
                        "Gate REST snapshot bridge",
                    )?;
                    require(
                        config,
                        config
                            .synchronization
                            .websocket_snapshot
                            .as_ref()
                            .is_some_and(|s| {
                                s.enabled
                                    && s.snapshot_action == "replace"
                                    && s.require_coverage_tracking
                            }),
                        "Gate WebSocket full-snapshot replacement and coverage",
                    )?;
                    (
                        DepthSnapshotSource::Rest,
                        DepthBridgeRule::NextRequiredInRange,
                        true,
                        true,
                        true,
                        false,
                        false,
                    )
                }
                DepthContinuityFamily::OkxV5Previous => {
                    field(
                        config,
                        config.sequence.sequence_field.as_deref(),
                        "sequence.sequence_field",
                    )?;
                    field(
                        config,
                        config.sequence.previous_sequence_field.as_deref(),
                        "sequence.previous_sequence_field",
                    )?;
                    require(
                        config,
                        config.sequence.continuity_strategy.as_deref()
                            == Some("previous_sequence_matches"),
                        "OKX previous-sequence strategy",
                    )?;
                    require(
                        config,
                        config.sequence.snapshot_previous_sequence_id == Some(-1),
                        "OKX snapshot previous sequence sentinel -1",
                    )?;
                    require(
                        config,
                        config.sequence.require_consecutive_integer_ids == Some(false),
                        "OKX sequences must not be consecutive",
                    )?;
                    require(
                        config,
                        config.sequence.keepalive.as_ref().is_some_and(|k| {
                            k.enabled
                                && k.allow_equal_sequence_id
                                && k.require_empty_bids
                                && k.require_empty_asks
                                && k.book_action == "none"
                        }),
                        "OKX empty-book keepalive exception",
                    )?;
                    require(
                        config,
                        config.sequence.reset.as_ref().is_some_and(|r| {
                            r.allow_sequence_decrease == Some(true)
                                && r.require_previous_sequence_match == Some(true)
                                && r.action == "accept_validated_transition"
                        }),
                        "OKX linked sequence reset",
                    )?;
                    require(
                        config,
                        config.synchronization.require_initial_snapshot == Some(true),
                        "synchronization.require_initial_snapshot=true",
                    )?;
                    (
                        DepthSnapshotSource::WebSocket,
                        DepthBridgeRule::WebSocketSnapshotBaseline,
                        false,
                        true,
                        false,
                        true,
                        true,
                    )
                }
            };

        require(
            config,
            snapshot_source == source,
            "snapshot_source consistent with the native strategy",
        )?;
        require(
            config,
            config.synchronization.buffer_updates_before_snapshot == Some(true) || !buffer,
            "REST snapshot strategy must buffer updates before snapshot",
        )?;
        require(
            config,
            !matches!(source, DepthSnapshotSource::Rest)
                || config
                    .synchronization
                    .snapshot_endpoint
                    .as_deref()
                    .is_some_and(|v| !v.is_empty()),
            "REST depth snapshot endpoint",
        )?;
        require(
            config,
            !matches!(source, DepthSnapshotSource::WebSocket)
                || config.recovery.rest_snapshot_fallback != Some(true),
            "unverified REST fallback on WebSocket-snapshot protocol",
        )?;

        Ok(CompiledDepthContinuityPlan {
            format_id: config.format.id.clone(),
            family,
            snapshot_source,
            initial_bridge: bridge,
            requires_update_buffer: buffer,
            accepts_websocket_snapshots: ws_snapshots,
            allows_overlapping_ranges: overlaps,
            allows_equal_sequence_keepalive: keepalive,
            allows_linked_sequence_decrease: linked_decrease,
            requires_generation_fencing: true,
        })
    }
}

fn require(
    config: &DepthFormatConfig,
    condition: bool,
    contract: &str,
) -> Result<(), DepthContinuityCompileError> {
    if condition {
        Ok(())
    } else {
        Err(DepthContinuityCompileError::Invalid {
            format_id: config.format.id.clone(),
            reason: format!("requires {contract}"),
        })
    }
}

fn field(
    config: &DepthFormatConfig,
    name: Option<&str>,
    contract: &str,
) -> Result<(), DepthContinuityCompileError> {
    let Some(name) = name.filter(|name| !name.trim().is_empty()) else {
        return require(config, false, contract);
    };
    require(config, config.fields.contains_key(name), contract)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn path(id: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{id}.toml"))
    }

    #[test]
    fn compiles_all_fifteen_depth_profiles() {
        let cases = [
            ("binance_spot", DepthContinuityFamily::BinanceSpotRange),
            (
                "binance_linear",
                DepthContinuityFamily::BinanceFuturesPrevious,
            ),
            (
                "binance_inverse",
                DepthContinuityFamily::BinanceFuturesPrevious,
            ),
            ("bybit_spot", DepthContinuityFamily::BybitV5),
            ("bybit_linear", DepthContinuityFamily::BybitV5),
            ("bybit_inverse", DepthContinuityFamily::BybitV5),
            ("bitget_spot", DepthContinuityFamily::BitgetV3Previous),
            ("bitget_linear", DepthContinuityFamily::BitgetV3Previous),
            ("bitget_inverse", DepthContinuityFamily::BitgetV3Previous),
            ("gateio_spot", DepthContinuityFamily::GateRangeCover),
            ("gateio_linear", DepthContinuityFamily::GateRangeCover),
            ("gateio_inverse", DepthContinuityFamily::GateRangeCover),
            ("okx_spot", DepthContinuityFamily::OkxV5Previous),
            ("okx_linear", DepthContinuityFamily::OkxV5Previous),
            ("okx_inverse", DepthContinuityFamily::OkxV5Previous),
        ];
        for (id, expected) in cases {
            let config = DepthFormatConfig::load(path(id)).unwrap();
            let compiled =
                DepthContinuityCompiler::compile(&config).unwrap_or_else(|e| panic!("{id}: {e}"));
            assert_eq!(compiled.family, expected, "{id}");
            assert!(compiled.requires_generation_fencing, "{id}");
        }
    }

    #[test]
    fn rejects_unsupported_sequence_contract() {
        let mut config = DepthFormatConfig::load(path("binance_spot")).unwrap();
        config.sequence.strategy = "unknown".into();
        assert!(DepthContinuityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_bybit_consecutive_assumption() {
        let mut config = DepthFormatConfig::load(path("bybit_spot")).unwrap();
        config.sequence.require_consecutive_update_id = Some(true);
        assert!(DepthContinuityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_missing_okx_keepalive_exception() {
        let mut config = DepthFormatConfig::load(path("okx_spot")).unwrap();
        config.sequence.keepalive = None;
        assert!(DepthContinuityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_gate_overlapping_range_downgrade() {
        let mut config = DepthFormatConfig::load(path("gateio_spot")).unwrap();
        config
            .sequence
            .incremental
            .as_mut()
            .unwrap()
            .allow_overlapping_ranges = false;
        assert!(DepthContinuityCompiler::compile(&config).is_err());
    }

    #[test]
    fn rejects_missing_generation_fencing() {
        let mut config = DepthFormatConfig::load(path("bitget_spot")).unwrap();
        config.recovery.require_generation_fencing = false;
        assert!(DepthContinuityCompiler::compile(&config).is_err());
    }
}
