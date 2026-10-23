pub mod common;
pub mod depth;
pub mod identity;
pub mod store;
pub mod trade;
pub mod validation;

// ============================================================
// FORMAT IDENTITY
// ============================================================

pub use identity::{FormatIdentity, FormatKind};

// ============================================================
// TRADE CONFIGURATION
// ============================================================

pub use trade::{
    SymbolTypeConfig, TradeAggregationConfig, TradeAttributeConfig, TradeFormatConfig,
    TradeIdentityConfig, TradeIntegrityConfig, TradeOutputConfig, TradeProtocolConfig,
    TradeSequenceConfig, TradeStreamInitializationConfig, UnifiedTradeConfig,
};

// ============================================================
// DEPTH CONFIGURATION
// ============================================================

pub use depth::{
    DepthBookConfig, DepthClassificationConfig, DepthCoverageConfig, DepthFormatConfig,
    DepthIntegrityConfig, DepthLevelSideConfig, DepthLevelsConfig, DepthOutputConfig,
    DepthProtocolConfig, DepthRecoveryConfig, DepthSequenceBridgeConfig, DepthSequenceConfig,
    DepthSequenceIncrementalConfig, DepthSequenceKeepaliveConfig, DepthSequenceMaintenanceConfig,
    DepthSequenceResetConfig, DepthSymbolTypeConfig, DepthSynchronizationBridgeConfig,
    DepthSynchronizationConfig, DepthWebSocketSnapshotConfig,
};

// ============================================================
// SHARED CONFIGURATION
// ============================================================

pub use common::{
    ContractNormalization, EnvelopeMappings, FieldMappings, FieldTypes, InstrumentConfig,
    MessageConfig, NormalizationConfig, OrderCountNormalization, PriceNormalization,
    QuantityNormalization, RecordsConfig, SideNormalization, TimestampConfig,
};

// ============================================================
// FORMAT STORE
// ============================================================

pub use store::FormatConfigStore;
