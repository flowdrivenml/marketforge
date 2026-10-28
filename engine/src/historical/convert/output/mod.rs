mod publication;
mod schema;
mod shared;
mod validation;
mod writers;

pub use publication::atomic::atomic_publish_directory;
pub use publication::manifest::{
    CONVERSION_MANIFEST_FILENAME, CONVERSION_MANIFEST_VERSION, ConversionManifest,
    ConversionManifestFile, ConversionManifestInput, ConversionManifestIntegrity,
    ConversionManifestOutput, ConversionManifestStatistics,
};
pub use publication::receipt::{
    CONVERSION_RECEIPT_FILENAME, CONVERSION_RECEIPT_VERSION, ConversionDepthStatistics,
    ConversionExecutionReceipt,
};
pub use publication::{
    ConversionPublication, publish_combined_conversion, publish_combined_conversion_with_progress,
    publish_conversion, publish_conversion_with_progress, publish_depth_conversion,
    publish_depth_conversion_with_progress,
};
pub use schema::depth::{ConvertedDepthBatchBuilder, DepthConversionBuffers, DepthConversionRow};
pub use schema::trade::{ConvertedTradeBatchBuilder, TradeConversionRow};
pub use schema::{
    CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE, CONVERSION_FAILURE_IDENTITY_MISMATCH,
    CONVERSION_FAILURE_MISSING, CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE,
    CONVERSION_SCHEMA_VERSION, CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
    CONVERSION_STATUS_UNAVAILABLE, conversion_fields, converted_combined_schema,
    converted_depth_schema, converted_trade_schema,
};
pub use shared::files::discover_conversion_files;
pub use validation::combined::{
    CombinedFileValidationResult, CombinedValidationMetrics, validate_combined_file,
    validate_converted_combined,
};
pub use validation::combined_numeric::{
    CombinedNumericValidationMetrics, validate_combined_numeric_batch,
};
pub use validation::combined_parallel::{
    validate_converted_combined_parallel, validate_converted_combined_parallel_with_progress,
};
pub use validation::depth::{
    DepthConversionValidationMetrics, DepthFileValidationResult, validate_converted_depth,
    validate_depth_file,
};
pub use validation::depth_numeric::{DepthNumericValidationMetrics, validate_depth_numeric_batch};
pub use validation::depth_parallel::{
    validate_converted_depth_parallel, validate_converted_depth_parallel_with_progress,
};
pub use validation::source::{
    SourcePreservationMetrics, SourcePreservationMode, validate_depth_source_preservation,
    validate_source_preservation, validate_source_preservation_with_mode,
    validate_source_preservation_with_progress,
};
pub use validation::trades::{
    CONVERSION_VALIDATION_BATCH_SIZE, ConversionValidationMetrics, validate_converted_trades,
    validate_converted_trades_with_progress,
};
pub use writers::combined_writer::ConvertedCombinedBatchBuilder;
pub use writers::writer::ConvertedParquetWriter;
