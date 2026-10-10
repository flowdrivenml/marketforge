mod assembler;
mod csv;
mod json;
mod outcome;
mod processor;
mod relative;
mod scalar;
mod snapshot;
mod spec;
pub mod transforms;
mod update;

pub use csv::DepthCsvAdapter;
pub use relative::{RelativeAction, apply_relative_update};

pub use spec::{
    DepthOperation, DepthSpec, EventFilter, FieldSpec, InputOrderingSpec, InstrumentFieldSpec,
    LevelArraySpec, QuantityFieldSpec, RelativeUpdateSpec, ScalarLevelSpec, SideSpec, SnapshotSpec,
};

pub use assembler::{AssembledSnapshot, DepthSnapshotAssembler};
pub use json::{json_scalar, matches_event_filter, required_json_field, resolve_json_path};
pub use outcome::{
    DepthEventBoundary, DepthProcessingOutcome, DepthSourceEventMetadata, DepthSourceOperation,
};
pub use processor::{DepthContext, DepthProcessor};
pub use scalar::extract_scalar_level;
pub use transforms::{
    ExtractedLevel, extract_decimal, extract_level_array, extract_optional_sequence,
    extract_timestamp_ns, extract_u64,
};
