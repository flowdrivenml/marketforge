mod json;
mod outcome;
mod processor;
mod snapshot;
mod spec;
mod transforms;
mod update;

pub use spec::{
    DepthOperation, DepthSpec, EventFilter, FieldSpec, InputOrderingSpec, InstrumentFieldSpec,
    LevelArraySpec, QuantityFieldSpec, RelativeUpdateSpec, ScalarLevelSpec, SideSpec, SnapshotSpec,
};

pub use json::{json_scalar, matches_event_filter, required_json_field, resolve_json_path};
pub use outcome::{DepthEventBoundary, DepthProcessingOutcome};
pub use processor::{DepthContext, DepthProcessor};
pub use transforms::{
    ExtractedLevel, extract_decimal, extract_level_array, extract_optional_sequence,
    extract_timestamp_ns, extract_u64,
};
