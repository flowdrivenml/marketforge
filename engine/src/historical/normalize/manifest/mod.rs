mod builder;
mod discovery;
mod inspection;
mod models;
mod persistence;

pub use discovery::{inspect_auxiliary_files, inspect_parquet_files};

pub use models::{
    AuxiliaryFile, AuxiliaryFileKind, DATASET_MANIFEST_FILENAME, DATASET_MANIFEST_VERSION,
    DatasetFile, DatasetManifest,
};
