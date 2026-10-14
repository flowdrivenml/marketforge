use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{
    error::Result,
    job::MergeParquetConfig,
    process::merge::{PhysicalParquetWriter, PhysicalWriterMetrics, UnifiedMergeMode},
};

pub struct ConvertedParquetWriter {
    inner: PhysicalParquetWriter,
}

impl ConvertedParquetWriter {
    pub fn new(output_dir: &Path, schema: SchemaRef, config: &MergeParquetConfig) -> Result<Self> {
        Self::with_mode(output_dir, UnifiedMergeMode::Trades, schema, config)
    }

    pub fn with_mode(
        output_dir: &Path,
        mode: UnifiedMergeMode,
        schema: SchemaRef,
        config: &MergeParquetConfig,
    ) -> Result<Self> {
        let inner = PhysicalParquetWriter::with_schema(output_dir, mode, schema, config)?;

        Ok(Self { inner })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }

    pub fn metrics(&self) -> &PhysicalWriterMetrics {
        self.inner.metrics()
    }

    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    pub fn is_failed(&self) -> bool {
        self.inner.is_failed()
    }
}
