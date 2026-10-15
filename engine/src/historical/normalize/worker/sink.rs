use crate::{
    canonical::Trade, error::Result, formats::depth::DepthProcessingOutcome,
    process::boundary::DepthBoundaryManifest,
};

pub trait TradeSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}

pub trait DepthSink {
    /// Accept one complete source-event outcome.
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()>;

    /// Accept completed boundary metadata.
    ///
    /// Every implementation must explicitly handle this operation.
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()>;

    fn finish(&mut self) -> Result<()>;
}
