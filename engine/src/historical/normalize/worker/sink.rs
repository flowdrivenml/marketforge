use {
    crate::historical::normalize::boundary::DepthBoundaryManifest,
    crate::shared::domain::canonical::Trade, crate::shared::error::Result,
    crate::shared::formats::depth::DepthProcessingOutcome,
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
