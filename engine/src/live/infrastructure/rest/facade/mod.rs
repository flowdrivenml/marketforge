mod capabilities;
mod factory;
mod handle;
mod history;
mod operations;

pub use capabilities::{
    HistoricalPagination, HistoricalTradesCapability, RestEndpointCapability,
    StreamRestCapabilities,
};

pub use factory::{RestFacadeError, RestFacadeFactory};

pub use handle::StreamRestHandle;

pub use operations::RestOperationError;

pub use history::{HistoricalTradeCursor, HistoricalTradePage};
