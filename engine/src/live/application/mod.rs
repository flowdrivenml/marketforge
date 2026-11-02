//! Owned headless application. Handles admit bounded commands; services own lifecycle work.
#[cfg(unix)]
mod runtime;
#[cfg(unix)]
pub use runtime::*;

mod infrastructure;
pub use infrastructure::*;
