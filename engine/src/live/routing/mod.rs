//! Canonical router: bounded ordered admission, delivery ownership and observations.
//! Concrete storage, publication, recovery and executor adapters use their frozen ports.
pub mod acknowledgement;
pub mod admission;
pub mod completion;
pub mod cursor;
pub mod destination;
pub mod driver;
pub mod ingress;
pub mod ingress_reporting;
pub mod namespaces;
pub mod ordered;
pub mod port;
pub mod queue;
pub mod reporting;
pub mod resolution;
pub mod router;
pub mod runtime;
pub mod shutdown;
pub mod snapshot;
pub mod supervisor;
pub mod tracking_recovery;
pub mod transaction;
pub mod worker;

pub mod metrics;
