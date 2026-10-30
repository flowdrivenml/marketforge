//! Bounded chronological head selection, independent of source transport.
pub mod kway;
pub mod ordering;
pub use kway::{HeadQueue, MergeError, MergePoll, SelectedHead};
pub use ordering::MergeKey;
