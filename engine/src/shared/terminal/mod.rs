//! Feature-gated rendering primitives shared by application screens.
pub mod format;
pub mod guard;
pub mod palette;

pub mod widgets;

#[cfg(feature = "historical")]
pub(crate) mod interrupt;
