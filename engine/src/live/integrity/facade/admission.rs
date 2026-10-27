/// Facade-level admission state.
///
/// This controls whether the source-stream runtime is
/// authorized to submit data to the current ingress.
///
/// It does not describe depth synchronization or prove
/// historical trade coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityAdmissionState {
    Active,
    Interrupted,
    Stopped,
}

impl IntegrityAdmissionState {
    pub fn allows_processing(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityAdmissionError {
    Interrupted,
    Stopped,
}

impl IntegrityAdmissionState {
    pub fn rejection(self) -> Option<IntegrityAdmissionError> {
        match self {
            Self::Active => None,
            Self::Interrupted => Some(IntegrityAdmissionError::Interrupted),
            Self::Stopped => Some(IntegrityAdmissionError::Stopped),
        }
    }
}
