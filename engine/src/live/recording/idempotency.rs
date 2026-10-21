//! Exact-content retry reconciliation; uniqueness/ON CONFLICT alone is not proof.
use super::schema::StoredEvent;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    ResubmitMissing,
    AlreadyCommitted,
    Conflict,
    Indeterminate,
}
pub enum ExistingEvent<'a> {
    Absent,
    IdentityOnly,
    Recorded(&'a StoredEvent),
}
pub fn reconcile(expected: &StoredEvent, existing: ExistingEvent<'_>) -> RetryDisposition {
    let Ok(expected_event) = expected.decode() else {
        return RetryDisposition::Conflict;
    };
    match existing {
        ExistingEvent::Absent => RetryDisposition::ResubmitMissing,
        ExistingEvent::IdentityOnly => RetryDisposition::Indeterminate,
        ExistingEvent::Recorded(actual) => match actual.decode() {
            Ok(e) if e == expected_event => RetryDisposition::AlreadyCommitted,
            Ok(_) => RetryDisposition::Conflict,
            Err(_) => RetryDisposition::Indeterminate,
        },
    }
}
