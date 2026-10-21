//! Checked advancement and exact fencing for independent lifecycle domains.
use super::{cursor::*, error::ErrorCode};

macro_rules! checked_generation {
    ($($ty:ident),+ $(,)?) => {$ (
        impl $ty {
            /// Exhaustion never wraps or mutates the current owner.
            pub fn checked_next(self) -> Result<Self, ErrorCode> {
                self.0.checked_add(1).map(Self).ok_or(ErrorCode::Limit)
            }
            /// An operation must match the current owner exactly, including
            /// rejecting speculative future generations.
            pub fn check_owner(self, expected: Self) -> Result<(), ErrorCode> {
                if self == expected { Ok(()) } else { Err(ErrorCode::StaleGeneration) }
            }
        }
    )+};
}
checked_generation!(
    ConnectionGeneration,
    SubscriptionGeneration,
    BookGeneration,
    DerivedGeneration,
    LogEpoch
);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_domains_exhaust_and_fence() {
        macro_rules! check {
            ($($ty:ident),+) => {$ (
                assert_eq!($ty(3).checked_next(), Ok($ty(4)));
                assert_eq!($ty(u64::MAX).checked_next(), Err(ErrorCode::Limit));
                assert_eq!($ty(3).check_owner($ty(3)), Ok(()));
                assert_eq!($ty(3).check_owner($ty(2)), Err(ErrorCode::StaleGeneration));
                assert_eq!($ty(3).check_owner($ty(4)), Err(ErrorCode::StaleGeneration));
            )+};
        }
        check!(
            ConnectionGeneration,
            SubscriptionGeneration,
            BookGeneration,
            DerivedGeneration,
            LogEpoch
        );
    }
    #[test]
    fn cursor_namespaces_are_incomparable() {
        use crate::live::model::identity::StreamId;
        let a = StreamCursor::new(StreamId("a".into()), LogEpoch(1), 7);
        let mut b = a.clone();
        b.offset = 8;
        assert_eq!(a.compare_position(&b), Some(std::cmp::Ordering::Less));
        b.log_epoch = LogEpoch(2);
        assert_eq!(a.compare_position(&b), None);
        b.log_epoch = a.log_epoch;
        b.stream_id = StreamId("b".into());
        assert_eq!(a.compare_position(&b), None);
    }
}
