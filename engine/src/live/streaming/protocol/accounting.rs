//! Checked charge of owned Rust payload storage, excluding allocator and queue overhead.
//! Inline fields are charged once, at the root or by their containing Vec allocation.
use super::{market::*, *};
use std::mem::size_of;
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("retained payload charge overflow")]
pub struct AccountingError;
type Charge = Result<usize, AccountingError>;
fn add(a: usize, b: usize) -> Charge {
    a.checked_add(b).ok_or(AccountingError)
}
fn allocation<T>(capacity: usize) -> Charge {
    capacity.checked_mul(size_of::<T>()).ok_or(AccountingError)
}
trait Owned {
    fn owned(&self) -> Charge;
}
impl Owned for String {
    fn owned(&self) -> Charge {
        Ok(self.capacity())
    }
}
impl<T: Owned> Owned for Option<T> {
    fn owned(&self) -> Charge {
        self.as_ref().map_or(Ok(0), Owned::owned)
    }
}
impl<T: Owned> Owned for Vec<T> {
    fn owned(&self) -> Charge {
        self.iter()
            .try_fold(allocation::<T>(self.capacity())?, |n, x| add(n, x.owned()?))
    }
}
macro_rules! owned {($ty:ty: $($field:ident),+)=>{impl Owned for $ty {fn owned(&self)->Charge{let mut n=0;$(n=add(n,self.$field.owned()?)?;)+Ok(n)}}};}
owned!(Decimal: coefficient);
owned!(Trade: exchange,symbol,stream_id,trade_id,side,price,quantity_base,quantity_quote,quantity_contracts,trade_iv,mark_iv,index_price,mark_price);
owned!(Position: stream_id);
owned!(Level: operation,side,price,quantity_base,quantity_quote,quantity_contracts);
owned!(Depth: kind,timestamp_provenance,composition,operations);
impl Owned for NativeEvent {
    fn owned(&self) -> Charge {
        match self {
            Self::Trade(t) => t.owned(),
            Self::Depth(d) => d.owned(),
        }
    }
}
owned!(Reference: stream_id,direction,price);
owned!(ConvertedValues: price,quantity_quote);
owned!(Valuation: target_quote,rate,numerator,denominator,references,reference_integrity,path,values);
owned!(Record: native,origin,upstream,integrity,valuation);
owned!(StreamPublication: stream_id,records);
owned!(EvidenceId: source);
owned!(SequenceObservation: kind);
owned!(DepthCoverage: snapshot_source);
owned!(DataIntegrityEvidence: id,coverage,diagnostic,depth_coverage,recovery_from,observations);
owned!(StreamDiscontinuity: detail);
owned!(DataIntegritySnapshot: stream_id,sources,evidence_history,discontinuity);
owned!(DataIntegrityUpdate: evidence);
owned!(SubscriptionEnd: reason);
/// Inline StreamMessage storage plus String/Vec capacities, recursively. This is
/// an application-owned representation charge, not an allocator or RSS estimate.
pub fn retained_charge(message: &StreamMessage) -> Charge {
    let owned = match message {
        StreamMessage::Publication(p) => p.owned(),
        StreamMessage::IntegritySnapshot(s) => s.owned(),
        StreamMessage::IntegrityUpdate(u) => u.owned(),
        StreamMessage::End(e) => e.owned(),
    }?;
    add(size_of::<StreamMessage>(), owned)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checked_capacity_arithmetic() {
        assert_eq!(allocation::<Record>(usize::MAX), Err(AccountingError));
        assert_eq!(add(usize::MAX, 1), Err(AccountingError));
    }
    #[test]
    fn capacities_not_lengths_and_no_inline_double_charge() {
        let mut reason = String::with_capacity(4096);
        reason.push('x');
        let m = StreamMessage::End(SubscriptionEnd { reason });
        assert_eq!(
            retained_charge(&m).unwrap(),
            size_of::<StreamMessage>() + 4096
        );
        let mut s = DataIntegritySnapshot {
            stream_id: String::with_capacity(20),
            runtime_generation: None,
            revision: 0,
            sources: Vec::with_capacity(17),
            scope_truncated: false,
            evidence_history: Vec::with_capacity(3),
            history_truncated: false,
            discontinuity: None,
        };
        let mut e = DataIntegrityEvidence::unavailable("a".into(), Some(1));
        e.observations = Vec::with_capacity(64);
        e.observations.push(SequenceObservation {
            ordinal: 0,
            kind: String::with_capacity(100),
            previous: 0,
            observed: 1,
            missing_records: None,
        });
        let extra = e.id.source.capacity()
            + e.coverage.capacity()
            + 64 * size_of::<SequenceObservation>()
            + 100;
        s.sources.push(e);
        assert_eq!(
            retained_charge(&StreamMessage::IntegritySnapshot(s)).unwrap(),
            size_of::<StreamMessage>() + 20 + 20 * size_of::<DataIntegrityEvidence>() + extra
        );
    }
}

#[cfg(test)]
mod nested_tests {
    use super::*;
    fn decimal() -> Decimal {
        Decimal {
            coefficient: "12345".into(),
            scale: 2,
        }
    }
    #[test]
    fn nested_depth_reference_and_path_capacity_is_retained() {
        let mut record = Record {
            ordinal: 0,
            native: NativeEvent::Depth(Depth {
                kind: "delta".into(),
                source_generation: 1,
                book_generation: 1,
                last_sequence: None,
                cross_sequence: None,
                timestamp_ns: None,
                timestamp_provenance: None,
                composition: None,
                internal_baseline_synchronized: None,
                client_book_initialized: false,
                operations: Vec::with_capacity(2048),
            }),
            origin: None,
            upstream: None,
            processing_input_position: None,
            processing_admission_ordinal: None,
            baseline_barrier: None,
            integrity: None,
            valuation: None,
        };
        let mut expected =
            size_of::<StreamMessage>() + size_of::<Record>() + 5 + 2048 * size_of::<Level>();
        let NativeEvent::Depth(d) = &mut record.native else {
            unreachable!()
        };
        for _ in 0..1024 {
            d.operations.push(Level {
                operation: "set".into(),
                side: "bid".into(),
                price: decimal(),
                quantity_base: Some(decimal()),
                quantity_quote: None,
                quantity_contracts: None,
                order_count: None,
            });
            expected += 3 + 3 + 5 + 5;
        }
        let mut path = Vec::with_capacity(256);
        for _ in 0..128 {
            path.push("x".to_string());
        }
        path.push("z".repeat(4096));
        let mut references = Vec::with_capacity(8);
        references.push(Reference {
            dataset_id: 1,
            stream_id: "ref".into(),
            runtime_generation: Some(1),
            timestamp_ns: 1,
            stream_rank: 0,
            row_sequence: 1,
            event_ordinal: 0,
            direction: "direct".into(),
            price: decimal(),
        });
        let evidence = DataIntegrityEvidence::unavailable("ref".into(), Some(1));
        let mut evidence_vec = Vec::with_capacity(4);
        evidence_vec.push(evidence);
        record.valuation = Some(Valuation {
            target_quote: "USD".into(),
            rate: decimal(),
            numerator: "1".into(),
            denominator: "1".into(),
            references,
            reference_integrity: evidence_vec,
            path,
            values: Vec::with_capacity(16),
        });
        expected += 3
            + 5
            + 1
            + 1
            + 8 * size_of::<Reference>()
            + 3
            + 6
            + 5
            + 4 * size_of::<DataIntegrityEvidence>()
            + 3
            + 11
            + 256 * size_of::<String>()
            + 128
            + 4096
            + 16 * size_of::<ConvertedValues>();
        let m = StreamMessage::Publication(StreamPublication {
            stream_id: String::new(),
            runtime_generation: 1,
            router_sequence: 1,
            recovered_records: false,
            records: vec![record],
        });
        assert_eq!(retained_charge(&m).unwrap(), expected);
    }
}
