//! Pure event planning, bounds and synchronization evidence. No source processing.
use super::*;
use crate::shared::domain::canonical::{Exchange, TradeSide};
use serde_json::json;
pub(super) fn event(
    source: usize,
    priority: u64,
    time: i64,
    id: Option<&str>,
    price: i64,
) -> TradeEntry {
    let trade:Trade=serde_json::from_value(json!({"envelope":{"event_timestamp_ns":time,"system_timestamp_ns":null,"exchange":"binance","instrument_id":1,"symbol":"SYNTHETIC","stream_id":"synthetic"},"trade_id":id,"sequence":null,"side":"Buy","price":price.to_string(),"quantity_base":"1","quantity_quote":price.to_string(),"quantity_contracts":null,"is_rpi":null,"trade_iv":null,"mark_iv":null,"index_price":null,"mark_price":null})).unwrap();
    TradeEntry {
        source,
        priority,
        ordinal: 1,
        trade,
    }
}
pub(super) fn observations(entries: &[TradeEntry]) -> BTreeMap<usize, SourceObservation> {
    let mut map = BTreeMap::new();
    for e in entries {
        let o = map.entry(e.source).or_insert_with(|| SourceObservation {
            source_priority: e.priority,
            ..Default::default()
        });
        o.observe(e.trade.envelope.event_timestamp_ns);
    }
    map
}
#[test]
fn priority_wins_before_emission_even_if_loser_has_an_earlier_timestamp() {
    let entries = vec![
        event(900, 0, 30, Some("same"), 3),
        event(2, 1, 10, Some("same"), 2),
        event(2, 1, 40, Some("tail"), 4),
    ];
    let mut observed = observations(&entries);
    let output = reconcile_trades(entries, &mut observed).unwrap();
    assert_eq!(
        output
            .iter()
            .map(|e| e.trade.envelope.event_timestamp_ns)
            .collect::<Vec<_>>(),
        [30, 40]
    );
    assert_eq!(output[0].source, 900);
    assert_eq!(observed[&2].conflicts, 1);
}
#[test]
fn overlap_keeps_unique_records_equal_timestamp_ids_and_later_tail() {
    let entries = vec![
        event(0, 0, 10, Some("a"), 1),
        event(0, 0, 30, Some("b"), 2),
        event(1, 1, 20, Some("unique-inside"), 3),
        event(1, 1, 30, Some("b"), 2),
        event(1, 1, 30, Some("different"), 2),
        event(1, 1, 40, Some("tail"), 4),
    ];
    let mut observed = observations(&entries);
    let output = reconcile_trades(entries, &mut observed).unwrap();
    assert_eq!(
        output
            .iter()
            .map(|e| e.trade.envelope.event_timestamp_ns)
            .collect::<Vec<_>>(),
        [10, 20, 30, 30, 40]
    );
    assert_eq!(observed[&1].duplicates, 1);
}
#[test]
fn identities_are_instrument_exchange_and_stream_scoped_and_within_source_is_unchanged() {
    let mut a = event(0, 0, 1, Some("00000000-0000-0000-0000-000000000001"), 1);
    a.ordinal = 1;
    let mut b = event(0, 0, 1, Some("00000000-0000-0000-0000-000000000001"), 1);
    b.ordinal = 2;
    let mut c = event(1, 1, 1, Some("00000000-0000-0000-0000-000000000001"), 1);
    c.trade.envelope.instrument_id = 2;
    let mut d = event(2, 2, 1, Some("00000000-0000-0000-0000-000000000001"), 1);
    d.trade.envelope.exchange = Exchange::Bybit;
    let mut e = event(3, 3, 1, Some("00000000-0000-0000-0000-000000000001"), 1);
    e.trade.envelope.stream_id = "other-feed".into();
    let entries = vec![a, b, c, d, e];
    let mut observed = observations(&entries);
    assert_eq!(reconcile_trades(entries, &mut observed).unwrap().len(), 5);
}
#[test]
fn missing_id_overlap_and_unknown_reset_domains_fail_explicitly() {
    let entries = vec![event(0, 0, 1, None, 1), event(1, 1, 1, Some("x"), 2)];
    let mut observed = observations(&entries);
    assert!(
        reconcile_trades(entries, &mut observed)
            .unwrap_err()
            .to_string()
            .contains("ambiguous trade overlap")
    );
    let mut entries = vec![
        event(0, 0, 1, Some("10"), 1),
        event(1, 1, 86_400_000_000_001, Some("10"), 2),
    ];
    for e in &mut entries {
        e.trade.envelope.exchange = Exchange::GateIo;
    }
    let mut observed = observations(&entries);
    assert!(
        reconcile_trades(entries, &mut observed)
            .unwrap_err()
            .to_string()
            .contains("identity/reset domain")
    );
}
#[test]
fn observed_first_last_and_min_max_are_not_invented() {
    let mut o = SourceObservation::default();
    assert_eq!(o.minimum_accepted_timestamp_ns, None);
    o.observe(30);
    o.observe(10);
    o.observe(20);
    assert_eq!(
        (
            o.first_accepted_timestamp_ns,
            o.last_accepted_timestamp_ns,
            o.minimum_accepted_timestamp_ns,
            o.maximum_accepted_timestamp_ns,
            o.ordering_validated
        ),
        (Some(30), Some(20), Some(10), Some(30), Some(false))
    );
    let entries = vec![event(0, 0, 30, Some("a"), 1), event(0, 0, 10, Some("b"), 2)];
    let mut observed = observations(&entries);
    assert!(reconcile_trades(entries, &mut observed).is_err());
}
#[test]
fn depth_uses_observed_chronology_and_rejects_ambiguous_equal_time_books() {
    let mut a = SourceObservation {
        source_priority: 0,
        ..Default::default()
    };
    a.observe(30);
    a.observe(40);
    let mut b = SourceObservation {
        source_priority: 1,
        ..Default::default()
    };
    b.observe(10);
    b.observe(20);
    let mut observed = BTreeMap::from([(900, a), (2, b), (1, SourceObservation::default())]);
    assert_eq!(depth_source_order(&observed).unwrap(), [2, 900, 1]);
    observed.get_mut(&2).unwrap().observe(30);
    assert!(
        depth_source_order(&observed)
            .unwrap_err()
            .to_string()
            .contains("unsupported depth overlap")
    );
}
#[test]
fn quota_rejects_before_growth_and_overflow_is_explicit() {
    let quota = Quota::new(3);
    quota.charge(3, "test queue").unwrap();
    let mut output = Vec::new();
    let result = quota.charge(1, "test queue").map(|()| output.push(1));
    assert!(
        matches!(result,Err(MarketForgeError::ResourceLimit(ref e)) if e.permitted==3 && e.requested==4 && e.stage=="test queue")
    );
    assert!(output.is_empty());
    let quota = Quota::new(u64::MAX);
    quota.charge(u64::MAX, "counter").unwrap();
    assert!(quota.charge(1, "counter").is_err());
}
#[test]
fn scheduling_and_completion_order_do_not_change_winners() {
    let mut logical = Vec::new();
    for workers in [1, 3] {
        let groups = crate::historical::normalize::scheduler::execute_parallel(
            vec![0, 1],
            workers,
            |source| {
                Ok(vec![event(
                    source,
                    source as u64,
                    if source == 0 { 30 } else { 10 },
                    Some("id"),
                    source as i64 + 1,
                )])
            },
        )
        .unwrap();
        let entries = groups
            .into_iter()
            .rev()
            .flat_map(Result::unwrap)
            .collect::<Vec<_>>();
        let mut observed = observations(&entries);
        let output = reconcile_trades(entries, &mut observed).unwrap();
        logical.push(
            output
                .into_iter()
                .map(|e| {
                    (
                        e.source,
                        e.trade.envelope.event_timestamp_ns,
                        e.trade.price,
                        e.trade.side == TradeSide::Buy,
                    )
                })
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(logical[0], logical[1]);
    assert_eq!(logical[0][0].0, 0);
}

#[test]
fn depth_recovery_never_fabricates_synchronization() {
    use crate::shared::domain::book::boundary::{BoundaryContinuity, DepthBoundaryManifest};
    let mut boundary =
        DepthBoundaryManifest::new(Exchange::Okx, 1, "SYNTHETIC".into(), "synthetic".into());
    boundary.continuity = BoundaryContinuity::GapDetected;
    assert!(
        validate_depth_recovery(&boundary)
            .unwrap_err()
            .to_string()
            .contains("unrecovered gap")
    );
    boundary.continuity = BoundaryContinuity::RecoveredFromSnapshot;
    validate_depth_recovery(&boundary).unwrap();
    boundary.continuity = BoundaryContinuity::Verified;
    validate_depth_recovery(&boundary).unwrap();
}

#[test]
fn mocked_jobs_preserve_definitive_results_after_another_reconciliation_fails() {
    let results = crate::historical::normalize::scheduler::execute_parallel(vec![0, 1], 2, |job| {
        let entries = if job == 0 {
            vec![event(0, 0, 1, Some("known"), 1)]
        } else {
            vec![event(0, 0, 1, None, 1), event(1, 1, 1, None, 1)]
        };
        let mut observed = observations(&entries);
        // Same shape as the session closure: retain each job's definitive error
        // inside its result rather than aborting the outer collection.
        Ok((
            job,
            reconcile_trades(entries, &mut observed).map(|events| events.len()),
        ))
    })
    .unwrap();
    let mut iter = results.into_iter();
    assert_eq!(iter.next().unwrap().unwrap().1.unwrap(), 1);
    assert!(
        iter.next()
            .unwrap()
            .unwrap()
            .1
            .unwrap_err()
            .to_string()
            .contains("ambiguous trade overlap")
    );
}
