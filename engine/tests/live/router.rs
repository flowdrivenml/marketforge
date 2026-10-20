use std::sync::Arc;

use marketforge_engine::live::{
    model::delivery::{DeliveryCapacity, DeliveryPolicy, DestinationKind},
    ports::routing::{
        DestinationAdmission, DestinationDescriptor, DestinationId, EventBatch,
        EventDestinationPort, RoutingFuture,
    },
    routing::router::{RouterRegistry, RouterRegistryError},
};

use crate::live::support::live_events::l2_event;

struct TestDestination {
    descriptor: DestinationDescriptor,
}

impl EventDestinationPort for TestDestination {
    fn descriptor(&self) -> DestinationDescriptor {
        self.descriptor
    }

    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
        Box::pin(async move {
            DestinationAdmission::Accepted {
                destination: self.descriptor.id,
                through: batch.last_cursor().clone(),
            }
        })
    }
}

fn destination(
    id: u64,
    kind: DestinationKind,
    policy: DeliveryPolicy,
) -> Arc<dyn EventDestinationPort> {
    Arc::new(TestDestination {
        descriptor: DestinationDescriptor {
            id: DestinationId(id),
            kind,
            policy,
            capacity: DeliveryCapacity::new(100, 100_000).unwrap(),
        },
    })
}

fn batch() -> EventBatch {
    EventBatch::new(vec![
        l2_event("router-source", 1, 1),
        l2_event("router-source", 1, 2),
    ])
    .unwrap()
}

#[test]
fn registry_accepts_multiple_destinations() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    assert_eq!(registry.destination_count(), 2);
}

#[test]
fn registry_rejects_duplicate_ids() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    assert_eq!(
        registry.register(destination(
            1,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        )),
        Err(RouterRegistryError::DuplicateDestination)
    );
}

#[test]
fn registry_rejects_invalid_policy() {
    let registry = RouterRegistry::new();

    assert_eq!(
        registry.register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Durable,
        )),
        Err(RouterRegistryError::InvalidDestinationPolicy)
    );
}

#[test]
fn plan_separates_required_and_best_effort() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::BestEffort,
        ))
        .unwrap();

    registry
        .register(destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(destination(
            3,
            DestinationKind::Retention,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    let plan = registry.plan(&batch()).unwrap();

    assert_eq!(plan.required_count(), 2);
    assert_eq!(plan.best_effort_count(), 1);
    assert_eq!(plan.destination_count(), 3);
}

#[test]
fn plan_orders_destinations_deterministically() {
    let registry = RouterRegistry::new();

    for id in [9, 2, 7, 1] {
        registry
            .register(destination(
                id,
                DestinationKind::Publication,
                DeliveryPolicy::Required,
            ))
            .unwrap();
    }

    let plan = registry.plan(&batch()).unwrap();

    let ids: Vec<_> = plan
        .required
        .iter()
        .map(|entry| entry.descriptor.id.0)
        .collect();

    assert_eq!(ids, vec![1, 2, 7, 9]);
}

#[test]
fn plan_preserves_original_cursor_range() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    let original = batch();
    let plan = registry.plan(&original).unwrap();

    assert_eq!(plan.first_cursor, *original.first_cursor());
    assert_eq!(plan.last_cursor, *original.last_cursor());
}

#[test]
fn unregister_changes_future_plans_not_existing_snapshots() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    let first = registry.plan(&batch()).unwrap();

    registry.unregister(DestinationId(1)).unwrap();

    assert_eq!(first.destination_count(), 1);
    assert_eq!(registry.destination_count(), 0);
}

#[test]
fn shutdown_rejects_future_plans() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry.shutdown();

    assert!(matches!(
        registry.plan(&batch()),
        Err(RouterRegistryError::ShuttingDown)
    ));
}
