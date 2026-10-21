//! Executor-independent bounded task admission. Concrete executor selection is
//! separate; the wrapper works with production and deterministic task ports.
use super::resources::{ResourceBudget, ResourceReservation, ResourceUsage};
use crate::live::{
    model::{error::ControlError, operation::ResourceVersion},
    ports::{
        context::CallContext,
        lifecycle::{TaskHandlePort, TaskSpawnerPort},
        routing::RoutingFuture,
    },
};
use std::sync::Arc;

pub struct BoundedTaskSpawner {
    inner: Arc<dyn TaskSpawnerPort>,
    slots: ResourceBudget,
}
impl BoundedTaskSpawner {
    pub fn new(
        inner: Arc<dyn TaskSpawnerPort>,
        max_tasks: usize,
    ) -> Result<Self, crate::live::model::error::ErrorCode> {
        Ok(Self {
            inner,
            slots: ResourceBudget::new(ResourceUsage {
                items: max_tasks,
                bytes: max_tasks,
            })?,
        })
    }
    pub fn close(&self) {
        self.slots.close();
    }
    /// Includes completed but unjoined handles; task capacity is not silently
    /// recycled while an executor can still retain the task or its result.
    pub fn admitted_tasks(&self) -> usize {
        self.slots.usage().items
    }
}
struct BoundedHandle {
    inner: Arc<dyn TaskHandlePort>,
    _reservation: Arc<ResourceReservation>,
}
impl TaskHandlePort for BoundedHandle {
    fn cancel(&self) {
        self.inner.cancel();
    }
    fn join<'a>(&'a self, context: &'a CallContext) -> RoutingFuture<'a, Result<(), ControlError>> {
        self.inner.join(context)
    }
}
impl TaskSpawnerPort for BoundedTaskSpawner {
    fn spawn(
        &self,
        owner: ResourceVersion,
        task: RoutingFuture<'static, Result<(), ControlError>>,
    ) -> Result<Arc<dyn TaskHandlePort>, ControlError> {
        let reservation = Arc::new(
            self.slots
                .reserve(ResourceUsage { items: 1, bytes: 1 })
                .map_err(|code| ControlError::new(code, Some(owner.resource.clone())))?,
        );
        let running = reservation.clone();
        let task = Box::pin(async move {
            // Keep capacity even if the caller drops its join handle before
            // completion. Captured futures release it on executor rejection.
            let _reservation = running;
            task.await
        });
        let inner = self.inner.spawn(owner, task)?;
        Ok(Arc::new(BoundedHandle {
            inner,
            _reservation: reservation,
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::model::{error::ErrorCode, operation::ResourceId};
    use std::sync::Mutex;
    struct Controlled {
        tasks: Mutex<Vec<RoutingFuture<'static, Result<(), ControlError>>>>,
    }
    struct Handle;
    impl TaskHandlePort for Handle {
        fn cancel(&self) {}
        fn join<'a>(&'a self, _: &'a CallContext) -> RoutingFuture<'a, Result<(), ControlError>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl TaskSpawnerPort for Controlled {
        fn spawn(
            &self,
            _: ResourceVersion,
            task: RoutingFuture<'static, Result<(), ControlError>>,
        ) -> Result<Arc<dyn TaskHandlePort>, ControlError> {
            self.tasks.lock().unwrap().push(task);
            Ok(Arc::new(Handle))
        }
    }
    #[test]
    fn dropped_handle_does_not_release_running_task_capacity() {
        let inner = Arc::new(Controlled {
            tasks: Mutex::new(vec![]),
        });
        let spawner = BoundedTaskSpawner::new(inner.clone(), 1).unwrap();
        let owner = ResourceVersion {
            resource: ResourceId::Runtime,
            generation: 1,
        };
        let handle = spawner
            .spawn(owner.clone(), Box::pin(async { Ok(()) }))
            .unwrap();
        drop(handle);
        assert_eq!(spawner.admitted_tasks(), 1);
        assert!(matches!(
            spawner.spawn(owner.clone(), Box::pin(async { Ok(()) })),
            Err(ControlError {
                code: ErrorCode::Capacity,
                ..
            })
        ));
        inner.tasks.lock().unwrap().clear();
        assert_eq!(spawner.admitted_tasks(), 0);
        spawner.close();
        assert!(matches!(
            spawner.spawn(owner, Box::pin(async { Ok(()) })),
            Err(ControlError {
                code: ErrorCode::Unavailable,
                ..
            })
        ));
    }
    #[test]
    fn rejected_spawn_releases_capacity() {
        struct Reject;
        impl TaskSpawnerPort for Reject {
            fn spawn(
                &self,
                _: ResourceVersion,
                _: RoutingFuture<'static, Result<(), ControlError>>,
            ) -> Result<Arc<dyn TaskHandlePort>, ControlError> {
                Err(ControlError::new(ErrorCode::Unavailable, None))
            }
        }
        let spawner = BoundedTaskSpawner::new(Arc::new(Reject), 1).unwrap();
        assert!(
            spawner
                .spawn(
                    ResourceVersion {
                        resource: ResourceId::Runtime,
                        generation: 1
                    },
                    Box::pin(async { Ok(()) })
                )
                .is_err()
        );
        assert_eq!(spawner.admitted_tasks(), 0);
    }
}
