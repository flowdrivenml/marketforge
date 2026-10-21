//! Subscriber admission remains separate from event routing and socket writes.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{
    cursor::StreamCursor,
    delivery::{DeliveryCapacity, DeliveryPolicy},
    demand::SubscriberId,
    derived::StreamRef,
    error::ControlError,
    operation::ResourceVersion,
    replay::{BookViewBoundary, ReplayRange},
};
#[derive(Debug, Clone)]
pub enum SubscriptionStart {
    Follow,
    ReplayThenFollow(ReplayRange),
    BookViewThenFollow { max_levels: usize },
}
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    pub subscriber: SubscriberId,
    pub stream: StreamRef,
    pub start: SubscriptionStart,
    pub policy: DeliveryPolicy,
    pub capacity: DeliveryCapacity,
}
#[derive(Debug, Clone)]
pub struct SubscriptionAdmission {
    pub subscriber: SubscriberId,
    pub generation: u64,
    pub cut: StreamCursor,
    pub book_view: Option<BookViewBoundary>,
}
pub trait PublicationPort: Send + Sync {
    fn subscribe<'a>(
        &'a self,
        context: &'a CallContext,
        request: SubscribeRequest,
    ) -> RoutingFuture<'a, Result<SubscriptionAdmission, ControlError>>;
    fn unsubscribe<'a>(
        &'a self,
        context: &'a CallContext,
        subscriber: SubscriberId,
        generation: u64,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
    fn fence<'a>(
        &'a self,
        context: &'a CallContext,
        stream: ResourceVersion,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
}
