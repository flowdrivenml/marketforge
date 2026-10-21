//! Explicit deterministic REST adapter. Empty scripts and mismatched requests
//! are errors, never fake-success production fallback behavior.
use super::failure::{FaultPlan, FaultPoint};
use marketforge_engine::live::{
    foundation::clock::MonotonicClock,
    model::error::{ControlError, ErrorCode},
    ports::{
        context::CallContext,
        routing::RoutingFuture,
        transport::{RestRequest, RestResponse, RestTransportPort},
    },
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
pub struct RestStep {
    pub endpoint_name: String,
    pub profile_revision: u64,
    pub weight: u32,
    pub response: RestResponse,
}
pub struct ScriptedRestTransport {
    clock: Arc<dyn MonotonicClock>,
    steps: Mutex<VecDeque<RestStep>>,
    faults: Arc<FaultPlan>,
}
impl ScriptedRestTransport {
    pub fn new(
        clock: Arc<dyn MonotonicClock>,
        steps: Vec<RestStep>,
        faults: Arc<FaultPlan>,
        max_steps: usize,
        max_bytes: usize,
    ) -> Result<Self, ErrorCode> {
        if steps.len() > max_steps {
            return Err(ErrorCode::Limit);
        }
        let bytes = steps
            .iter()
            .try_fold(0usize, |n, s| {
                n.checked_add(s.response.body.len())
                    .and_then(|n| n.checked_add(s.endpoint_name.len()))
            })
            .ok_or(ErrorCode::Limit)?;
        if bytes > max_bytes {
            return Err(ErrorCode::Limit);
        }
        Ok(Self {
            clock,
            steps: Mutex::new(steps.into()),
            faults,
        })
    }
    pub fn remaining(&self) -> usize {
        self.steps.lock().unwrap().len()
    }
}
impl RestTransportPort for ScriptedRestTransport {
    fn execute<'a>(
        &'a self,
        context: &'a CallContext,
        request: RestRequest,
    ) -> RoutingFuture<'a, Result<RestResponse, ControlError>> {
        Box::pin(async move {
            context
                .check(self.clock.as_ref(), None)
                .map_err(|e| ControlError::new(e, None))?;
            self.faults
                .check(FaultPoint::Read)
                .map_err(|e| ControlError::new(e, None))?;
            let mut steps = self.steps.lock().unwrap();
            let expected = steps
                .front()
                .ok_or_else(|| ControlError::new(ErrorCode::NotFound, None))?;
            if expected.endpoint_name != request.endpoint.endpoint_name
                || expected.profile_revision != request.endpoint.profile_revision
                || expected.weight != request.weight
            {
                return Err(ControlError::new(ErrorCode::InvalidInput, None));
            }
            if expected.response.body.len() > request.max_response_bytes {
                return Err(ControlError::new(ErrorCode::Limit, None));
            }
            Ok(steps.pop_front().unwrap().response)
        })
    }
}
