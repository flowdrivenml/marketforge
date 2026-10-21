//! Reusable bounded, ordered fault script for deterministic port adapters.
use marketforge_engine::live::model::error::ErrorCode;
use std::{collections::VecDeque, sync::Mutex};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    Spawn,
    Read,
}
pub struct FaultPlan(Mutex<VecDeque<(FaultPoint, ErrorCode)>>);
impl FaultPlan {
    pub fn new(steps: Vec<(FaultPoint, ErrorCode)>, limit: usize) -> Result<Self, ErrorCode> {
        if steps.len() > limit {
            return Err(ErrorCode::Limit);
        }
        Ok(Self(Mutex::new(steps.into())))
    }
    pub fn check(&self, point: FaultPoint) -> Result<(), ErrorCode> {
        let mut steps = self.0.lock().unwrap();
        if steps
            .front()
            .is_some_and(|(expected, _)| *expected == point)
        {
            Err(steps.pop_front().unwrap().1)
        } else {
            Ok(())
        }
    }
    pub fn remaining(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}
