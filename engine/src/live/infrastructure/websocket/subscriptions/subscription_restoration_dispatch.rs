use std::time::Duration;

use thiserror::Error;
use tokio::{sync::mpsc, time};

use super::{ConnectionFence, ConnectionStamp, ConnectionState, SubscriptionCommand};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RestorationDispatchError {
    #[error("restoration dispatch configuration is invalid")]
    InvalidConfiguration,

    #[error("connection generation is no longer current")]
    StaleGeneration,

    #[error("desired demand revision changed")]
    StaleDemand,

    #[error("subscription driver command channel is closed")]
    DriverClosed,

    #[error("subscription driver admission timed out")]
    AdmissionTimeout,

    #[error(
        "partial restoration admission: {admitted} of {total} commands; connection recycle required"
    )]
    PartialAdmission { admitted: usize, total: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestorationDispatchReceipt {
    /// Number of commands accepted by the driver channel.
    ///
    /// This is NOT the number sent to the exchange,
    /// acknowledged, or recovered.
    pub admitted: usize,

    pub demand_revision: u64,
}

pub struct SubscriptionRestorationDispatcher {
    admission_timeout: Duration,
}

impl SubscriptionRestorationDispatcher {
    pub fn new(admission_timeout: Duration) -> Result<Self, RestorationDispatchError> {
        if admission_timeout.is_zero() {
            return Err(RestorationDispatchError::InvalidConfiguration);
        }

        Ok(Self { admission_timeout })
    }

    /// Submit a precompiled restoration batch to one
    /// connection-scoped SubscriptionDriver.
    ///
    /// The caller owns:
    /// - The physical connection fence.
    /// - The demand revision authority.
    /// - The driver task lifecycle.
    /// - Connection recycling after partial admission.
    ///
    /// No socket transmission occurs here.
    ///
    /// A successful return means that all commands entered
    /// the driver channel, NOT that subscriptions succeeded.
    pub async fn dispatch(
        &self,
        commands: Vec<SubscriptionCommand>,
        sender: &mpsc::Sender<SubscriptionCommand>,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        expected_revision: u64,
        current_revision: impl Fn() -> u64,
    ) -> Result<RestorationDispatchReceipt, RestorationDispatchError> {
        let total = commands.len();
        let mut admitted = 0;

        for command in commands {
            // Check authority before admitting each command.
            //
            // Demand can change while previous commands are
            // being admitted.
            if !current_generation(fence, stamp) {
                return Err(dispatch_failure(
                    RestorationDispatchError::StaleGeneration,
                    admitted,
                    total,
                ));
            }

            if current_revision() != expected_revision {
                return Err(dispatch_failure(
                    RestorationDispatchError::StaleDemand,
                    admitted,
                    total,
                ));
            }

            // Ensure the command belongs to this exact
            // physical connection generation.
            if command.token.connection() != stamp {
                return Err(dispatch_failure(
                    RestorationDispatchError::StaleGeneration,
                    admitted,
                    total,
                ));
            }

            // Reserve capacity before transferring ownership
            // of the command.
            let permit = match time::timeout(self.admission_timeout, sender.reserve()).await {
                Ok(Ok(permit)) => permit,

                Ok(Err(_)) => {
                    return Err(dispatch_failure(
                        RestorationDispatchError::DriverClosed,
                        admitted,
                        total,
                    ));
                }

                Err(_) => {
                    return Err(dispatch_failure(
                        RestorationDispatchError::AdmissionTimeout,
                        admitted,
                        total,
                    ));
                }
            };

            // Recheck after waiting for queue capacity.
            //
            // A demand update or disconnect may have
            // happened during the await.
            if !current_generation(fence, stamp) {
                return Err(dispatch_failure(
                    RestorationDispatchError::StaleGeneration,
                    admitted,
                    total,
                ));
            }

            if current_revision() != expected_revision {
                return Err(dispatch_failure(
                    RestorationDispatchError::StaleDemand,
                    admitted,
                    total,
                ));
            }

            permit.send(command);
            admitted += 1;
        }

        Ok(RestorationDispatchReceipt {
            admitted,
            demand_revision: expected_revision,
        })
    }
}

fn current_generation(fence: &ConnectionFence, stamp: &ConnectionStamp) -> bool {
    fence.current_stamp().as_ref() == Some(stamp)
        && matches!(
            fence.state(),
            ConnectionState::Connected | ConnectionState::Ready
        )
}

fn dispatch_failure(
    original: RestorationDispatchError,
    admitted: usize,
    total: usize,
) -> RestorationDispatchError {
    if admitted > 0 {
        RestorationDispatchError::PartialAdmission { admitted, total }
    } else {
        original
    }
}
