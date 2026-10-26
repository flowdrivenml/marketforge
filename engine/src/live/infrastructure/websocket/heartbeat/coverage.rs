use crate::live::config::exchange::ExchangeConfig;

use super::plan::{HeartbeatExecutionMode, HeartbeatPlanCompiler};

// ============================================================
// COVERAGE CLASSIFICATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatCoverageKind {
    ControlFrames,
    Text,
    Json,
    ServerInitiated,
    ChannelProtocol,
    SeparateApplicationHeartbeat,
    Unspecified,
}

// ============================================================
// COVERAGE REPORT
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatCoverage {
    pub profile: String,
    pub kinds: Vec<HeartbeatCoverageKind>,
    pub executable: bool,
    pub diagnostics: Vec<String>,
}

// ============================================================
// AUDITOR
// ============================================================

pub struct HeartbeatCoverageAuditor;

impl HeartbeatCoverageAuditor {
    /// Verify that one exchange heartbeat configuration
    /// compiles into a supported runtime execution plan.
    ///
    /// The execution-plan compiler is the authoritative
    /// source of heartbeat capability validation.
    ///
    /// This auditor must never duplicate protocol rules
    /// or infer behavior from an exchange identifier.
    pub fn audit(config: &ExchangeConfig) -> HeartbeatCoverage {
        let mut report = HeartbeatCoverage {
            profile: config.profile().to_owned(),
            kinds: Vec::new(),
            executable: false,
            diagnostics: Vec::new(),
        };

        match HeartbeatPlanCompiler::compile(config) {
            Ok(plan) => {
                report.executable = true;

                let kind = match plan.mode() {
                    HeartbeatExecutionMode::ServerControl => HeartbeatCoverageKind::ServerInitiated,

                    HeartbeatExecutionMode::ClientControl => HeartbeatCoverageKind::ControlFrames,

                    HeartbeatExecutionMode::PeriodicApplication
                    | HeartbeatExecutionMode::InactivityApplication => {
                        match config.websocket.heartbeat.as_ref() {
                            Some(heartbeat) => {
                                use crate::live::config::exchange::HeartbeatStrategy;

                                match heartbeat.strategy {
                                    HeartbeatStrategy::TextPingPong => HeartbeatCoverageKind::Text,

                                    HeartbeatStrategy::JsonPingPong => HeartbeatCoverageKind::Json,

                                    HeartbeatStrategy::JsonChannelPing => {
                                        HeartbeatCoverageKind::ChannelProtocol
                                    }

                                    HeartbeatStrategy::WebsocketControlFrames => {
                                        HeartbeatCoverageKind::ControlFrames
                                    }
                                }
                            }

                            None => HeartbeatCoverageKind::Unspecified,
                        }
                    }
                };

                report.kinds.push(kind);

                report
                    .diagnostics
                    .push(format!("compiled execution mode: {:?}", plan.mode(),));
            }

            Err(error) => {
                report.kinds.push(HeartbeatCoverageKind::Unspecified);

                report.diagnostics.push(error.to_string());
            }
        }

        report
    }
}
