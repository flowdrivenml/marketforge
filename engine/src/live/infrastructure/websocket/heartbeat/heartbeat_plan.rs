use std::time::Duration;

use crate::live::config::exchange::{ExchangeConfig, HeartbeatConfig, HeartbeatStrategy};

use super::{HeartbeatPolicy, HeartbeatProtocol, WebSocketTransportError};

// ============================================================
// GENERIC EXECUTION MODES
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatExecutionMode {
    ServerControl,
    ClientControl,
    PeriodicApplication,
    InactivityApplication,
}

// ============================================================
// SERVER-INITIATED CONTROL FRAMES
// ============================================================

#[derive(Debug, Clone)]
pub struct ServerControlHeartbeat {
    /// The server initiates WebSocket Ping frames.
    ///
    /// Tungstenite automatically constructs a Pong with
    /// the identical payload when a Ping is received.
    pub respond_to_ping: bool,

    pub echo_ping_payload: bool,

    /// Advertised server Ping interval, if configured.
    ///
    /// This does not automatically establish a strict
    /// missing-Ping deadline.
    pub expected_server_ping_interval: Option<Duration>,

    /// Server-side deadline for receiving our Pong.
    ///
    /// This is NOT a client-side Pong timeout.
    pub peer_pong_deadline: Option<Duration>,
}

// ============================================================
// APPLICATION HEARTBEAT SCHEDULING
// ============================================================

#[derive(Debug, Clone, Copy)]
pub enum ApplicationHeartbeatSchedule {
    Periodic { interval: Duration },

    ReceiveInactivity { threshold: Duration },
}

#[derive(Debug, Clone)]
pub struct CompiledApplicationHeartbeat {
    pub protocol: HeartbeatProtocol,

    pub schedule: ApplicationHeartbeatSchedule,

    pub response_timeout: Duration,

    /// Optional documented server-side inactivity limit.
    ///
    /// Used for configuration consistency validation.
    /// It is not a substitute for the response deadline.
    pub server_inactivity_limit: Option<Duration>,
}

// ============================================================
// COMPLETE EXECUTION PLAN
// ============================================================

#[derive(Debug, Clone)]
pub enum CompiledHeartbeatPlan {
    ServerControl(ServerControlHeartbeat),

    ClientControl(HeartbeatPolicy),

    Application(CompiledApplicationHeartbeat),
}

impl CompiledHeartbeatPlan {
    pub fn mode(&self) -> HeartbeatExecutionMode {
        match self {
            Self::ServerControl(_) => HeartbeatExecutionMode::ServerControl,

            Self::ClientControl(_) => HeartbeatExecutionMode::ClientControl,

            Self::Application(application) => match application.schedule {
                ApplicationHeartbeatSchedule::Periodic { .. } => {
                    HeartbeatExecutionMode::PeriodicApplication
                }

                ApplicationHeartbeatSchedule::ReceiveInactivity { .. } => {
                    HeartbeatExecutionMode::InactivityApplication
                }
            },
        }
    }
}

// ============================================================
// COMPILER
// ============================================================

pub struct HeartbeatPlanCompiler;

impl HeartbeatPlanCompiler {
    pub fn compile(
        exchange: &ExchangeConfig,
    ) -> Result<CompiledHeartbeatPlan, WebSocketTransportError> {
        let config =
            exchange.websocket.heartbeat.as_ref().ok_or_else(|| {
                invalid("WebSocket profile has no primary heartbeat configuration")
            })?;

        if config.enabled == Some(false) {
            return Err(invalid("primary heartbeat is disabled"));
        }

        // A separate application heartbeat may be configured.
        //
        // Disabled optional strategies are intentionally
        // ignored. Enabled strategies must be implemented
        // explicitly rather than silently discarded.
        if let Some(application) = exchange.websocket.application_heartbeat.as_ref() {
            if application.enabled {
                return Err(invalid(
                    "enabled separate application heartbeat \
                     requires a combined heartbeat plan",
                ));
            }
        }

        match config.strategy {
            HeartbeatStrategy::WebsocketControlFrames => Self::compile_control(config),

            HeartbeatStrategy::TextPingPong | HeartbeatStrategy::JsonPingPong => {
                Self::compile_application(config)
            }

            HeartbeatStrategy::JsonChannelPing => Err(invalid(
                "primary JSON channel heartbeat is not \
                     supported by the current protocol compiler",
            )),
        }
    }

    // ========================================================
    // CONTROL-FRAME COMPILATION
    // ========================================================

    fn compile_control(
        config: &HeartbeatConfig,
    ) -> Result<CompiledHeartbeatPlan, WebSocketTransportError> {
        let server_initiated =
            config.server_ping_interval_seconds.is_some() || config.respond_to_ping == Some(true);

        if server_initiated {
            if config.respond_to_ping != Some(true) {
                return Err(invalid(
                    "server-initiated control heartbeat requires \
                     respond_to_ping = true",
                ));
            }

            if config.echo_ping_payload != Some(true) {
                return Err(invalid(
                    "server-initiated control heartbeat requires \
                     echo_ping_payload = true",
                ));
            }

            // Client-initiated Ping scheduling must not
            // accidentally be enabled in server-only mode.
            if config.interval_seconds.is_some() {
                return Err(invalid(
                    "server-initiated control heartbeat also \
                     declares a client Ping interval",
                ));
            }

            let expected_server_ping_interval = optional_seconds(
                config.server_ping_interval_seconds,
                "server_ping_interval_seconds",
            )?;

            let peer_pong_deadline =
                optional_seconds(config.pong_deadline_seconds, "pong_deadline_seconds")?;

            return Ok(CompiledHeartbeatPlan::ServerControl(
                ServerControlHeartbeat {
                    respond_to_ping: true,
                    echo_ping_payload: true,
                    expected_server_ping_interval,
                    peer_pong_deadline,
                },
            ));
        }

        // The existing compiler already implements
        // correlated client-initiated control Ping/Pong.
        let policy = HeartbeatPolicy::from_config(config)?;

        Ok(CompiledHeartbeatPlan::ClientControl(policy))
    }

    // ========================================================
    // APPLICATION HEARTBEAT COMPILATION
    // ========================================================

    fn compile_application(
        config: &HeartbeatConfig,
    ) -> Result<CompiledHeartbeatPlan, WebSocketTransportError> {
        let protocol = HeartbeatProtocol::compile(config)?;

        let response_seconds = config
            .response_timeout_seconds
            .or(config.timeout_seconds)
            .ok_or_else(|| invalid("application heartbeat requires a response timeout"))?;

        let response_timeout = required_seconds(response_seconds, "response_timeout_seconds")?;

        let server_inactivity_limit = optional_seconds(
            config.server_inactivity_limit_seconds,
            "server_inactivity_limit_seconds",
        )?;

        let schedule = if config.reset_timer_on_message == Some(true) {
            // The exchange requires heartbeats after
            // periods without incoming traffic.
            //
            // An unrelated incoming message resets the
            // scheduling timer, but must NOT satisfy an
            // already outstanding heartbeat response.
            let seconds = config.inactivity_seconds.ok_or_else(|| {
                invalid(
                    "inactivity-triggered heartbeat requires \
                     inactivity_seconds",
                )
            })?;

            if config.interval_seconds.is_some() {
                return Err(invalid(
                    "inactivity heartbeat must not also \
                     declare a periodic interval",
                ));
            }

            ApplicationHeartbeatSchedule::ReceiveInactivity {
                threshold: required_seconds(seconds, "inactivity_seconds")?,
            }
        } else {
            let seconds = config.interval_seconds.ok_or_else(|| {
                invalid(
                    "periodic application heartbeat requires \
                     interval_seconds",
                )
            })?;

            if config.inactivity_seconds.is_some() {
                return Err(invalid(
                    "periodic heartbeat declares an unused \
                     inactivity_seconds threshold",
                ));
            }

            ApplicationHeartbeatSchedule::Periodic {
                interval: required_seconds(seconds, "interval_seconds")?,
            }
        };

        // Check the configured schedule against a known
        // server inactivity limit without inventing one.
        if let Some(limit) = server_inactivity_limit {
            let scheduled = match schedule {
                ApplicationHeartbeatSchedule::Periodic { interval } => interval,

                ApplicationHeartbeatSchedule::ReceiveInactivity { threshold } => threshold,
            };

            if scheduled >= limit {
                return Err(invalid(
                    "heartbeat scheduling threshold must be \
                     below server inactivity limit",
                ));
            }
        }

        Ok(CompiledHeartbeatPlan::Application(
            CompiledApplicationHeartbeat {
                protocol,
                schedule,
                response_timeout,
                server_inactivity_limit,
            },
        ))
    }
}

// ============================================================
// DURATION VALIDATION
// ============================================================

fn required_seconds(seconds: u64, field: &str) -> Result<Duration, WebSocketTransportError> {
    if seconds == 0 {
        return Err(invalid(format!("{field} must be positive")));
    }

    Ok(Duration::from_secs(seconds))
}

fn optional_seconds(
    seconds: Option<u64>,
    field: &str,
) -> Result<Option<Duration>, WebSocketTransportError> {
    seconds
        .map(|value| required_seconds(value, field))
        .transpose()
}

fn invalid(reason: impl Into<String>) -> WebSocketTransportError {
    WebSocketTransportError::Configuration(reason.into())
}
