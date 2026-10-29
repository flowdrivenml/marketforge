//! Standalone assembly using the existing PostgreSQL definitions, transport,
//! integrity, recovery coordinator and supervisor. No top-level runtime is needed.
use super::*;
use crate::live::{
    config::{ExchangeConfigStore, FormatConfigStore},
    infrastructure::{
        normalization::{NormalizerFactory, NormalizerLimits},
        websocket::{StreamRequest, WebSocketManager},
    },
    integrity::{
        CreateDepthIntegrity, CreateTradeIntegrity, DepthBufferLimits, DepthContinuityCompiler,
        DepthResourceLimits, DepthSnapshotSource, DepthSourceBinding, DepthSourceEpoch,
        IntegrityFacadeFactory, RestDepthEndpointConfig, TradeIdentityLimits,
        TradeIntegrityCompiler, TradeSourceBinding, TradeSourceEpoch,
    },
    recovery::{RecoveryHandle, StreamRecoveryBinding, StreamRecoveryHandle},
    stream::{
        persistence::{PersistedStreamDefinition, StreamDefinition, StreamDefinitionRepository},
        request::SourceChannel,
    },
};
use crate::shared::domain::canonical::Exchange;
use sqlx::PgPool;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("source initialization failed: {0}")]
pub struct SourceInitializationError(pub String);

/// Pure assembly limits; all queues and integrity stores have explicit bounds.
#[derive(Clone, Copy)]
pub struct SourceInitializationLimits {
    pub input_capacity: usize,
    pub normalization: NormalizerLimits,
    pub trade_identities: TradeIdentityLimits,
    pub trade_buffer: SourceTradeBufferLimits,
    pub depth_buffer: DepthBufferLimits,
    pub depth_resources: DepthResourceLimits,
}
impl Default for SourceInitializationLimits {
    fn default() -> Self {
        Self {
            input_capacity: 256,
            normalization: NormalizerLimits::default(),
            trade_buffer: SourceTradeBufferLimits {
                max_batches: 1024,
                max_records: 8192,
                max_bytes: 16 * 1024 * 1024,
            },
            trade_identities: TradeIdentityLimits {
                max_ids: 16384,
                max_id_bytes: 128,
                max_batch_records: 3000,
            },
            depth_buffer: DepthBufferLimits {
                max_messages: 1024,
                max_bytes: 16 * 1024 * 1024,
            },
            depth_resources: DepthResourceLimits::default(),
        }
    }
}

/// Prepares sources from already accepted definitions. This factory performs no
/// persistence writes or network effects; attach consumes the prepared owner.
pub struct SourceRuntimeBuilder {
    exchanges: Arc<ExchangeConfigStore>,
    formats: Arc<FormatConfigStore>,
    factory: SourceStreamFactory,
    limits: SourceInitializationLimits,
}

pub struct PreparedSourceStream {
    runner: SourceStreamRunner,
    binder: SourceAcquisitionBinder,
    request: StreamRequest,
    definition: PersistedStreamDefinition,
    depth_policy: Option<SourceDepthRecoveryPolicy>,
    trade_policy: Option<SourceTradeRecoveryPolicy>,
    input_capacity: usize,
    formats: Arc<FormatConfigStore>,
}

impl SourceRuntimeBuilder {
    pub fn new(
        exchanges: Arc<ExchangeConfigStore>,
        formats: Arc<FormatConfigStore>,
        limits: SourceInitializationLimits,
    ) -> Result<Self, SourceInitializationError> {
        if limits.input_capacity == 0 {
            return Err(SourceInitializationError(
                "input capacity must be positive".into(),
            ));
        }
        let factory =
            NormalizerFactory::new(exchanges.clone(), formats.clone(), limits.normalization)
                .map_err(|e| SourceInitializationError(e.to_string()))?;
        Ok(Self {
            exchanges,
            formats,
            factory: SourceStreamFactory::new(Arc::new(factory)),
            limits,
        })
    }

    /// Load the durable definition before preparing any acquisition resources.
    pub async fn load(
        &self,
        pool: &PgPool,
        stream_id: &str,
    ) -> Result<PreparedSourceStream, SourceInitializationError> {
        let definition = StreamDefinitionRepository::new(pool.clone())
            .find(stream_id)
            .await
            .map_err(|e| SourceInitializationError(e.to_string()))?
            .ok_or_else(|| SourceInitializationError("source definition not found".into()))?;
        self.prepare(definition)
    }

    /// Assemble an accepted definition (also the controlled fixture boundary for
    /// deterministic tests). Every call constructs fresh unsynchronized integrity.
    pub fn prepare(
        &self,
        definition: PersistedStreamDefinition,
    ) -> Result<PreparedSourceStream, SourceInitializationError> {
        let fail = |e: String| SourceInitializationError(e);
        if !definition.desired.enabled
            || definition.revision <= 0
            || definition.recording_stream_key.is_none_or(|key| key <= 0)
        {
            return Err(fail(
                "source must have an enabled, valid persistent registration".into(),
            ));
        }
        let StreamDefinition::Source(source) = &definition.definition else {
            return Err(fail("expected source definition".into()));
        };
        let instance = self
            .factory
            .create(&definition.stream_id, source.clone())
            .map_err(|e| fail(e.to_string()))?;
        let profile = format!(
            "{}_{}",
            source.exchange.as_str(),
            source.market_category.as_str()
        );
        let exchange = self
            .exchanges
            .get(&profile)
            .ok_or_else(|| fail("missing exchange profile".into()))?;
        let channel = exchange
            .channel(source.channel.as_str())
            .ok_or_else(|| fail("missing channel".into()))?;
        let native_exchange = match source.exchange.as_str() {
            "bybit" => Exchange::Bybit,
            "binance" => Exchange::Binance,
            "okx" => Exchange::Okx,
            "bitget" => Exchange::Bitget,
            "gateio" => Exchange::GateIo,
            _ => unreachable!(),
        };
        let mut request = StreamRequest::new(&profile, &source.symbol, source.channel.as_str())
            .map_err(|e| fail(e.to_string()))?
            .with_stream_id(&definition.stream_id)
            .map_err(|e| fail(e.to_string()))?;
        let mut depth_policy = None;
        let mut trade_policy = None;
        let (integrity, binder) = match source.channel {
            SourceChannel::Trades => {
                let plan = TradeIntegrityCompiler::compile(
                    self.formats
                        .trade(&profile)
                        .ok_or_else(|| fail("missing trade format".into()))?,
                )
                .map_err(|e| fail(e.to_string()))?;
                let binding = TradeSourceBinding::new(
                    &definition.stream_id,
                    native_exchange,
                    source.market_category.as_str(),
                    source.instrument_id,
                    &source.symbol,
                    TradeSourceEpoch {
                        stream_generation: 1,
                        connection_generation: 1,
                        subscription_generation: 1,
                    },
                )
                .map_err(|e| fail(format!("{e:?}")))?;
                let binder = SourceAcquisitionBinder::trades(binding.clone());
                trade_policy = SourceTradeRecoveryPolicy::for_source(source).ok();
                let integrity = IntegrityFacadeFactory::trades(CreateTradeIntegrity {
                    binding,
                    plan,
                    identity_limits: self.limits.trade_identities,
                })
                .map_err(|e| fail(format!("{e:?}")))?;
                (integrity, binder)
            }
            SourceChannel::Depth => {
                let format = self
                    .formats
                    .depth(&profile)
                    .ok_or_else(|| fail("missing depth format".into()))?;
                let plan =
                    DepthContinuityCompiler::compile(format).map_err(|e| fail(e.to_string()))?;
                let depth = channel.default_depth.or_else(|| match channel.depth {
                    Some(crate::live::config::exchange::DepthValue::Levels(n)) => Some(n),
                    _ => None,
                });
                if let Some(depth) = depth {
                    request = request
                        .with_parameter("depth", depth.to_string())
                        .map_err(|e| fail(e.to_string()))?;
                }
                if let Some(interval) = &channel.default_interval {
                    request = request
                        .with_parameter("interval", interval)
                        .map_err(|e| fail(e.to_string()))?;
                }
                if let Some(interval) = channel.default_interval_ms {
                    request = request
                        .with_parameter("interval_ms", interval.to_string())
                        .map_err(|e| fail(e.to_string()))?;
                }
                let binding = DepthSourceBinding::new(
                    &definition.stream_id,
                    native_exchange,
                    source.market_category.as_str(),
                    source.instrument_id,
                    &source.symbol,
                    depth,
                    DepthSourceEpoch {
                        stream_generation: 1,
                        connection_generation: 1,
                        subscription_generation: 1,
                    },
                )
                .map_err(|e| fail(format!("{e:?}")))?;
                let binder = SourceAcquisitionBinder::depth(binding.clone());
                let rest_endpoint = if plan.snapshot_source == DepthSnapshotSource::Rest {
                    let endpoint = crate::live::integrity::CompiledRestDepthEndpoint::compile(
                        &binding, exchange, format,
                    )
                    .map_err(|e| fail(format!("{e:?}")))?;
                    depth_policy = Some(
                        SourceDepthRecoveryPolicy::new(
                            endpoint.endpoint_key(),
                            depth.unwrap_or(100).min(endpoint.maximum_limit()),
                        )
                        .map_err(|e| fail(e.to_string()))?,
                    );
                    Some(RestDepthEndpointConfig { exchange, format })
                } else {
                    None
                };
                let integrity = IntegrityFacadeFactory::depth(CreateDepthIntegrity {
                    binding,
                    plan,
                    buffer_limits: self.limits.depth_buffer,
                    resource_limits: self.limits.depth_resources,
                    rest_endpoint,
                })
                .map_err(|e| fail(format!("{e:?}")))?;
                (integrity, binder)
            }
        };
        let processor = SourceProcessor::new(integrity).map_err(|e| fail(e.to_string()))?;
        let runner = SourceStreamRunner::new(instance, processor)
            .map_err(|e| fail(e.to_string()))?
            .with_trade_buffer_limits(self.limits.trade_buffer)
            .map_err(|e| fail(e.to_string()))?;
        Ok(PreparedSourceStream {
            runner,
            binder,
            request,
            definition,
            depth_policy,
            trade_policy,
            input_capacity: self.limits.input_capacity,
            formats: self.formats.clone(),
        })
    }
}

impl PreparedSourceStream {
    /// Supply a catalog-resolved native subscription value when canonical market
    /// classification alone is ambiguous (for example Bitget linear products).
    pub fn with_subscription_parameter(
        mut self,
        name: &str,
        value: &str,
    ) -> Result<Self, SourceInitializationError> {
        self.request = self
            .request
            .with_parameter(name, value)
            .map_err(|e| SourceInitializationError(e.to_string()))?;
        Ok(self)
    }
    pub fn runner(&self) -> &SourceStreamRunner {
        &self.runner
    }
    pub fn stream_request(&self) -> &StreamRequest {
        &self.request
    }
    pub fn definition(&self) -> &PersistedStreamDefinition {
        &self.definition
    }
    /// Attach only after preparation has validated all source-bound dependencies.
    /// A single shared WebSocket manager and recovery coordinator serve all sources.
    pub async fn attach(
        self,
        manager: Arc<WebSocketManager>,
        recovery: RecoveryHandle,
    ) -> Result<SourceStreamExecutor, SourceInitializationError> {
        let fail = |e: String| SourceInitializationError(e);
        let epoch = self.runner.processor().integrity().stream_generation();
        let initial = crate::live::recovery::RecoverySourceEpoch {
            stream_generation: epoch,
            connection_generation: 1,
            subscription_generation: 1,
        };
        let StreamDefinition::Source(source) = &self.definition.definition else {
            unreachable!()
        };
        let handle = StreamRecoveryHandle::new(
            recovery,
            StreamRecoveryBinding::new(
                &self.definition.stream_id,
                source.clone(),
                self.definition.revision as u64,
                initial,
            )
            .map_err(|e| fail(e.to_string()))?,
        );
        // Validate everything fallible before attaching; an error must not leak an attachment.
        let attachment = SourceAttachment::attach_with_formats(
            manager,
            self.request,
            self.input_capacity,
            &self.formats,
        )
        .await
        .map_err(|e| fail(e.to_string()))?;
        let mut executor = SourceStreamExecutor::new(self.runner, attachment, self.binder);
        if let Some(policy) = self.depth_policy {
            executor = executor
                .with_depth_recovery(
                    SourceDepthAttempt::new(&self.definition.stream_id, initial),
                    handle,
                )
                .map_err(|e| fail(e.to_string()))?
                .with_depth_recovery_policy(policy);
        } else if let Some(policy) = self.trade_policy {
            executor = executor
                .with_trade_recovery(
                    SourceTradeAttempt::new(&self.definition.stream_id, initial),
                    handle,
                )
                .map_err(|e| fail(e.to_string()))?
                .with_trade_recovery_policy(policy)
                .map_err(|e| fail(e.to_string()))?;
        }
        Ok(executor)
    }
}
