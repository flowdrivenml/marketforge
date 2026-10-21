//! Constructor contracts for planned owners. Each bundle names actual ports;
//! no component receives a registry of arbitrary runtime services.
use crate::live::{
    model::{acceptance::EventAcceptor, configuration::*},
    ports::*,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct InstrumentedOwner {
    pub context: context::OwnerContext,
    pub metrics: Arc<dyn observability::ScopedMetricsPort>,
    pub diagnostics: Arc<dyn observability::ObservationSinkPort>,
}
impl InstrumentedOwner {
    pub fn validate(&self) -> Result<(), crate::live::model::error::ErrorCode> {
        self.context.limits.validate()?;
        if self.metrics.owner() != &self.context.identity {
            return Err(crate::live::model::error::ErrorCode::StaleGeneration);
        }
        Ok(())
    }
}
pub struct ConnectionManagerDependencies {
    pub owner: InstrumentedOwner,
    pub recovery: RecoveryPolicy,
    pub descriptor: transport::ConnectionDescriptor,
    pub frames: Arc<dyn protocol::FrameRouterPort>,
    pub connector: Arc<dyn transport::WebSocketConnectorPort>,
    pub protocol: Arc<dyn protocol::ExchangeProtocolPort>,
}
pub struct RestSchedulerDependencies {
    pub owner: InstrumentedOwner,
    pub rate: transport::RestRatePolicy,
    pub transport: Arc<dyn transport::RestTransportPort>,
    pub policy: RecoveryPolicy,
}
pub struct SubscriptionManagerDependencies {
    pub owner: InstrumentedOwner,
    pub recovery: RecoveryPolicy,
    pub connections: Arc<dyn registry::ConnectionWriterPort>,
    pub protocol: Arc<dyn protocol::ExchangeProtocolPort>,
}
pub struct SourceRegistryDependencies {
    pub owner: InstrumentedOwner,
    pub idle_grace_ns: u64,
    pub processors: Arc<dyn processing::ProcessorFactoryPort>,
    pub subscriptions: Arc<dyn registry::SubscriptionPort>,
    pub protocol: Arc<dyn protocol::ExchangeProtocolPort>,
}
pub struct SourceProcessorDependencies {
    pub owner: InstrumentedOwner,
    pub plan: protocol::AcquisitionPlan,
    pub acceptor: EventAcceptor,
    pub normalizer: Arc<dyn protocol::NormalizationPort>,
    pub output: Arc<dyn routing::EventRouterPort>,
    pub recovery: Arc<dyn recovery::RecoveryPort>,
}
pub struct RecoveryCoordinatorDependencies {
    pub owner: InstrumentedOwner,
    pub registry: Arc<dyn registry::SourceRegistryPort>,
    pub rest: Arc<dyn transport::RestTransportPort>,
    pub processor: Arc<dyn processing::SourceProcessorPort>,
    pub connections: Arc<dyn registry::ConnectionControlPort>,
    pub policy: RecoveryPolicy,
}
pub struct DerivedRegistryDependencies {
    pub owner: InstrumentedOwner,
    pub sources: Arc<dyn registry::SourceRegistryPort>,
    pub factory: Arc<dyn processing::DerivedFactoryPort>,
}
pub struct MergerDependencies {
    pub owner: InstrumentedOwner,
    pub definition: crate::live::model::derived::DerivedDefinition,
    pub inputs: Vec<Arc<dyn processing::EventInputPort>>,
    pub acceptor: EventAcceptor,
    pub output: Arc<dyn routing::EventRouterPort>,
}
pub struct ReferenceRegistryDependencies {
    pub owner: InstrumentedOwner,
    pub registry: Arc<dyn registry::SourceRegistryPort>,
    pub inputs: Vec<Arc<dyn processing::EventInputPort>>,
}
pub struct ConverterDependencies {
    pub owner: InstrumentedOwner,
    pub definition: crate::live::model::derived::DerivedDefinition,
    pub input: Arc<dyn processing::EventInputPort>,
    pub references: Arc<dyn processing::ReferenceReaderPort>,
    pub acceptor: EventAcceptor,
    pub output: Arc<dyn routing::EventRouterPort>,
}
pub struct PublisherDependencies {
    pub owner: InstrumentedOwner,
    pub listener: Arc<dyn transport::DataListenerPort>,
    pub registry: Arc<dyn registry::SourceRegistryPort>,
    pub retention: Arc<dyn retention::RetentionReaderPort>,
    pub protection: Arc<dyn retention::RetentionControlPort>,
    pub book: Arc<dyn processing::BookViewPort>,
    pub input: Arc<dyn processing::EventInputPort>,
}
pub struct RecorderDependencies {
    pub owner: InstrumentedOwner,
    pub registry: Arc<dyn registry::SourceRegistryPort>,
    pub backend: Arc<dyn recording::RecordingBackendPort>,
    pub input: Arc<dyn processing::EventInputPort>,
    pub recovery: Arc<dyn recovery::RecoveryPort>,
    pub settings: RecordingSettings,
}
pub struct RetentionDependencies {
    pub owner: InstrumentedOwner,
    pub storage: Arc<dyn retention::SegmentStoragePort>,
    pub input: Arc<dyn processing::EventInputPort>,
}
pub struct MetricsDependencies {
    pub context: context::OwnerContext,
    pub diagnostics: Arc<dyn observability::ObservationSinkPort>,
    pub settings: MetricsSettings,
    pub max_series: usize,
}
pub struct ConfigurationManagerDependencies {
    pub owner: InstrumentedOwner,
    pub compiler: Arc<dyn configuration::ConfigurationCompilerPort>,
    pub store: Arc<dyn configuration::ConfigurationStorePort>,
    pub reconciler: Arc<dyn configuration::ConfigurationApplyPort>,
}
pub struct RuntimeReconcilerDependencies {
    pub owner: InstrumentedOwner,
    pub source_configuration: Arc<dyn registry::SourceConfigurationPort>,
    pub derived_configuration: Arc<dyn registry::DerivedConfigurationPort>,
    pub recorder: Arc<dyn lifecycle::OwnerLifecyclePort>,
    pub retention: Arc<dyn lifecycle::OwnerLifecyclePort>,
    pub metrics: Arc<dyn lifecycle::OwnerLifecyclePort>,
    pub sources: Arc<dyn registry::SourceRegistryPort>,
    pub derived: Arc<dyn registry::DerivedRegistryPort>,
    pub publication: Arc<dyn publication::PublicationPort>,
    pub recovery: Arc<dyn recovery::RecoveryPort>,
}
pub struct SupervisorDependencies {
    pub owner: InstrumentedOwner,
    pub children: Vec<Arc<dyn lifecycle::OwnerLifecyclePort>>,
}
pub struct ManagementDependencies {
    pub owner: InstrumentedOwner,
    pub management: Arc<dyn control::ManagementPort>,
    pub max_frame_bytes: u32,
    pub max_sessions: usize,
    pub max_operations: usize,
}
