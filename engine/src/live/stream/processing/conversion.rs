//! Frozen Shared conversion plan and last-trade valuation adapter.
use crate::{
    live::stream::{persistence::StreamDefinition, registry::StreamGraph, request::*, router::*},
    shared::{
        conversion::{evaluation::*, planner::*, reference::*, *},
        domain::instrument::*,
    },
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

pub(crate) struct LiveConversion {
    plan: ConversionPlan,
    references: BTreeMap<String, ResolvedConversionInstrument>,
    state: ReferenceState,
    generations: BTreeMap<String, crate::live::stream::registry::RuntimeGeneration>,
    settings: ConversionSettings,
}
pub(crate) fn decimal(value: Decimal) -> Result<ConversionDecimal, String> {
    ConversionDecimal::from_ratio(
        value.mantissa().into(),
        num_bigint::BigInt::from(10u8).pow(value.scale()),
    )
    .map_err(|e| format!("ArithmeticFailure: {e}"))
}
fn instrument(
    graph: &StreamGraph,
    id: &str,
    rank: u32,
) -> Result<ResolvedConversionInstrument, String> {
    let d = graph.get(id).ok_or("unknown conversion instrument")?;
    let StreamDefinition::Source(source) = &d.definition else {
        return Err("reference must be a declared last-trade Source".into());
    };
    Ok(ResolvedConversionInstrument {
        dataset_id: u64::try_from(d.recording_stream_key.ok_or("missing source key")?)
            .map_err(|_| "invalid source key")?,
        stream_rank: rank,
        stream_id: id.into(),
        identity: InstrumentIdentity {
            exchange: source.exchange.as_str().into(),
            symbol: source.symbol.clone(),
            instrument_kind: serde_json::from_value(serde_json::Value::String(
                source.instrument_type.clone(),
            ))
            .map_err(|e| format!("invalid instrument kind: {e}"))?,
            contract_kind: match source.market_category.as_str() {
                "linear" => Some(ContractKind::Linear),
                "inverse" => Some(ContractKind::Inverse),
                _ => None,
            },
        },
        base_currency: Currency::new(source.base_asset.clone().ok_or("missing base asset")?)
            .map_err(|e| e.to_string())?,
        quote_currency: Currency::new(source.quote_asset.clone().ok_or("missing quote asset")?)
            .map_err(|e| e.to_string())?,
    })
}
impl LiveConversion {
    pub(crate) fn resolve(
        graph: &StreamGraph,
        request: &CreateDerivedStreamRequest,
        max_sources: usize,
        planning_steps: usize,
    ) -> Result<Self, String> {
        let settings = request.conversion.unwrap_or_default();
        let sources = graph
            .data_sources(&request.inputs[0])
            .map_err(|e| e.to_string())?;
        if sources.len() > max_sources {
            return Err("conversion source fan-in exceeds limit".into());
        }
        let sources = sources
            .iter()
            .enumerate()
            .map(|(rank, id)| instrument(graph, id, rank as u32))
            .collect::<Result<Vec<_>, _>>()?;
        let mut references = vec![];
        for spec in request
            .input_specs()
            .iter()
            .filter(|s| s.role == InputRole::Reference)
        {
            let source = instrument(graph, &spec.stream_id, spec.position as u32)?;
            let StreamDefinition::Source(def) = &graph.get(&spec.stream_id).unwrap().definition
            else {
                unreachable!()
            };
            if def.channel != SourceChannel::Trades {
                return Err("reference basis is last trade; depth reference unsupported".into());
            }
            references.push(source);
        }
        let plan = plan_conversion_limited(
            Currency::new(
                request
                    .target_quote_asset
                    .clone()
                    .ok_or("missing target quote")?,
            )
            .map_err(|e| e.to_string())?,
            &sources,
            &references,
            |_| true,
            settings.max_hops,
            crate::shared::conversion::graph::PathSearchLimits {
                max_paths: DEFAULT_MAX_PATHS,
                max_edges_examined: planning_steps,
            },
        )
        .map_err(|e| e.to_string())?;
        if plan.unresolved_streams != 0 {
            return Err("MissingReference: no declared conversion path".into());
        }
        Ok(Self {
            plan,
            references: references
                .into_iter()
                .map(|r| (r.stream_id.clone(), r))
                .collect(),
            state: ReferenceState::new(),
            generations: BTreeMap::new(),
            settings,
        })
    }
    pub(crate) fn reference(
        &mut self,
        input: &str,
        key: ReferenceKey,
        event: &DerivedEvent,
    ) -> Result<(), String> {
        let source = self
            .references
            .get(input)
            .ok_or("ReferenceIdentityMismatch: unknown reference")?;
        if event.origin.stream_id != source.stream_id {
            return Err("ReferenceIdentityMismatch: reference provenance".into());
        }
        let NativeMarketEvent::Trade(t) = &event.native else {
            return Err("reference requires a trade price".into());
        };
        let price = decimal(t.price)?;
        if !price.is_positive() {
            return Err("MissingReference: reference price must be positive".into());
        }
        self.generations
            .insert(input.into(), event.origin.generation);
        self.state
            .update(ReferenceObservation {
                dataset_id: source.dataset_id,
                stream_id: source.stream_id.clone(),
                key,
                price,
            })
            .map_err(|e| e.to_string())
    }
    pub(crate) fn value(&self, key: ReferenceKey, event: &mut DerivedEvent) -> Result<(), String> {
        if event.valuation.is_some() {
            return Err("already converted input is unsupported; use native or Merge input".into());
        }
        let source = self
            .plan
            .streams
            .iter()
            .find(|s| s.source.stream_id == event.origin.stream_id)
            .ok_or("ReferenceIdentityMismatch: unknown data provenance")?;
        let identity = ConversionPath {
            currencies: vec![self.plan.target_quote.clone()],
            edges: vec![],
        };
        let paths = if source.identity_conversion {
            std::slice::from_ref(&identity)
        } else {
            source.paths.as_slice()
        };
        let mut failure = "MissingReference: no usable path".to_owned();
        for path in paths {
            let evaluation = match evaluate_conversion_path(
                path,
                &self.state,
                key,
                self.settings.max_staleness_ns,
            ) {
                Ok(v) => v,
                Err(e) => {
                    failure = format!("{e:?}");
                    continue;
                }
            };
            let converted = (|| {
                let values = match &event.native {
                    NativeMarketEvent::Trade(t) => vec![
                        convert_trade_values(
                            decimal(t.price)?,
                            t.quantity_quote.map(decimal).transpose()?,
                            &evaluation,
                        )
                        .map_err(|e| format!("ArithmeticFailure: {e}"))?,
                    ],
                    NativeMarketEvent::Depth(
                        RouterDepthEvent::Snapshot { levels, .. }
                        | RouterDepthEvent::Delta {
                            changes: levels, ..
                        },
                    ) => levels
                        .iter()
                        .map(|(_, price, level)| {
                            let v = convert_depth_level(
                                decimal(*price)?,
                                level.quantity_quote.map(decimal).transpose()?,
                                &evaluation,
                            )
                            .map_err(|e| format!("ArithmeticFailure: {e}"))?;
                            Ok(ConvertedTradeValues {
                                price_target: v.price_target,
                                quantity_quote_target: v.quantity_quote_target,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    _ => return Err("unsupported conversion control event".into()),
                };
                Ok::<_, String>(EventValuation {
                    target_quote: self.plan.target_quote.as_str().into(),
                    reference_generations: evaluation
                        .references
                        .iter()
                        .map(|r| (r.stream_id.clone(), self.generations[&r.stream_id]))
                        .collect(),
                    path: path.currencies.iter().map(|c| c.as_str().into()).collect(),
                    evaluation,
                    values: values.into(),
                })
            })();
            match converted {
                Ok(v) => {
                    event.valuation = Some(v);
                    return Ok(());
                }
                Err(e) => failure = e,
            }
        }
        Err(failure)
    }
}
