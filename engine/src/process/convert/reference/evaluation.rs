use num_bigint::BigInt;
use num_traits::{One, Zero};

use crate::process::convert::{ConversionDecimal, ConversionDirection, ConversionPath};

use super::state::{ReferenceKey, ReferenceState, ReferenceUnavailable};

// -----------------------------------------------------------------------------
// Evaluation results
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EvaluatedReference {
    pub dataset_id: u64,
    pub stream_id: String,
    pub key: ReferenceKey,
    pub direction: ConversionDirection,
    pub price: ConversionDecimal,
}

#[derive(Debug, Clone)]
pub struct EvaluatedConversion {
    pub rate: ConversionDecimal,
    pub references: Vec<EvaluatedReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionUnavailable {
    MissingReference(String),
    FutureReference(String),
    StaleReference(String),
    ReferenceIdentityMismatch(String),
    ArithmeticFailure(String),
}

// -----------------------------------------------------------------------------
// Evaluate conversion path
// -----------------------------------------------------------------------------

pub fn evaluate_conversion_path(
    path: &ConversionPath,
    state: &ReferenceState,
    source_key: ReferenceKey,
    max_staleness_ns: u64,
) -> std::result::Result<EvaluatedConversion, ConversionUnavailable> {
    let mut numerator = BigInt::one();
    let mut denominator = BigInt::one();

    let mut references = Vec::with_capacity(path.edges.len());

    for edge in &path.edges {
        let observation = state
            .evaluate(&edge.reference_stream_id, source_key, max_staleness_ns)
            .map_err(|reason| match reason {
                ReferenceUnavailable::Missing => {
                    ConversionUnavailable::MissingReference(edge.reference_stream_id.clone())
                }

                ReferenceUnavailable::Future => {
                    ConversionUnavailable::FutureReference(edge.reference_stream_id.clone())
                }

                ReferenceUnavailable::Stale => {
                    ConversionUnavailable::StaleReference(edge.reference_stream_id.clone())
                }
            })?;

        if observation.dataset_id != edge.reference_dataset_id
            || observation.stream_id != edge.reference_stream_id
            || observation.key.stream_rank != edge.reference_stream_rank
        {
            return Err(ConversionUnavailable::ReferenceIdentityMismatch(
                edge.reference_stream_id.clone(),
            ));
        }

        let mantissa = BigInt::from_signed_bytes_be(&observation.price.mantissa().to_be_bytes());

        let scale = BigInt::from(10u8).pow(28);

        match edge.direction {
            ConversionDirection::Direct => {
                numerator *= &mantissa;
                denominator *= &scale;
            }

            ConversionDirection::Inverse => {
                if mantissa.is_zero() {
                    return Err(ConversionUnavailable::MissingReference(
                        edge.reference_stream_id.clone(),
                    ));
                }

                numerator *= &scale;
                denominator *= &mantissa;
            }
        }

        references.push(EvaluatedReference {
            dataset_id: observation.dataset_id,
            stream_id: observation.stream_id.clone(),
            key: observation.key,
            direction: edge.direction,
            price: observation.price,
        });
    }

    let rate = ConversionDecimal::from_ratio(numerator, denominator)
        .map_err(|error| ConversionUnavailable::ArithmeticFailure(error.to_string()))?;

    Ok(EvaluatedConversion { rate, references })
}
