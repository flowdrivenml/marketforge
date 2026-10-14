use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal256Array, RecordBatch, StringArray, UInt8Array};
use arrow_buffer::i256;
use arrow_schema::SchemaRef;

use crate::{
    error::{MarketForgeError, Result},
    process::convert::{ConversionDecimal, reference::EvaluatedReference},
};

use super::{
    provenance::{build_conversion_paths, build_conversion_references},
    schema::{
        CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
        converted_trade_schema,
    },
};

// -----------------------------------------------------------------------------
// Row conversion result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TradeConversionRow {
    pub target_quote: String,

    pub status: u8,
    pub failure: Option<u8>,

    pub rate: Option<ConversionDecimal>,
    pub price_target: Option<ConversionDecimal>,
    pub quantity_quote_target: Option<ConversionDecimal>,

    pub path: Option<Vec<String>>,
    pub references: Vec<EvaluatedReference>,
}

// -----------------------------------------------------------------------------
// Arrow trade builder
// -----------------------------------------------------------------------------

pub struct ConvertedTradeBatchBuilder {
    source_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl ConvertedTradeBatchBuilder {
    pub fn new(source_schema: SchemaRef) -> Result<Self> {
        let output_schema = Arc::new(converted_trade_schema(&source_schema));

        Ok(Self {
            source_schema,
            output_schema,
        })
    }

    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub fn build(&self, source: &RecordBatch, rows: &[TradeConversionRow]) -> Result<RecordBatch> {
        if source.schema() != self.source_schema {
            return invalid("trade source schema mismatch");
        }

        if source.num_rows() != rows.len() {
            return invalid("trade conversion row count mismatch");
        }

        // ---------------------------------------------------------------------
        // Validate conversion results
        // ---------------------------------------------------------------------

        for row in rows {
            if row.target_quote.is_empty() {
                return invalid("conversion target quote is empty");
            }

            match row.status {
                CONVERSION_STATUS_IDENTITY => {
                    if row.rate != Some(ConversionDecimal::one())
                        || row.price_target.is_none()
                        || row.failure.is_some()
                        || !row.references.is_empty()
                        || row
                            .path
                            .as_ref()
                            .is_none_or(|path| path.len() != 1 || path[0] != row.target_quote)
                    {
                        return invalid("invalid identity conversion");
                    }
                }

                CONVERSION_STATUS_CONVERTED => {
                    if row.rate.is_none_or(|rate| !rate.is_positive())
                        || row.price_target.is_none()
                        || row.failure.is_some()
                        || row.references.is_empty()
                        || row.path.as_ref().is_none_or(|path| {
                            path.len() < 2
                                || path.last().map(String::as_str)
                                    != Some(row.target_quote.as_str())
                                || path.len() != row.references.len() + 1
                        })
                    {
                        return invalid("invalid converted trade");
                    }
                }

                CONVERSION_STATUS_UNAVAILABLE => {
                    if row.rate.is_some()
                        || row.price_target.is_some()
                        || row.quantity_quote_target.is_some()
                        || row.failure.is_none()
                        || row.path.is_some()
                        || !row.references.is_empty()
                    {
                        return invalid("invalid unavailable conversion");
                    }
                }

                _ => return invalid("unknown conversion status"),
            }
        }

        // ---------------------------------------------------------------------
        // Preserve original Arrow columns
        // ---------------------------------------------------------------------

        let mut columns = source.columns().to_vec();

        // ---------------------------------------------------------------------
        // Converted Decimal256 columns
        // ---------------------------------------------------------------------

        columns.push(decimal_array(rows.iter().map(|row| row.price_target))?);

        columns.push(decimal_array(
            rows.iter().map(|row| row.quantity_quote_target),
        )?);

        // ---------------------------------------------------------------------
        // Conversion metadata
        // ---------------------------------------------------------------------

        columns.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.target_quote.as_str())
                .collect::<Vec<_>>(),
        )));

        columns.push(Arc::new(UInt8Array::from(
            rows.iter().map(|row| row.status).collect::<Vec<_>>(),
        )));

        columns.push(decimal_array(rows.iter().map(|row| row.rate))?);

        // ---------------------------------------------------------------------
        // Conversion path and reference provenance
        // ---------------------------------------------------------------------

        columns.push(build_conversion_paths(rows)?);
        columns.push(build_conversion_references(rows)?);

        // ---------------------------------------------------------------------
        // Failure code
        // ---------------------------------------------------------------------

        columns.push(Arc::new(UInt8Array::from(
            rows.iter().map(|row| row.failure).collect::<Vec<_>>(),
        )));

        // ---------------------------------------------------------------------
        // Construct output batch
        // ---------------------------------------------------------------------

        RecordBatch::try_new(self.output_schema.clone(), columns).map_err(|error| {
            invalid_error(format!(
                "failed to construct converted trade batch: {error}"
            ))
        })
    }
}

// -----------------------------------------------------------------------------
// Decimal256 array construction
// -----------------------------------------------------------------------------

fn decimal_array<I>(values: I) -> Result<ArrayRef>
where
    I: IntoIterator<Item = Option<ConversionDecimal>>,
{
    let values = values
        .into_iter()
        .map(|value| value.map(|decimal| decimal.mantissa()))
        .collect::<Vec<Option<i256>>>();

    let array = Decimal256Array::from(values)
        .with_precision_and_scale(76, 28)
        .map_err(|error| {
            invalid_error(format!(
                "failed to construct conversion Decimal256 array: {error}"
            ))
        })?;

    Ok(Arc::new(array))
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
