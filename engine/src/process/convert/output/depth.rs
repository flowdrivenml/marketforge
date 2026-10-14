use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal256Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array,
};

use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::{DataType, SchemaRef};

use crate::{
    error::{MarketForgeError, Result},
    process::convert::{ConversionDecimal, ConvertedDepthLevel},
};

use super::{
    provenance::{build_conversion_paths, build_conversion_references},
    schema::{
        CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
        converted_depth_schema,
    },
    trade::TradeConversionRow,
};

// -----------------------------------------------------------------------------
// Depth conversion result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DepthConversionRow {
    pub metadata: TradeConversionRow,

    /// Converted levels corresponding to the original changes list.
    ///
    /// Unavailable conversions contain None.
    pub levels: Option<Vec<ConvertedDepthLevel>>,
}

// -----------------------------------------------------------------------------
// Arrow depth builder
// -----------------------------------------------------------------------------

pub struct ConvertedDepthBatchBuilder {
    source_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl ConvertedDepthBatchBuilder {
    pub fn new(source_schema: SchemaRef) -> Result<Self> {
        let output_schema = Arc::new(converted_depth_schema(source_schema.as_ref()));

        Ok(Self {
            source_schema,
            output_schema,
        })
    }

    pub fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub fn build(&self, source: &RecordBatch, rows: &[DepthConversionRow]) -> Result<RecordBatch> {
        if source.schema() != self.source_schema {
            return invalid("depth source schema mismatch");
        }

        if source.num_rows() != rows.len() {
            return invalid("depth conversion row count mismatch");
        }

        let changes = source
            .column_by_name("changes")
            .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| invalid_error("missing depth changes list"))?;

        let mut prices = Vec::<Option<i256>>::new();
        let mut quantities = Vec::<Option<i256>>::new();

        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0_i32);

        for (index, row) in rows.iter().enumerate() {
            let original_levels = changes.value_length(index) as usize;

            match row.metadata.status {
                CONVERSION_STATUS_IDENTITY | CONVERSION_STATUS_CONVERTED => {
                    let levels = row.levels.as_ref().ok_or_else(|| {
                        invalid_error("successful depth conversion missing levels")
                    })?;

                    if levels.len() != original_levels {
                        return invalid(format!("depth level count mismatch at event {index}"));
                    }

                    for level in levels {
                        prices.push(Some(level.price_target.mantissa()));

                        quantities.push(level.quantity_quote_target.map(|value| value.mantissa()));
                    }
                }

                CONVERSION_STATUS_UNAVAILABLE => {
                    if row.levels.is_some() {
                        return invalid("unavailable depth conversion contains converted levels");
                    }

                    prices.extend(std::iter::repeat_n(None, original_levels));

                    quantities.extend(std::iter::repeat_n(None, original_levels));
                }

                _ => return invalid("invalid depth conversion status"),
            }

            offsets.push(
                i32::try_from(prices.len())
                    .map_err(|_| invalid_error("converted depth level offset overflow"))?,
            );
        }

        // ---------------------------------------------------------------------
        // Construct converted Decimal256 arrays
        // ---------------------------------------------------------------------

        let prices = Decimal256Array::from(prices)
            .with_precision_and_scale(76, 28)
            .map_err(|error| invalid_error(format!("invalid converted depth prices: {error}")))?;

        let quantities = Decimal256Array::from(quantities)
            .with_precision_and_scale(76, 28)
            .map_err(|error| {
                invalid_error(format!("invalid converted depth quantities: {error}"))
            })?;

        // ---------------------------------------------------------------------
        // Construct nested level struct
        // ---------------------------------------------------------------------

        let target_field = self
            .output_schema
            .field_with_name("changes_target")
            .map_err(|error| invalid_error(error.to_string()))?;

        let DataType::List(item_field) = target_field.data_type() else {
            return invalid("invalid converted depth list schema");
        };

        let DataType::Struct(level_fields) = item_field.data_type() else {
            return invalid("invalid converted depth level schema");
        };

        let levels = StructArray::try_new(
            level_fields.clone(),
            vec![Arc::new(prices), Arc::new(quantities)],
            None,
        )
        .map_err(|error| {
            invalid_error(format!(
                "failed to construct converted depth levels: {error}"
            ))
        })?;

        let changes_target = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(levels),
            None,
        )
        .map_err(|error| {
            invalid_error(format!("failed to construct converted depth list: {error}"))
        })?;

        // ---------------------------------------------------------------------
        // Preserve all original columns
        // ---------------------------------------------------------------------

        let mut columns = source.columns().to_vec();

        columns.push(Arc::new(changes_target));

        // ---------------------------------------------------------------------
        // Conversion metadata
        // ---------------------------------------------------------------------

        let metadata = rows
            .iter()
            .map(|row| row.metadata.clone())
            .collect::<Vec<_>>();

        columns.push(Arc::new(StringArray::from(
            metadata
                .iter()
                .map(|row| row.target_quote.as_str())
                .collect::<Vec<_>>(),
        )));

        columns.push(Arc::new(UInt8Array::from(
            metadata.iter().map(|row| row.status).collect::<Vec<_>>(),
        )));

        columns.push(decimal_array(metadata.iter().map(|row| row.rate))?);

        columns.push(build_conversion_paths(&metadata)?);
        columns.push(build_conversion_references(&metadata)?);

        columns.push(Arc::new(UInt8Array::from(
            metadata.iter().map(|row| row.failure).collect::<Vec<_>>(),
        )));

        // ---------------------------------------------------------------------
        // Construct output
        // ---------------------------------------------------------------------

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            invalid_error(format!(
                "failed to construct converted depth batch: {error}"
            ))
        })
    }
}

// -----------------------------------------------------------------------------
// Decimal256 helper
// -----------------------------------------------------------------------------

fn decimal_array<I>(values: I) -> Result<ArrayRef>
where
    I: IntoIterator<Item = Option<ConversionDecimal>>,
{
    let values = values
        .into_iter()
        .map(|value| value.map(|decimal| decimal.mantissa()))
        .collect::<Vec<_>>();

    let array = Decimal256Array::from(values)
        .with_precision_and_scale(76, 28)
        .map_err(|error| invalid_error(format!("invalid conversion Decimal256 array: {error}")))?;

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
