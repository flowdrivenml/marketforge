use arrow_array::{
    Array, ArrayRef, Decimal256Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array,
};
use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::{DataType, SchemaRef};
use std::sync::Arc;
use {
    crate::historical::convert::execution::CombinedConversionRow,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};
use {
    crate::historical::convert::output::publication::provenance::build_conversion_paths,
    crate::historical::convert::output::publication::provenance::build_conversion_references,
    crate::historical::convert::output::schema::CONVERSION_STATUS_CONVERTED,
    crate::historical::convert::output::schema::CONVERSION_STATUS_IDENTITY,
    crate::historical::convert::output::schema::CONVERSION_STATUS_SKIPPED,
    crate::historical::convert::output::schema::CONVERSION_STATUS_UNAVAILABLE,
    crate::historical::convert::output::schema::converted_combined_schema,
    crate::historical::convert::output::schema::trade::TradeConversionRow,
};

// -----------------------------------------------------------------------------
// Combined Arrow batch builder
// -----------------------------------------------------------------------------

pub struct ConvertedCombinedBatchBuilder {
    source_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl ConvertedCombinedBatchBuilder {
    pub fn new(source_schema: SchemaRef) -> Result<Self> {
        let output_schema = Arc::new(converted_combined_schema(source_schema.as_ref()));

        Ok(Self {
            source_schema,
            output_schema,
        })
    }

    pub fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub fn build(
        &self,
        source: &RecordBatch,
        rows: &[CombinedConversionRow],
    ) -> Result<RecordBatch> {
        if source.schema() != self.source_schema {
            return invalid("combined source schema mismatch");
        }

        if source.num_rows() != rows.len() {
            return invalid("combined conversion row count mismatch");
        }

        let kinds = source
            .column_by_name("event_kind")
            .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
            .ok_or_else(|| invalid_error("missing combined event_kind"))?;

        let depths = source
            .column_by_name("depth")
            .and_then(|array| array.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| invalid_error("missing combined depth payload"))?;

        let changes = depths
            .column_by_name("changes")
            .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| invalid_error("missing combined depth changes"))?;

        let mut trade_prices = Vec::with_capacity(rows.len());
        let mut trade_quantities = Vec::with_capacity(rows.len());
        let mut trade_validity = Vec::with_capacity(rows.len());

        let mut depth_prices = Vec::<Option<i256>>::new();
        let mut depth_quantities = Vec::<Option<i256>>::new();
        let mut depth_offsets = Vec::with_capacity(rows.len() + 1);
        let mut depth_validity = Vec::with_capacity(rows.len());

        let mut metadata = Vec::<TradeConversionRow>::with_capacity(rows.len());

        depth_offsets.push(0_i32);

        for (index, row) in rows.iter().enumerate() {
            if kinds.is_null(index) {
                return invalid("null combined event_kind");
            }

            match row {
                CombinedConversionRow::Trade(trade) => {
                    if kinds.value(index) != 0 {
                        return invalid("trade conversion event_kind mismatch");
                    }

                    trade_prices.push(trade.price_target.map(|value| value.mantissa()));

                    trade_quantities
                        .push(trade.quantity_quote_target.map(|value| value.mantissa()));

                    trade_validity.push(true);
                    depth_validity.push(false);

                    metadata.push(trade.clone());
                }

                CombinedConversionRow::Depth(depth) => {
                    if kinds.value(index) != 1 {
                        return invalid("depth conversion event_kind mismatch");
                    }

                    trade_prices.push(None);
                    trade_quantities.push(None);
                    trade_validity.push(false);
                    depth_validity.push(true);

                    let original_levels = changes.value_length(index) as usize;

                    match depth.metadata.status {
                        CONVERSION_STATUS_IDENTITY | CONVERSION_STATUS_CONVERTED => {
                            let levels = depth.levels.as_ref().ok_or_else(|| {
                                invalid_error("successful depth conversion missing levels")
                            })?;

                            if levels.len() != original_levels {
                                return invalid(format!(
                                    "combined depth level count mismatch at event {index}"
                                ));
                            }

                            for level in levels {
                                depth_prices.push(Some(level.price_target.mantissa()));

                                depth_quantities.push(
                                    level.quantity_quote_target.map(|value| value.mantissa()),
                                );
                            }
                        }

                        CONVERSION_STATUS_UNAVAILABLE | CONVERSION_STATUS_SKIPPED => {
                            if depth.levels.is_some() {
                                return invalid(
                                    "unavailable or skipped depth conversion contains levels",
                                );
                            }

                            if depth.metadata.status == CONVERSION_STATUS_SKIPPED
                                && (depth.metadata.failure.is_some()
                                    || depth.metadata.rate.is_some()
                                    || depth.metadata.path.is_some()
                                    || !depth.metadata.references.is_empty())
                            {
                                return invalid("invalid skipped combined depth metadata");
                            }

                            depth_prices.extend(std::iter::repeat_n(None, original_levels));
                            depth_quantities.extend(std::iter::repeat_n(None, original_levels));
                        }

                        _ => return invalid("invalid depth conversion status"),
                    }

                    metadata.push(depth.metadata.clone());
                }
            }

            depth_offsets.push(
                i32::try_from(depth_prices.len())
                    .map_err(|_| invalid_error("depth offset overflow"))?,
            );
        }

        // ---------------------------------------------------------------------
        // Construct converted trade struct
        // ---------------------------------------------------------------------

        let trade_field = self
            .output_schema
            .field_with_name("trade_target")
            .map_err(|error| invalid_error(error.to_string()))?;

        let DataType::Struct(trade_fields) = trade_field.data_type() else {
            return invalid("invalid combined trade target schema");
        };

        let trade_target = StructArray::try_new(
            trade_fields.clone(),
            vec![
                decimal_array(trade_prices)?,
                decimal_array(trade_quantities)?,
            ],
            Some(trade_validity.into()),
        )
        .map_err(|error| {
            invalid_error(format!(
                "failed to construct combined trade target: {error}"
            ))
        })?;

        // ---------------------------------------------------------------------
        // Construct converted depth list
        // ---------------------------------------------------------------------

        let depth_field = self
            .output_schema
            .field_with_name("depth_target")
            .map_err(|error| invalid_error(error.to_string()))?;

        let DataType::List(item_field) = depth_field.data_type() else {
            return invalid("invalid combined depth target schema");
        };

        let DataType::Struct(level_fields) = item_field.data_type() else {
            return invalid("invalid combined depth level schema");
        };

        let depth_levels = StructArray::try_new(
            level_fields.clone(),
            vec![
                decimal_array(depth_prices)?,
                decimal_array(depth_quantities)?,
            ],
            None,
        )
        .map_err(|error| {
            invalid_error(format!(
                "failed to construct combined depth levels: {error}"
            ))
        })?;

        let depth_target = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::new(depth_offsets.into()),
            Arc::new(depth_levels),
            Some(depth_validity.into()),
        )
        .map_err(|error| {
            invalid_error(format!(
                "failed to construct combined depth target: {error}"
            ))
        })?;

        // ---------------------------------------------------------------------
        // Preserve original source columns
        // ---------------------------------------------------------------------

        let mut columns = source.columns().to_vec();

        columns.push(Arc::new(trade_target));
        columns.push(Arc::new(depth_target));

        // ---------------------------------------------------------------------
        // Shared conversion metadata
        // ---------------------------------------------------------------------

        columns.push(Arc::new(StringArray::from(
            metadata
                .iter()
                .map(|row| row.target_quote.as_str())
                .collect::<Vec<_>>(),
        )));

        columns.push(Arc::new(UInt8Array::from(
            metadata.iter().map(|row| row.status).collect::<Vec<_>>(),
        )));

        columns.push(decimal_array(
            metadata
                .iter()
                .map(|row| row.rate.map(|value| value.mantissa()))
                .collect(),
        )?);

        columns.push(build_conversion_paths(&metadata)?);
        columns.push(build_conversion_references(&metadata)?);

        columns.push(Arc::new(UInt8Array::from(
            metadata.iter().map(|row| row.failure).collect::<Vec<_>>(),
        )));

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            invalid_error(format!(
                "failed to construct combined conversion batch: {error}"
            ))
        })
    }
}

// -----------------------------------------------------------------------------
// Decimal256 construction
// -----------------------------------------------------------------------------

fn decimal_array(values: Vec<Option<i256>>) -> Result<ArrayRef> {
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(76, 28)
        .map_err(|error| {
            invalid_error(format!("invalid combined conversion Decimal256: {error}"))
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
