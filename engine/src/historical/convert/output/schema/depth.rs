use arrow_array::{
    Array, ArrayRef, Decimal256Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array,
};
use arrow_buffer::BooleanBufferBuilder;
use arrow_buffer::i256;
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, SchemaRef};
use std::sync::Arc;
use {
    crate::historical::convert::ConversionDecimal, crate::historical::convert::ConvertedDepthLevel,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};
use {
    crate::historical::convert::output::publication::provenance::build_conversion_paths,
    crate::historical::convert::output::publication::provenance::build_conversion_references,
    crate::historical::convert::output::schema::CONVERSION_STATUS_CONVERTED,
    crate::historical::convert::output::schema::CONVERSION_STATUS_IDENTITY,
    crate::historical::convert::output::schema::CONVERSION_STATUS_UNAVAILABLE,
    crate::historical::convert::output::schema::converted_depth_schema,
    crate::historical::convert::output::schema::trade::TradeConversionRow,
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

impl DepthConversionRow {
    pub fn skipped(target_quote: &str) -> Self {
        Self {
            metadata: TradeConversionRow::skipped(target_quote),
            levels: None,
        }
    }
}

// -----------------------------------------------------------------------------
// Direct depth conversion buffers
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Contiguous depth conversion buffers
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct DepthConversionBuffers {
    pub prices: Vec<i256>,
    pub quantities: Vec<i256>,

    pub price_validity: BooleanBufferBuilder,
    pub quantity_validity: BooleanBufferBuilder,

    pub offsets: Vec<i32>,
}

impl DepthConversionBuffers {
    pub fn with_capacity(events: usize, levels: usize) -> Self {
        let mut offsets = Vec::with_capacity(events + 1);
        offsets.push(0);

        Self {
            prices: Vec::with_capacity(levels),
            quantities: Vec::with_capacity(levels),
            price_validity: BooleanBufferBuilder::new(levels),
            quantity_validity: BooleanBufferBuilder::new(levels),
            offsets,
        }
    }

    pub fn append_level(&mut self, price: Option<i256>, quantity: Option<i256>) {
        self.prices.push(price.unwrap_or(i256::ZERO));
        self.quantities.push(quantity.unwrap_or(i256::ZERO));

        self.price_validity.append(price.is_some());
        self.quantity_validity.append(quantity.is_some());
    }

    pub fn rollback(&mut self, position: usize) {
        self.prices.truncate(position);
        self.quantities.truncate(position);

        self.price_validity.truncate(position);
        self.quantity_validity.truncate(position);
    }

    pub fn append_unavailable(&mut self, levels: usize) {
        for _ in 0..levels {
            self.append_level(None, None);
        }
    }

    pub fn finish_event(&mut self) -> Result<()> {
        if self.prices.len() != self.quantities.len() {
            return invalid("depth conversion buffer length mismatch");
        }

        let offset = i32::try_from(self.prices.len())
            .map_err(|_| invalid_error("converted depth level offset overflow"))?;

        self.offsets.push(offset);

        Ok(())
    }

    pub fn append_identity(
        &mut self,
        prices: &Decimal256Array,
        quantities: &Decimal256Array,
        start: usize,
        end: usize,
    ) -> Result<()> {
        if end < start || end > prices.len() || end > quantities.len() {
            return invalid("invalid identity depth level range");
        }

        // Validate before modifying the buffers.
        for index in start..end {
            if prices.is_null(index) {
                return invalid("null identity depth price");
            }

            ConversionDecimal::new(prices.value(index))?;

            if !quantities.is_null(index) {
                ConversionDecimal::new(quantities.value(index))?;
            }
        }

        // Append contiguous values.
        self.prices.extend_from_slice(&prices.values()[start..end]);
        self.quantities
            .extend_from_slice(&quantities.values()[start..end]);

        for index in start..end {
            self.price_validity.append(true);
            self.quantity_validity.append(!quantities.is_null(index));
        }

        Ok(())
    }
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

        let offsets = changes.value_offsets();

        let total_levels = usize::try_from(offsets[offsets.len() - 1] - offsets[0])
            .map_err(|_| invalid_error("invalid depth level count"))?;

        let mut prices = Vec::<Option<i256>>::with_capacity(total_levels);
        let mut quantities = Vec::<Option<i256>>::with_capacity(total_levels);

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
    pub fn build_from_buffers(
        &self,
        source: &RecordBatch,
        metadata: &[TradeConversionRow],
        buffers: DepthConversionBuffers,
    ) -> Result<RecordBatch> {
        if source.schema() != self.source_schema {
            return invalid("depth source schema mismatch");
        }

        if source.num_rows() != metadata.len() {
            return invalid("depth conversion metadata count mismatch");
        }

        if buffers.prices.len() != buffers.quantities.len() {
            return invalid("depth conversion buffer length mismatch");
        }

        if buffers.offsets.len() != source.num_rows() + 1 {
            return invalid("depth conversion offset count mismatch");
        }

        if buffers.offsets.first() != Some(&0)
            || buffers.offsets.last().copied()
                != Some(
                    i32::try_from(buffers.prices.len())
                        .map_err(|_| invalid_error("depth conversion offset overflow"))?,
                )
        {
            return invalid("depth conversion offset boundary mismatch");
        }

        // ---------------------------------------------------------------------
        // Construct Decimal256 arrays
        // ---------------------------------------------------------------------

        let DepthConversionBuffers {
            prices,
            quantities,
            mut price_validity,
            mut quantity_validity,
            offsets,
        } = buffers;

        let price_nulls = NullBuffer::new(price_validity.finish());
        let quantity_nulls = NullBuffer::new(quantity_validity.finish());

        let prices = Decimal256Array::new(ScalarBuffer::from(prices), Some(price_nulls))
            .with_precision_and_scale(76, 28)
            .map_err(|error| invalid_error(format!("invalid converted depth prices: {error}")))?;

        let quantities = Decimal256Array::new(ScalarBuffer::from(quantities), Some(quantity_nulls))
            .with_precision_and_scale(76, 28)
            .map_err(|error| {
                invalid_error(format!("invalid converted depth quantities: {error}"))
            })?;

        // ---------------------------------------------------------------------
        // Construct nested depth arrays
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
        .map_err(|error| invalid_error(error.to_string()))?;

        let changes_target = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(levels),
            None,
        )
        .map_err(|error| invalid_error(error.to_string()))?;

        // ---------------------------------------------------------------------
        // Preserve original source columns
        // ---------------------------------------------------------------------

        let mut columns = source.columns().to_vec();

        columns.push(Arc::new(changes_target));

        // ---------------------------------------------------------------------
        // Conversion metadata
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

        columns.push(decimal_array(metadata.iter().map(|row| row.rate))?);

        columns.push(build_conversion_paths(metadata)?);
        columns.push(build_conversion_references(metadata)?);

        columns.push(Arc::new(UInt8Array::from(
            metadata.iter().map(|row| row.failure).collect::<Vec<_>>(),
        )));

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
