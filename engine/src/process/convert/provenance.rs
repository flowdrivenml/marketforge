use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal256Array, Int64Array, ListArray, StringArray, StructArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};

use crate::{
    error::{MarketForgeError, Result},
    process::convert::{ConversionDirection, reference::EvaluatedReference},
};

use super::trade::TradeConversionRow;

// -----------------------------------------------------------------------------
// Conversion paths
// -----------------------------------------------------------------------------

pub fn build_conversion_paths(rows: &[TradeConversionRow]) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut values = Vec::<String>::new();
    let mut validity = Vec::with_capacity(rows.len());

    offsets.push(0_i32);

    for row in rows {
        match &row.path {
            Some(path) => {
                validity.push(true);

                values.extend(path.iter().cloned());
            }

            None => validity.push(false),
        }

        offsets.push(
            i32::try_from(values.len())
                .map_err(|_| invalid_error("conversion path offset overflow"))?,
        );
    }

    let array = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(values)),
        Some(NullBuffer::from(validity)),
    );

    Ok(Arc::new(array))
}

// -----------------------------------------------------------------------------
// Reference provenance
// -----------------------------------------------------------------------------

pub fn build_conversion_references(rows: &[TradeConversionRow]) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);

    let mut dataset_ids = Vec::new();
    let mut stream_ids = Vec::new();
    let mut timestamps = Vec::new();
    let mut stream_ranks = Vec::new();
    let mut task_ids = Vec::new();
    let mut ordinals = Vec::new();
    let mut directions = Vec::new();
    let mut prices = Vec::new();

    offsets.push(0_i32);

    for row in rows {
        for reference in &row.references {
            dataset_ids.push(reference.dataset_id);
            stream_ids.push(reference.stream_id.as_str());
            timestamps.push(reference.key.timestamp_ns);
            stream_ranks.push(reference.key.stream_rank);
            task_ids.push(reference.key.task_rank);
            ordinals.push(reference.key.event_ordinal);

            directions.push(match reference.direction {
                ConversionDirection::Direct => 0_u8,
                ConversionDirection::Inverse => 1_u8,
            });

            prices.push(reference.price.mantissa());
        }

        offsets.push(
            i32::try_from(dataset_ids.len())
                .map_err(|_| invalid_error("conversion reference offset overflow"))?,
        );
    }

    let prices = Decimal256Array::from(prices)
        .with_precision_and_scale(76, 28)
        .map_err(|error| invalid_error(format!("invalid reference Decimal256 array: {error}")))?;

    let fields = Fields::from(vec![
        Field::new("dataset_id", DataType::UInt64, false),
        Field::new("stream_id", DataType::Utf8, false),
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("stream_rank", DataType::UInt32, false),
        Field::new("task_id", DataType::UInt64, false),
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("direction", DataType::UInt8, false),
        Field::new("reference_price", DataType::Decimal256(76, 28), false),
    ]);

    let values = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(UInt64Array::from(dataset_ids)),
            Arc::new(StringArray::from(stream_ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(UInt32Array::from(stream_ranks)),
            Arc::new(UInt64Array::from(task_ids)),
            Arc::new(UInt64Array::from(ordinals)),
            Arc::new(UInt8Array::from(directions)),
            Arc::new(prices),
        ],
        None,
    );

    let array = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        None,
    );

    Ok(Arc::new(array))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
