use serde_json::Value;

use super::super::decoder::{TradeDecodeError, TradeDecoder};
use crate::live::infrastructure::normalization::context::NormalizationContext;
use crate::live::infrastructure::normalization::message::{NormalizationSource, NormalizedTrades};

impl TradeDecoder {
    // ========================================================
    // DECODE ONE NATIVE MESSAGE
    // ========================================================

    /// Decode a complete native JSON trade message.
    ///
    /// Record extraction preserves the source order.
    ///
    /// The message is constructed only after every
    /// individual trade has been decoded successfully.
    ///
    /// A malformed record therefore rejects the entire
    /// message rather than publishing a partial batch.
    pub fn decode_message(
        &self,
        message: &Value,
        context: &NormalizationContext,
        source: NormalizationSource,
    ) -> Result<NormalizedTrades, TradeDecodeError> {
        self.validate_context(context)?;

        // Preserve WebSocket message validation.
        self.plan
            .envelope
            .validate(message, context.symbol())
            .map_err(|error| {
                TradeDecodeError::InvalidContext(format!("native message envelope: {error}"))
            })?;

        let records = self
            .plan
            .extraction
            .extract(message)
            .map_err(|error| TradeDecodeError::Fields(error.to_string()))?;

        self.decode_records(records, context, source)
    }
}
