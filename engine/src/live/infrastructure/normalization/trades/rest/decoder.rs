use crate::live::config::exchange::{RestApplicationResponseStrategy, RestEndpoint};
use crate::live::infrastructure::normalization::context::NormalizationContext;
use crate::live::infrastructure::normalization::message::{
    NormalizationSource, NormalizedTrades, TradeRecordSemantics,
};
use crate::live::infrastructure::normalization::trades::decoder::{TradeDecodeError, TradeDecoder};
use crate::live::infrastructure::rest::RestRequest;
use crate::live::infrastructure::rest::{
    ExchangeRestResponse, RestApplicationOutcome, RestApplicationRule, RestApplicationValidator,
};
use crate::live::stream::request::SourceChannel;
use serde_json::Value;

// ============================================================
// REST TRADE DECODER
// ============================================================

/// Decodes a successfully validated REST trade response.
///
/// HTTP status and exchange application-level success
/// must be validated by the REST infrastructure before
/// this decoder is invoked.
///
/// Record extraction and canonical conversion are atomic:
/// a malformed record rejects the entire response batch.
///
#[derive(Debug, thiserror::Error)]
pub enum RestTradeResponseError {
    #[error("REST response provenance rejected: {0}")]
    Provenance(String),

    #[error("REST application response rejected: {0}")]
    Application(String),

    #[error("REST trade normalization failed: {0}")]
    Normalization(#[from] TradeDecodeError),
}

pub struct RestTradeDecoder;

impl RestTradeDecoder {
    pub fn decode_validated_response(
        decoder: &TradeDecoder,
        response: &Value,
        context: &NormalizationContext,
    ) -> Result<NormalizedTrades, TradeDecodeError> {
        // ====================================================
        // REST RECORD EXTRACTION
        // ====================================================

        let records = decoder
            .plan()
            .extraction
            .extract(response)
            .map_err(|error| TradeDecodeError::Fields(error.to_string()))?;

        // ====================================================
        // CANONICAL CONVERSION
        // ====================================================

        let mut trades = Vec::with_capacity(records.len());

        for record in records.iter() {
            // Reuse existing identity validation,
            // side interpretation, quantity conversion,
            // timestamp conversion, and canonical output.
            let trade = decoder.decode_record(record, context)?;

            trades.push(trade);
        }

        // ====================================================
        // ATOMIC BATCH CONSTRUCTION
        // ====================================================

        NormalizedTrades::new(
            context.stream_id().to_owned(),
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            trades,
        )
        .map_err(TradeDecodeError::Batch)
    }
    /// Decode an exchange REST response after binding it to
    /// the logical request's authoritative native instrument.
    ///
    /// `request` must be the same logical request passed to
    /// ExchangeRestService for this response.
    ///
    /// The REST service must have successfully resolved and
    /// executed that request before this method is invoked.
    pub fn decode_bound_exchange_response(
        decoder: &TradeDecoder,
        response: &ExchangeRestResponse,
        request: &RestRequest,
        endpoint_config: &RestEndpoint,
        context: &NormalizationContext,
    ) -> Result<NormalizedTrades, RestTradeResponseError> {
        use RestTradeResponseError as Error;

        // --------------------------------------------------------
        // REQUEST AND RESPONSE IDENTITY
        // --------------------------------------------------------

        if request.profile().is_empty() || request.endpoint().is_empty() {
            return Err(Error::Provenance(
                "request profile and endpoint must be nonempty".into(),
            ));
        }

        if request.profile() != response.profile() || request.endpoint() != response.endpoint() {
            return Err(Error::Provenance(
                "REST request and response identity mismatch".into(),
            ));
        }

        // --------------------------------------------------------
        // COMPILED FORMAT AND STREAM IDENTITY
        // --------------------------------------------------------

        let plan = decoder.plan();

        if plan.expected_exchange != context.exchange() {
            return Err(Error::Provenance(
                "compiled exchange differs from request context".into(),
            ));
        }

        if plan.expected_market != context.market_category() {
            return Err(Error::Provenance(
                "compiled market differs from request context".into(),
            ));
        }

        if context.channel() != SourceChannel::Trades {
            return Err(Error::Provenance(
                "REST trade decoder requires a trade stream".into(),
            ));
        }

        // --------------------------------------------------------
        // CONFIGURED NATIVE INSTRUMENT PARAMETER
        // --------------------------------------------------------

        let mapping = endpoint_config
            .trade_normalization
            .as_ref()
            .ok_or_else(|| {
                Error::Provenance("endpoint has no REST trade normalization mapping".into())
            })?;

        let parameter = mapping.request_symbol_parameter.as_str();

        if parameter.is_empty() || parameter.trim() != parameter {
            return Err(Error::Provenance(
                "invalid REST request symbol parameter mapping".into(),
            ));
        }

        let requested_symbol = request.parameters().get(parameter).ok_or_else(|| {
            Error::Provenance(format!(
                "request lacks native instrument parameter '{parameter}'"
            ))
        })?;

        if requested_symbol != context.symbol() {
            return Err(Error::Provenance(format!(
                "REST request instrument '{}' differs from \
             normalization context '{}'",
                requested_symbol,
                context.symbol(),
            )));
        }

        // --------------------------------------------------------
        // EXISTING RESPONSE VALIDATION AND ATOMIC DECODING
        // --------------------------------------------------------

        Self::decode_exchange_response(
            decoder,
            response,
            request.profile(),
            request.endpoint(),
            endpoint_config,
            context,
        )
    }
    /// Decode a response accepted by the REST infrastructure.
    ///
    /// Validates the response's declared source identity and
    /// configured application status before trade normalization.
    ///
    /// The caller remains responsible for ensuring that the
    /// resolved HTTP request was authorized for this instrument.
    pub fn decode_exchange_response(
        decoder: &TradeDecoder,
        response: &ExchangeRestResponse,
        expected_profile: &str,
        expected_endpoint: &str,
        endpoint_config: &RestEndpoint,
        context: &NormalizationContext,
    ) -> Result<NormalizedTrades, RestTradeResponseError> {
        use RestTradeResponseError as Error;

        // --------------------------------------------------------
        // SOURCE PROVENANCE
        // --------------------------------------------------------

        if expected_profile.is_empty() || expected_endpoint.is_empty() {
            return Err(Error::Provenance(
                "expected profile and endpoint must be nonempty".into(),
            ));
        }

        if response.profile() != expected_profile {
            return Err(Error::Provenance(format!(
                "expected profile '{expected_profile}', got '{}'",
                response.profile(),
            )));
        }

        if response.endpoint() != expected_endpoint {
            return Err(Error::Provenance(format!(
                "expected endpoint '{expected_endpoint}', got '{}'",
                response.endpoint(),
            )));
        }

        if !(200..=299).contains(&response.status()) {
            return Err(Error::Provenance(format!(
                "unsuccessful HTTP status {}",
                response.status(),
            )));
        }

        if response.body_bytes() == 0 {
            return Err(Error::Provenance("empty REST response body".into()));
        }

        // --------------------------------------------------------
        // APPLICATION RESPONSE CONFIGURATION
        // --------------------------------------------------------

        let application = endpoint_config
            .application_response
            .as_ref()
            .ok_or_else(|| {
                Error::Application("missing application_response configuration".into())
            })?;

        application.validate().map_err(Error::Application)?;

        // --------------------------------------------------------
        // APPLICATION SUCCESS
        // --------------------------------------------------------

        match application.strategy {
            RestApplicationResponseStrategy::Raw => {
                // Binance and Gate.io return raw JSON trade arrays.
                // Structural validation happens during extraction.
            }

            RestApplicationResponseStrategy::Envelope => {
                let code_field = application
                    .code_field
                    .as_ref()
                    .ok_or_else(|| Error::Application("missing application code field".into()))?;

                let rule = RestApplicationRule::new(
                    code_field.clone(),
                    application.success_codes.clone(),
                    application.message_field.clone(),
                    application.data_field.clone(),
                )
                .map_err(|error| Error::Application(error.to_string()))?;

                match RestApplicationValidator::validate(&rule, response.body()) {
                    RestApplicationOutcome::Success => {}

                    RestApplicationOutcome::Failure { code, message } => {
                        return Err(Error::Application(format!(
                            "exchange rejected response: code={code}, message={message:?}"
                        )));
                    }

                    RestApplicationOutcome::InvalidEnvelope { reason } => {
                        return Err(Error::Application(reason));
                    }
                }
            }
        }

        // --------------------------------------------------------
        // ATOMIC CANONICAL TRADE DECODING
        // --------------------------------------------------------

        Self::decode_validated_response(decoder, response.body(), context)
            .map_err(Error::Normalization)
    }
}
