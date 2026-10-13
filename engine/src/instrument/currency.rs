use serde::{Deserialize, Serialize};

use crate::error::{MarketForgeError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Currency(pub String);

impl Currency {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();

        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "invalid currency code: {value}"
            )));
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
