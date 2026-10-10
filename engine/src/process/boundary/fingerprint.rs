use sha2::{Digest, Sha256};

use rust_decimal::Decimal;

use super::state::{BoundaryBookLevel, BoundaryBookState};

/// Fingerprint version.
///
/// Changing the canonical encoding requires a new version.
const FINGERPRINT_VERSION: &[u8] = b"marketforge-depth-book-v1";

fn write_decimal(hasher: &mut Sha256, value: Decimal) {
    // Normalize decimal scale so equivalent values hash identically.
    let normalized = value.normalize();

    let text = normalized.to_string();

    hasher.update((text.len() as u64).to_le_bytes());
    hasher.update(text.as_bytes());
}

fn write_optional_decimal(hasher: &mut Sha256, value: Option<Decimal>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            write_decimal(hasher, value);
        }

        None => {
            hasher.update([0]);
        }
    }
}

fn write_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        }

        None => {
            hasher.update([0]);
        }
    }
}

fn write_level(hasher: &mut Sha256, level: &BoundaryBookLevel) {
    write_decimal(hasher, level.price);

    write_optional_decimal(hasher, level.quantity_base);
    write_optional_decimal(hasher, level.quantity_quote);
    write_optional_decimal(hasher, level.quantity_contracts);

    write_optional_u64(hasher, level.order_count);
}

/// Calculate a deterministic fingerprint of an exact boundary book state.
///
/// Bids and asks are encoded separately.
///
/// The fingerprint includes exact canonical quantities and order counts.
/// It does not include timestamps or source sequence identifiers.
pub fn fingerprint_book(state: &BoundaryBookState) -> String {
    let mut hasher = Sha256::new();

    hasher.update(FINGERPRINT_VERSION);

    hasher.update((state.bids.len() as u64).to_le_bytes());

    for level in &state.bids {
        hasher.update([0]);
        write_level(&mut hasher, level);
    }

    hasher.update((state.asks.len() as u64).to_le_bytes());

    for level in &state.asks {
        hasher.update([1]);
        write_level(&mut hasher, level);
    }

    format!("sha256:{:x}", hasher.finalize())
}
