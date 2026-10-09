use crate::canonical::Quantity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevel {
    pub quantity_base: Option<Quantity>,
    pub quantity_quote: Option<Quantity>,
    pub quantity_contracts: Option<Quantity>,
    pub order_count: Option<u64>,
}

impl BookLevel {
    pub fn is_zero(&self) -> bool {
        [
            self.quantity_base,
            self.quantity_quote,
            self.quantity_contracts,
        ]
        .into_iter()
        .flatten()
        .all(|quantity| quantity.is_zero())
    }
}
