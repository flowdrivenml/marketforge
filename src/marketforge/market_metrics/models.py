from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class InstrumentMetrics(BaseModel):
    """Canonical current-market metrics for one instrument."""

    model_config = ConfigDict(frozen=True)

    instrument_id: int = Field(gt=0)

    last_price: Decimal | None = None

    high_24h: Decimal | None = None
    low_24h: Decimal | None = None
    price_change_pct_24h: Decimal | None = None

    volume_24h_native: Decimal | None = None
    volume_24h_base: Decimal | None = None
    volume_24h_quote: Decimal | None = None
    volume_24h_contracts: Decimal | None = None

    turnover_24h: Decimal | None = None
    turnover_denomination: str | None = None

    open_interest: Decimal | None = None
    open_interest_value: Decimal | None = None

    funding_rate: Decimal | None = None

    measured_at: datetime

    @model_validator(mode="after")
    def validate_metrics(self) -> InstrumentMetrics:
        non_negative = (
            "last_price",
            "high_24h",
            "low_24h",
            "volume_24h_native",
            "volume_24h_base",
            "volume_24h_quote",
            "volume_24h_contracts",
            "turnover_24h",
            "open_interest",
            "open_interest_value",
        )

        for field_name in non_negative:
            value = getattr(self, field_name)

            if value is not None and value < 0:
                raise ValueError(f"{field_name} cannot be negative")

        if self.turnover_24h is not None and self.turnover_denomination is None:
            raise ValueError(
                "turnover_denomination is required when " "turnover_24h is present"
            )

        return self


class InstrumentMarketView(BaseModel):
    """
    Catalog instrument enriched with its current market metrics.

    Used for market discovery and instrument selection.
    """

    model_config = ConfigDict(frozen=True)

    instrument_id: int = Field(gt=0)

    exchange: str
    symbol: str

    instrument_type: str
    market_category: str

    base_asset: str | None = None
    quote_asset: str | None = None
    settlement_asset: str | None = None

    status: str

    last_price: Decimal | None = None

    high_24h: Decimal | None = None
    low_24h: Decimal | None = None
    price_change_pct_24h: Decimal | None = None

    volume_24h_native: Decimal | None = None
    volume_24h_base: Decimal | None = None
    volume_24h_quote: Decimal | None = None
    volume_24h_contracts: Decimal | None = None

    turnover_24h: Decimal | None = None
    turnover_denomination: str | None = None

    open_interest: Decimal | None = None
    open_interest_value: Decimal | None = None

    funding_rate: Decimal | None = None

    measured_at: datetime | None = None
