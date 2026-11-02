from __future__ import annotations

from .binance import RAW_FORMATS as BINANCE_RAW_FORMATS
from .bitget import RAW_FORMATS as BITGET_RAW_FORMATS
from .bybit import RAW_FORMATS as BYBIT_RAW_FORMATS
from .gateio import RAW_FORMATS as GATEIO_RAW_FORMATS
from .okx import RAW_FORMATS as OKX_RAW_FORMATS

RAW_FORMATS = {
    "binance": BINANCE_RAW_FORMATS,
    "bitget": BITGET_RAW_FORMATS,
    "bybit": BYBIT_RAW_FORMATS,
    "gateio": GATEIO_RAW_FORMATS,
    "okx": OKX_RAW_FORMATS,
}
