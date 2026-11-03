class MarketForgeError(Exception):
    """Base exception for all MarketForge errors."""


class AcquisitionError(MarketForgeError):
    """Base acquisition error."""


class UnsupportedExchangeError(AcquisitionError):
    pass


class UnsupportedMarketError(AcquisitionError):
    pass


class InstrumentNotFoundError(AcquisitionError):
    pass


class HistoricalDataNotFoundError(AcquisitionError):
    pass


class RateLimitError(AcquisitionError):
    pass


class DownloadError(AcquisitionError):
    pass
