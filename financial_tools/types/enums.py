from enum import Enum


class AssetClass(Enum):
    US_EQUITY = "us_equity"

    def __str__(self):
        return self.value

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return any(value == asset_class.value for asset_class in cls)

    def __repr__(self):
        return f"<AssetClass {self.value}>"

    def __hash__(self):
        return hash((self.value))

    def __eq__(self, other):
        if not isinstance(other, AssetClass):
            return NotImplemented
        return self.value == other.value


class DataProviderName(Enum):
    POLYGON = "polygon"
    TEST_POLYGON = "test_polygon"
    YAHOO = "yahoo"
    TEST_YAHOO = "test_yahoo"
    BIOPHARMCATALYST = "biopharmcatalyst"
    TEST_BIOPHARMCATALYST = "test_biopharmcatalyst"

    def __str__(self):
        return self.value


class ConfigType(Enum):
    ALLOCATION = "allocation"
    GLOBAL = "global"
    STRATEGY = "strategy"

    def __str__(self):
        return self.value


class ProdType(Enum):
    TEST = "test"
    DEV = "dev"
    PROD = "prod"

    def __str__(self):
        return self.value


class DataType(Enum):
    DAILY_OHLC = "daily_ohlc"
    FINANCIALS = "financials"
    NEWS = "news"
    SYMBOLS = "symbols"

    def __str__(self):
        return self.value


class DBConnections(Enum):
    CSV = "csv"
    SQLITE = "sqlite"

    def __str__(self):
        return self.value


class SignalType(Enum):
    Z_SCORE_LONG = "z_score_long"
    Z_SCORE_SHORT = "z_score_short"

    def __str__(self):
        return self.value


class TradeType(Enum):
    SIMPLE_FIXED = "simple_fixed"

    def __str__(self):
        return self.value


class TradingMode(Enum):
    BACKTEST = "backtest"
    LIVE = "Live"

    def __str__(self):
        return self.value


class TradingTimes(Enum):
    NYC_DAILY_OPEN = "nyc_daily_open"
    NYC_DAILY_CLOSE = "nyc_daily_close"
    NYC_INTRADAY = "nyc_intraday"

    def __str__(self):
        return self.value
