from collections import defaultdict

import pandas as pd
import pytest

from financial_tools import types as ft
from framework.data.manager import DataManager

# Prepare mock data
GLOBAL_CONFIG = {
    "strategy_configs": [],
    "data_providers": {
        ft.AssetClass.US_EQUITY: {
            ft.DataType.DAILY_OHLC: {
                ft.DataProviderName.YAHOO: ft.DataType.DAILY_OHLC
            }
        }
    },
    "db_connections": {ft.DataProviderName.YAHOO: "csv"},
    "symbols": {ft.AssetClass.US_EQUITY: [ft.Symbol("AAPL")]},
    "data_start_date": "2021-01-01",
    "data_end_date": "2021-01-05",
    "run_start_date": "2021-01-01",
    "run_end_date": "2021-01-05",
    "max_cores": 1,
    "calendar": ft.TradingCalendar(
        "2000-01-1", "2023-03-01", "00:00:00", "NYSE"
    ),
    "delta_to_close_timestamp": "00:00:00",
    "observed_data_lookback": "30_days",
    "future_data_lookahead": "30_days",
}

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "simple": {
            "format": "{levelname} - {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": "INFO",
        },
    },
    "loggers": {
        "data_manager": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
}


@pytest.fixture
def data_manager():
    return DataManager(GLOBAL_CONFIG, LOGGING_CONFIG)


def test_data_manager_initialization(data_manager):
    assert data_manager.global_config == GLOBAL_CONFIG
    assert data_manager.strategy_configs == GLOBAL_CONFIG["strategy_configs"]
    assert data_manager.logging_config == LOGGING_CONFIG
    assert isinstance(data_manager.timers, defaultdict)
    assert isinstance(data_manager.data_dict, dict)


def test_fetch_data(data_manager):
    data_type = ft.DataType.DAILY_OHLC
    provider_name = ft.DataProviderName.POLYGON
    symbol = ft.Symbol("AAPL")
    db_connection = "csv"

    result = data_manager._fetch_data(
        data_type, provider_name, symbol, db_connection
    )

    assert isinstance(result, pd.DataFrame)


def test_filter_df(data_manager):
    data_type = ft.DataType.DAILY_OHLC
    market_timestamps = [1609459200, 1609545600, 1609632000]
    df = pd.DataFrame({"Timestamp": market_timestamps})

    result = data_manager._filter_df(data_type, market_timestamps, df)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(df)


def test_append_data(data_manager):
    asset_class = ft.AssetClass.US_EQUITY
    data_type = ft.DataType.DAILY_OHLC
    symbol = ft.Symbol("AAPL")
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    df = pd.DataFrame(
        {
            "Timestamp": [1609459200],
            "Open": [100],
            "High": [101],
            "Low": [99],
            "Close": [100.5],
            "Volume": [1000],
        }
    )

    data_manager._append_data(data_type, symbol, data, df)

    assert 1609459200 in data
    assert asset_class in data[1609459200]
    assert data_type in data[1609459200][asset_class]
    assert symbol in data[1609459200][asset_class][data_type]
    payload = data[1609459200][asset_class][data_type][symbol]
    assert payload["Open"] == 100
    assert payload["High"] == 101
    assert payload["Low"] == 99
    assert payload["Close"] == 100.5
    assert payload["Volume"] == 1000
