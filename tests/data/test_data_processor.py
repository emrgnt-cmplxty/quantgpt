import pytest
from quantgpt.core.data.manager import DataManager
from quantgpt.core.data.processor import DataProcessor
from quantgpt.financial_tools import types as ft

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
def data_processor():
    data_manager = DataManager(GLOBAL_CONFIG, LOGGING_CONFIG)
    data_processor = DataProcessor(data_manager, GLOBAL_CONFIG)
    return data_processor


def test_iter_data(data_processor):
    data_iter = data_processor.iter_data()

    assert hasattr(data_iter, "__iter__")
    assert hasattr(data_iter, "__next__")

    for timestamp, prev_data, cur_data, next_data in data_iter:
        assert isinstance(timestamp, int)
        assert isinstance(prev_data, dict)
        assert isinstance(cur_data, dict)
        assert isinstance(next_data, dict)
