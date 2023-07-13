from typing import Any, Dict, Union
from unittest.mock import MagicMock

import pandas as pd
import pytest

from financial_tools import types as ft

from ..biotech_news import StrategyBiotechNews

# Custom Types
Release = Dict[str, Union[str, Any]]
Signal = Dict[str, Union[str, int]]


# Test data
@pytest.fixture
def data_processor():
    return MagicMock()


@pytest.fixture
def global_config():
    return {
        "some_global_config": "value",
        "trading_times": "NYC_DAILY_OPEN",
        "calendar": ft.TradingCalendar("2000-01-1", "2023-03-01", "00:00:00", "NYSE"),
    }


@pytest.fixture
def strategy_config():
    return {
        "name": "StrategyBiotechNews",
        "symbol_sources": [
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "provider": ft.DataProviderName.POLYGON,
                "list_name": "test_all",
            }
        ],
        "data_providers": [
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "data_type": ft.DataType.DAILY_OHLC,
                "provider": ft.DataProviderName.POLYGON,
            },
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "data_type": ft.DataType.NEWS,
                "provider": ft.DataProviderName.POLYGON,
            },
        ],
        "trade_config": {
            "starting_cash": 50000,
            ft.AssetClass.US_EQUITY: {
                "type": "simple_fixed",
                "trade_size_in_dollars": 12500,
                "holding_period": "30_days",
            },
        },
        "config_name": "test_strat",
        "specific_config": {
            "blacklisted_symbols": ["BLK1"],
            "text_filters": ["FILTER1", "FILTER2"],
            "min_prev_daily_volume": 100000,
            "max_prev_daily_volume": 1000000,
        },
    }


@pytest.fixture
def simple_biotech_news_instance(data_processor, global_config, strategy_config):
    return StrategyBiotechNews(data_processor, global_config, strategy_config)


# TODO - Revive these tests

# def test_check_relevance():
#     title = "This FILTER1 contains FILTER2"
#     text_filters = ["FILTER1", "FILTER2"]
#     assert _check_relevance(title, text_filters)

#     title = "This title contains only FILTER1"
#     assert _check_relevance(title, text_filters)

#     title = "This title contains no filters"
#     assert not _check_relevance(title, text_filters)


# def test_check_weighted_volume():
#     weighted_volume = 1500
#     min_vol = 1000
#     max_vol = 2000
#     assert _check_weighted_volume(weighted_volume, min_vol, max_vol)

#     weighted_volume = 500
#     assert not _check_weighted_volume(weighted_volume, min_vol, max_vol)

#     weighted_volume = 2500
#     assert not _check_weighted_volume(weighted_volume, min_vol, max_vol)


def test_generate_signals(simple_biotech_news_instance):
    # Prepare input data
    timestamp = 1234567890
    trailing_close = 1234501200  # midnight of previous day
    observed_data = {
        ft.AssetClass.US_EQUITY: {
            ft.DataType.DAILY_OHLC: {
                ft.Symbol("MRNA"): pd.DataFrame.from_dict({"Volume": [50_000], "Close": [10]}),
                ft.Symbol("IBB"): pd.DataFrame.from_dict({"Volume": [50_000], "Close": [10]}),
            },
            ft.DataType.NEWS: {
                # TODO - cleanup
                ft.Symbol("MRNA"): pd.DataFrame.from_dict(
                    {
                        "Title": ["This FILTER1 contains FILTER2"],
                        "Timestamp": [trailing_close],  # nearest mid
                    }
                ),
            },
        }
    }

    observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC][ft.Symbol("MRNA")][
        "Timestamp"
    ] = [timestamp]
    observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC][ft.Symbol("MRNA")].set_index(
        "Timestamp", inplace=True
    )

    # Run the method
    signals = simple_biotech_news_instance.generate_signals(timestamp, observed_data)

    # Assert the results
    assert len(signals) == 1
    assert signals[0].symbol == ft.Symbol("MRNA")
    assert signals[0].timestamp == timestamp
    assert signals[0].signal_strength == 1.0
    assert signals[0].signal_type == ft.SignalType("z_score_long")
    assert signals[0].strategy_name == "test_strat"
