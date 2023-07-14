import pandas as pd
import pytest

from quantgpt.core.performance.manager import PerformanceManager
from quantgpt.financial_tools import types as ft


@pytest.fixture
def global_config():
    return {
        "mode": ft.TradingMode.BACKTEST,
        "name": "test_strategy",
        "run_end_date": "2050-01-01",
    }


@pytest.fixture
def test_data():
    return {
        ft.AssetClass.US_EQUITY: {
            ft.DataType.DAILY_OHLC: {
                ft.Symbol("AAPL"): {
                    "Open": [110],
                    "Close": [120, 130],
                    "Timestamp": [1, 2],
                },
                ft.Symbol("GOOGL"): {
                    "Open": [90],
                    "Close": [130, 140],
                    "Timestamp": [1, 2],
                },
            }
        }
    }


@pytest.fixture
def test_positions():
    return {ft.Symbol("GOOGL"): ft.Position(ft.Symbol("GOOGL"), 110, 100)}


@pytest.fixture
def test_trades():
    return {
        ft.Symbol("AAPL"): ft.Trade(
            1, ft.Symbol("AAPL"), 100, float("inf"), ft.TradeType.SIMPLE_FIXED
        )
    }


def test_calculate_new_trade_pnl(global_config, test_data, test_trades):
    pm = PerformanceManager(global_config)

    new_trade_pnl = pm._calculate_new_trade_pnl(test_trades, test_data)
    assert new_trade_pnl[ft.Symbol("AAPL")] == 10 * 100


def test_calculate_positional_pnl(global_config, test_data, test_positions):
    pm = PerformanceManager(global_config)

    positional_pnl = pm._calculate_positional_pnl(test_positions, test_data)

    assert positional_pnl[ft.Symbol("GOOGL")] == 10 * 110


def test_calculate_pnl(global_config, test_data, test_positions, test_trades):
    pm = PerformanceManager(global_config)
    timestamp = 1
    next_timestamp = 2

    calculated_pnl_t, calculated_pnl_tp1 = pm._calculate_pnl(
        timestamp,
        next_timestamp,
        test_data,
        test_positions,
        test_trades,
    )

    assert calculated_pnl_t[ft.Symbol("AAPL")]["Timestamp"][0] == timestamp
    assert calculated_pnl_t[ft.Symbol("AAPL")]["NewTrade"][0] == 10 * 100
    assert calculated_pnl_t[ft.Symbol("AAPL")]["Positional"][0] == 0
    assert (
        calculated_pnl_tp1[ft.Symbol("GOOGL")]["Timestamp"][0]
        == next_timestamp
    )
    assert calculated_pnl_tp1[ft.Symbol("GOOGL")]["Positional"][0] == 10 * 110
    assert calculated_pnl_tp1[ft.Symbol("GOOGL")]["NewTrade"][0] == 0


def test_update_total_pnl(
    global_config, test_data, test_positions, test_trades
):
    pm = PerformanceManager(global_config)
    timestamp = 1
    next_timestamp = 2

    positions_by_asset_class = test_positions
    test_trades_by_asset_class = test_trades

    aggregated_positions_by_strategy = {
        "test_strategy": positions_by_asset_class
    }
    aggregated_trades_by_strategy = {
        "test_strategy": test_trades_by_asset_class
    }

    pm._update_total_pnl(
        timestamp,
        next_timestamp,
        test_data,
        positions_by_asset_class,
        aggregated_positions_by_strategy,
        test_trades_by_asset_class,
        aggregated_trades_by_strategy,
    )

    expected_df = pd.DataFrame(
        {
            "Timestamp": [timestamp, next_timestamp],
            "NewTrade": [10 * 100, 0],
            "Positional": [0, 10 * 110],
        }
    )

    # TODO - Add checks for pnl_by_strategy e.g. GOOGL, AAPL

    assert (
        pm._get_aggregated_pnl()
        .fillna(0)
        .reset_index(drop=True)
        .astype(expected_df.dtypes)
        .equals(expected_df)
    )

    assert (
        pm._get_aggregated_pnl_by_strategy()["test_strategy"]
        .fillna(0)
        .reset_index(drop=True)
        .astype(expected_df.dtypes)
        .equals(expected_df)
    )
