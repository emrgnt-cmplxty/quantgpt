from unittest.mock import MagicMock

import pytest

from quantgpt.core.portfolio.processor import PortfolioProcessor
from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    convert_est_datestr_to_unix_timestamp,
)

strategy_config = {
    "config_name": "test_strategy",
    "trade_config": {
        "starting_cash": 10_000,
        "by_asset_class": {
            ft.AssetClass.US_EQUITY: {
                "type": ft.TradeType.SIMPLE_FIXED,
                "trade_size_in_dollars": 200,
                "holding_period": "5_days",
            }
        },
    },
}

data = {
    ft.AssetClass.US_EQUITY: {
        ft.DataType.DAILY_OHLC: {
            ft.Symbol("AAPL", ft.AssetClass.US_EQUITY): {"Open": 120},
            ft.Symbol("MSFT"): {"Open": 200},
            ft.Symbol("GOOGL"): {"Open": 1000},
        }
    }
}
timestamp = convert_est_datestr_to_unix_timestamp("2020-01-07")


@pytest.fixture
def portfolio():
    base_config = MagicMock()
    return PortfolioProcessor(base_config, strategy_config)


def test_update_position_and_cash(portfolio):
    aapl_symbol = ft.Symbol("AAPL")
    portfolio._update_position_and_cash(aapl_symbol, 10, 120)
    assert portfolio.cash == 10_000 - 10 * 120
    assert portfolio.open_positions_by_symbol[aapl_symbol].quantity == 10
    assert portfolio.open_positions_by_symbol[aapl_symbol].cost_basis == 120


def test_get_trade_size(portfolio):
    aapl_symbol = ft.Symbol("AAPL")
    weight = 1
    live_data = ft.LiveData(
        ft.TradingTimes.NYC_DAILY_OPEN,
        {
            aapl_symbol.asset_class: {
                ft.DataType.DAILY_OHLC: {aapl_symbol: {"Open": 120}}
            }
        },
    )

    trade_size = portfolio._get_trade_size(live_data, aapl_symbol, weight)
    assert trade_size == 1


def test_execute_signals(portfolio):
    portfolio.global_config = {"trading_times": ft.TradingTimes.NYC_DAILY_OPEN}

    aapl_symbol = ft.Symbol("AAPL")
    live_data = ft.LiveData(
        ft.TradingTimes.NYC_DAILY_OPEN,
        {
            aapl_symbol.asset_class: {
                ft.DataType.DAILY_OHLC: {aapl_symbol: {"Open": 120}}
            }
        },
    )

    signals = [
        ft.Signal(0, aapl_symbol, ft.SignalType("z_score_long"), 1, "test1")
    ]
    weight = 1
    trades = portfolio.execute_trades(0, live_data, signals, weight)
    positions = portfolio.open_positions_by_symbol
    assert len(trades) == 1
    assert trades[0].symbol == aapl_symbol
    assert len(positions.keys()) == 1
    assert positions[aapl_symbol].quantity == 1
    assert positions[aapl_symbol].cost_basis == 120


def test_get_open_positions(portfolio):
    assert len(portfolio.get_open_positions_by_asset_class()) == 0


def test_get_open_trades(portfolio):
    assert len(portfolio.get_open_trades()) == 0


def test_save_positions():
    class TestStrategyPortfolio(PortfolioProcessor):
        def __getitem__(self, key):
            return "N/A"

    mock_portfolio = MagicMock(spec=TestStrategyPortfolio)
    mock_portfolio.__getitem__.return_value = "N/A"
    _ = mock_portfolio.update_positions(0)
    mock_portfolio.save_positions()


def test_load_positions(portfolio):
    portfolio.load_positions(0)
