from typing import Dict, cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

from quantgpt.core.portfolio.manager import PortfolioManager
from quantgpt.core.runner import BacktestMode
from quantgpt.core.strategy.base import StrategyBase
from quantgpt.financial_tools import types as ft

ASSET_CLASS = ft.AssetClass.US_EQUITY
SYMBOL_AAPL = ft.Symbol("AAPL")
DATA_TYPE = ft.DataType.DAILY_OHLC

# Test data
observed_data: ft.ObservedData = {
    ASSET_CLASS: {
        DATA_TYPE: {
            SYMBOL_AAPL: pd.DataFrame.from_dict({"Timestamp": [0]}),
        }
    }
}
future_data: ft.FutureData = {
    ASSET_CLASS: {
        DATA_TYPE: {
            SYMBOL_AAPL: pd.DataFrame.from_dict({"Timestamp": [2]}),
        }
    }
}


def create_mock_strategy() -> StrategyBase:
    """Create a mock StrategyBase object."""
    strategy = MagicMock(spec=StrategyBase)
    strategy.strategy_config = MagicMock()
    strategy.portfolio_processor = MagicMock()
    return strategy


def test_backtest_mode_run(global_config, allocation_table) -> None:
    """Test the run method of BacktestMode."""
    # Set up mocks
    data_processor = MagicMock()
    data_processor.iter_data.return_value = [
        (1, 2, observed_data, {}, future_data)
    ]

    strategies = [create_mock_strategy()]
    strategies[0].strategy_config = {"config_name": "A", "table_name": "A"}  # type: ignore

    portfolio_manager = MagicMock(spec=PortfolioManager)
    performance_manager = MagicMock()

    # Run the test
    backtest_mode = BacktestMode(
        global_config,
        data_processor,
        strategies,  # type: ignore
        portfolio_manager,
        performance_manager,
        allocation_table,
    )

    backtest_mode.run()
    strategy_mock = cast(MagicMock, strategies[0])
    data_processor.iter_data.assert_called_once()
    strategy_mock.generate_signals.assert_called_once()
    strategy_mock.portfolio_processor.execute_trades.assert_called_once()
    strategy_mock.portfolio_processor.update_positions.assert_called_once()
    portfolio_manager.update.assert_called_once()
    performance_manager.update.assert_called_once()


def test_empty_data(global_config, allocation_table) -> None:
    """Test BacktestMode's run method with empty data."""
    # Set up mocks
    data_processor = MagicMock()
    data_processor.iter_data.return_value = []

    strategies = [create_mock_strategy()]

    portfolio_manager = MagicMock()
    performance_manager = MagicMock()

    backtest_mode = BacktestMode(
        global_config,
        data_processor,
        strategies,  # type: ignore
        portfolio_manager,
        performance_manager,
        allocation_table,
    )
    backtest_mode.run()

    strategy_mock = cast(MagicMock, strategies[0])
    data_processor.iter_data.assert_called_once()
    strategy_mock.generate_signals.assert_not_called()
    strategy_mock.portfolio_processor.execute_trades.assert_not_called()
    strategy_mock.portfolio_processor.update_positions.assert_not_called()
    portfolio_manager.update.assert_not_called()
    performance_manager.update.assert_not_called()


# Test the run method of BacktestMode with an empty allocation_table
def test_backtest_mode_run_empty_allocation_table(global_config) -> None:
    data_processor = MagicMock()
    data_processor.iter_data.return_value = [
        (
            0,
            1,
            observed_data,
            {},
            future_data,
        )
    ]
    strategyA = create_mock_strategy()
    strategyA.strategy_config = {"config_name": "A", "table_name": "A"}  # type: ignore

    strategies = [strategyA]

    portfolio_manager = MagicMock()
    performance_manager = MagicMock()

    allocation_table_empty: Dict[
        ft.Timestamp, Dict[str, ft.AllocationEntry]
    ] = {}

    # Run the test
    backtest_mode = BacktestMode(
        global_config,
        data_processor,
        strategies,  # type: ignore
        portfolio_manager,
        performance_manager,
        allocation_table_empty,
    )

    with pytest.raises(
        ValueError,
        match="The allocation_table must contain an entry for every day.",
    ):
        backtest_mode.run()
