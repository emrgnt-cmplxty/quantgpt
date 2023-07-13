from unittest.mock import MagicMock

import pytest

from financial_tools import types as ft

from ..base import StrategyBase


@pytest.fixture
def global_config():
    return {"some_global_config": "value", "trading_times": "NYC_DAILY_OPEN"}


@pytest.fixture
def strategy_config():
    return {
        "name": "StrategyBiotechNews",
        "symbol_sources": [
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "provider": ft.DataProviderName.YAHOO,
                "list_name": "test_all",
            }
        ],
        "data_providers": [
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "data_type": ft.DataType.DAILY_OHLC,
                "provider": ft.DataProviderName.YAHOO,
            },
            {
                "asset_class": ft.AssetClass.US_EQUITY,
                "data_type": ft.DataType.NEWS,
                "provider": ft.DataProviderName.YAHOO,
            },
        ],
        "trade_config": {
            "starting_cash": 50000,
            ft.AssetClass.US_EQUITY: {},
        },
        "config_name": "test_all",
    }


class TestStrategyBase:
    @pytest.fixture
    def base_strategy(self, global_config, strategy_config) -> StrategyBase:
        data_processor_mock = MagicMock()
        return StrategyBase(data_processor_mock, global_config, strategy_config)

    def test_generate_signals_raises_not_implemented_error(self, base_strategy):
        with pytest.raises(NotImplementedError):
            base_strategy.generate_signals(1234, {})

    def test_get_open_positions_returns_dict(self, base_strategy):
        positions = base_strategy.get_open_positions_by_asset_class()
        assert isinstance(positions, dict)

    def test_get_open_trades_returns_list(self, base_strategy):
        trades = base_strategy.get_open_trades()
        assert isinstance(trades, list)

    def test_save_calls_portfolio_save_positions(self, base_strategy):
        base_strategy.portfolio_processor.save_positions = MagicMock()
        base_strategy.save()
        assert base_strategy.portfolio_processor.save_positions.called
