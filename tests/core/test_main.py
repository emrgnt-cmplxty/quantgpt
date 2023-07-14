import argparse

import pytest

import quantgpt.core as core
from quantgpt.core.main import main
from quantgpt.financial_tools import types as ft


@pytest.fixture
def mock_setup_argparse(mocker):
    mock_namespace = argparse.Namespace(
        global_config="core_configs_global_test_test_simple_biotech_news_v0p0",
        data_load_interval="30_days",
        start="2022-08-01",
        end="2022-10-01",
        log_level="INFO",
    )
    mocker.patch(
        "quantgpt.core.setup_argparse.setup_argparse",
        return_value=mock_namespace,
    )
    return mock_namespace


def test_main_function(mock_setup_argparse, mocker):
    # Mocking external functions and classes
    mocker.patch("quantgpt.core.main.ConfigBuilder")
    mocker.patch("quantgpt.core.main.DataManager")
    mocker.patch("quantgpt.core.main.DataProcessor")
    mocker.patch("quantgpt.core.main.load_strategy")
    mocker.patch("quantgpt.core.main.PortfolioManager")
    mocker.patch("quantgpt.core.main.PerformanceManager")
    mocker.patch("quantgpt.core.main.BacktestMode")
    mocker.patch("quantgpt.core.main.LiveMode")

    def global_config_getitem_side_effect(key):
        config_values = {
            "mode": ft.TradingMode.BACKTEST,
            "allocation_table": "value_1",
            "key_2": "value_2",
            # Add more key-value pairs as needed
        }
        return config_values.get(key, None)

    # Instantiate the mocked classes
    mock_global_config = core.main.ConfigBuilder.return_value
    mock_global_config.build_global_config.return_value = {
        "mode": ft.TradingMode.BACKTEST,
        "allocation_table": None,
        "run_start_date": None,
        "run_end_date": None,
        "data_start_date": None,
        "data_end_date": None,
        "strategy_configs": {},
    }
    mock_data_processor = core.main.DataProcessor.return_value
    mock_portfolio_manager = core.main.PortfolioManager.return_value
    mock_performance_manager = core.main.PerformanceManager.return_value
    mock_backtest_mode = core.main.BacktestMode.return_value

    # Call the main function with the mocked argparse.Namespace
    main(mock_setup_argparse)

    # Assert the expected methods are called with the proper arguments
    core.main.ConfigBuilder.assert_called_once_with(
        mock_setup_argparse, mocker.ANY
    )
    core.main.DataProcessor.assert_called_once_with(mocker.ANY, mocker.ANY)
    core.main.PortfolioManager.assert_called_once_with(mocker.ANY)
    core.main.PerformanceManager.assert_called_once_with(mocker.ANY)
    core.main.BacktestMode.assert_called_once_with(
        {
            "mode": ft.TradingMode.BACKTEST,
            "allocation_table": None,
            "run_start_date": None,
            "run_end_date": None,
            "data_start_date": None,
            "data_end_date": None,
            "strategy_configs": {},
        },
        mock_data_processor,
        mocker.ANY,
        mock_portfolio_manager,
        mock_performance_manager,
        mocker.ANY,
    )
    core.main.LiveMode.assert_not_called()

    # Assert the run() and save() methods are called on the framework_mode object
    mock_backtest_mode.run.assert_called_once()
    mock_backtest_mode.save.assert_called_once()
