import argparse

import pytest

import framework
from financial_tools import types as ft

from ..main import main


@pytest.fixture
def mock_setup_argparse(mocker):
    mock_namespace = argparse.Namespace(
        global_config="framework_configs_global_test_test_simple_biotech_news_v0p0",
        data_load_interval="30_days",
        start="2022-08-01",
        end="2022-10-01",
        log_level="INFO",
    )
    mocker.patch(
        "framework.setup_argparse.setup_argparse", return_value=mock_namespace
    )
    return mock_namespace


def test_main_function(mock_setup_argparse, mocker):
    # Mocking external functions and classes
    mocker.patch("framework.main.ConfigBuilder")
    mocker.patch("framework.main.DataManager")
    mocker.patch("framework.main.DataProcessor")
    mocker.patch("framework.main.load_strategy")
    mocker.patch("framework.main.PortfolioManager")
    mocker.patch("framework.main.PerformanceManager")
    mocker.patch("framework.main.PerformancePlotter")
    mocker.patch("framework.main.BacktestMode")
    mocker.patch("framework.main.LiveMode")

    def global_config_getitem_side_effect(key):
        config_values = {
            "mode": ft.TradingMode.BACKTEST,
            "allocation_table": "value_1",
            "key_2": "value_2",
            # Add more key-value pairs as needed
        }
        return config_values.get(key, None)

    # Instantiate the mocked classes
    mock_global_config = framework.main.ConfigBuilder.return_value
    mock_global_config.build_global_config.return_value = {
        "mode": ft.TradingMode.BACKTEST,
        "allocation_table": None,
        "run_start_date": None,
        "run_end_date": None,
        "data_start_date": None,
        "data_end_date": None,
        "strategy_configs": {},
    }
    mock_data_processor = framework.main.DataProcessor.return_value
    mock_portfolio_manager = framework.main.PortfolioManager.return_value
    mock_performance_manager = framework.main.PerformanceManager.return_value
    mock_performance_plotter = framework.main.PerformancePlotter.return_value
    mock_backtest_mode = framework.main.BacktestMode.return_value

    # Call the main function with the mocked argparse.Namespace
    main(mock_setup_argparse)

    # Assert the expected methods are called with the proper arguments
    framework.main.ConfigBuilder.assert_called_once_with(
        mock_setup_argparse, mocker.ANY
    )
    framework.main.DataProcessor.assert_called_once_with(
        mocker.ANY, mocker.ANY
    )
    framework.main.PortfolioManager.assert_called_once_with(mocker.ANY)
    framework.main.PerformanceManager.assert_called_once_with(mocker.ANY)
    framework.main.PerformancePlotter.assert_called_once_with(
        mock_performance_manager
    )
    framework.main.BacktestMode.assert_called_once_with(
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
        mock_performance_plotter,
        mocker.ANY,
    )
    framework.main.LiveMode.assert_not_called()

    # Assert the run() and save() methods are called on the framework_mode object
    mock_backtest_mode.run.assert_called_once()
    mock_backtest_mode.save.assert_called_once()
