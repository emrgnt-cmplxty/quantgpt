import argparse
import logging

import pytest

from financial_tools import types as ft

from ..builder import ConfigBuilder


@pytest.fixture
def logger():
    logger = logging.getLogger()
    return logger


@pytest.fixture
def args():
    args = argparse.Namespace()
    args.mode = ft.TradingMode.BACKTEST
    args.start = "2022-01-01"
    args.end = "2022-02-05"
    args.global_config_path_str = (
        "framework_config_global_test_test_simple_biotech_news_v0p0"
    )
    args.data_lookahead_days = "5_days"
    args.future_data_lookahead = "1_days"
    return args


@pytest.fixture
def args_generate():
    args = argparse.Namespace()
    args.mode = ft.TradingMode.BACKTEST
    args.start = "2022-01-01"
    args.end = "2022-02-05"
    args.global_config_path_str = (
        "framework_config_global_test_test_crossover_v0p0"
    )
    args.data_lookahead_days = "5_days"
    args.future_data_lookahead = "1_days"
    return args


def test_build_global_config(args, logger):
    config_builder = ConfigBuilder(args, logger)
    global_config = ConfigBuilder.build_global_config(config_builder)

    assert global_config["mode"] == ft.TradingMode.BACKTEST
    assert global_config["data_start_date"] == "2021-11-02"
    assert global_config["data_end_date"] == "2022-02-10"
    assert global_config["run_start_date"] == "2022-01-01"
    assert global_config["run_end_date"] == "2022-02-05"

    # Add more assertions based on the test_global_config.yaml content
    # Examples:
    assert global_config["name"] == "test_simple_biotech_news_v0p0"
    assert global_config["calendar"].name == "NYSE"
    assert global_config["delta_to_close_timestamp"] == "00:00:00"
    assert global_config["max_cores"] == 8
    assert global_config["observed_data_lookback"] == "30_days"
    assert global_config["future_data_lookahead"] == "1_days"


def test_load_configurations(args, logger):
    config_builder = ConfigBuilder(args, logger)
    config_builder.build_global_config()
    (
        strategy_configs,
        symbols_dict,
        providers_dict,
    ) = config_builder.load_configurations()

    # Add assertions to check if the loaded configurations are as expected
    # This will depend on your specific test configuration files
    # Example: check if the correct strategy configs are loaded
    assert "test_simple_biotech_news_v0p0" in strategy_configs
    assert "test_simple_biotech_news_v0p1" in strategy_configs

    # Example: check if the correct symbols are loaded for each asset class
    assert ft.AssetClass.US_EQUITY in symbols_dict
    # TODO - Why do we need to cast .value for this to work???
    assert ft.Symbol("ACRX").value in [
        ele.value for ele in symbols_dict[ft.AssetClass.US_EQUITY]
    ]
    # Example: check if the correct data providers are loaded for each asset class and data type
    assert ft.AssetClass.US_EQUITY in providers_dict
    assert ft.DataType.DAILY_OHLC in providers_dict[ft.AssetClass.US_EQUITY]
    assert (
        ft.DataProviderName.TEST_POLYGON
        in providers_dict[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC]
    )


def test_build_allocation_table(args, logger):
    config_builder = ConfigBuilder(args, logger)
    config_builder.build_global_config()
    config_builder.build_allocation_table()

    # Add assertions to check if the built allocation table is as expected
    # This will depend on your specific test allocation config
    first_key = list(config_builder.allocation_table.keys())[0]
    assert (
        "test_simple_biotech_news_v0p0"
        in config_builder.allocation_table[first_key]
    )
    assert (
        "test_simple_biotech_news_v0p1"
        in config_builder.allocation_table[first_key]
    )
    assert len(config_builder.allocation_table) > 0
    assert (
        config_builder.allocation_table[first_key][
            "test_simple_biotech_news_v0p0"
        ]["weight"]
        == 1.0
    )
    assert (
        config_builder.allocation_table[first_key][
            "test_simple_biotech_news_v0p1"
        ]["weight"]
        == 2.0
    )


def test_generate_allocation_table(args_generate, logger):
    config_builder = ConfigBuilder(args_generate, logger)
    config_builder.build_global_config()

    # Add assertions to check if the generated allocation table is as expected
    # This will depend
    # Check if the generated allocation table has the correct weights
    first_key = list(config_builder.allocation_table.keys())[0]
    assert (
        config_builder.allocation_table[first_key]["test_crossover_v0p0"][
            "weight"
        ]
        == 1
    )
    assert (
        config_builder.allocation_table[first_key]["test_crossover_v0p0"][
            "path"
        ].get_name()
        == "test_crossover_v0p0"
    )
