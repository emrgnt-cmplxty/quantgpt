# conftest.py
import pytest

from quantgpt.financial_tools import types as ft


@pytest.fixture(scope="module")
def global_config():
    return ft.GlobalConfig(
        name="Test",
        mode=ft.TradingMode.BACKTEST,  # Replace with the appropriate TradingMode value
        trading_times=...,  # Replace with the appropriate TradingTimes value
        data_start_date="2021-12-01",
        data_end_date="2021-03-01",
        run_start_date="2022-01-01",
        run_end_date="2022-02-01",
        calendar_name="NYSE",
        delta_to_close_timestamp="00:00:00",
        max_cores=1,
        observed_data_lookback="1_days",
        future_data_lookahead="1_days",
        allocation_table={
            0: [
                {
                    "test_simple_biotech_news_v0p0": {
                        "weight": 1,
                        "config_path": {
                            "base": "core",
                            "interior": "config",
                            "config_type": "strategy",
                            "prod_type": "test",
                            "name": "test_simple_biotech_news_v0p0",
                        },
                    }
                },
            ]
        },
    )


@pytest.fixture(scope="module")
def allocation_table():
    allocation_table = {
        0: {
            "A": ft.AllocationEntry(
                {"weight": 0.5, "path": ft.ConfigPath(name="A")}
            )
        },
        1: {
            "A": ft.AllocationEntry(
                {"weight": 0.5, "path": ft.ConfigPath(name="A")}
            )
        },
        2: {
            "A": ft.AllocationEntry(
                {"weight": 0.5, "path": ft.ConfigPath(name="A")}
            )
        },
    }
    return allocation_table
