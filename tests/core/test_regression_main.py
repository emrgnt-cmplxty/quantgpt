import os
import sys
from glob import glob
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from quantgpt.core.config.builder import ConfigBuilder
from quantgpt.core.main import main
from quantgpt.core.setup_argparse import setup_argparse
from quantgpt.financial_tools.utils import (
    convert_est_datestr_to_unix_timestamp,
    home_path,
)


def check_target_files(global_config, file_type="pnl", max_date=""):
    """
    Check if the target files are equal.
    """
    global_config_name = global_config["name"]

    target_dir = os.path.join(
        home_path(), "data", "regressions", global_config_name, file_type
    )
    if file_type == "positions":
        target_dir = os.path.join(target_dir, "backtest")
    regression_file_paths = glob(os.path.join(target_dir, "*.csv"))
    assert len(regression_file_paths) > 0, "No regression files found"
    checked_strategy, checked_aggregate = False, False
    for file_path in regression_file_paths:
        for strategy in global_config["strategy_configs"]:
            if file_type == "pnl":
                expected_suffix = f"{strategy}_{global_config_name}_PnL.csv"
            elif file_type == "positions":
                expected_suffix = f"{strategy}.csv"
            if expected_suffix in file_path:
                regression_file = pd.read_csv(file_path)
                if max_date != "":
                    regression_file = regression_file[
                        regression_file["Timestamp"]
                        < convert_est_datestr_to_unix_timestamp(max_date)
                    ]
                file_name = os.path.split(file_path)[-1]

                new_file_path = os.path.join(home_path(), "results", file_type)
                if file_type == "positions":
                    new_file_path = os.path.join(new_file_path, "backtest")
                new_file = pd.read_csv(os.path.join(new_file_path, file_name))

                # Check if files are equal
                assert regression_file.equals(
                    new_file
                ), "PnL files are not equal"
                checked_strategy = True
        # Check aggregate pnl
        if file_type == "pnl" and "Aggregated" in file_path:
            regression_file = pd.read_csv(file_path)
            file_name = os.path.split(file_path)[-1]
            new_file = pd.read_csv(
                os.path.join(home_path(), "results", file_type, file_name)
            )

            if max_date != "":
                regression_file = regression_file[
                    regression_file["Timestamp"]
                    < convert_est_datestr_to_unix_timestamp(max_date)
                ]

            # Check if files are equal
            assert regression_file.equals(new_file), "PnL files are not equal"
            checked_aggregate = True

    if not checked_strategy:
        raise AssertionError("Did not check strategy PnL.")
    if file_type == "pnl" and not checked_aggregate:
        raise AssertionError("Did not check aggregate PnL.")


def run_main_with_args(args_list):
    # Set up the test environment by mocking the command line arguments
    with patch.object(sys, "argv", args_list):
        # Get the command line arguments
        args = setup_argparse()
        main(args)
        # Create a mock logger and configure it using the command line arguments
        logger = MagicMock()
        config_builder = ConfigBuilder(args, logger)
        global_config = config_builder.build_global_config()

    return global_config


def run_tests(global_config, max_date=""):
    check_target_files(global_config, file_type="pnl", max_date=max_date)
    check_target_files(global_config, file_type="positions", max_date=max_date)


@pytest.mark.regression
def test_main_regression_subset():
    args_list = [
        "your_script_name",
        "--mode",
        "backtest",
        "--global_config_path_str",
        "quantgpt_core_config_global_test_test_simple_biotech_news_v0p0",
        "--start",
        "2022-08-01",
        "--end",
        "2022-09-01",
        "--log_level",
        "ERROR",
    ]
    global_config = run_main_with_args(args_list)
    run_tests(global_config, max_date="2022-09-01")
