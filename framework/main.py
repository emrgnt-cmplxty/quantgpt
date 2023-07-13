import argparse
import json
import logging.config
import time
from typing import List
from unittest.mock import MagicMock

from financial_tools.types import (
    EnumEncoder,
    TradingMode,
    prepare_json_for_dump,
)
from financial_tools.utils import get_logging_config
from framework.config.builder import ConfigBuilder
from framework.data.manager import DataManager
from framework.performance.manager import PerformanceManager
from framework.performance.plotter import PerformancePlotter
from framework.portfolio.manager import PortfolioManager
from framework.runner import BacktestMode, LiveMode
from framework.setup_argparse import setup_argparse
from framework.strategy.loader import StrategyType, load_strategy

from .data.processor import DataProcessor


def main(args: argparse.Namespace) -> None:
    """
    Main function for running the trading framework.

    Args:
        args (argparse.Namespace): Command line arguments parsed by argparse.
        log_level (str, optional): Logging level. Defaults to "INFO".
    """
    logging_config = get_logging_config(args.log_level)
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    config_builder = ConfigBuilder(args, logger)

    global_config = config_builder.build_global_config()
    allocation_table = global_config["allocation_table"]

    # Log global config if not running tests
    if not isinstance(global_config, MagicMock):
        logger.warning(
            f"Running with global_config =\n{json.dumps(prepare_json_for_dump(global_config), indent=2, cls=EnumEncoder)}"
        )

    logger.info(
        "Running backtest over date range %s to %s",
        global_config["run_start_date"],
        global_config["run_end_date"],
    )
    logger.info(
        "Consuming data over date range %s to %s",
        global_config["data_start_date"],
        global_config["data_end_date"],
    )

    run_start_time = time.time()

    data_manager = DataManager(global_config, logging_config)
    data_processor = DataProcessor(data_manager, global_config)

    strategies: List[StrategyType] = [
        load_strategy(strategy_config, global_config, data_processor)
        for _, strategy_config in global_config["strategy_configs"].items()
    ]
    portfolio_manager = PortfolioManager(global_config)
    performance_manager = PerformanceManager(global_config)
    performance_plotter = PerformancePlotter(performance_manager)
    framework_mode = (
        BacktestMode(
            global_config,
            data_processor,
            strategies,
            portfolio_manager,
            performance_manager,
            performance_plotter,
            allocation_table,
        )
        if global_config["mode"] == TradingMode.BACKTEST
        else LiveMode(
            global_config,
            data_processor,
            strategies,
            portfolio_manager,
            performance_manager,
            performance_plotter,
            allocation_table,
        )
    )

    framework_mode.run()
    framework_mode.save()
    if isinstance(config_builder, MagicMock) or isinstance(
        framework_mode, BacktestMode
    ):
        performance_manager.generate_report()
        performance_plotter.plot()

    logger.warning(f"Runtime: {time.time() - run_start_time} seconds")


if __name__ == "__main__":
    args = setup_argparse()
    main(args)
