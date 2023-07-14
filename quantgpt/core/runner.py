import logging
import time
from typing import Any, Dict, List, Tuple

from quantgpt.core.data.processor import DataProcessor
from quantgpt.core.performance.manager import PerformanceManager
from quantgpt.core.portfolio.manager import PortfolioManager
from quantgpt.core.strategy.loader import StrategyType
from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    convert_time_delta_str,
    convert_timestamp_to_est_datetime,
)

logger = logging.getLogger(__name__)


class Mode:
    """
    The base class for different modes (e.g., backtesting, live trading) in the trading core.

    Attributes:
        data_processor (DataProcessor): An instance of the DataProcessor class to process the strategy data.
        strategies (List[StrategyType]): A list of instantiated strategies to be used in the mode.
        portfolio_manager (PortfolioManager): An instance of the PortfolioManager class to handle portfolio management.
        performance_manager (PerformanceManager): An instance of the PerformanceManager class to handle performance evaluation.
        allocation_table (Dict[Timestamp, Dict[str, float]]): A dictionary mapping timestamps to strategy allocations.
    """

    def __init__(
        self,
        global_config: ft.GlobalConfig,
        data_processor: DataProcessor,
        strategies: List[StrategyType],
        portfolio_manager: PortfolioManager,
        performance_manager: PerformanceManager,
        allocation_table: Dict[int, Dict[str, ft.AllocationEntry]] = {},
    ):
        self.global_config = global_config
        self.data_processor = data_processor
        self.strategies = strategies
        self.portfolio_manager = portfolio_manager
        self.performance_manager = performance_manager
        self.allocation_table = allocation_table
        self.process_timers = {
            "full_run": 0.0,
            "process_signals": 0.0,
            "process_strategies": 0.0,
            "portfolio_manager.update": 0.0,
            "performance_manager.update": 0.0,
        }

    def run(self) -> None:
        """Abstract method for running the mode. Must be implemented in derived classes."""
        raise NotImplementedError(
            "run() must be implemented in the derived mode class"
        )

    def save(self) -> None:
        """Save the state of each strategy."""
        for strategy in self.strategies:
            strategy.save()


class BacktestMode(Mode):
    """
    A class for running backtesting mode, derived from the Mode class.

    This class is responsible for iterating through the historical data and processing
    each timestamp by executing the strategies, updating the portfolio, and evaluating performance.
    """

    def run(self) -> None:
        """Run the backtesting mode."""

        for timestamp_data in self.data_processor.iter_data():
            self._process_timestamp(timestamp_data)
        logger.info(
            "The process timers for the BacktestMode logic read = ",
            self.process_timers,
        )

    def _process_timestamp(
        self,
        timestamp_data: Tuple[
            ft.Timestamp,
            ft.Timestamp,
            ft.ObservedData,
            ft.LiveData,
            ft.FutureData,
        ],
    ) -> None:
        """Process a single timestamp during backtesting."""
        run_start = time.time()

        (
            timestamp,
            next_timestamp,
            observed_data,
            mock_live_data,
            future_data,
        ) = timestamp_data
        logging.info(
            "Processing Timestamp: %s Which Corresponds to EST time %s",
            timestamp,
            convert_timestamp_to_est_datetime(timestamp),
        )
        # TODO - Make these more robust and uncomment at a later date
        # self._check_observed_data(observed_data)
        # self._check_live_data(mock_live_data)
        # self._check_future_data(timestamp, future_data)

        logging.debug(
            "On Timestamp %s, the mock_live_data is: %s",
            timestamp,
            mock_live_data,
        )
        now = time.time()
        signals_by_strategy = self._process_signals(timestamp, observed_data)
        self.process_timers["process_signals"] += time.time() - now

        now = time.time()
        trades_by_strategy, positions_by_strategy = self._process_strategies(
            timestamp, mock_live_data, signals_by_strategy
        )
        self.process_timers["process_strategies"] += time.time() - now

        now = time.time()
        self.portfolio_manager.update(
            timestamp, trades_by_strategy, positions_by_strategy
        )
        self.process_timers["portfolio_manager.update"] += time.time() - now

        now = time.time()
        self.performance_manager.update(
            timestamp, next_timestamp, future_data, self.portfolio_manager
        )
        self.process_timers["performance_manager.update"] += time.time() - now
        self.process_timers["full_run"] += time.time() - run_start

    def _process_signals(
        self, timestamp: ft.Timestamp, observed_data: ft.ObservedData
    ) -> List[Tuple[StrategyType, ft.StrategyName, List[ft.Signal]]]:
        """Generate signals for each strategy at the current timestamp."""

        signals_by_strategy: List[
            Tuple[StrategyType, ft.StrategyName, List[ft.Signal]]
        ] = []

        if timestamp not in self.allocation_table:
            raise ValueError(
                "The allocation_table must contain an entry for every day."
            )

        for strategy in self.strategies:
            strategy_name = strategy.strategy_config["table_name"]
            if strategy_name not in self.allocation_table[timestamp]:
                continue

            signals = strategy.generate_signals(timestamp, observed_data)
            signals_by_strategy.append((strategy, strategy_name, signals))

        return signals_by_strategy

    def _process_strategies(
        self,
        timestamp: ft.Timestamp,
        mock_live_data: ft.LiveData,
        signals_by_strategy: List[
            Tuple[StrategyType, ft.StrategyName, List[ft.Signal]]
        ],
    ) -> Tuple[
        List[Tuple[ft.StrategyName, List[ft.Trade]]],
        List[Tuple[ft.StrategyName, List[ft.Position]]],
    ]:
        """Process signals for each strategy and return trades and positions."""

        trades_by_strategy: List[Tuple[ft.StrategyName, List[ft.Trade]]] = []
        positions_by_strategy: List[
            Tuple[ft.StrategyName, List[ft.Position]]
        ] = []

        for strategy, strategy_name, signals in signals_by_strategy:
            trades, positions = self._execute_signals_and_update_positions(
                strategy, timestamp, mock_live_data, signals
            )

            if trades:
                trades_by_strategy.append((strategy_name, trades))

            if positions:
                positions_by_strategy.append((strategy_name, positions))

        return trades_by_strategy, positions_by_strategy

    def _execute_signals_and_update_positions(
        self, strategy: Any, timestamp: int, mock_live_data: Any, signals: Any
    ) -> Tuple[List[Any], List[Any]]:
        """Execute the signals, process trades, and update positions for the current strategy."""

        weight = self.allocation_table[timestamp][
            strategy.strategy_config["table_name"]
        ]["weight"]
        trades = strategy.portfolio_processor.execute_trades(
            timestamp, mock_live_data, signals, weight
        )
        positions = strategy.portfolio_processor.update_positions(timestamp)

        return trades, positions

    # TODO - Add timestamp checks to observed data
    def _check_observed_data(self, observed_data: ft.ObservedData) -> None:
        """
        Check that the observed data is not empty.

        Args:
            observed_data (Dict[str, List]): A dictionary mapping data types to lists of data.
        """
        expected_length = convert_time_delta_str(
            self.global_config["observed_data_lookback"], "to_days"
        )

        if not observed_data:
            raise ValueError("The observed data must not be empty.")
        for asset_type in observed_data:
            if not observed_data[asset_type]:
                raise ValueError(
                    f"The observed data for {asset_type} must not be empty."
                )
            for data_type in observed_data[asset_type]:
                if not observed_data[asset_type][data_type]:
                    logger.warning(
                        f"The observed data for {asset_type} and {data_type} was empty."
                    )
                for symbol in observed_data[asset_type][data_type]:
                    if data_type == ft.DataType.DAILY_OHLC:
                        observed_data_symbol = observed_data[asset_type][
                            data_type
                        ][symbol]
                        observed_length = len(observed_data_symbol)
                        if len(observed_data_symbol) != expected_length:
                            logger.warning(
                                f"The observed data for {symbol} was {observed_length} instead of {expected_length}."
                            )

    def _check_live_data(self, mock_live_data: ft.LiveData) -> None:
        pass

    def _check_future_data(
        self, timestamp: ft.Timestamp, future_data: ft.FutureData
    ) -> None:
        """
        Check that the observed data is not empty.

        Args:
            observed_data (Dict[str, List]): A dictionary mapping data types to lists of data.
        """
        expected_future_length = convert_time_delta_str(
            self.global_config["future_data_lookahead"], "to_days"
        )
        if not future_data:
            raise ValueError("The future data must not be empty.")
        for asset_type in future_data:
            if not future_data[asset_type]:
                raise ValueError(
                    f"The future data for {asset_type} must not be empty."
                )
            for data_type in future_data[asset_type]:
                if not future_data[asset_type][data_type]:
                    logger.warning(
                        f"The future data for {asset_type} and {data_type} was empty."
                    )
                for symbol in future_data[asset_type][data_type]:
                    if data_type == ft.DataType.DAILY_OHLC:
                        observed_data_symbol = future_data[asset_type][
                            data_type
                        ][symbol]
                        observed_timestamp = observed_data_symbol["Timestamp"][
                            0
                        ]
                        observed_future_data_length = int(
                            convert_time_delta_str(
                                self.global_config["future_data_lookahead"],
                                "to_days",
                            )
                        )
                        if (
                            observed_future_data_length
                            != expected_future_length
                        ):
                            logger.warning(
                                f"The future data for {symbol} was legnth {observed_future_data_length} instead of {expected_future_length}. This could be due to delisting."
                            )

                        if timestamp != observed_timestamp:
                            logger.warning(
                                f"The future data for {symbol} ended on timestamp {observed_timestamp} instead of {timestamp}.  This could be due to delisting or late listing."
                            )


class LiveMode(Mode):
    """
    This class is responsible for implementing the logic for live trading with multiple strategies.
    The implementation should handle processing real-time data, executing trades, and managing
    the portfolio according to the strategies' signals and positions.
    """

    def run(self) -> None:
        # Implement the logic for live trading with multiple strategies
        # This could include:
        # 1. For the LiveMode implementation, consider using a real-time data feed instead of historical data. This will likely require modifying the DataManager class to handle both historical and real-time data sources.
        # 2. Implement risk management features to control the exposure of the portfolio during live trading. This could include stop-loss orders, position sizing adjustments, and other risk controls.
        # 3. To handle dynamic strategy allocation and adaptation, consider implementing a mechanism to monitor and evaluate the strategies' performance during live trading. This could be based on rolling performance metrics or other criteria that allow for adjustments in the allocation and selection of strategies.
        # 4. Ensure the code is robust and can handle various scenarios, such as disconnections from the data feed, API rate limits, and order execution issues. Adding error handling and recovery mechanisms can help to mitigate these challenges.
        pass
