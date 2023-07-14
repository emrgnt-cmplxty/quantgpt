from typing import Dict, List

from quantgpt.core.data.processor import DataProcessor
from quantgpt.core.portfolio.processor import PortfolioProcessor
from quantgpt.financial_tools import types as ft


class StrategyBase:
    def __init__(
        self,
        data_processor: DataProcessor,
        global_config: ft.GlobalConfig,
        strategy_config: ft.StrategyConfig,
    ) -> None:
        """
        Initialize the StrategyBase class with the data manager, configuration, and strategy configuration.

        :param data_processor: The data manager that handles data retrieval.
        :param config: A dictionary containing the configuration settings.
        :param strategy_config: A dictionary containing the strategy-specific settings.
        """
        self.data_processor = data_processor
        self.global_config = global_config
        self.strategy_config = strategy_config
        self.portfolio_processor = PortfolioProcessor(
            global_config, strategy_config
        )

    def generate_signals(
        self, timestamp: ft.Timestamp, observed_data: ft.ObservedData
    ) -> List[ft.Signal]:
        """
        Generate trade signals based on the given timestamp and data.
        This method should be implemented in the derived strategy class.

        :param timestamp: A timestamp corresponding to the date on which trade signals are generated.
        :param data: A list containing the data needed to generate trade signals.
        :raise NotImplementedError: If this method is not implemented in the derived class.
        """
        raise NotImplementedError(
            "generate_signals() must be implemented in the derived strategy class"
        )

    def get_open_positions_by_asset_class(
        self,
    ) -> Dict[ft.Symbol, ft.Position]:
        """
        Get the current positions for this strategy.

        :return: A dictionary containing the current positions.
        """
        return self.portfolio_processor.get_open_positions_by_asset_class()

    def get_open_trades(self) -> List[ft.Trade]:
        """
        Get the current positions for this strategy.

        :return: A dictionary containing the current positions.
        """
        return self.portfolio_processor.get_open_trades()

    def save(self) -> None:
        """
        Save the current state of the strategy, including positions.
        """
        self.portfolio_processor.save_positions()
