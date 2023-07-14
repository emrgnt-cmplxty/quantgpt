import logging
from typing import Dict, List, Tuple

from quantgpt.financial_tools import types as ft

logger = logging.getLogger(__name__)


class PortfolioManager:
    """
    A class to represent a portfolio manager
    """

    def __init__(self, global_config: ft.GlobalConfig) -> None:
        self.global_config = global_config
        self.aggregated_positions: Dict[ft.Symbol, ft.Position] = {}
        self.aggregated_trades: Dict[ft.Symbol, ft.Trade] = {}
        self.aggregated_positions_by_strategy: Dict[
            ft.StrategyName, Dict[ft.Symbol, ft.Position]
        ] = {}
        self.aggregated_trades_by_strategy: Dict[
            ft.StrategyName, Dict[ft.Symbol, ft.Trade]
        ] = {}

    def update(
        self,
        timestamp: ft.Timestamp,
        trades_by_strategy: List[Tuple[ft.StrategyName, List[ft.Trade]]],
        positions_by_strategy: List[Tuple[ft.StrategyName, List[ft.Position]]],
    ) -> None:
        """
        Update positions and trades for the portfolio manager

        :param date: Date of update
        :param all_trades: List of all trades
        :param all_positions: List of all positions
        """
        # self.scale_trades_and_positions(timestamp, trades_by_strategy, positions_by_strategy)

        # Aggregate the trades and positions
        logger.debug("trades_by_strategy = %s", trades_by_strategy)
        trade_aggregator = ft.AggregatedTrades(trades_by_strategy)
        position_aggregator = ft.AggregatedPositions(positions_by_strategy)

        # Update the positions and trades for the portfolio manager
        self.aggregated_positions = position_aggregator.aggregated_positions
        self.aggregated_positions_by_strategy = (
            position_aggregator.aggregated_positions_by_strategy
        )
        self.aggregated_trades = trade_aggregator.aggregated_trades

        self.aggregated_trades_by_strategy = (
            trade_aggregator.aggregated_trades_by_strategy
        )
