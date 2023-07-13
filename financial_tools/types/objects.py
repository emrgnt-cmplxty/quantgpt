from typing import Dict, List, Optional, Tuple

import numpy as np

from ..untyped_utils import nested_dict
from .basic_types import StrategyName, Timestamp
from .enums import SignalType, TradeType
from .symbol import Symbol


# Classes for Signal, Position, and Trade
class Signal:
    def __init__(
        self,
        timestamp: Timestamp,
        symbol: Symbol,
        signal_type: SignalType,
        signal_strength: float,
        strategy_name: StrategyName,
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.signal_type = signal_type
        self.signal_strength = signal_strength
        self.strategy_name = strategy_name

    def __repr__(self) -> str:
        return f"Signal(timestamp={self.timestamp}, symbol={self.symbol}, signal_type={self.signal_type}, signal_strength={self.signal_strength}, strategy_name={self.strategy_name})"

    def copy(self) -> "Signal":
        return Signal(
            self.timestamp,
            self.symbol,
            self.signal_type,
            self.signal_strength,
            self.strategy_name,
        )


class Position:
    def __init__(self, symbol: Symbol, quantity: int, cost_basis: float) -> None:
        self.symbol = symbol
        self.quantity = quantity
        self.cost_basis = cost_basis

    def __repr__(self) -> str:
        return f"Position(symbol={self.symbol}, quantity={self.quantity}, cost_basis={self.cost_basis})"

    def copy(self) -> "Position":
        return Position(self.symbol, self.quantity, self.cost_basis)


class Trade:
    def __init__(
        self,
        timestamp: Timestamp,
        symbol: Symbol,
        quantity: int,
        limit_price: float,
        # Used to monitor the position and implement downstream actions, e.g. stop loss or fixed exit
        trade_type: TradeType,
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.quantity = quantity
        self.limit_price = limit_price
        self.trade_type = trade_type

    def __repr__(self) -> str:
        return f"Trade(timestamp={self.timestamp}, symbol={self.symbol}, quantity={self.quantity}, limit_price={self.limit_price})"

    def copy(self) -> "Trade":
        return Trade(self.timestamp, self.symbol, self.quantity, self.limit_price, self.trade_type)


class AggregatedPositions:
    """
    A class to represent aggregated positions
    """

    def __init__(self, positions: List[Tuple[str, List[Position]]]) -> None:
        self.raw_positions = positions
        self.aggregate()

    def aggregate(self) -> None:
        """
        Aggregates positions by asset class and symbol

        :param positions: List of tuples containing strategy name and position
        :return: Aggregated positions by asset class and symbol
        """
        aggregated_positions: Dict[Symbol, Position] = {}
        aggregated_positions_by_strategy: Dict[
            StrategyName, Dict[Symbol, Position]
        ] = nested_dict()

        for strategy, positions in self.raw_positions:
            for position in positions:
                symbol = position.symbol
                existing_position = aggregated_positions.get(symbol)

                updated_position = self.update_position(
                    existing_position,
                    position.copy(),
                )
                aggregated_positions[symbol] = updated_position
                updated_position_by_strategy = self.update_position(
                    aggregated_positions_by_strategy[strategy].get(symbol),
                    position.copy(),
                )
                aggregated_positions_by_strategy[strategy][symbol] = updated_position_by_strategy
        self.aggregated_positions_by_strategy = aggregated_positions_by_strategy
        self.aggregated_positions = aggregated_positions

    def update_position(self, aggregate: Optional[Position], position: Position) -> Position:
        """
        Update position in the given aggregate position.

        :param aggregate: The aggregate Position object.
        :param position: The Position object to update.
        :return: Updated Position object.
        """
        if aggregate:
            total_quantity = aggregate.quantity + position.quantity
            new_avg_price = (
                aggregate.cost_basis * aggregate.quantity + position.cost_basis * position.quantity
            ) / total_quantity
            return Position(
                position.symbol,
                total_quantity,
                new_avg_price,
            )
        else:
            return position

    def __repr__(self) -> str:
        return f"AggregatedPositions(aggregated_positions={self.aggregated_positions})"


class AggregatedTrades:
    """
    A class to represent aggregated trades
    """

    def __init__(self, trades: List[Tuple[StrategyName, List[Trade]]]) -> None:
        self.raw_trades = trades
        self.aggregate()

    def aggregate(self) -> None:
        """
        Aggregates trades by asset class and symbol

        :param trades: List of tuples containing strategy name and trade
        :return: Aggregated trades by asset class and symbol
        """
        aggregated_trades: Dict[Symbol, Trade] = {}
        aggregated_trades_by_strategy: Dict[StrategyName, Dict[Symbol, Trade]] = nested_dict()
        for strategy, trades in self.raw_trades:
            for trade in trades:
                symbol = trade.symbol

                updated_trade = self.update_trade(aggregated_trades.get(symbol), trade.copy())
                aggregated_trades[symbol] = updated_trade
                updated_trade_by_strategy = self.update_trade(
                    aggregated_trades_by_strategy[strategy].get(symbol),
                    trade.copy(),
                )
                aggregated_trades_by_strategy[strategy][symbol] = updated_trade_by_strategy
        self.aggregated_trades = aggregated_trades
        self.aggregated_trades_by_strategy = aggregated_trades_by_strategy

    def update_trade(self, aggregate: Optional[Trade], trade: Trade) -> Trade:
        """
        Update trade in the given aggregate trade.

        :param aggregate: The aggregate Trade object.
        :param trade: The Trade object to update.
        :return: Updated Trade object.
        """
        if aggregate:
            if aggregate.trade_type != trade.trade_type:
                raise ValueError(
                    f"Trade types do not match: {aggregate.trade_type} != {trade.trade_type}"
                )
            if np.sign(trade.quantity) != np.sign(
                aggregate.quantity
            ):  # signs are different, so net the trades out
                aggregate.quantity += trade.quantity
            else:
                # TODO - Add logic around the limit price. For now, we just assume it is always zero or infinite
                if (
                    trade.limit_price == np.inf
                    and trade.quantity > 0
                    or trade.limit_price == 0
                    and trade.quantity < 0
                ):
                    aggregate.quantity += trade.quantity
            return Trade(
                aggregate.timestamp,
                aggregate.symbol,
                aggregate.quantity,
                aggregate.limit_price,
                aggregate.trade_type,
            )
        else:
            return trade

    def __repr__(self) -> str:
        return f"AggregatedTrades(aggregated_trades={self.aggregated_trades})"
