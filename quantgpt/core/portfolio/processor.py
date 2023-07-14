import logging
import os
from enum import Enum
from typing import Dict, List, Optional, Tuple, cast

import pandas as pd

from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    convert_time_delta_str,
    convert_timestamp_to_est_datetime,
    home_path,
)

logger = logging.getLogger(__name__)


class PortfolioProcessor:
    """
    A class that represents a trading portfolio.
    """

    class Action(Enum):
        OPEN = 1
        CLOSE = 2

    def __init__(
        self,
        global_config: ft.GlobalConfig,
        strategy_config: ft.StrategyConfig,
    ):
        self.global_config = global_config
        self.strategy_config = strategy_config
        self.trade_config = strategy_config["trade_config"]
        self.starting_cash = self.trade_config["starting_cash"]
        self.cash: float = self.starting_cash
        # Keep open_positions_by_symbol in a dictionary to efficiently compress multiple positions into one
        self.open_positions_by_symbol: Dict[ft.Symbol, ft.Position] = {}
        self.open_trades: List[ft.Trade] = []
        self.positions_df: pd.DataFrame = pd.DataFrame()

    def execute_trades(
        self,
        timestamp: ft.Timestamp,
        live_data: ft.LiveData,
        signals: List[ft.Signal],
        weight: float,
    ) -> List[ft.Trade]:
        adj_open_trades, trades = [], []
        # Generate new open trades from signals
        for signal in signals:
            asset_class = signal.symbol.asset_class
            properties = self.trade_config["by_asset_class"][asset_class]

            if properties["type"] == ft.TradeType.SIMPLE_FIXED:
                holding_period = convert_time_delta_str(
                    cast(str, properties["holding_period"])
                )

                is_buy_signal = (
                    signal.signal_type == ft.SignalType.Z_SCORE_LONG
                )
                is_sell_signal = (
                    signal.signal_type == ft.SignalType.Z_SCORE_SHORT
                )

                if not (is_buy_signal or is_sell_signal):
                    continue

                sign = 1 if is_buy_signal else -1
                trade_size = self._get_trade_size(
                    live_data, signal.symbol, weight
                )
                if trade_size == 0:
                    continue
                quantity = int(sign * trade_size * signal.signal_strength)
                # Open new position

                new_trade = self._process_position(
                    self.Action.OPEN,
                    ft.Trade(
                        timestamp,
                        signal.symbol,
                        quantity,
                        float("inf") if is_buy_signal else 0,
                        cast(ft.TradeType, properties["type"]),
                    ),
                    live_data,
                )
                trades.append(new_trade)
                adj_open_trades.append(new_trade)

            else:
                raise ValueError(
                    "Invalid trade type: %s" % (properties["type"])
                )

        # Close out expired positions
        for trade in self.open_trades:
            asset_class = trade.symbol.asset_class
            properties = self.trade_config["by_asset_class"][asset_class]

            if properties["type"] == ft.TradeType.SIMPLE_FIXED:
                holding_period = convert_time_delta_str(
                    cast(str, properties["holding_period"])
                )

                if timestamp >= trade.timestamp + holding_period:
                    closed_trade = self._process_position(
                        self.Action.CLOSE, trade, live_data
                    )
                    trades.append(closed_trade)
                else:
                    adj_open_trades.append(trade)
            else:
                raise ValueError(
                    "Invalid trade type: %s" % (properties["type"])
                )

        self.open_trades = adj_open_trades
        return trades

    def update_positions(
        self,
        timestamp: ft.Timestamp,
    ) -> List[ft.Position]:
        """
        Update portfolio positions after executing trades.

        :param timestamp: The timestamp of the trade
        :return: A list of updated positions
        """

        if len(self.open_positions_by_symbol) > 0:
            logger.info(
                "{} - Positions: {}".format(
                    self.strategy_config["config_name"],
                    self.open_positions_by_symbol,
                )
            )

        def get_positions_list() -> List[ft.Position]:
            positions_list = []
            for symbol in self.open_positions_by_symbol:
                position = self.open_positions_by_symbol[symbol]
                positions_list.append(position)
            return positions_list

        self._update_positions_df(timestamp)
        return get_positions_list()

    def get_open_trades(self) -> List[ft.Trade]:
        """
        Retrieve the current positions in the portfolio.
        """

        return self.open_trades

    def get_open_positions_by_asset_class(
        self,
    ) -> Dict[ft.Symbol, ft.Position]:
        """
        Retrieve the current positions in the portfolio.
        """
        return self.open_positions_by_symbol

    def save_positions(self) -> None:
        """
        Save the portfolio positions to a CSV file.
        """
        mode = self.global_config["mode"]
        config_name = self.strategy_config["config_name"]
        out_location = (
            f"{home_path()}/results/positions/{mode.value}/{config_name}.csv"
        )
        # make directories if they don't exist
        os.makedirs(os.path.dirname(out_location), exist_ok=True)
        logger.info(f"Saving Portfolio Positions to {out_location}")
        if len(self.positions_df) > 0:
            # Add the ESTDateStr column for readability
            self.positions_df["ESTDateStr"] = [
                convert_timestamp_to_est_datetime(ele)
                for ele in self.positions_df["Timestamp"]
            ]
        self.positions_df.to_csv(out_location, index=False)

    def load_positions(self, timestamp: ft.Timestamp) -> None:
        """
        Load positions from a saved CSV file. To be implemented.
        """
        # TODO - Implement loading positions from a CSV file

    def _update_position_and_cash(
        self,
        trade_symbol: ft.Symbol,
        trade_quantity: int,
        trade_price: Optional[float],
    ) -> None:
        """
        Update the position and cash after a trade is executed.

        :param trade_symbol: The symbol of the traded asset (e.g., 'AAPL')
        :param trade_quantity: The number of shares traded
        :param trade_price: The price at which the trade was executed, or None if the trade was not executed
        """
        # ft.Trade failed to execute for one reason ro another, there is no update to positions or cash
        if trade_price is None:
            return None

        # The first time we are executing a trade of this asset class, initialize the dictionary

        # If we already have a position in this asset, update the position
        if trade_symbol in self.open_positions_by_symbol:
            existing_position = self.open_positions_by_symbol[trade_symbol]
            existing_quantity = existing_position.quantity
            existing_avg_price = existing_position.cost_basis
            new_quantity = trade_quantity + existing_quantity
            # If the new quantity is 0, delete the position
            if new_quantity == 0:
                del self.open_positions_by_symbol[trade_symbol]
            # Otherwise, update the position
            else:
                updated_avg_price = (
                    existing_avg_price * existing_quantity
                    + trade_price * trade_quantity
                ) / new_quantity
                existing_position.quantity = new_quantity
                existing_position.cost_basis = updated_avg_price

        else:
            # If we don't have a position in this asset, create a new position
            position = ft.Position(trade_symbol, trade_quantity, trade_price)
            self.open_positions_by_symbol[trade_symbol] = position

        # Update the cash
        self.cash -= trade_quantity * trade_price

    def _get_trade_size(
        self, live_data: ft.LiveData, symbol: ft.Symbol, weight: float
    ) -> int:
        """
        Calculate the trade size based on the given data and asset class.

        :param timestamp: The timestamp of the trade
        :param live_data: The live data used calculate the trade size
        :param symbol: The symbol of the traded asset (e.g., 'AAPL')
        :return: The calculated trade size
        """

        trade_config = self.trade_config["by_asset_class"][symbol.asset_class]
        trade_type, trade_size = trade_config["type"], float(
            trade_config["trade_size_in_dollars"]
        )
        live_symbol_data = live_data.data[symbol.asset_class][
            ft.DataType.DAILY_OHLC
        ][symbol]
        if (
            "Open"
            not in live_data.data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                symbol
            ]
        ):
            logger.error(
                "LiveData was missing Open for symbol: {}".format(symbol)
            )
            return 0
        if trade_type == ft.TradeType.SIMPLE_FIXED:
            trade_size = int(
                weight * trade_size / float(live_symbol_data["Open"])
            )
            return trade_size
        else:
            raise ValueError(f"Invalid trade type: {trade_type}")

    def _is_valid_trade(
        self, trade_price: Optional[float], quantity: int, limit_price: float
    ) -> bool:
        return trade_price is not None and (
            (quantity > 0 and trade_price <= limit_price)
            or (quantity < 0 and trade_price >= limit_price)
        )

    def _get_trade_details(
        self, trade: ft.Trade, live_data: ft.LiveData
    ) -> Optional[Tuple[ft.Symbol, int, Optional[float]]]:
        """
        Obtain trade details based on the given trade and input data.
        """
        symbol, quantity, limit_price = (
            trade.symbol,
            trade.quantity,
            trade.limit_price,
        )
        trading_times = self.global_config["trading_times"]
        trade_price = None
        if trading_times == ft.TradingTimes.NYC_DAILY_OPEN:
            # When trading at the open, the trade price is the open price
            trade_price = float(
                live_data.data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                    symbol
                ]["Open"]
            )
        else:
            raise ValueError(f"Invalid trading times: {trading_times}")
        return (
            (symbol, quantity, trade_price)
            if self._is_valid_trade(trade_price, quantity, limit_price)
            else None
        )

    def _process_position(
        self, action: Action, trade: ft.Trade, live_data: ft.LiveData
    ) -> ft.Trade:
        trade_details = self._get_trade_details(trade, live_data)

        if trade_details is not None:
            symbol, quantity, trade_price = trade_details

            if action == self.Action.OPEN:
                self._update_position_and_cash(symbol, quantity, trade_price)
            elif action == self.Action.CLOSE:
                self._update_position_and_cash(symbol, -quantity, trade_price)
            else:
                raise ValueError(f"Invalid action: {action}")

        return trade

    def _update_positions_df(self, timestamp: ft.Timestamp) -> None:
        """
        Update the positions DataFrame with the latest positions.
        """
        positions_to_print = {}
        for symbol in self.open_positions_by_symbol:
            positions_to_print[symbol.value] = {
                "Symbol": symbol.value,
                "Quantity": self.open_positions_by_symbol[symbol].quantity,
                "AvgPrice": self.open_positions_by_symbol[symbol].cost_basis,
            }
        positions_df = pd.DataFrame.from_dict(
            positions_to_print, orient="index"
        )
        positions_df.index.name = "asset"
        positions_df["Timestamp"] = timestamp
        self.positions_df = pd.concat(
            [self.positions_df, positions_df], sort=True
        )
