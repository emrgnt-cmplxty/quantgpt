import logging
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from financial_tools import types as ft
from financial_tools.constants import ANNUALIZATION_FACTOR, RISK_FREE_RATE
from financial_tools.utils import (
    convert_est_datestr_to_unix_timestamp,
    convert_timestamp_to_est_datetime,
    home_path,
)

logger = logging.getLogger(__name__)


class CustomDefaultDict(defaultdict):
    def __missing__(self, key):
        self[key] = defaultdict(pd.DataFrame)
        return self[key]


class PerformanceManager:
    def __init__(self, global_config: ft.GlobalConfig):
        """
        Initialize performance evaluation variables and data structures.

        Args:
        - config (Dict): Configuration settings.
        """
        self.global_config = global_config
        # PnL by symbol, to aggregate call _get_aggregated_pnl()
        self.pnl_by_symbol: Dict[ft.Symbol, pd.DataFrame] = {}
        # PnL by symbol, by strategy, to aggregate over symbols call get_aggregated_pnl_by_strategy()
        self.pnl_by_strategy_by_symbol: Dict[
            ft.StrategyName, Dict[ft.Symbol, pd.DataFrame]
        ] = CustomDefaultDict()

    def update(
        self,
        timestamp: ft.Timestamp,
        next_timestamp: ft.Timestamp,
        future_data: ft.FutureData,
        portfolio_manager,
    ) -> None:
        """
        Update performance evaluation data based on portfolio manager's trades and positions.

        Args:
        - future_data (ft.FutureData): Lookahead data for PnL calculation.
        - portfolio_manager: Instance of PortfolioManager class.
        """
        aggregated_positions = portfolio_manager.aggregated_positions
        aggregated_positions_by_strategy = (
            portfolio_manager.aggregated_positions_by_strategy
        )
        aggregated_trades = portfolio_manager.aggregated_trades
        aggregated_trades_by_strategy = (
            portfolio_manager.aggregated_trades_by_strategy
        )

        self._update_total_pnl(
            timestamp,
            next_timestamp,
            future_data,
            aggregated_positions,
            aggregated_positions_by_strategy,
            aggregated_trades,
            aggregated_trades_by_strategy,
        )

    def generate_report(self) -> None:
        """
        Calculate performance metrics and generate a report.
        """

        def calculate_performance_metrics(
            pnl_df: pd.DataFrame, strategy_name: str = "", symbol: str = ""
        ) -> None:
            """
            Calculate and log the performance metrics.

            Args:
            - pnl_df (pd.DataFrame): DataFrame containing PnL data.
            - strategy_name (str): Name of the strategy (default: empty string).
            """
            PnL = pnl_df["Positional"].fillna(0) + pnl_df["NewTrade"].fillna(0)
            positional_sum = pnl_df["Positional"].fillna(0).sum()
            new_trade_sum = pnl_df["NewTrade"].fillna(0).sum()
            annualized_avg_return = np.mean(PnL) * ANNUALIZATION_FACTOR
            annualized_std_dev = np.std(PnL) * np.sqrt(ANNUALIZATION_FACTOR)
            sharpe_ratio = (
                annualized_avg_return - RISK_FREE_RATE
            ) / annualized_std_dev
            cumulative_returns = np.cumsum(PnL)
            max_drawdown = np.max(
                np.maximum.accumulate(cumulative_returns) - cumulative_returns
            )
            symbol_str = f"for {symbol}" if symbol else ""

            if strategy_name:
                logger.warning(
                    f"--> Strategy {strategy_name} Results {symbol_str} <--"
                )
            else:
                logger.warning("--> Portfolio Results {symbol_str} <--")

            logger.warning(
                f"Annualized Average Return -> {annualized_avg_return}"
            )
            logger.warning(
                f"Annualized Standard Deviation -> {annualized_std_dev}"
            )
            logger.warning(f"Sharpe Ratio -> {sharpe_ratio}")
            logger.warning(f"Max Drawdown -> {max_drawdown}")
            logger.warning("-- PnL --")
            logger.warning(f"Positional -> {positional_sum}")
            logger.warning(f"NewTrade -> {new_trade_sum}")
            logger.warning(f"Total -> {cumulative_returns.iloc[-1]}")

        def save_pnl_data(df: pd.DataFrame, path: str) -> None:
            """
            Save PnL data to the specified path.

            Args:
            - df (pd.DataFrame): DataFrame containing PnL data.
            - path (str): Path to save the file.
            """
            logger.info(f"Saving PnL to {path}")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_csv(path, index=False)

        def process_individual_symbols() -> None:
            """
            Process performance metrics for individual symbols.
            """
            logging.info("- Now analyzing by global performance - ")

            for symbol in self.pnl_by_symbol.keys():
                out_location = os.path.join(
                    home_path(),
                    "results",
                    "pnl",
                    f'{symbol.value}_{self.global_config["name"]}_PnL.csv',
                )
                self.pnl_by_symbol[symbol] = self._aggregate_pnl_by_timestamp(
                    self.pnl_by_symbol[symbol]
                )
                self.pnl_by_symbol[symbol]["ESTDateStr"] = [
                    convert_timestamp_to_est_datetime(ele)
                    for ele in self.pnl_by_symbol[symbol]["Timestamp"]
                ]
                save_pnl_data(self.pnl_by_symbol[symbol], out_location)
                self.pnl_by_symbol[symbol].drop(
                    ["ESTDateStr"], axis=1, inplace=True
                )
                calculate_performance_metrics(
                    self.pnl_by_symbol[symbol], symbol=symbol.value
                )

        def process_strategy_performance() -> None:
            """
            Process performance metrics for strategy performance.
            """
            logging.info("- Now analyzing strategy performance - ")

            for strategy in self.pnl_by_strategy_by_symbol:
                for symbol in self.pnl_by_strategy_by_symbol[strategy]:
                    out_location = os.path.join(
                        home_path(),
                        "results",
                        "pnl",
                        f'{symbol.value}_{strategy}_{self.global_config["name"]}_PnL.csv',
                    )
                    self.pnl_by_strategy_by_symbol[strategy][
                        symbol
                    ] = self._aggregate_pnl_by_timestamp(
                        self.pnl_by_strategy_by_symbol[strategy][symbol]
                    )
                    self.pnl_by_strategy_by_symbol[strategy][symbol][
                        "ESTDateStr"
                    ] = [
                        convert_timestamp_to_est_datetime(ele)
                        for ele in self.pnl_by_strategy_by_symbol[strategy][
                            symbol
                        ]["Timestamp"]
                    ]

                    save_pnl_data(
                        self.pnl_by_strategy_by_symbol[strategy][symbol],
                        out_location,
                    )

                    calculate_performance_metrics(
                        self.pnl_by_strategy_by_symbol[strategy][symbol],
                        strategy,
                        symbol.value,
                    )
                    self.pnl_by_strategy_by_symbol[strategy][symbol].drop(
                        ["ESTDateStr"], axis=1, inplace=True
                    )

        def process_aggregate_strategy_performance() -> None:
            aggregated_strategy_pnl: Dict[
                ft.StrategyName, pd.DataFrame
            ] = defaultdict(pd.DataFrame)
            aggregate_pnl = 0
            for strategy in self.pnl_by_strategy_by_symbol:
                for symbol in self.pnl_by_strategy_by_symbol[strategy]:
                    if not aggregated_strategy_pnl[strategy].empty:
                        # Merge dataframes on Timestamp with an outer join
                        merged_df = pd.merge(
                            aggregated_strategy_pnl[strategy],
                            self._aggregate_pnl_by_timestamp(
                                self.pnl_by_strategy_by_symbol[strategy][
                                    symbol
                                ]
                            )[["Timestamp", "NewTrade", "Positional"]],
                            on="Timestamp",
                            how="outer",
                        )
                        aggregate_pnl += self.pnl_by_strategy_by_symbol[
                            strategy
                        ][symbol]["NewTrade"].sum()
                        aggregate_pnl += self.pnl_by_strategy_by_symbol[
                            strategy
                        ][symbol]["Positional"].sum()
                        # Fill missing values with 0 and calculate the sums
                        merged_df["NewTrade_x"].fillna(0, inplace=True)
                        merged_df["NewTrade_y"].fillna(0, inplace=True)
                        merged_df["Positional_x"].fillna(0, inplace=True)
                        merged_df["Positional_y"].fillna(0, inplace=True)
                        merged_df["NewTrade"] = (
                            merged_df["NewTrade_x"] + merged_df["NewTrade_y"]
                        )
                        merged_df["Positional"] = (
                            merged_df["Positional_x"]
                            + merged_df["Positional_y"]
                        )

                        # Drop unnecessary columns and update the aggregated_strategy_pnl
                        merged_df.drop(
                            columns=[
                                "NewTrade_x",
                                "NewTrade_y",
                                "Positional_x",
                                "Positional_y",
                            ],
                            inplace=True,
                        )
                        aggregated_strategy_pnl[strategy] = merged_df.copy()
                    else:
                        aggregated_strategy_pnl[
                            strategy
                        ] = self.pnl_by_strategy_by_symbol[strategy][symbol][
                            ["Timestamp", "NewTrade", "Positional"]
                        ].copy()
                        aggregated_strategy_pnl[strategy]["NewTrade"].fillna(
                            0, inplace=True
                        )
                        aggregated_strategy_pnl[strategy]["Positional"].fillna(
                            0, inplace=True
                        )

                    aggregated_strategy_pnl[
                        strategy
                    ] = self._aggregate_pnl_by_timestamp(
                        aggregated_strategy_pnl[strategy]
                    )

                out_location = os.path.join(
                    home_path(),
                    "results",
                    "pnl",
                    f'{strategy}_{self.global_config["name"]}_Aggregated_PnL.csv',
                )

                aggregated_strategy_pnl[
                    strategy
                ] = self._aggregate_pnl_by_timestamp(
                    aggregated_strategy_pnl[strategy]
                )
                aggregated_strategy_pnl[strategy]["ESTDateStr"] = [
                    convert_timestamp_to_est_datetime(ele)
                    for ele in aggregated_strategy_pnl[strategy]["Timestamp"]
                ]

                save_pnl_data(aggregated_strategy_pnl[strategy], out_location)

                calculate_performance_metrics(
                    aggregated_strategy_pnl[strategy], f"Aggregated {strategy}"
                )

        process_individual_symbols()
        process_strategy_performance()
        process_aggregate_strategy_performance()

    def _calculate_new_trade_pnl(
        self,
        trades: Dict[ft.Symbol, ft.Trade],
        future_data: ft.FutureData,
    ) -> Dict[ft.Symbol, float]:
        """
        Calculate NewTrade PnL for a given asset class.

        Args:
        - trades (Dict[ft.Symbol, ft.Trade]): Aggregated trades made for each symbol.
        - future_data (ft.FutureData): Lookahead data for PnL calculation.

        Returns:
        - new_trade_pnl (float): NewTrade PnL.
        """
        pnl_by_symbol = {}

        for symbol in trades.keys():
            trade = trades[symbol]
            # Calculate trade PnL
            returns_on_day = float(
                future_data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                    symbol
                ]["Close"][0]
            ) - float(
                future_data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                    symbol
                ]["Open"][0]
            )
            pnl_by_symbol[symbol] = returns_on_day * float(trade.quantity)

        return pnl_by_symbol

    def _calc_aggregated_pnl(self, pnl_obj) -> pd.DataFrame:
        # Combine all DataFrames into a single DataFrame
        df_list = []
        for symbol, df in pnl_obj.items():
            df["Symbol"] = symbol
            df_list.append(df)

        combined_df = pd.concat(df_list, axis=0, ignore_index=True, sort=False)
        combined_df = combined_df.set_index("Timestamp")
        # Specify data types of columns
        dtype_dict = {
            "NewTrade": float,
            "Positional": float,
            "Symbol": "category",
        }
        combined_df = combined_df.astype(dtype_dict)

        combined_df = pd.concat(df_list, axis=0, ignore_index=True, sort=False)

        combined_df = combined_df.set_index("Timestamp")
        # Calculate total PnL by summing over all rows grouped by ft.Timestamp
        total_pnl = combined_df.groupby(
            level=0,
        ).sum(numeric_only=True)

        # This returns the total PnL for each timestamp
        return total_pnl.reset_index()

    def _calculate_positional_pnl(
        self,
        positions: Dict[ft.Symbol, ft.Position],
        future_data: ft.FutureData,
    ) -> Dict[ft.Symbol, float]:
        """
        Calculate Positional PnL for a given asset class.

        Args:
        - positions (Dict[ft.Symbol, ft.Position]): Aggregated positions held for each symbol.
        - future_data (ft.FutureData): Lookahead data for PnL calculation.

        Returns:
        - positional_pnl (float): Positional PnL.
        """
        pnl_by_symbol = {}

        for symbol in positions.keys():
            position = positions[symbol]

            # Calculate position PnL
            close_diff = (
                future_data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                    symbol
                ]["Close"][1]
                - future_data[symbol.asset_class][ft.DataType.DAILY_OHLC][
                    symbol
                ]["Close"][0]
            )
            pnl_by_symbol[symbol] = close_diff * position.quantity

        return pnl_by_symbol

    def _calculate_pnl(
        self,
        timestamp: ft.Timestamp,
        next_timestamp: ft.Timestamp,
        future_data: ft.FutureData,
        positions: Dict[ft.Symbol, ft.Position],
        trades: Dict[ft.Symbol, ft.Trade],
    ) -> Tuple[
        Dict[ft.Symbol, Dict[str, List[float]]],
        Dict[ft.Symbol, Dict[str, List[float]]],
    ]:
        """
        Calculate local PnL for a given asset class.

        Args:
        - timestamp (int): ft.Timestamp corresponding to current date.
        - next_timestamp (int): ft.Timestamp corresponding to next date.
        - future_data (ft.FutureData): Lookahead data for PnL calculation.
        - positions (Dict[ft.Symbol, ft.Position]): Aggregated positions held for each symbol.
        - trades (Dict[ft.Symbol, ft.Trade]): Aggregated trades made for each symbol.

        Returns:
        - calculated_pnl (Dict[str, List[float]]): Dictionary containing PnL data.
        """
        new_trade_pnl_t = self._calculate_new_trade_pnl(trades, future_data)

        positional_pnl_tp1 = self._calculate_positional_pnl(
            positions, future_data
        )

        if len(trades) > 0:
            logger.info("Observed Trades = %s" % (trades))
        calculated_pnl_t: Dict[ft.Symbol, Dict[str, List[float]]] = {
            symbol: {
                "Timestamp": [timestamp],
                "NewTrade": [new_trade_pnl_t[symbol]],
                "Positional": [0],
            }
            for symbol in new_trade_pnl_t.keys()
        }
        calculated_pnl_tp1: Dict[ft.Symbol, Dict[str, List[float]]] = {
            symbol: {
                "Timestamp": [next_timestamp],
                "NewTrade": [0],
                "Positional": [positional_pnl_tp1[symbol]],
            }
            for symbol in positional_pnl_tp1.keys()
        }

        return calculated_pnl_t, calculated_pnl_tp1

    def _aggregate_pnl_by_timestamp(
        self, pnl_df: pd.DataFrame
    ) -> pd.DataFrame:
        timestamp_dict = {}
        columns = [col for col in pnl_df.columns if col != "Timestamp"]

        for _, row in pnl_df.iterrows():
            ts = row["Timestamp"]
            if ts not in timestamp_dict:
                timestamp_dict[ts] = {"Timestamp": ts}
                for col in columns:
                    timestamp_dict[ts][col] = 0

            for col in columns:
                if not pd.isna(row[col]):
                    timestamp_dict[ts][col] += row[col]
                else:
                    # Check if the current column in the dictionary has a valid value before performing addition
                    if not pd.isna(timestamp_dict[ts][col]):
                        timestamp_dict[ts][col] += 0
                    else:
                        timestamp_dict[ts][col] = 0

        return pd.DataFrame.from_dict(list(timestamp_dict.values()))  # type: ignore

    def _update_total_pnl(
        self,
        timestamp: ft.Timestamp,
        next_timestamp: ft.Timestamp,
        future_data: ft.FutureData,
        positions: Dict[ft.Symbol, ft.Position],
        aggregated_positions_by_strategy: Dict[
            ft.StrategyName, Dict[ft.Symbol, ft.Position]
        ],
        trades: Dict[ft.Symbol, ft.Trade],
        aggregated_trades_by_strategy: Dict[
            ft.StrategyName, Dict[ft.Symbol, ft.Trade]
        ],
    ) -> None:
        """
        Update performance evaluation data for total portfolio.

        Args:
        - timestamp (int): ft.Timestamp corresponding to current date.
        - next_timestamp (int): ft.Timestamp corresponding to next date.
        - future_data (ft.FutureData): Lookahead data for PnL calculation.
        - positions (Dict[ft.Symbol, ft.Position]): Positions held for each symbol.
        - aggregated_positions_by_strategy (Dict[str, Dict[ft.Symbol, ft.Position]]): Aggregated positions by strategy.
        - trades (Dict[ft.Symbol, ft.Trade]): Trades made for each symbol.
        - aggregated_trades_by_strategy (Dict[str, Dict[ft.Symbol, ft.Trade]]): Aggregated trades by strategy.
        """

        calculated_pnl_t, calculated_pnl_tp1 = self._calculate_pnl(
            timestamp,
            next_timestamp,
            future_data,
            positions,
            trades,
        )

        for symbol in calculated_pnl_t:
            if symbol not in self.pnl_by_symbol:
                self.pnl_by_symbol[symbol] = pd.DataFrame.from_dict(
                    calculated_pnl_t[symbol]
                )
            else:
                self.pnl_by_symbol[symbol] = pd.concat(
                    [
                        self.pnl_by_symbol[symbol],
                        pd.DataFrame.from_dict(calculated_pnl_t[symbol]),
                    ],
                    ignore_index=True,
                )

        # check that pnl_tp1 is not past the end of the backtest
        if next_timestamp < convert_est_datestr_to_unix_timestamp(
            self.global_config["run_end_date"]
        ):
            for symbol in calculated_pnl_tp1:
                if symbol not in self.pnl_by_symbol:
                    self.pnl_by_symbol[symbol] = pd.DataFrame.from_dict(
                        calculated_pnl_tp1[symbol]
                    )
                else:
                    self.pnl_by_symbol[symbol] = pd.concat(
                        [
                            self.pnl_by_symbol[symbol],
                            pd.DataFrame.from_dict(calculated_pnl_tp1[symbol]),
                        ],
                        ignore_index=True,
                    )

        # Update PnL by strategy
        unique_strategies = list(
            set(aggregated_positions_by_strategy.keys())
            | set(aggregated_trades_by_strategy.keys())
        )
        for strategy in unique_strategies:
            calculated_pnl_t, calculated_pnl_tp1 = self._calculate_pnl(
                timestamp,
                next_timestamp,
                future_data,
                aggregated_positions_by_strategy[strategy],
                aggregated_trades_by_strategy[strategy],
            )

            for symbol in calculated_pnl_t:
                if symbol not in self.pnl_by_strategy_by_symbol[strategy]:
                    self.pnl_by_strategy_by_symbol[strategy][
                        symbol
                    ] = pd.DataFrame.from_dict(calculated_pnl_t[symbol])
                else:
                    self.pnl_by_strategy_by_symbol[strategy][
                        symbol
                    ] = pd.concat(
                        [
                            self.pnl_by_strategy_by_symbol[strategy][symbol],
                            pd.DataFrame.from_dict(calculated_pnl_t[symbol]),
                        ],
                        ignore_index=True,
                    )

            # check that pnl_tp1 is not past the end of the backtest
            if next_timestamp < convert_est_datestr_to_unix_timestamp(
                self.global_config["run_end_date"]
            ):
                for symbol in calculated_pnl_tp1:
                    if symbol not in self.pnl_by_strategy_by_symbol[strategy]:
                        self.pnl_by_strategy_by_symbol[strategy][
                            symbol
                        ] = pd.DataFrame.from_dict(calculated_pnl_tp1[symbol])
                    else:
                        self.pnl_by_strategy_by_symbol[strategy][
                            symbol
                        ] = pd.concat(
                            [
                                self.pnl_by_strategy_by_symbol[strategy][
                                    symbol
                                ],
                                pd.DataFrame.from_dict(
                                    calculated_pnl_tp1[symbol]
                                ),
                            ],
                            ignore_index=True,
                        )

    def _get_aggregated_pnl(self) -> pd.DataFrame:
        return self._calc_aggregated_pnl(self.pnl_by_symbol)

    def _get_aggregated_pnl_by_strategy(
        self,
    ) -> Dict[ft.StrategyName, pd.DataFrame]:
        return {
            strategy: self._calc_aggregated_pnl(
                self.pnl_by_strategy_by_symbol[strategy]
            )
            for strategy in self.pnl_by_strategy_by_symbol
        }
