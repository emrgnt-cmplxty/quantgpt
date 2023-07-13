import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union, cast

import numpy as np

from financial_tools import types as ft

from ..data.processor import DataProcessor
from .base import StrategyBase

Release = Dict[str, Union[str, Any]]

logger = logging.getLogger(__name__)
IBB_SYMBOL = ft.Symbol("IBB", ft.AssetClass.US_EQUITY, "biotech")


class StrategyBiotechNews(StrategyBase):
    def __init__(
        self,
        data_processor: DataProcessor,
        global_config: ft.GlobalConfig,
        strategy_config: ft.StrategyConfig,
    ) -> None:
        super().__init__(data_processor, global_config, strategy_config)
        # Initialize any additional variables for StrategyBiotechNews
        self.min_prev_daily_volume = float(
            cast(float, self.strategy_config["specific_config"]["min_prev_daily_volume"])
        )
        self.max_prev_daily_volume = float(
            cast(float, self.strategy_config["specific_config"]["max_prev_daily_volume"])
        )
        self.blacklisted_symbols = cast(
            List[str], self.strategy_config["specific_config"]["blacklisted_symbols"]
        )
        self.text_filters = cast(
            List[str], self.strategy_config["specific_config"]["text_filters"]
        )
        self.returns_window_leq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("returns_window_leq_bound"),
        )
        self.returns_window_geq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("returns_window_geq_bound"),
        )
        self.returns_window_beta_adj_z_leq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("returns_window_beta_adj_z_leq_bound"),
        )
        self.returns_window_beta_adj_z_geq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("returns_window_beta_adj_z_geq_bound"),
        )

        self.observed_releases: Set[str] = set([])
        self.include_short_trades = cast(
            bool, self.strategy_config["specific_config"].get("include_short_trades", False)
        )
        self.hedge_strategy = cast(
            bool, self.strategy_config["specific_config"].get("hedge_strategy", False)
        )
        self.scale_trade_to_etf_vol = cast(
            bool, self.strategy_config["specific_config"].get("scale_trade_to_etf_vol", False)
        )
        self.exclude_intraday = cast(
            bool, self.strategy_config["specific_config"].get("exclude_intraday", False)
        )
        self.signal_window_size = cast(
            int, self.strategy_config["specific_config"].get("signal_window_size", 10)
        )

        assert len(self.text_filters) > 0, "Must pass a non-empty list of text filters."
        logger.info(f"Running strategy biotech news with config: {strategy_config}")
        if self.hedge_strategy:
            assert (
                IBB_SYMBOL in self.global_config["symbols"][ft.AssetClass.US_EQUITY]
            ), "IBB symbol must be in global config symbols."

        self.short_trade_counter = 0
        self.long_trade_counter = 0

    def generate_signals(
        self, timestamp: ft.Timestamp, observed_data: ft.ObservedData
    ) -> List[ft.Signal]:
        """
        Implements the logic for generating trade signals for StrategyBiotechNews.

        :param timestamp: The timestamp corresponding to the date on which trades are evaluated.
        :param observed_data: Data from previous trading days.
        :return: A list of trade signals.
        """
        signals = []

        assert (
            IBB_SYMBOL in observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC]
        ), "IBB symbol must be in observed data."

        index_daily_ohlc = observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC][
            IBB_SYMBOL
        ]
        index_returns_window, index_returns_series, index_volatility = self.get_series_vars(
            index_daily_ohlc
        )
        assert (
            ft.AssetClass.US_EQUITY in observed_data
        ), "No US equities data found in observed data."
        if ft.DataType.NEWS not in observed_data[ft.AssetClass.US_EQUITY]:
            logger.warning(
                f"No US equities news data found in observed data on timestamp = {timestamp}, please double check the data to be sure this is expected."
            )
        if (
            ft.AssetClass.US_EQUITY in observed_data
            and ft.DataType.NEWS in observed_data[ft.AssetClass.US_EQUITY]
        ):
            news_data = observed_data[ft.AssetClass.US_EQUITY][ft.DataType.NEWS]
            for symbol, releases in news_data.items():
                symbol_daily_ohlc = observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC][
                    symbol
                ]
                if len(symbol_daily_ohlc["Close"]) != len(index_daily_ohlc["Close"]):
                    continue

                (
                    symbol_returns_window,
                    symbol_returns_series,
                    symbol_volatility,
                ) = self.get_series_vars(symbol_daily_ohlc)

                beta = self.calculate_beta(symbol_returns_series, index_returns_series)
                beta_adj_z_returns = symbol_returns_series - beta * index_returns_series
                beta_adj_volatility = self.calculate_volatility(beta_adj_z_returns)
                symbol_returns_window_beta_adj_z = (
                    symbol_returns_window - beta * index_returns_window
                ) / (beta_adj_volatility * math.sqrt(self.signal_window_size))
                symbol_signal_value = symbol_returns_window
                if (
                    self.returns_window_beta_adj_z_leq_bound is not None
                    or self.returns_window_beta_adj_z_geq_bound is not None
                ):
                    symbol_signal_value = symbol_returns_window_beta_adj_z

                if self._check_conditions_for_symbol(
                    symbol,
                    symbol_daily_ohlc,
                    symbol_returns_window,
                    symbol_returns_window_beta_adj_z,
                ):
                    for _, release in releases[::-1].iterrows():
                        if self._check_conditions_for_release(release, timestamp):
                            title = release["Title"]

                            trade_size = 1.0
                            if self.scale_trade_to_etf_vol or self.hedge_strategy:
                                assert (
                                    index_daily_ohlc is not None
                                ), "IBB daily OHLC data is None when it is expected to be not-none."

                            if self.scale_trade_to_etf_vol and index_daily_ohlc is not None:
                                vol_ratio = index_volatility / symbol_volatility
                                trade_size *= vol_ratio

                            if symbol_signal_value > 0:
                                # Half weight for short trades
                                trade_size *= 0.5

                            signal = self._create_signal(
                                symbol_signal_value, trade_size, symbol, timestamp, release
                            )
                            signals.append(signal)

                            if self.hedge_strategy and index_daily_ohlc is not None:
                                signal = self._create_signal(
                                    -1
                                    * symbol_signal_value,  # flip the sign of the return to trade the opposite direction
                                    trade_size * beta,
                                    IBB_SYMBOL,
                                    timestamp,
                                    release,
                                )
                                signals.append(signal)

                            self.observed_releases.add(title)

                            logger.info(
                                "{} Observed Positive ft.Signal Headline: {} and Produced an Output ft.Signal {} on {}".format(
                                    self.strategy_config["config_name"], title, signal, timestamp
                                )
                            )

            return signals
        return []

    def _create_signal(self, symbol_signal_value, trade_size, symbol, timestamp, release):
        signal_type = ft.SignalType.Z_SCORE_LONG
        if self.include_short_trades and symbol_signal_value > 0:
            signal_type = ft.SignalType.Z_SCORE_SHORT

        signal: ft.Signal = ft.Signal(
            timestamp,
            symbol,
            signal_type,
            trade_size,
            self.strategy_config["config_name"],
        )

        self.short_trade_counter += 1 if signal_type == ft.SignalType.Z_SCORE_SHORT else 0
        self.long_trade_counter += 1 if signal_type == ft.SignalType.Z_SCORE_LONG else 0

        logger.info(
            "{} Observed Signal Headline: {} and Produced an Output ft.Signal {} on {}".format(
                self.strategy_config["config_name"], release["Title"], signal, timestamp
            )
        )
        logger.info("Signal: {}".format(signal))
        logger.info("Signal Value: {}".format(symbol_signal_value))
        logger.info("Long Trade Counter: {}".format(self.long_trade_counter))
        logger.info("Short Trade Counter: {}".format(self.short_trade_counter))
        return signal

    def _check_conditions_for_symbol(
        self, symbol, symbol_daily_ohlc, symbol_returns_window, returns_window_beta_adj_z
    ):
        return (
            not self._check_blacklisted_symbols(symbol)
            and StrategyBiotechNews._check_symbol_volume(symbol_daily_ohlc)
            and self._check_returns_window(symbol_returns_window)
            and self._check_returns_window_beta_adj_z(returns_window_beta_adj_z)
            and self._check_weighted_volume(symbol_daily_ohlc)
        )

    def _check_conditions_for_release(self, release, timestamp):
        title = release["Title"]
        contains_relevant_text: bool = StrategyBiotechNews._check_relevance(
            title, self.text_filters
        )

        during_valid_hours: bool = True
        if self.exclude_intraday:
            # Remove events that are during trading hours
            date_obj = datetime.strptime(release["Created"], "%a, %d %b %Y %H:%M:%S %z")
            hour = date_obj.hour
            during_valid_hours = hour < 9 or hour > 16

        return (
            not self._check_release_already_observed(title)
            and during_valid_hours
            and contains_relevant_text
            and self._check_release_time(release, timestamp)
        )

    def _check_release_time(self, release, timestamp):
        return release["Timestamp"] == self.global_config["calendar"].nearest(timestamp, "before")

    def _check_release_already_observed(self, title):
        return title in self.observed_releases

    def _check_blacklisted_symbols(self, symbol):
        return symbol.value in self.blacklisted_symbols

    def _check_weighted_volume(self, symbol_daily_ohlc):
        weighted_volume = (
            symbol_daily_ohlc["Volume"].values[-20:] * symbol_daily_ohlc["Close"].values[-20:]
        ).mean()

        return self.min_prev_daily_volume <= weighted_volume <= self.max_prev_daily_volume

    def _check_returns_window(self, returns_window):
        if self.returns_window_leq_bound is None and self.returns_window_geq_bound is None:
            return True
        elif self.returns_window_geq_bound is not None and self.returns_window_leq_bound is None:
            return returns_window > self.returns_window_geq_bound
        elif self.returns_window_leq_bound is not None and self.returns_window_geq_bound is None:
            return returns_window < self.returns_window_leq_bound
        else:
            return (
                returns_window < self.returns_window_leq_bound
                or returns_window > self.returns_window_geq_bound
            )

    def _check_returns_window_beta_adj_z(self, returns_window_beta_adj_z):
        if (
            self.returns_window_beta_adj_z_leq_bound is None
            and self.returns_window_beta_adj_z_geq_bound is None
        ):
            return True
        elif (
            self.returns_window_beta_adj_z_geq_bound is not None
            and self.returns_window_beta_adj_z_leq_bound is None
        ):
            return returns_window_beta_adj_z > self.returns_window_beta_adj_z_geq_bound
        elif (
            self.returns_window_beta_adj_z_leq_bound is not None
            and self.returns_window_beta_adj_z_geq_bound is None
        ):
            return returns_window_beta_adj_z < self.returns_window_beta_adj_z_leq_bound
        else:
            return (
                returns_window_beta_adj_z < self.returns_window_beta_adj_z_leq_bound
                or returns_window_beta_adj_z > self.returns_window_beta_adj_z_geq_bound
            )

    def _trailing_n_day_open_to_close_return(self, x, n=1):
        if len(x) < self.signal_window_size:
            return 0.0

        return float(100 * (x["Close"].iloc[-1] - x["Open"].iloc[-n]) / x["Open"].iloc[-n])

    def get_series_vars(self, data_ohlc):
        data_returns_window = self._trailing_n_day_open_to_close_return(
            data_ohlc, n=self.signal_window_size
        )
        data_returns_series = self.calc_ret_series(data_ohlc)
        data_volatility = self.calculate_volatility(data_returns_series)
        return data_returns_window, data_returns_series, data_volatility

    @staticmethod
    def _check_symbol_volume(symbol_daily_ohlc):
        if "Volume" not in symbol_daily_ohlc:
            return False
        return True

    @staticmethod
    def _check_relevance(title: str, text_filters: List[str]) -> bool:
        """
        Check if the press release title is relevant to the text filters.

        :param title: Press release title.
        :param text_filters: List of text filters.
        :return: True if relevant, False otherwise.
        """
        for text_filter in text_filters:
            if text_filter.upper() in title.upper():
                return True

        return False

    @staticmethod
    def calculate_volatility(returns):
        return np.std(returns) * 100

    @staticmethod
    def calculate_correlation(asset_a_prices, asset_b_prices):
        returns_a = np.diff(asset_a_prices) / asset_a_prices[:-1]
        returns_b = np.diff(asset_b_prices) / asset_b_prices[:-1]
        correlation = np.corrcoef(returns_a, returns_b)[0, 1]
        return correlation

    @staticmethod
    def calculate_beta(stock_returns, index_returns):
        covariance = np.cov(stock_returns, index_returns)
        stock_index_covariance = covariance[0, 1]
        index_variance = covariance[1, 1]

        beta = stock_index_covariance / index_variance
        return beta

    @staticmethod
    def calc_ret_series(x):
        return np.diff(x["Close"]) / x["Close"][:-1]
