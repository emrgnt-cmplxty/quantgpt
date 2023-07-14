import logging
import math
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union, cast

import numpy as np

from quantgpt.core.data.cache import DataCache
from quantgpt.core.data.processor import DataProcessor
from quantgpt.core.strategy.base import StrategyBase
from quantgpt.financial_tools import types as ft

Release = Dict[str, Union[str, Any]]

logger = logging.getLogger(__name__)
IBB_SYMBOL = ft.Symbol("IBB", ft.AssetClass.US_EQUITY, "biotech")


class StrategyBiotechNews(StrategyBase):
    class StrategyVersion(Enum):
        SENTIMENT_V0 = "sentiment_v0"
        SENTIMENT_V1 = "sentiment_v1"
        N_A = "n/a"

        def __str__(self):
            return self.value

    def __init__(
        self,
        data_processor: DataProcessor,
        global_config: ft.GlobalConfig,
        strategy_config: ft.StrategyConfig,
    ) -> None:
        super().__init__(data_processor, global_config, strategy_config)
        # Initialize any additional variables for StrategyBiotechNews
        self.min_avg_daily_volume = float(
            cast(
                float,
                self.strategy_config["specific_config"][
                    "min_avg_daily_volume"
                ],
            )
        )
        self.max_avg_daily_volume = float(
            cast(
                float,
                self.strategy_config["specific_config"][
                    "max_avg_daily_volume"
                ],
            )
        )
        self.blacklisted_symbols = cast(
            List[str],
            self.strategy_config["specific_config"]["blacklisted_symbols"],
        )
        self.primary_title_text_filters = cast(
            List[str],
            self.strategy_config["specific_config"][
                "primary_title_text_filters"
            ],
        )
        self.secondary_title_text_filters = cast(
            List[str],
            self.strategy_config["specific_config"].get(
                "secondary_title_text_filters", [""]
            ),
        )
        self.body_text_filters = cast(
            List[str],
            self.strategy_config["specific_config"].get(
                "body_text_filters", ["ZZZ_ThisWillFail_ZZZ"]
            ),
        )
        self.signal_leq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("signal_leq_bound"),
        )
        self.signal_geq_bound = cast(
            Optional[float],
            self.strategy_config["specific_config"].get("signal_geq_bound"),
        )
        self.signal_window_size = cast(
            int,
            self.strategy_config["specific_config"].get(
                "signal_window_size", 10
            ),
        )

        self.do_short = cast(
            bool,
            self.strategy_config["specific_config"].get("do_short", False),
        )
        self.short_adj_fraction = cast(
            float,
            self.strategy_config["specific_config"].get(
                "short_adj_fraction", 0.5
            ),
        )

        self.do_hedge = cast(
            bool,
            self.strategy_config["specific_config"].get("do_hedge", False),
        )
        self.do_scale_trade_to_etf_vol = cast(
            bool,
            self.strategy_config["specific_config"].get(
                "do_scale_trade_to_etf_vol", False
            ),
        )
        self.do_intraday = cast(
            bool,
            self.strategy_config["specific_config"].get("do_intraday", False),
        )
        self.use_news_sentiment = cast(
            bool,
            self.strategy_config["specific_config"].get(
                "use_news_sentiment", False
            ),
        )
        self.signal_type = cast(
            str,
            self.strategy_config["specific_config"].get(
                "signal_type", "symbol_beta_adj_z_window"
            ),
        )
        self.sentiment_strategy_version = cast(
            str,
            self.StrategyVersion(
                self.strategy_config["specific_config"].get(
                    "sentiment_strategy_version", "sentiment_v0"
                )
            ),
        )

        assert (
            len(self.primary_title_text_filters) > 0
        ), "Must pass a non-empty list of text filters."
        logger.info(
            f"Running strategy biotech news with config: {strategy_config}"
        )

        self.observed_releases: Set[str] = set([])
        if self.use_news_sentiment:
            self.data_cache = DataCache(
                cache_file="cache.pkl",
                initial_prompt_file="prompt_init.txt",
                final_prompt_file="prompt_suffix.txt",
            )
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
        # Simple quality control checks
        assert (
            ft.AssetClass.US_EQUITY in observed_data
        ), "No US equities data found in observed data."
        assert (
            IBB_SYMBOL
            in observed_data[ft.AssetClass.US_EQUITY][ft.DataType.DAILY_OHLC]
        ), "IBB symbol must be in observed data."
        if ft.DataType.NEWS not in observed_data[ft.AssetClass.US_EQUITY]:
            logger.warning(
                f"No US equities news data found in observed data on timestamp = {timestamp}, please double check the data to be sure this is expected."
            )
            return []

        signals = []
        index_daily_ohlc = observed_data[ft.AssetClass.US_EQUITY][
            ft.DataType.DAILY_OHLC
        ][IBB_SYMBOL]
        (
            index_returns_window,
            index_returns_series,
            index_vol,
        ) = self.get_series_vars(index_daily_ohlc)

        for symbol, releases in observed_data[ft.AssetClass.US_EQUITY][
            ft.DataType.NEWS
        ].items():
            symbol_daily_ohlc = observed_data[ft.AssetClass.US_EQUITY][
                ft.DataType.DAILY_OHLC
            ][symbol]
            if len(symbol_daily_ohlc["Close"]) != len(
                index_daily_ohlc["Close"]
            ):
                continue

            (
                symbol_returns_window,
                symbol_returns_series,
                symbol_vol,
            ) = self.get_series_vars(symbol_daily_ohlc)

            # Beta is used to determine hedge sizes later on
            beta = StrategyBiotechNews._calculate_beta(
                symbol_returns_series, index_returns_series
            )

            if self.signal_type == "symbol_beta_adj_z_window":
                symbol_beta_adj_z_returns = (
                    symbol_returns_series - beta * index_returns_series
                )
                symbol_beta_adj_vol = StrategyBiotechNews._calculate_vol(
                    symbol_beta_adj_z_returns
                )
                symbol_signal_value = (
                    symbol_returns_window - beta * index_returns_window
                ) / (symbol_beta_adj_vol * math.sqrt(self.signal_window_size))
            elif self.signal_type == "symbol_z_window":
                symbol_signal_value = symbol_returns_window / symbol_vol
            else:
                raise ValueError("Invalid signal type.")

            if self._check_conditions_for_symbol(
                symbol,
                symbol_daily_ohlc,
                symbol_signal_value,
            ):
                for _, release in releases[::-1].iterrows():
                    if self._check_conditions_for_release(release, timestamp):
                        title = release["Title"]
                        body = release["Body"]
                        trade_size = 1.0

                        long_signal, short_signal = False, False
                        category = ""
                        if self.use_news_sentiment:
                            sentiment = self.data_cache.get_result(title, body)
                            category = self.data_cache.categorize_result(
                                sentiment
                            )

                            # # SENTIMENT MEAN REVERSION V0
                            # Filter long/short signals on category
                            # Very simple approach, just re-maps NEGATIVE to short signal
                            if (
                                self.sentiment_strategy_version
                                == self.StrategyVersion.SENTIMENT_V0
                            ):
                                long_signal = symbol_signal_value <= 0
                                long_signal = long_signal and (
                                    category
                                    == DataCache.Category.EXTREMELY_POSITIVE
                                    or category
                                    == DataCache.Category.VERY_POSITIVE
                                    or category == DataCache.Category.POSITIVE
                                    or category == DataCache.Category.NEUTRAL
                                )

                                short_signal = (
                                    symbol_signal_value > 0
                                    or category == DataCache.Category.NEGATIVE
                                )
                            elif (
                                self.sentiment_strategy_version
                                == self.StrategyVersion.SENTIMENT_V1
                            ):
                                # # SENTIMENT MEAN REVERSION V1
                                # Category dependent long/short signals
                                long_signal = (
                                    symbol_signal_value < -1
                                    and category
                                    == DataCache.Category.EXTREMELY_POSITIVE
                                )
                                long_signal = long_signal or (
                                    symbol_signal_value < -1.5
                                    and category
                                    == DataCache.Category.VERY_POSITIVE
                                )
                                long_signal = long_signal or (
                                    symbol_signal_value < -2
                                    and category == DataCache.Category.POSITIVE
                                )

                                short_signal = (
                                    category == DataCache.Category.NEGATIVE
                                )
                                short_signal = short_signal or (
                                    symbol_signal_value > 1.5
                                    and DataCache.Category.NEUTRAL
                                )
                                short_signal = short_signal or (
                                    symbol_signal_value > 2
                                    and DataCache.Category.POSITIVE
                                )
                            else:
                                raise ValueError(
                                    f"Invalid sentiment_strategy_version: {self.sentiment_strategy_version}"
                                )
                        else:
                            # # NO SENTIMENT MEAN REVERSION
                            long_signal = symbol_signal_value < 0
                            short_signal = symbol_signal_value > 0

                        if not long_signal and not (
                            self.do_short and short_signal
                        ):
                            continue

                        if short_signal and self.short_adj_fraction:
                            trade_size *= self.short_adj_fraction

                        if self.do_scale_trade_to_etf_vol:
                            vol_ratio = index_vol / symbol_vol
                            trade_size *= vol_ratio

                        symbol_signal_value = -1 if short_signal else 1

                        signal = self._create_signal(
                            symbol_signal_value,
                            trade_size,
                            symbol,
                            timestamp,
                            release,
                        )
                        signals.append(signal)

                        if self.do_hedge:
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
                            "{} Observed Positive ft.Signal Headline: {} and Produced an Output ft.Signal {} on {} with Category {}".format(
                                self.strategy_config["config_name"],
                                title,
                                signal,
                                timestamp,
                                category,
                            )
                        )

        return signals

    def _create_signal(
        self, symbol_signal_value, trade_size, symbol, timestamp, release
    ):
        signal_type = (
            ft.SignalType.Z_SCORE_LONG
            if symbol_signal_value > 0
            else ft.SignalType.Z_SCORE_SHORT
        )

        signal: ft.Signal = ft.Signal(
            timestamp,
            symbol,
            signal_type,
            trade_size,
            self.strategy_config["config_name"],
        )

        # Log recent backtest information
        self.short_trade_counter += (
            1 if signal_type == ft.SignalType.Z_SCORE_SHORT else 0
        )
        self.long_trade_counter += (
            1 if signal_type == ft.SignalType.Z_SCORE_LONG else 0
        )
        logger.info(
            "{} Observed Signal Headline: {} on {}".format(
                self.strategy_config["config_name"],
                release["Title"],
                timestamp,
            )
        )
        logger.info("Signal: {}".format(signal))
        logger.info("Long Trade Counter: {}".format(self.long_trade_counter))
        logger.info("Short Trade Counter: {}".format(self.short_trade_counter))
        return signal

    def _check_conditions_for_symbol(
        self, symbol, symbol_daily_ohlc, signal_value
    ):
        return (
            self._check_not_blacklisted_symbol(symbol)
            and StrategyBiotechNews._check_symbol_has_volume(symbol_daily_ohlc)
            and self._check_signal_strength(signal_value)
            and self._check_weighted_volume(symbol_daily_ohlc)
        )

    def _check_conditions_for_release(self, release, timestamp):
        title, body, created_at = (
            release["Title"],
            release["Body"],
            release["Created"],
        )
        contains_relevant_primary_title_text: bool = (
            StrategyBiotechNews._check_relevance(
                title, self.primary_title_text_filters
            )
        )
        contains_relevant_secondary_title_text: bool = (
            StrategyBiotechNews._check_relevance(
                title, self.secondary_title_text_filters
            )
        )
        contains_relevant_body_text: bool = (
            StrategyBiotechNews._check_relevance(body, self.body_text_filters)
        )

        return (
            self._check_release_not_observed(title)
            and (
                self.do_intraday
                or StrategyBiotechNews._check_not_intraday(created_at)
            )
            and contains_relevant_primary_title_text
            and (
                contains_relevant_body_text
                or contains_relevant_secondary_title_text
            )
            and self._check_release_is_prev_timestamp(release, timestamp)
        )

    def _check_release_is_prev_timestamp(self, release, timestamp):
        return release["Timestamp"] == self.global_config["calendar"].nearest(
            timestamp, "before"
        )

    def _check_release_not_observed(self, title):
        return title not in self.observed_releases

    def _check_not_blacklisted_symbol(self, symbol):
        return symbol.value not in self.blacklisted_symbols

    def _check_weighted_volume(self, symbol_daily_ohlc):
        weighted_volume = (
            symbol_daily_ohlc["Volume"].values[-20:]
            * symbol_daily_ohlc["Close"].values[-20:]
        ).mean()

        return (
            self.min_avg_daily_volume
            <= weighted_volume
            <= self.max_avg_daily_volume
        )

    def _check_signal_strength(self, signal_value):
        if self.signal_leq_bound is None and self.signal_geq_bound is None:
            return True
        elif (
            self.signal_geq_bound is not None and self.signal_leq_bound is None
        ):
            return signal_value > self.signal_geq_bound
        elif (
            self.signal_leq_bound is not None and self.signal_geq_bound is None
        ):
            return signal_value < self.signal_leq_bound
        else:
            return (
                signal_value < self.signal_leq_bound
                or signal_value > self.signal_geq_bound
            )

    def _trailing_n_day_open_to_close_return(self, x, n=1):
        if len(x) < self.signal_window_size:
            return 0.0

        return float(
            100
            * (x["Close"].iloc[-1] - x["Open"].iloc[-n])
            / x["Open"].iloc[-n]
        )

    def get_series_vars(self, data_ohlc):
        data_returns_window = self._trailing_n_day_open_to_close_return(
            data_ohlc, n=self.signal_window_size
        )
        data_returns_series = StrategyBiotechNews._calc_ret_series(data_ohlc)
        data_vol = StrategyBiotechNews._calculate_vol(data_returns_series)
        return data_returns_window, data_returns_series, data_vol

    @staticmethod
    def _check_not_intraday(created_ts):
        # Remove events that are during trading hours
        date_obj = datetime.strptime(created_ts, "%a, %d %b %Y %H:%M:%S %z")
        hour = date_obj.hour
        during_valid_hours = hour < 9 or hour > 16
        return during_valid_hours

    @staticmethod
    def _check_symbol_has_volume(symbol_daily_ohlc):
        if "Volume" not in symbol_daily_ohlc:
            return False
        return True

    @staticmethod
    def _check_relevance(
        title: str, primary_title_text_filters: List[str]
    ) -> bool:
        """
        Check if the press release title is relevant to the text filters.

        :param title: Press release title.
        :param primary_title_text_filters: List of text filters.
        :return: True if relevant, False otherwise.
        """
        for text_filter in primary_title_text_filters:
            if text_filter.upper() in title.upper():
                return True

        return False

    @staticmethod
    def _calculate_vol(returns):
        return np.std(returns) * 100

    @staticmethod
    def _calculate_beta(stock_returns, index_returns):
        covariance = np.cov(stock_returns, index_returns)
        stock_index_covariance = covariance[0, 1]
        index_variance = covariance[1, 1]

        beta = stock_index_covariance / index_variance
        return beta

    @staticmethod
    def _calc_ret_series(x):
        return np.diff(x["Close"]) / x["Close"][:-1]
