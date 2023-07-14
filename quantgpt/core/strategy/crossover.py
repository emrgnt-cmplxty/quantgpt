import logging
from typing import Any, Dict, List, Union, cast

import numpy as np

from quantgpt.core.data.processor import DataProcessor
from quantgpt.core.strategy.base import StrategyBase
from quantgpt.financial_tools import types as ft

Release = Dict[str, Union[str, Any]]

logger = logging.getLogger(__name__)


class StrategyCrossover(StrategyBase):
    def __init__(
        self,
        data_processor: DataProcessor,
        global_config: ft.GlobalConfig,
        strategy_config: ft.StrategyConfig,
    ) -> None:
        super().__init__(data_processor, global_config, strategy_config)
        self.short_moving_average = cast(
            int, strategy_config["specific_config"]["short_moving_average"]
        )
        self.long_moving_average = cast(
            int, strategy_config["specific_config"]["long_moving_average"]
        )

    def generate_signals(
        self, timestamp: ft.Timestamp, observed_data: ft.ObservedData
    ) -> List[ft.Signal]:
        signals = []

        assert (
            ft.AssetClass.US_EQUITY in observed_data
        ), "No US equities data found."
        for symbol, ohlc_data in observed_data[ft.AssetClass.US_EQUITY][
            ft.DataType.DAILY_OHLC
        ].items():
            close_values = ohlc_data["Close"].values
            short_ma = np.mean(close_values[-self.short_moving_average :])  # type: ignore
            long_ma = np.mean(close_values[-self.long_moving_average :])  # type: ignore[E203]

            if short_ma > long_ma:
                signal = ft.Signal(
                    timestamp,
                    symbol,
                    ft.SignalType("z_score_long"),
                    1.0,
                    self.strategy_config["config_name"],
                )
                signals.append(signal)
            elif short_ma < long_ma:
                signal = ft.Signal(
                    timestamp,
                    symbol,
                    ft.SignalType("z_score_short"),
                    1.0,
                    self.strategy_config["config_name"],
                )
                signals.append(signal)

        return signals
