from typing import Union

from quantgpt.core.data.processor import DataProcessor
from quantgpt.core.strategy.biotech_news import (  # noqa: F401
    StrategyBiotechNews,
)
from quantgpt.core.strategy.crossover import StrategyCrossover  # noqa: F401
from quantgpt.financial_tools import types as ft

# TODO - figure out how to expand this type hint as we add more strategies
StrategyType = Union[StrategyBiotechNews, StrategyCrossover]


def load_strategy(
    strategy_config: ft.StrategyConfig,
    global_config: ft.GlobalConfig,
    data_processor: DataProcessor,
) -> StrategyType:
    def get_strategy_class(strategy_name):
        return globals()[strategy_name]

    """Loads the strategy."""
    return get_strategy_class(strategy_config["name"])(
        data_processor, global_config, strategy_config
    )
