import os
from typing import Any, Dict, List, Optional, TypedDict, Union

import pandas as pd

from quantgpt.financial_tools.untyped_utils import home_path
from quantgpt.financial_tools.types.basic_types import (
    Path,
    StrategyName,
    Timestamp,
)
from quantgpt.financial_tools.types.enums import (
    AssetClass,
    ConfigType,
    DataProviderName,
    DataType,
    DBConnections,
    ProdType,
    TradingMode,
    TradingTimes,
)
from .symbol import Symbol

Config = Dict[str, Union[str, int, float, None, "Config"]]
Data = Dict[
    AssetClass, Dict[DataType, Dict[Symbol, Dict[str, Union[str, int, float]]]]
]
DataDict = Dict[Timestamp, Data]
ObservedData = Dict[AssetClass, Dict[DataType, Dict[Symbol, pd.DataFrame]]]
FutureData = ObservedData


class ConfigPathDict(TypedDict):
    workdir: Path
    base: Path
    interior: Path
    config_type: str
    prod_type: str
    name: Path


class ConfigPath:
    def __init__(
        self,
        workdir: Path = "quantgpt",
        base: Path = "core",
        interior: Path = "config",
        config_type: str = "allocation",
        prod_type: str = "test",
        name: Path = "test",
    ):
        if base not in ["core"]:
            raise ValueError("Base must be 'core'")
        if interior not in ["config"]:
            raise ValueError("Interior must be 'config'")

        # verify we can cast config_type to ConfigType
        _ = ProdType(prod_type), ConfigType(config_type)

        self.path: ConfigPathDict = ConfigPathDict(
            workdir=workdir,
            base=base,
            interior=interior,
            config_type=config_type,
            prod_type=prod_type,
            name=name,
        )

    def get_name(self) -> str:
        return self.path["name"]

    def get(self) -> Path:
        raw_path: Path = os.path.join(
            home_path(),
            self.path["workdir"],
            self.path["base"],
            self.path["interior"],
            self.path["config_type"],
            self.path["prod_type"],
            self.path["name"],
        )
        return f"{raw_path}.jsonc"


class DataSource(TypedDict):
    asset_class: AssetClass
    provider: DataProviderName
    data_type: DataType


class LiveData:
    def __init__(self, trading_time: TradingTimes, data: Data):
        self.trading_time = trading_time
        self.data = data

    def transform(self, target_trading_time: TradingTimes) -> "LiveData":
        if self.trading_time == target_trading_time:
            return self

        transformed_data = self._transform_data(target_trading_time)
        return LiveData(target_trading_time, transformed_data)

    def _transform_data(self, target_trading_time: TradingTimes) -> Data:
        # Implement the transformation logic here
        raise ValueError("Transform must be implemented in subclass")


class SymbolSource(TypedDict):
    asset_class: AssetClass
    sub_class: Optional[str]


class StrategyAllocation(TypedDict):
    config_path: dict
    weight: int


class TradeConfig(TypedDict):
    starting_cash: float
    by_asset_class: Dict[AssetClass, Dict[str, Union[str, int]]]


AllocationConfig = Dict[str, List[Dict[str, StrategyAllocation]]]


class AllocationEntry(TypedDict):
    weight: float
    path: ConfigPath


class StrategyConfig(TypedDict):
    name: str
    # Name as it appears in the allocation table
    table_name: str
    # Name of the config file
    config_name: str
    text_filters: List[str]
    symbol_sources: List[SymbolSource]
    data_providers: List[DataSource]
    blacklisted_symbols: List[Symbol]
    trade_config: TradeConfig
    specific_config: Config


class GlobalConfig(TypedDict):
    name: str
    mode: TradingMode
    trading_times: TradingTimes
    # TODO - Can we find a way to import TradingCalendar without a cicular import?
    calendar: Any
    data_start_date: str
    data_end_date: str
    run_start_date: str
    run_end_date: str
    delta_to_close_timestamp: str
    max_cores: int
    observed_data_lookback: str
    future_data_lookahead: str
    allocation_table: Dict[Timestamp, Dict[StrategyName, AllocationEntry]]
    strategy_configs: Dict[StrategyName, StrategyConfig]
    data_providers: Dict[AssetClass, Dict[DataType, List[DataProviderName]]]
    symbols: Dict[AssetClass, List[Symbol]]
    db_connections: Dict[DataProviderName, DBConnections]
