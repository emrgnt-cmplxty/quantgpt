import argparse
import logging.config
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, cast

from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    build_symbols,
    convert_datestr_to_datetime,
    convert_est_dt_to_unix_timestamp,
    convert_time_delta_str,
    load_config,
)

OBSERVED_DATA_LOOKBACK_SCALER = 2.0


class ConfigBuilder:
    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.allocation_table: Dict[
            ft.Timestamp, Dict[ft.StrategyName, ft.AllocationEntry]
        ] = {}

    def generate_allocation_table(
        self, strategy_config_list: List[ft.Config]
    ) -> None:
        """
        Generates a new allocation table based on the global config
        """
        run_start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        run_end_date = datetime.strptime(self.args.end, "%Y-%m-%d")
        allocation_table_by_timestamps: Dict[
            ft.Timestamp, Dict[ft.StrategyName, ft.AllocationEntry]
        ] = {}

        # Loop through all dates from run_start_date to just before run_end_date
        current_date = run_start_date
        while current_date < run_end_date:
            allocation_table_at_date: Dict[
                ft.StrategyName, ft.AllocationEntry
            ] = {}

            # Loop through all strategies
            for strategy_config in strategy_config_list:
                config_path: ft.ConfigPath = self._build_config_path(
                    cast(ft.Config, strategy_config["config_path"])
                )
                allocation_table_at_date[
                    config_path.path["name"]
                ] = ft.AllocationEntry(
                    {
                        "weight": cast(float, strategy_config["weight"]),
                        "path": config_path,
                    }
                )

            hours, minutes, seconds = (
                int(ele)
                for ele in str(self.delta_to_close_timestamp).split(":")
            )
            allocation_table_by_timestamps[
                convert_est_dt_to_unix_timestamp(
                    current_date
                    + timedelta(
                        hours=int(hours),
                        minutes=int(minutes),
                        seconds=int(seconds),
                    )
                )
            ] = allocation_table_at_date

            current_date += timedelta(days=1)

        if len(allocation_table_by_timestamps) == 0:
            raise ValueError("The allocation_table must not be empty.")

        self.allocation_table = allocation_table_by_timestamps

    def build_allocation_table(self) -> None:
        """
        Initializes the allocation table

        :param allocation_table: Allocation table
        """
        allocation_table_by_timestamps = {}
        for date in self.allocation_config:
            allocation_table_at_date: Dict[
                ft.StrategyName, ft.AllocationEntry
            ] = {}
            for daily_allocation in self.allocation_config[date]:
                for strategy, values in daily_allocation.items():
                    config_path: ft.ConfigPath = self._build_config_path(
                        cast(ft.Config, values["config_path"])
                    )
                    allocation_table_at_date[strategy] = ft.AllocationEntry(
                        {"weight": values["weight"], "path": config_path}
                    )

            hours, minutes, seconds = (
                int(ele)
                for ele in str(self.delta_to_close_timestamp).split(":")
            )
            # To load the allocation table properly we must map the midnight start time to the
            # market open time, which is done by adding hours and minutes to the date.
            allocation_table_by_timestamps[
                convert_est_dt_to_unix_timestamp(
                    convert_datestr_to_datetime(date)
                    + timedelta(
                        hours=int(hours),
                        minutes=int(minutes),
                        seconds=int(seconds),
                    )
                )
            ] = allocation_table_at_date

        if len(allocation_table_by_timestamps) == 0:
            raise ValueError("The allocation_table must not be empty.")
        self.allocation_table = allocation_table_by_timestamps

    def build_global_config(self) -> ft.GlobalConfig:
        """
        Builds the global configuration by loading the necessary configurations
        and updating with command line arguments.
        """
        # Load the global config path and load the global config
        # the path is structured base_interior_config_type_prod_type_name
        config_path_args = self.args.global_config_path_str.split("_")
        global_config_path: ft.ConfigPath = ft.ConfigPath(
            workdir=ft.Path(config_path_args[0]),
            base=ft.Path(config_path_args[1]),
            interior=ft.Path(config_path_args[2]),
            config_type=config_path_args[3],
            prod_type=config_path_args[4],
            name="_".join(config_path_args[5:]),
        )
        global_config_loader: ft.Config = load_config(global_config_path)
        self.delta_to_close_timestamp = global_config_loader[
            "delta_to_close_timestamp"
        ]
        generate_unit_allocation = global_config_loader.get(
            "generate_unit_allocation", False
        )
        if not generate_unit_allocation:
            config_path_args = cast(
                ft.ConfigPathDict, global_config_loader["allocation_config"]
            )

            allocation_config_path = ft.ConfigPath(**config_path_args)
            allocation_config_loader = load_config(allocation_config_path)
            self.allocation_config: ft.AllocationConfig = cast(
                ft.AllocationConfig, allocation_config_loader
            )
            self.build_allocation_table()
        else:
            # ensure that the config does not have both an allocation flag and a generation flag
            if "allocation_config" in global_config_loader:
                raise ValueError(
                    "Both allocation_config and generate_unit_allocation are set to True. "
                    "Please set only one of these flags to True."
                )
            strategy_config_list = global_config_loader["strategy_config_list"]
            self.generate_allocation_table(
                cast(List[ft.Config], strategy_config_list)
            )

        (
            strategy_configs,
            symbols_dict,
            providers_dict,
        ) = self.load_configurations()

        db_connections: Dict[ft.DataProviderName, ft.DBConnections] = {
            ft.DataProviderName(key): ft.DBConnections(value)
            for key, value in cast(
                Dict[ft.DataProviderName, ft.DBConnections],
                global_config_loader["db_connections"],
            ).items()
        }

        # WARNING - This method of calculating the data start and end dates is not exact
        # it is hacky and should be replaced with a more exact method
        # TODO - dynamicaly calculate the data start and end dates exactly
        data_start_date = (
            convert_datestr_to_datetime(self.args.start)
            - timedelta(
                seconds=convert_time_delta_str(
                    cast(str, global_config_loader["observed_data_lookback"])
                )
                * OBSERVED_DATA_LOOKBACK_SCALER
            )
        ).strftime("%Y-%m-%d")

        data_end_date = (
            convert_datestr_to_datetime(self.args.end)
            + timedelta(
                seconds=convert_time_delta_str(
                    cast(str, self.args.data_lookahead_days)
                )
            )
        ).strftime("%Y-%m-%d")

        calendar = ft.TradingCalendar(
            data_start_date,
            data_end_date,
            str(global_config_loader["delta_to_close_timestamp"]),
            str(global_config_loader["calendar_name"]),
        )
        return ft.GlobalConfig(
            mode=ft.TradingMode(self.args.mode),
            trading_times=ft.TradingTimes(
                global_config_loader["trading_times"]
            ),
            data_start_date=str(data_start_date),
            data_end_date=str(data_end_date),
            run_start_date=str(self.args.start),
            run_end_date=str(self.args.end),
            name=str(global_config_path.get_name()),
            delta_to_close_timestamp=str(
                global_config_loader["delta_to_close_timestamp"]
            ),
            allocation_table=self.allocation_table,
            strategy_configs=strategy_configs,
            symbols=symbols_dict,
            data_providers=providers_dict,
            max_cores=cast(int, global_config_loader["max_cores"]),
            db_connections=db_connections,
            observed_data_lookback=str(
                global_config_loader["observed_data_lookback"]
            ),
            future_data_lookahead=str(self.args.future_data_lookahead),
            calendar=calendar,
        )

    def load_configurations(
        self,
    ) -> Tuple[
        Dict[str, ft.StrategyConfig],
        Dict[ft.AssetClass, List[ft.Symbol]],
        Dict[ft.AssetClass, Dict[ft.DataType, List[ft.DataProviderName]]],
    ]:
        """
        Loads in strategy configurations and updates the global configuration
        with necessary sources and symbols.

        Returns:
            Tuple containing strategy configurations, symbols dictionary, and data_providers dictionary.
        """
        loaded_strategy_configs = set([])
        loaded_sources: Dict[
            ft.AssetClass, Dict[ft.DataType, Set[ft.DataProviderName]]
        ] = defaultdict(lambda: defaultdict(set))
        loaded_symbols: Dict[ft.AssetClass, Set[ft.Symbol]] = defaultdict(set)
        strategy_configs: Dict[str, ft.StrategyConfig] = {}

        # Get strategy_table_names from the first timestamp
        first_timestamp = next(iter(self.allocation_table))
        initial_strategy_table_names = set(
            self.allocation_table[first_timestamp].keys()
        )

        for timestamp, strategy_configs_ts in self.allocation_table.items():
            strategy_table_names = set(strategy_configs_ts.keys())
            for strategy_table_name in strategy_configs_ts:
                if strategy_table_names != initial_strategy_table_names:
                    raise ValueError(
                        f"Inconsistent strategy_table_names at timestamp {timestamp}"
                    )

                allocation_entry = strategy_configs_ts[strategy_table_name]

                # Load each config name only once
                if allocation_entry["path"].get() in loaded_strategy_configs:
                    continue

                loaded_strategy_configs.add(allocation_entry["path"].get())
                self.logger.info(
                    f"Loading strategy config {strategy_table_name}"
                )

                loaded_config = load_config(allocation_entry["path"])
                loaded_config["table_name"] = strategy_table_name
                loaded_config["config_name"] = allocation_entry[
                    "path"
                ].get_name()

                def process_strategy_config(
                    loaded_config: ft.Config,
                ) -> ft.StrategyConfig:
                    data_providers: Dict = cast(
                        Dict, loaded_config["data_providers"]
                    )
                    for data_source in data_providers:
                        data_source["asset_class"] = ft.AssetClass(
                            data_source["asset_class"]
                        )

                    symbol_sources: Dict = cast(
                        Dict, loaded_config["symbol_sources"]
                    )
                    for symbol_provider in symbol_sources:
                        symbol_provider["asset_class"] = ft.AssetClass(
                            symbol_provider["asset_class"]
                        )
                    trade_config = cast(Dict, loaded_config["trade_config"])
                    trade_config["by_asset_class"] = {}
                    trade_config_keys = list(trade_config.keys()).copy()
                    for key in trade_config_keys:
                        if ft.AssetClass.is_valid(key):
                            trade_config["by_asset_class"][
                                ft.AssetClass(key)
                            ] = trade_config.pop(key)
                            trade_type_def = trade_config["by_asset_class"][
                                ft.AssetClass(key)
                            ]["type"]
                            trade_config["by_asset_class"][ft.AssetClass(key)][
                                "type"
                            ] = ft.TradeType(trade_type_def)
                    return cast(ft.StrategyConfig, loaded_config)

                # Use the custom function to process the configuration
                strategy_config = process_strategy_config(loaded_config)

                for data_source in strategy_config["data_providers"]:
                    asset_class: ft.AssetClass = data_source["asset_class"]
                    data_type: ft.DataType = ft.DataType(
                        data_source["data_type"]
                    )
                    source: ft.DataProviderName = ft.DataProviderName(
                        data_source["provider"]
                    )

                    loaded_sources[asset_class][data_type].add(source)

                for symbol_provider in strategy_config["symbol_sources"]:
                    fetched_symbols = build_symbols(
                        symbol_provider["asset_class"],
                        symbol_provider["sub_class"],
                    )
                    for result in fetched_symbols:
                        loaded_symbols[symbol_provider["asset_class"]].add(
                            result
                        )

                strategy_configs[strategy_table_name] = strategy_config
        symbols_dict: Dict[ft.AssetClass, List[ft.Symbol]] = {
            asset_class: list(symbols)
            for asset_class, symbols in loaded_symbols.items()
        }

        providers_dict: Dict[
            ft.AssetClass, Dict[ft.DataType, List[ft.DataProviderName]]
        ] = {
            asset_class: {
                data_type: list(sources)
                for data_type, sources in asset_data_sources.items()
            }
            for asset_class, asset_data_sources in loaded_sources.items()
        }
        return strategy_configs, symbols_dict, providers_dict

    def _build_config_path(self, json_config_path: ft.Config) -> ft.ConfigPath:
        config_path_args = {
            "base": ft.Path(json_config_path["base"]),
            "interior": ft.Path(json_config_path["interior"]),
            "config_type": str(json_config_path["config_type"]),
            "prod_type": str(json_config_path["prod_type"]),
            "name": ft.Path(json_config_path["name"]),
        }

        config_path = ft.ConfigPath(**config_path_args)
        return config_path
