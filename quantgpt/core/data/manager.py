import logging
import logging.config
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import pandas as pd

from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    nested_dict,
    read_data_file,
    run_multi_processed_code,
)

logger = logging.getLogger(__name__)


class DataWranger:
    def __init__(self):
        """
        Initializes the DataWranger object, used to do bespoke data wrangling.
        """
        pass

    def wrangle_loaded_data(
        self,
        df: pd.DataFrame,
        data_type: ft.DataType,
        data_provider: ft.DataProviderName,
    ) -> pd.DataFrame:
        if len(df) == 0:
            return df
        if data_type == ft.DataType.NEWS and (
            data_provider == ft.DataProviderName.BIOPHARMCATALYST
            or data_provider == ft.DataProviderName.TEST_BIOPHARMCATALYST
        ):
            df["Timestamp"] = df["MappedCloseTimestamp"]
            df = df.drop(columns=["MappedCloseTimestamp"])
        return df


class DataManager:
    def __init__(
        self, global_config: ft.GlobalConfig, logging_config: Dict[str, Any]
    ):
        """
        Initializes the DataManager object.

        Args:
            global_config: A dictionary containing global configuration settings.
            strategy_configs: A list of dictionaries containing strategy configurations.
        """
        logger.info("Initializing ft.Data Manager...")
        self.global_config = global_config
        self.strategy_configs = global_config["strategy_configs"]
        self.logging_config = logging_config

        self.timers = defaultdict(float)
        start = time.time()
        self.wrangler = DataWranger()
        self.data_dict = self._load_data()

        self.timers["load_data"] = time.time() - start
        logger.info("Data Manager Initialized")

        logger.info("Data Manager Aggregate Compute Timers:")
        for key in self.timers:
            logger.info(f"{key} - {self.timers[key]}")

    def _fetch_data(
        self,
        data_type: ft.DataType,
        provider_name: ft.DataProviderName,
        symbol: ft.Symbol,
        db_connection: ft.DBConnections,
    ) -> pd.DataFrame:
        """
        Fetches data from the given database connection.

        Args:
            data_type (str): The type of data (e.g. daily_ohlc, news, ..).
            provider_name (ft.DataProviderName): The provider of the data.
            symbol: The symbol string.
            db_connection: The database connection string (e.g. 'parquet', 'csv', or 'sqlite').

        Returns:
            A pandas DataFrame containing the fetched data.
        """
        # Set up the logging configuration, this is necessary because this runs in a multiprocessing pool
        logging.config.dictConfig(self.logging_config)

        try:
            if db_connection == ft.DBConnections.CSV:
                df = read_data_file(
                    data_type,
                    provider_name,
                    symbol,
                    db_connection,
                )

            elif db_connection == ft.DBConnections.SQLITE:
                raise NotImplementedError("SQLite not yet implemented")
            else:
                raise NotImplementedError(
                    f"db_connection={db_connection} not implemented"
                )
            if df.empty:
                logger.warning(
                    f"No ft.Data Found for connection={db_connection}, data_type={data_type}, provider_name={provider_name}, symbol={symbol}"
                )
                return pd.DataFrame()
            return df

        except Exception as e:
            logger.error(
                f"Error - {e} while fetching data with connection={db_connection}, data_type={data_type}, provider_name={provider_name}, symbol={symbol}"
            )
            return pd.DataFrame()

    def _append_data(
        self,
        data_type: ft.DataType,
        symbol: ft.Symbol,
        data: ft.DataDict,
        df: pd.DataFrame,
    ) -> None:
        """
        Appends data from input DataFrame to the input data dictionary

        Args:
            data_type (str): The type of data, e.g. 'daily_ohlc' or 'news'.
            symbol (str): The symbol of the asset for which data is being appended.
            data (Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]]): The target dictionary
                where data needs to be appended.
            df (pd.DataFrame): Input DataFrame containing rows of data to be appended.
        """

        if not data:
            data.update(nested_dict())
        for row in df.itertuples(index=False):
            timestamp = int(row.Timestamp)
            row_data = row._asdict()
            data[timestamp][symbol.asset_class][data_type][symbol] = row_data

    def _filter_df(
        self,
        data_type: ft.DataType,
        market_timestamps: List[ft.Timestamp],
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Filters the input dataframe based on the data_type and market_timestamps.

        Args:
            data_type (str): The type of data, e.g. 'daily_ohlc' or 'news'.
            market_timestamps (List[int]): List of market timestamps.
            df (pd.DataFrame): The input DataFrame with 'ft.Timestamp' column.

        Returns:
            pd.DataFrame: Filtered DataFrame.
        """
        if len(df) == 0:
            return df
        if data_type == ft.DataType.DAILY_OHLC and "Timestamp" in df.columns:
            df = df[df["Timestamp"].isin(market_timestamps)]
        elif data_type == ft.DataType.NEWS and "Timestamp" in df.columns:
            # For now News data is filtered with the same logic as daily_ohlc
            # this could change in the future as more exotic data_types
            # and logic are added to the framework
            df = df[df["Timestamp"].isin(market_timestamps)]
        return df

    def _load_symbol(
        self, args: Tuple[ft.Symbol, List[int], ft.GlobalConfig]
    ) -> Tuple[ft.DataDict, Dict[str, float]]:
        """
        Processes the symbol and fetches data.

        Args:
            args: A tuple containing symbol, market_timestamps and global_config.

        Returns:
            A tuple containing the fetched data and a dictionary of the time consumed by various sub-tasks.
        """
        symbol, market_timestamps, global_config = args
        data: ft.DataDict = defaultdict(nested_dict)
        times: Dict[str, float] = defaultdict(float)
        for data_type in global_config["data_providers"][symbol.asset_class]:
            for provider_name in global_config["data_providers"][
                symbol.asset_class
            ][data_type]:
                start = time.time()
                df = self._fetch_data(
                    data_type,
                    provider_name,
                    symbol,
                    global_config["db_connections"][provider_name],
                )
                times["fetch_data"] += time.time() - start

                df = self.wrangler.wrangle_loaded_data(
                    df, data_type, provider_name
                )
                start = time.time()
                filtered_df = self._filter_df(data_type, market_timestamps, df)
                times["filter_data"] += time.time() - start

                start = time.time()
                self._append_data(data_type, symbol, data, filtered_df)
                times["append_data"] += time.time() - start

        return data, times

    def _load_data(self) -> ft.DataDict:
        """
        Loads data using multi-processing.

        Returns:
            A dictionary containing the loaded data.
        """
        data = self._run_multi_processed_code()
        return data

    def _run_multi_processed_code(
        self,
    ) -> Dict[ft.Timestamp, Any]:
        all_args: List[Tuple] = [
            (
                symbol,
                self.global_config["calendar"].timestamps,
                self.global_config,
            )
            for _, symbols in self.global_config["symbols"].items()
            for symbol in symbols
        ]
        logger.info(
            f"Loading data with multi-processed code with {self.global_config['max_cores']} cores."
        )

        results = run_multi_processed_code(
            self._load_symbol, all_args, self.global_config["max_cores"]
        )
        data = self._unpack_and_process_results(results)
        sorted_data = dict(sorted(data.items()))

        return sorted_data

    def _unpack_and_process_results(
        self, results: List[Tuple[ft.DataDict, Dict[str, float]]]
    ) -> ft.DataDict:
        data: ft.DataDict = {}
        start = time.time()

        for result, times in results:
            for time_key in times:
                self.timers[time_key] += times[time_key]

            for timestamp, asset_classes in result.items():
                if timestamp not in data:
                    data[timestamp] = {}

                for asset_class, data_types in asset_classes.items():
                    if asset_class not in data[timestamp]:
                        data[timestamp][asset_class] = {}

                    for data_type, symbols in data_types.items():
                        if data_type not in data[timestamp][asset_class]:
                            data[timestamp][asset_class][data_type] = {}

                        for symbol, symbol_data in symbols.items():
                            data[timestamp][asset_class][data_type][
                                symbol
                            ] = symbol_data

        self.timers["process_results"] = time.time() - start
        return data
