import copy
import logging
import logging.config
import time
from multiprocessing import Pool
from typing import Dict, Generator, List, Tuple

import pandas as pd

from financial_tools import types as ft
from financial_tools.utils import (
    convert_datestr_to_datetime,
    convert_est_dt_to_unix_timestamp,
    convert_time_delta_str,
    nested_dict,
)

from .manager import DataManager

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(
        self,
        data_manager: DataManager,
        global_config: ft.GlobalConfig,
    ):
        self.data_manager = data_manager
        self.global_config = global_config
        self.observed_data: ft.ObservedData = nested_dict()

    def iter_data(
        self,
    ) -> Generator[
        Tuple[
            ft.Timestamp,
            ft.Timestamp,
            ft.ObservedData,
            ft.LiveData,
            ft.FutureData,
        ],
        None,
        None,
    ]:
        now = time.time()
        keys = list(self.data_manager.data_dict.keys())
        run_start_date = convert_est_dt_to_unix_timestamp(
            convert_datestr_to_datetime(self.global_config["run_start_date"])
        )
        run_end_date = convert_est_dt_to_unix_timestamp(
            convert_datestr_to_datetime(self.global_config["run_end_date"])
        )
        logger.info(
            "Iterating through data with timestamps between %s and %s.",
            run_start_date,
            run_end_date,
        )
        lookback = convert_time_delta_str(self.global_config["observed_data_lookback"], "to_days")
        lookahead = convert_time_delta_str(self.global_config["future_data_lookahead"], "to_days")

        logger.info("Pre-computing future observable data")
        precomputed_future_data = self._precompute_future_data(keys, lookahead)

        logger.info("Took %s seconds to pre-compute future observable data.", time.time() - now)
        total_yield_time = 0.0
        for i in range(1, len(self.data_manager.data_dict) - 1):
            now = time.time()

            timestamp_tm1: ft.Timestamp = int(keys[i - 1])
            timestamp: ft.Timestamp = int(keys[i])
            next_timestamp: ft.Timestamp = int(keys[i + 1])

            self._update_observed_data(timestamp_tm1, lookback)

            mock_live_data = self._get_clean_live_data(self.data_manager.data_dict[timestamp])

            future_data = precomputed_future_data[timestamp]

            if run_start_date <= timestamp < run_end_date:
                yield (
                    timestamp,
                    next_timestamp,
                    self.observed_data,
                    mock_live_data,
                    future_data,
                )
            total_yield_time += time.time() - now
        logger.info("Took %s seconds to yield data.", total_yield_time)

    def _get_clean_live_data(self, data: ft.Data) -> ft.LiveData:
        if (
            self.global_config["mode"] == ft.TradingMode.BACKTEST
            and self.global_config["trading_times"] == ft.TradingTimes.NYC_DAILY_OPEN
        ):
            mock_live_data = self._build_mock_live_data(data)
            return ft.LiveData(ft.TradingTimes.NYC_DAILY_OPEN, mock_live_data)

        raise ValueError("Only backtest with NYC open trading times is supported at this time.")

    def _build_mock_live_data(self, data: ft.Data) -> ft.Data:
        mock_live_data: ft.Data = nested_dict()

        for asset_class, data_type_dict in data.items():
            for data_type, symbol_dict in data_type_dict.items():
                for symbol, symbol_data in symbol_dict.items():
                    if data_type == ft.DataType.DAILY_OHLC:
                        mock_live_data[asset_class][data_type][symbol] = {
                            "Open": symbol_data["Open"]
                        }

        return mock_live_data

    def _update_observed_data(self, timestamp: ft.Timestamp, lookback: int):
        for asset_class, data_type_dict in self.data_manager.data_dict[timestamp].items():
            for data_type, symbol_dict in data_type_dict.items():
                for symbol, data in symbol_dict.items():
                    observed_data_df = self.observed_data[asset_class][data_type][symbol]
                    if not isinstance(observed_data_df, pd.DataFrame):
                        observed_data_df = pd.DataFrame(columns=data.keys())
                    new_data_df = pd.DataFrame(data, index=[0])

                    # Cast df boolean columns
                    self._cast_to_booleans(new_data_df)
                    self._cast_to_booleans(observed_data_df)
                    if not new_data_df.empty and not observed_data_df.empty:
                        observed_data_df = pd.concat(
                            [observed_data_df, new_data_df], ignore_index=True
                        ).tail(lookback)
                    elif not new_data_df.empty:
                        observed_data_df = new_data_df
                    self.observed_data[asset_class][data_type][symbol] = observed_data_df

    def _precompute_future_data_worker(
        self, args: Tuple["DataProcessor", int, int, List[ft.Timestamp], int]
    ) -> Dict[ft.Timestamp, ft.FutureData]:
        """
        Worker function for _precompute_future_data.

        :param args: A tuple containing the necessary arguments.
        :return: A dictionary with the precomputed future data for a chunk of timestamps.
        """

        data_processor, start_idx, end_idx, keys, lookahead = args
        chunk_future_data: Dict[ft.Timestamp, ft.FutureData] = {}

        future_data: ft.ObservedData = nested_dict()

        # Preload the necessary data for the chunk
        preload_data: ft.ObservedData = nested_dict()
        for i in range(end_idx + 1, min(end_idx + lookahead, len(keys))):
            timestamp = int(keys[i])
            for asset_class, data_type_dict in data_processor.data_manager.data_dict[
                timestamp
            ].items():
                for data_type, symbol_dict in data_type_dict.items():
                    for symbol, data in symbol_dict.items():
                        if not isinstance(
                            preload_data[asset_class][data_type][symbol], pd.DataFrame
                        ):
                            preload_data[asset_class][data_type][symbol] = pd.DataFrame()

                        data_df = pd.DataFrame.from_dict({key: [val] for key, val in data.items()})
                        preload_data_df = preload_data[asset_class][data_type][symbol]
                        # Cast df boolean columns
                        self._cast_to_booleans(data_df)
                        self._cast_to_booleans(preload_data_df)
                        if not data_df.empty and not preload_data_df.empty:
                            preload_data[asset_class][data_type][symbol] = pd.concat(
                                [data_df, preload_data_df],
                                ignore_index=True,
                            ).head(lookahead)
                        elif not data_df.empty:
                            preload_data[asset_class][data_type][symbol] = data_df

        # Process the chunk
        # TODO - modify this to stop maintaining the preload data after
        # we are past the buffer point (e.g. when we are at end_indx - lookahead)
        for i in range(end_idx, start_idx - 1, -1):
            timestamp = int(keys[i])
            for asset_class, data_type_dict in data_processor.data_manager.data_dict[
                timestamp
            ].items():
                for data_type, symbol_dict in data_type_dict.items():
                    for symbol, data in symbol_dict.items():
                        data_df = pd.DataFrame.from_dict({key: [val] for key, val in data.items()})
                        if not isinstance(
                            preload_data[asset_class][data_type][symbol], pd.DataFrame
                        ):
                            preload_data[asset_class][data_type][symbol] = pd.DataFrame()

                        preload_df = preload_data[asset_class][data_type][symbol]
                        future_data[asset_class][data_type][symbol] = data_df

                        if not data_df.empty and not preload_df.empty:
                            # Cast df boolean columns
                            self._cast_to_booleans(preload_df)
                            self._cast_to_booleans(data_df)
                            concat_df = pd.concat(
                                [data_df, preload_df],
                                ignore_index=True,
                            ).head(lookahead)
                            future_data[asset_class][data_type][symbol] = concat_df

                            # Update the preload_data
                            preload_data[asset_class][data_type][symbol] = concat_df

                        elif not data_df.empty:
                            future_data[asset_class][data_type][symbol] = data_df

                            # Update the preload_data
                            preload_data[asset_class][data_type][symbol] = data_df

            chunk_future_data[timestamp] = copy.deepcopy(future_data)

        return chunk_future_data

    @staticmethod
    def _cast_to_booleans(df: pd.DataFrame) -> None:
        """
        Cast all object-dtype columns containing all boolean values to bool dtype.

        :param df: A pandas DataFrame.
        :return: A pandas DataFrame with the casted columns.
        """
        return
        # # Identify object-dtype columns containing all boolean values
        # bool_columns = [
        #     col
        #     for col in df.columns
        #     if df[col].dtype == "object" and all(isinstance(x, bool) for x in df[col])
        # ]

        # # Cast these columns to bool dtype
        # for col in bool_columns:
        #     df[col] = df[col].astype("bool")

    def _precompute_future_data(
        self, keys: List[ft.Timestamp], lookahead: int
    ) -> Dict[ft.Timestamp, ft.FutureData]:
        """
        Precompute future data for a list of keys and a given lookahead.

        :param keys: A list of timestamps.
        :param lookahead: The number of future time steps to precompute.
        :return: A dictionary with the precomputed future data for each timestamp.
        """

        precomputed_future_data: Dict[ft.Timestamp, ft.FutureData] = {}

        # Define the number of chunks and calculate the chunk size
        num_chunks = self.global_config["max_cores"]
        chunk_size = (len(keys) + num_chunks - 1) // num_chunks

        # Prepare the arguments for multiprocessing
        precompute_future_data_args = [
            (self, i * chunk_size, min((i + 1) * chunk_size, len(keys)) - 1, keys, lookahead)
            for i in range(num_chunks)
        ]

        # Use a process pool to parallelize the computation
        with Pool(processes=num_chunks) as pool:
            results = pool.map(self._precompute_future_data_worker, precompute_future_data_args)
        # Combine the results from different processes
        for chunk_future_data in results:
            for timestamp in chunk_future_data:
                if timestamp not in precomputed_future_data:
                    precomputed_future_data[timestamp] = chunk_future_data[timestamp]
        return precomputed_future_data
