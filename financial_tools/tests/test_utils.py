import unittest
from datetime import datetime

import pandas as pd

from ..types import (
    AssetClass,
    ConfigPath,
    DataProviderName,
    DataType,
    DBConnections,
    Symbol,
    prepare_json_for_dump,
)
from ..untyped_utils import (
    convert_datestr_to_datetime,
    convert_dt_to_est_fixed_time,
    convert_est_datestr_to_unix_timestamp,
    convert_est_dt_to_unix_timestamp,
    convert_time_delta_str,
    convert_timestamp_to_est_datetime,
    get_logging_config,
    home_path,
)
from ..utils.typed_utils import build_symbols, load_config, read_data_file


class TestUtils(unittest.TestCase):
    def test_home_path(self):
        self.assertIsInstance(home_path(), str)

    def test_prepare_json_for_dump(self):
        input_data = {
            "test_symbol": Symbol("AAPL", AssetClass.US_EQUITY, None),
            "test_list": [1, 2, 3],
            "test_dict": {"key1": "value1", "key2": 2},
        }
        result = prepare_json_for_dump(input_data)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["test_symbol"], Symbol("AAPL").__repr__())
        self.assertEqual(result["test_list"], [1, 2, 3])
        self.assertEqual(result["test_dict"], {"key1": "value1", "key2": 2})

    def test_get_logging_config(self):
        config = get_logging_config()
        self.assertIsInstance(config, dict)
        self.assertIn("root", config)

    def test_convert_datestr_to_datetime(self):
        date_str = "2021-01-01"
        result = convert_datestr_to_datetime(date_str)
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2021)

    def test_convert_est_datestr_to_unix_timestamp(self):
        date_str = "2021-01-01"
        result = convert_est_datestr_to_unix_timestamp(date_str)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 1609477200)

    def test_convert_est_dt_to_unix_timestamp(self):
        dt_eastern = datetime(2021, 1, 1, 0, 0, 0)
        result = convert_est_dt_to_unix_timestamp(dt_eastern)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 1609477200)

    def test_convert_dt_to_est_fixed_time(self):
        dt = datetime(2021, 1, 1, 0, 0, 0)
        result = convert_dt_to_est_fixed_time(dt)
        self.assertIsInstance(result, datetime)
        self.assertIsNotNone(result.tzinfo)

    def test_convert_timestamp_to_est_datetime(self):
        timestamp = 1609477200
        result = convert_timestamp_to_est_datetime(timestamp)
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2021)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 1)
        self.assertIsNotNone(result.tzinfo)

    def test_convert_time_delta_str_to_seconds(self):
        holding_period = "3_days"
        result = convert_time_delta_str(holding_period)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 259200)

    def test_load_config(self):
        config_name = "test_simple_biotech_news_v0p0"
        # Assume the test_simple_biotech_news_v0p0.jsonc exists in the framework/configs/global directory
        result = load_config(
            ConfigPath(
                base="framework",
                interior="config",
                config_type="global",
                prod_type="test",
                name=config_name,
            )
        )
        self.assertIsInstance(result, dict)

    def test_build_symbols(self):
        asset_class = AssetClass.US_EQUITY
        sub_class = None
        # Assume the all.csv file exists in the data/equity/symbols directory
        result = build_symbols(asset_class, sub_class)
        self.assertIsInstance(result, list)
        self.assertTrue(all(isinstance(symbol, Symbol) for symbol in result))

    def test_read_data_file(self):
        data_type = DataType.DAILY_OHLC
        provider_name = DataProviderName.TEST_POLYGON
        symbol = Symbol("ACRX", AssetClass.US_EQUITY, None)
        db_connection = DBConnections.CSV
        # Assume the ACRX.csv file exists in the data/equity/daily_ohlc/test directory
        result = read_data_file(
            data_type, provider_name, symbol, db_connection
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertFalse(result.empty)


if __name__ == "__main__":
    unittest.main()
