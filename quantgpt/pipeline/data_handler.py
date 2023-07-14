import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
from quantgpt.financial_tools import types as ft

logger = logging.getLogger(__name__)


class DataHandler:
    def __init__(
        self,
        jsonc_file: str = "config.jsonc",
    ):
        logger.info("Initializing DataHandler")
        self.jsonc_file = jsonc_file
        self._read_jsonc_file()

        if self.mode == "db":
            self._setup_db()
        elif self.mode == "csv":
            self._setup_csv_directory()

    def _read_jsonc_file(self):
        with open(self.jsonc_file, "r") as file:
            content = "".join(
                [line for line in file if not line.strip().startswith("//")]
            )
            config = json.loads(content)

        self.csv_dir = config.get("csv_dir")
        self.db_name = config.get("db_name")
        self.mode = config.get("data_mode")
        self.source = config.get("data_source")
        self.asset_class = config.get("data_asset_class")
        self.other_settings = config.get("other_settings")

    # Note, the commented method has not been tested and was generated algorithmically as part of set-up
    def _setup_db(self):
        raise NotImplementedError("Database mode is not implemented yet")

    def _setup_csv_directory(self):
        Path(self.csv_dir).mkdir(parents=True, exist_ok=True)

    def save_data(
        self,
        file_name_prefix: str,
        data: List[Dict],
        data_type: ft.DataType,
        is_test_mode: bool = False,
        column_map: Dict[str, str] = {},
    ):
        if self.mode == "db":
            raise NotImplementedError("Database mode is not implemented yet")
        elif self.mode == "csv":
            self._save_to_csv(
                file_name_prefix, data, data_type, is_test_mode, column_map
            )

    # Note, the commented method has not been tested and was generated algorithmically as part of set-up
    def _save_to_db(self, file_name_prefix: str, data: List[Dict]):
        raise NotImplementedError("Database mode is not implemented yet")

    def _save_to_csv(
        self,
        file_name_prefix: str,
        data: List[Dict],
        data_type: ft.DataType,
        is_test_mode: bool,
        column_map: Dict[str, str],
    ):
        if not data:
            return
        # Get the column names dynamically from the first row of data
        column_names = list(data[0].keys())

        csv_file = os.path.join(
            self.csv_dir,
            self.asset_class,
            data_type.value,
        )
        if data_type != ft.DataType.SYMBOLS:
            csv_file = os.path.join(
                csv_file,
                f"test_{self.source}" if is_test_mode else self.source,
                f"{file_name_prefix}.csv",
            )
        else:
            csv_file = os.path.join(
                csv_file,
                f"test_{file_name_prefix}.csv"
                if is_test_mode
                else f"{file_name_prefix}.csv",
            )

        # Create path if it does not yet exist
        Path(os.path.dirname(csv_file)).mkdir(parents=True, exist_ok=True)
        logger.info("Saving a File to Path = %s " % (csv_file))
        df = pd.DataFrame(data)
        for column in df.columns:
            if column not in column_names:
                df = df.drop(columns=column)
        df = df.rename(columns=column_map)
        df.to_csv(csv_file, index=False)
