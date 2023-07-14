import os
from typing import List, Optional

import jsoncomment
import pandas as pd

from quantgpt.financial_tools.untyped_utils import home_path

from ..types.advanced_types import (
    AssetClass,
    Config,
    ConfigPath,
    DataProviderName,
    DataType,
    DBConnections,
    Symbol,
)


def load_config(config_path: ConfigPath) -> Config:
    """
    Loads the configuration file from the local configs directory.

    Args:
        config_path (str): The path to the configuration file

    Returns:
        Dict[str, Any]: Configuration containing the subconfigs and values.
    """
    with open(config_path.get(), "r") as f:
        config: Config = jsoncomment.JsonComment().load(f)

    return config


def build_symbols(
    asset_class: AssetClass, sub_class: Optional[str]
) -> List[Symbol]:
    """
    Builds symbols from a given asset_class and subclass.

    Args:
        asset_class (str): The asset class to fetch symbols from.
        sub_class (str): The name of the asset sub class.

    Returns:
        List[str]: List containing constructed symbols.
    """
    list_name = sub_class if sub_class is not None else "all"
    csv_file_path = os.path.join(
        home_path(), "data", asset_class.value, "symbols", f"{list_name}.csv"
    )
    df = pd.read_csv(csv_file_path)
    # Filter out bad data
    # TODO - This is a hack to avoid doing this upstream and should be removed once data is cleaned
    df = df[df.Symbol == df.Symbol]

    return [
        Symbol(ele, asset_class, sub_class) for ele in df["Symbol"].tolist()
    ]


def read_data_file(
    data_type: DataType,
    provider_name: DataProviderName,
    symbol: Symbol,
    db_connection: DBConnections,
) -> pd.DataFrame:
    """
    Reads data from the specified file path.

    Args:
        file_path (str): Path of the file to be read.
        data_type (DataType): The type of data (e.g. daily_ohlc, news, ..).
        provider_name (DataProviderName): The provider of the data.
        symbol : The symbol for the asset to be loaded
    Returns:
        pd.DataFrame: DataFrame containing the data from the file.
                     Returns an empty DataFrame if file doesn't exist.
    """
    file_path = os.path.join(
        home_path(),
        "data",
        symbol.asset_class.value,
        data_type.value,
        provider_name.value,
        f"{symbol.value}.{db_connection}",
    )

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        if "parquet" in file_path:
            return pd.read_parquet(file_path)
        elif "csv" in file_path:
            return pd.read_csv(file_path)
        else:
            raise ValueError(f"File type not supported: {file_path}")
    except Exception as e:
        raise ValueError(
            f"Error reading data file: {file_path}. Error: {str(e)}"
        )
