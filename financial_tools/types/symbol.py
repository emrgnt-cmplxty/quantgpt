import os
from typing import List, Optional, Tuple, cast

import pandas as pd

from ..untyped_utils import home_path
from .enums import AssetClass


class SymbolChecker:
    _cache: dict[Tuple[AssetClass, Optional[str]], List[str]] = {}

    @classmethod
    def is_valid(
        cls,
        symbol: str,
        asset_class: AssetClass,
        subclass: Optional[str] = None,
    ):
        if (asset_class, subclass) not in cls._cache:
            cls._load_symbols(asset_class, subclass)

        return symbol in cls._cache[(asset_class, subclass)]

    @classmethod
    def _load_symbols(
        cls, asset_class: AssetClass, subclass: Optional[str] = None
    ):
        if subclass:
            file_path = os.path.join(
                home_path(),
                "data",
                asset_class.value,
                "symbols",
                f"{subclass}.csv",
            )
        else:
            file_path = os.path.join(
                home_path(), "data", asset_class.value, "symbols", "all.csv"
            )
        if not os.path.exists(file_path):
            raise ValueError(
                f"Invalid asset class or subclass: {asset_class}, {subclass}"
            )
        df = pd.read_csv(file_path)
        df_list = cast(List, df.Symbol.values)
        cls._cache[(asset_class, subclass)] = df_list


class Symbol:
    def __init__(
        self,
        symbol: str,
        asset_class: Optional[AssetClass] = None,
        subclass: Optional[str] = None,
    ):
        self.value = symbol
        self.asset_class = (
            asset_class if asset_class is not None else AssetClass.US_EQUITY
        )
        self.subclass = subclass

        if not SymbolChecker.is_valid(
            self.value, self.asset_class, self.subclass
        ):
            raise ValueError(f"Invalid symbol: {self.value}")

    def __repr__(self):
        return f"<Symbol {self.value} ({self.asset_class.value})>"

    def __hash__(self):
        return hash((self.value, self.asset_class))

    def __eq__(self, other):
        if not isinstance(other, Symbol):
            return NotImplemented
        return (
            self.value == other.value and self.asset_class == other.asset_class
        )
