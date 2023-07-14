import json
from enum import Enum
from typing import Any

from ..constants import ANNUALIZATION_FACTOR  # noqa: F401
from ..constants import (  # noqa: F401
    RISK_FREE_RATE,
    SECONDS_IN_DAY,
    SECONDS_IN_MINUTE,
)
from ..ft_calendar import TradingCalendar  # noqa: F401
from .advanced_types import AllocationConfig  # noqa: F401
from .advanced_types import AllocationEntry  # noqa: F401
from .advanced_types import Config  # noqa: F401
from .advanced_types import ConfigPath  # noqa: F401
from .advanced_types import ConfigPathDict  # noqa: F401
from .advanced_types import ConfigType  # noqa: F401
from .advanced_types import Data  # noqa: F401
from .advanced_types import DataDict  # noqa: F401
from .advanced_types import DataSource  # noqa: F401
from .advanced_types import FutureData  # noqa: F401
from .advanced_types import GlobalConfig  # noqa: F401
from .advanced_types import LiveData  # noqa: F401
from .advanced_types import ObservedData  # noqa: F401
from .advanced_types import ProdType  # noqa: F401
from .advanced_types import StrategyAllocation  # noqa: F401
from .advanced_types import StrategyConfig  # noqa: F401
from .advanced_types import SymbolSource  # noqa: F401
from .advanced_types import TradeConfig  # noqa: F401
from .basic_types import DBSource, Path, StrategyName, Timestamp  # noqa: F401
from .enums import AssetClass  # noqa: F401
from .enums import DataProviderName  # noqa: F401
from .enums import DataType  # noqa: F401
from .enums import DBConnections  # noqa: F401
from .enums import SignalType  # noqa: F401
from .enums import TradingMode  # noqa: F401
from .enums import TradingTimes  # noqa: F401
from .objects import (  # noqa: F401
    AggregatedPositions,
    AggregatedTrades,
    Position,
    Signal,
    Trade,
    TradeType,
)
from .symbol import Symbol, SymbolChecker  # noqa: F401


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, AssetClass):
            return obj.value
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, ConfigPath):
            return obj.get()
        if isinstance(obj, TradingCalendar):
            return "Calendar %s, Timestamps: %s" % (obj.name, obj.timestamps)
        return super().default(obj)


def prepare_json_for_dump(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            prepare_json_for_dump(key): prepare_json_for_dump(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [prepare_json_for_dump(value) for value in obj]
    elif isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, Symbol):
        return obj.__repr__()
    elif isinstance(obj, TradingCalendar):
        return "Calendar %s, Timestamps: %s" % (obj.name, obj.timestamps)
    else:
        return obj
