import logging
import multiprocessing
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, List, Tuple

import pytz

from financial_tools.constants import SECONDS_IN_DAY, SECONDS_IN_MINUTE

logger = logging.getLogger(__name__)


def get_logging_config(log_level=logging.INFO) -> dict:
    """Returns logging configuration."""

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": log_level,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": log_level,
        },
    }
    return logging_config


def convert_datestr_to_datetime(date_str: str) -> datetime:
    """Converts date_str to datetime object, attempting multiple formats."""
    formats = [
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass

    raise ValueError(f"Failed to parse date string {date_str} using available formats")


# Assumes date_str is in EST
def convert_est_datestr_to_unix_timestamp(date_str: str) -> int:
    # """Converts date_str (YYYY-MM-DD) to datetime object."""
    return convert_est_dt_to_unix_timestamp(convert_datestr_to_datetime(date_str))


def convert_est_dt_to_unix_timestamp(dt_eastern: datetime) -> int:
    """
    Converts datetime object to timestamp (UTC).
    Args:
        dt (datetime): Input datetime object.
    Returns:
        int: Timestamp in UTC.
    """
    dt_eastern = (
        convert_dt_to_est_fixed_time(dt_eastern) if dt_eastern.tzinfo is None else dt_eastern
    )
    return int(dt_eastern.astimezone(pytz.utc).timestamp())


def convert_dt_to_est_fixed_time(dt: datetime) -> datetime:
    return pytz.timezone("US/Eastern").localize(dt.replace(tzinfo=None))


def convert_timestamp_to_est_datetime(timestamp: int) -> datetime:
    """
    Converts timestamp to datetime object in EST.

    Args:
        timestamp (int): Input timestamp.

    Returns:
        datetime: Datetime object in EST.
    """
    return datetime.utcfromtimestamp(timestamp).astimezone(pytz.timezone("US/Eastern"))


def convert_time_delta_str(holding_period: str, conversion_type: str = "to_seconds") -> int:
    """
    Converts holding period to number of days.

    Args:
        holding_period (str): Holding period in the format 'Xd' or 'Xm' where X is an integer.

    Returns:
        int: Number of days.
    """
    if not isinstance(holding_period, str) or not holding_period:
        raise ValueError("Invalid holding period format: " + str(holding_period))

    if "_days" in holding_period:
        try:
            if conversion_type == "to_seconds":
                return int(holding_period.replace("_days", "")) * SECONDS_IN_DAY
            elif conversion_type == "to_days":
                return int(holding_period.replace("_days", ""))
        except ValueError:
            raise ValueError("Invalid holding period format: " + holding_period)
    elif "_minutes" in holding_period:
        try:
            if conversion_type == "to_seconds":
                return int(holding_period.replace("_minutes", "")) * SECONDS_IN_MINUTE
            elif conversion_type == "to_days":
                return int(holding_period.replace("_minutes", "")) // SECONDS_IN_MINUTE
        except ValueError:
            raise ValueError("Invalid holding period format: " + holding_period)
    raise ValueError("Invalid holding period: " + holding_period)


def home_path() -> str:
    """Returns the path to the home folder."""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    data_folder = os.path.join(script_dir, "..")
    return data_folder


def nested_dict() -> defaultdict:
    """Returns a nested defaultdict."""
    return defaultdict(nested_dict)


def run_multi_processed_code(
    function: Callable,
    all_args: List[Tuple],
    max_cores: int,
) -> List[Any]:
    logger.info(f"Running multi-processed code with {max_cores} cores.")

    with multiprocessing.Pool(max_cores) as pool:
        results = pool.map(function, all_args)

    return results


def run_multi_threaded_code(
    function: Callable,
    all_args: List[Tuple],
    max_workers: int,
) -> List[Any]:
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(function, *args) for args in all_args]

        for future in as_completed(futures):
            results.append(future.result())

    return results
