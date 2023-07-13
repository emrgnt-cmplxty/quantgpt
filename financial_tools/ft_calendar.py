import bisect
import datetime
from datetime import timedelta
from typing import List

import pandas_market_calendars as mcal

from .types.basic_types import Timestamp
from .untyped_utils import convert_dt_to_est_fixed_time, convert_est_dt_to_unix_timestamp


class TradingCalendar:
    def __init__(
        self,
        data_start_date: str,
        data_end_date: str,
        delta_to_close_timestamp: str,
        calendar_name: str,
        freq="D",
    ):
        calendar = mcal.get_calendar(calendar_name)
        calendar_dates = calendar.valid_days(start_date=data_start_date, end_date=data_end_date)

        hours, minutes, seconds = map(int, delta_to_close_timestamp.split(":"))
        # We add the delta to close timestamp to the market close time to get the timestamp
        # for instance, if the delta is 24 hours and the initial timestamp is 12 am, the final
        # timestamp will be 12 am the next day
        timestamps = [
            convert_est_dt_to_unix_timestamp(
                convert_dt_to_est_fixed_time(ele)
                + timedelta(hours=hours, minutes=minutes, seconds=seconds)
            )
            for ele in calendar_dates
        ]

        self.timestamps = sorted(timestamps)
        self.freq = datetime.timedelta(days=1) if freq == "D" else None
        self.name = calendar_name

    def get_timestamps(self) -> List[Timestamp]:
        return self.timestamps

    def _delta(self, timestamp):
        if self.freq is not None:
            return self.freq
        else:
            raise NotImplementedError("Only daily frequency is currently supported.")

    def nearest(self, timestamp: Timestamp, direction="nearest") -> Timestamp:
        index = bisect.bisect_left(self.timestamps, timestamp)
        if direction == "before":
            if index == 0:
                raise ValueError("No timestamp before the given timestamp.")
            return self.timestamps[index - 1]
        elif direction == "after":
            if index == len(self.timestamps):
                raise ValueError("No timestamp after the given timestamp.")
            return self.timestamps[index]
        elif direction == "nearest":
            if index == 0:
                return self.timestamps[0]
            if index == len(self.timestamps):
                return self.timestamps[-1]
            before = self.timestamps[index - 1]
            after = self.timestamps[index]
            if after - timestamp < timestamp - before:
                return after
            else:
                return before
        else:
            raise ValueError("Invalid direction specified. Use 'before', 'after', or 'nearest'.")

    def range_search(self, start: Timestamp, end: Timestamp) -> List[Timestamp]:
        start_index = bisect.bisect_left(self.timestamps, start)
        end_index = bisect.bisect_right(self.timestamps, end)
        return self.timestamps[start_index:end_index]

    def add_timestamp(self, timestamp: Timestamp) -> None:
        index = bisect.bisect_left(self.timestamps, timestamp)
        if index > 0 and index < len(self.timestamps):
            if self.timestamps[index] - timestamp == timestamp - self.timestamps[index - 1]:
                bisect.insort(self.timestamps, timestamp)
            else:
                raise ValueError("Adding this timestamp would violate uniform sampling.")
        else:
            raise ValueError(
                "Adding this timestamp is not supported at the edges of the calendar."
            )

    def remove_timestamp(self, timestamp: Timestamp) -> None:
        index = bisect.bisect_left(self.timestamps, timestamp)
        if index < len(self.timestamps) and self.timestamps[index] == timestamp:
            self.timestamps.pop(index)
        else:
            raise ValueError("Timestamp not found in calendar.")
