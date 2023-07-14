import datetime

import pytest

from quantgpt.financial_tools.ft_calendar import TradingCalendar
from quantgpt.financial_tools.untyped_utils import (
    convert_dt_to_est_fixed_time,
    convert_est_dt_to_unix_timestamp,
)


@pytest.fixture
def calendar_instance():
    global_config = {
        "calendar_name": "NYSE",
        "data_start_date": "2000-01-01",
        "data_end_date": "2023-03-31",
        "delta_to_close_timestamp": "00:00:00",
    }

    return TradingCalendar(**global_config)


def test_nearest(calendar_instance):
    query_timestamp = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5, 12))
    )
    nearest_before = calendar_instance.nearest(
        query_timestamp, direction="before"
    )
    nearest_after = calendar_instance.nearest(
        query_timestamp, direction="after"
    )
    nearest_overall = calendar_instance.nearest(
        query_timestamp, direction="nearest"
    )

    assert nearest_before == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5))
    )
    assert nearest_after == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 6))
    )
    assert nearest_overall == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5))
    )


def test_nearest_2(calendar_instance):
    query_timestamp = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5, 13))
    )
    nearest_before = calendar_instance.nearest(
        query_timestamp, direction="before"
    )
    nearest_after = calendar_instance.nearest(
        query_timestamp, direction="after"
    )
    nearest_overall = calendar_instance.nearest(
        query_timestamp, direction="nearest"
    )

    assert nearest_before == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5))
    )
    assert nearest_after == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 6))
    )
    assert nearest_overall == convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 6))
    )


def test_range_search(calendar_instance):
    start = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 4))
    )
    end = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 7))
    )
    range_result = calendar_instance.range_search(start, end)
    expected_result = [
        convert_est_dt_to_unix_timestamp(
            convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 4))
        ),
        convert_est_dt_to_unix_timestamp(
            convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 5))
        ),
        convert_est_dt_to_unix_timestamp(
            convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 6))
        ),
    ]

    assert range_result == expected_result


def test_add_remove_timestamp(calendar_instance):
    new_timestamp = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 11))
    )

    calendar_instance.remove_timestamp(new_timestamp)
    assert new_timestamp not in calendar_instance.timestamps

    calendar_instance.add_timestamp(new_timestamp)
    assert new_timestamp in calendar_instance.timestamps


def test_invalid_add_timestamp(calendar_instance):
    # invalid because it already exists in the range
    invalid_timestamp = convert_est_dt_to_unix_timestamp(
        convert_dt_to_est_fixed_time(datetime.datetime(2023, 1, 11))
    )
    with pytest.raises(ValueError):
        calendar_instance.add_timestamp(invalid_timestamp)
