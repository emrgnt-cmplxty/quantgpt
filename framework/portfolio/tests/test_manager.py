from datetime import datetime, timedelta

import pytest
import pytz

from financial_tools import types as ft
from financial_tools.utils import convert_est_datestr_to_unix_timestamp
from framework.portfolio.manager import PortfolioManager

timestamp = convert_est_datestr_to_unix_timestamp("2022-01-01")


def test_convert_datestr_to_unix_timestamp():
    assert timestamp == int(
        (datetime(2022, 1, 1) + timedelta(hours=5, minutes=0))
        .replace(tzinfo=pytz.utc)
        .timestamp()
    )  # Add 5 hours to UTC to account for EST conversion


@pytest.fixture
def mock_portfolio_manager():
    return PortfolioManager({})


@pytest.fixture
def mock_initialized_portfolio_manager():
    mock_portfolio_manager = PortfolioManager({})
    return mock_portfolio_manager


def test_portfolio_manager_update(mock_initialized_portfolio_manager):
    trades_by_strategy = [
        (
            "strategy1",
            [
                ft.Trade(
                    timestamp,
                    ft.Symbol("AAPL"),
                    100,
                    float("inf"),
                    "simple_fixed",
                ),
                ft.Trade(
                    timestamp,
                    ft.Symbol("GOOGL"),
                    50,
                    float("inf"),
                    "simple_fixed",
                ),
            ],
        ),
        (
            "strategy2",
            [
                ft.Trade(
                    timestamp,
                    ft.Symbol("AAPL"),
                    50,
                    float("inf"),
                    "simple_fixed",
                ),
                ft.Trade(
                    timestamp,
                    ft.Symbol("GOOGL"),
                    80,
                    float("inf"),
                    "simple_fixed",
                ),
            ],
        ),
    ]

    # Generate positions by strategy from trades
    positions_by_strategy = [
        (
            "strategy1",
            [
                ft.Position(ft.Symbol("AAPL"), 100, 1),
                ft.Position(ft.Symbol("GOOGL"), 50, 1),
            ],
        ),
        (
            "strategy2",
            [
                ft.Position(ft.Symbol("AAPL"), 50, 2),
                ft.Position(ft.Symbol("GOOGL"), 80, 1.2),
            ],
        ),
    ]

    mock_initialized_portfolio_manager.update(
        timestamp, trades_by_strategy, positions_by_strategy
    )

    # Constants for easier readability and maintainability

    AAPL_strategy1_quantity = 100
    GOOGL_strategy1_quantity = 50

    AAPL_strategy2_quantity = 50
    GOOGL_strategy2_quantity = 80

    AAPL_aggregated_quantity = (
        AAPL_strategy1_quantity + AAPL_strategy2_quantity
    )
    AAPL_aggregated_avg_price = (
        AAPL_strategy1_quantity * 1 + AAPL_strategy2_quantity * 2
    )
    AAPL_aggregated_avg_price /= AAPL_aggregated_quantity

    GOOGL_aggregated_quantity = (
        GOOGL_strategy1_quantity + GOOGL_strategy2_quantity
    )
    GOOGL_aggregated_avg_price = (
        GOOGL_strategy1_quantity + GOOGL_strategy2_quantity * 1.2
    )
    GOOGL_aggregated_avg_price /= GOOGL_aggregated_quantity

    # Assertions for aggregated_positions
    assert (
        mock_initialized_portfolio_manager.aggregated_positions[
            ft.Symbol("AAPL")
        ].quantity
        == AAPL_aggregated_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions[
            ft.Symbol("AAPL")
        ].cost_basis
        == AAPL_aggregated_avg_price
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions[
            ft.Symbol("GOOGL")
        ].quantity
        == GOOGL_aggregated_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions[
            ft.Symbol("GOOGL")
        ].cost_basis
        == GOOGL_aggregated_avg_price
    )

    # Assertions for positions by strategy1
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy1"
        ][ft.Symbol("AAPL")].quantity
        == AAPL_strategy1_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy1"
        ][ft.Symbol("AAPL")].cost_basis
        == 1
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy1"
        ][ft.Symbol("GOOGL")].quantity
        == GOOGL_strategy1_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy1"
        ][ft.Symbol("GOOGL")].cost_basis
        == 1
    )

    # Assertions for positions by strategy2
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy2"
        ][ft.Symbol("AAPL")].quantity
        == AAPL_strategy2_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
            "strategy2"
        ][ft.Symbol("AAPL")].cost_basis
        == 2
    )
    assert mock_initialized_portfolio_manager.aggregated_positions_by_strategy[
        "strategy2"
    ][ft.Symbol("GOOGL")].quantity == int(80)

    # Assertions for aggregated_trades
    assert (
        mock_initialized_portfolio_manager.aggregated_trades[
            ft.Symbol("AAPL")
        ].quantity
        == AAPL_aggregated_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_trades[
            ft.Symbol("GOOGL")
        ].quantity
        == GOOGL_aggregated_quantity
    )

    # Assertions for aggregated_trades_by_strategy
    assert (
        mock_initialized_portfolio_manager.aggregated_trades_by_strategy[
            "strategy1"
        ][ft.Symbol("AAPL")].quantity
        == AAPL_strategy1_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_trades_by_strategy[
            "strategy1"
        ][ft.Symbol("GOOGL")].quantity
        == GOOGL_strategy1_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_trades_by_strategy[
            "strategy2"
        ][ft.Symbol("AAPL")].quantity
        == AAPL_strategy2_quantity
    )
    assert (
        mock_initialized_portfolio_manager.aggregated_trades_by_strategy[
            "strategy2"
        ][ft.Symbol("GOOGL")].quantity
        == GOOGL_strategy2_quantity
    )
