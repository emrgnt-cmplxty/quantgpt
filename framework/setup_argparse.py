import argparse


def setup_argparse() -> argparse.Namespace:
    """
    Sets up and returns the argparse.Namespace with parsed command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        help="Mode to run the framework in",
        type=str,
        default="backtest",
    )
    parser.add_argument(
        "--global_config_path_str",
        help="Which global configuration file to load?",
        type=str,
        default="framework_config_global_test_test_simple_biotech_news_v0p0",
    )
    parser.add_argument(
        "--data_lookahead_days",
        help="(Calendar) Days past the end of the of backtest to load data for. This directly adds days to run_start_date, which can cause failures if not selected properly in conjunction with future_data_lookahead_days. Moreover, this variable can impact performance tracking behavior, depending on how the implemented performance manager consumes observed data.",
        type=str,
        default="7_days",  # Pick a large value to be safe, generally 1.5x future_data_lookahead_days works unless future_data_lookahead_days is very small.
    )
    parser.add_argument(
        "--future_data_lookahead",
        help="(Business) Day length of future_data yielded by the DataManager. This can cause failures if not selected properly in conjunction with data_lookahead_days. Moreover, this variable can impact performance tracking behavior, depending on how the implemented performance manager consumes observed data.",
        type=str,
        default="2_days",
    )
    parser.add_argument(
        "--start",
        help="Start date of the run",
        type=str,
        default="2022-08-01",
    )
    parser.add_argument("--end", help="End date of the run", type=str, default="2022-10-01")
    parser.add_argument(
        "--log_level",
        help="Logging level",
        type=str,
        default="INFO",
    )

    args = parser.parse_args()
    return args
