import argparse
import logging.config
import os

from quantgpt.financial_tools.utils import get_logging_config, home_path

from quantgpt.pipeline.data_handler import DataHandler
from quantgpt.pipeline.scrapers.biopharmcatalyst import (
    BiopharmCatalystDataScraper,
)
from quantgpt.pipeline.scrapers.polygon import PolygonDataScraper


def main(args: argparse.Namespace) -> None:
    logging_config = get_logging_config(args.log_level)
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    logger.info("Running Main")
    if args.is_test_run:
        logger.info("-" * 100 + "Running in Test Mode" + "-" * 100)

    # get absolute path to config file
    config_path = os.path.join(
        home_path(),
        "pipeline",
        "config",
        args.provider_config
        if args.provider_config != ""
        else (args.scraper + ".jsonc"),
    )
    logger.info("Config Path %s ", config_path)
    data_handler = DataHandler(
        jsonc_file=config_path
    )  # Use "db" for database mode
    if args.scraper == "polygon":
        PolygonDataScraper(
            data_handler,
            args.api_key,
            regenerate_tickers=args.regenerate_tickers,
            exclude_existing=args.exclude_existing,
            is_test_mode=args.is_test_run,
        ).backfill()
    elif args.scraper == "biopharmcatalyst":
        BiopharmCatalystDataScraper(
            data_handler,
            args.chrome_driver_path,
            is_test_mode=args.is_test_run,
        ).backfill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scraper",
        help="Which provider to fetch data from?",
        type=str,
        default="polygon",
    )
    parser.add_argument(
        "--provider_config",
        help="Which provider config to use?",
        type=str,
        default="",
    )
    parser.add_argument(
        "--log_level",
        help="Logging level",
        type=str,
        default="INFO",
    )
    parser.add_argument(
        "--api_key",
        help="API Key for provider",
        type=str,
        default="DEMO_KEY",
    )
    parser.add_argument(
        "--chrome_driver_path",
        help="API Key for provider",
        type=str,
        default="/Users/ocolegrove/Downloads/chromedriver_mac_arm64/chromedrive",
    )
    parser.add_argument(
        "--is_test_run",
        help="Run in test mode?",
        type=bool,
        default=False,
    )
    parser.add_argument(
        "--exclude_existing",
        help="Exclude existing data?",
        type=bool,
        default=True,
    )
    parser.add_argument(
        "--regenerate_tickers",
        help="Regenerate the ticker set?",
        type=bool,
        default=False,
    )

    args = parser.parse_args()
    main(args)
