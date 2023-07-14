import ast
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from bs4 import BeautifulSoup
from selenium import webdriver

from quantgpt.financial_tools import types as ft
from quantgpt.financial_tools.utils import (
    build_symbols,
    convert_datestr_to_datetime,
    convert_dt_to_est_fixed_time,
    convert_est_dt_to_unix_timestamp,
)
from quantgpt.pipeline.data_handler import DataHandler
from quantgpt.pipeline.scrapers.data_scraper import DataScraper

logger = logging.getLogger(__name__)


# Note, this function will sometimes "send" the data to the wrong day
# This is intentional, as the close timestamp from the previous day
# will be used in strategies, and we send data back to the previous day
# when it is observed before the T+1 Open
def map_to_appropriate_market_close_unix(
    timestamp_str: str, calendar: ft.TradingCalendar
) -> int:
    # Convert the timestamp string to a datetime object
    dt = convert_dt_to_est_fixed_time(
        convert_datestr_to_datetime(timestamp_str)
    )
    # Check if the time is before 9:30 AM
    if dt.time() < datetime.strptime("9:30", "%H:%M").time():
        # If it's before 9:30 AM, set the datetime object to the previous day
        # we do this because we want to send the data to the previous day's close
        dt = dt - timedelta(days=1)

    return calendar.nearest(convert_est_dt_to_unix_timestamp(dt), "before")


class scrapedDataScraper(DataScraper):
    def __init__(
        self,
        data_handler: DataHandler,
        driver_path: str,
        is_test_mode: bool = False,
    ):
        super().__init__(data_handler)
        self.driver_path = driver_path
        self.is_test_mode = is_test_mode
        self.test_tickers = data_handler.other_settings["test_tickers"]
        self.news_data_mapping = data_handler.other_settings[
            "news_data_mapping"
        ]
        self.max_pages = data_handler.other_settings["max_pages"]

        options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(
            executable_path=driver_path, options=options
        )
        self.calendar = ft.TradingCalendar(
            "2000-01-1",
            datetime.now().strftime("%Y-%m-%d"),
            "00:00:00",
            "NYSE",
        )

    def get_tickers(self):
        if self.is_test_mode:
            return self.test_tickers
        symbols = [
            {"Symbol": ele.value}
            for ele in build_symbols(ft.AssetClass.US_EQUITY, "biotech")
        ]
        return symbols

    def get_news(self, ticker, page=0):
        self.driver.get(
            f"https://www.scraped.com/api/news/{ticker}?page={page}"
        )
        u = self.driver.page_source
        try:
            v = "{" + "{".join(u.split("{")[1:]).replace(
                """</pre></body></html>""", ""
            )
            v = json.loads(v)
            return v["data"]["news"]["data"]
        except Exception as e:
            logger.error(f"Error in parsing news data for {ticker}")
            logger.error(e)
            return []

    def get_daily_ohlc(self, *args, **kwargs) -> List[Dict]:
        raise NotImplementedError(
            "get_daily_ohlc is not implemented for NewsDataScraper"
        )

    def get_financials(self, *args, **kwargs) -> List[Dict]:
        raise NotImplementedError(
            "get_financials is not implemented for NewsDataScraper"
        )

    def remove_html_tags(self, text) -> str:
        clean = re.compile("<.*?>")
        return re.sub(clean, "", text)

    def backfill(self) -> None:
        tickers = self.get_tickers()

        for ticker_info in tickers:
            ticker = ticker_info["Symbol"]
            logger.info(f"Fetching news data for {ticker}...")
            news_ticker = []
            for page in range(0, self.max_pages):
                news_data = self.get_news(ticker, page)
                if len(news_data) == 0:
                    break
                else:
                    for release in news_data:
                        try:
                            release_dict: Dict[str, Union[str, Any]] = {}
                            for key, value in release.items():
                                if key == "data":
                                    data_dict = ast.literal_eval(
                                        str(release[key])
                                    )
                                    for (
                                        sub_key,
                                        sub_value,
                                    ) in data_dict.items():
                                        if sub_key == "body":
                                            soup = BeautifulSoup(
                                                sub_value, "lxml"
                                            )
                                            raw_text = soup.get_text(
                                                separator="\n"
                                            )
                                            raw_text = self.remove_html_tags(
                                                raw_text
                                            )
                                            release_dict[
                                                self.news_data_mapping[
                                                    f"sub_{sub_key}"
                                                ]
                                            ] = raw_text

                                        else:
                                            release_dict[
                                                self.news_data_mapping[
                                                    f"sub_{sub_key}"
                                                ]
                                            ] = sub_value
                                else:
                                    release_dict[
                                        self.news_data_mapping[key]
                                    ] = value
                            release_dict[
                                "MappedCloseTimestamp"
                            ] = map_to_appropriate_market_close_unix(
                                release_dict["Created"], self.calendar
                            )
                            news_ticker.append(release_dict)

                        except Exception as e:
                            logger.error(
                                f"Error =  in parsing news data for {ticker} for key {key} and value {value}"
                            )
                            logger.error(e)
            self.data_handler.save_data(
                f"{ticker}", news_ticker, ft.DataType.NEWS, self.is_test_mode
            )
            logger.info(f"News data for {ticker} saved successfully.")
        logger.info("News data scraping completed.")

    def close(self):
        self.driver.quit()

    def daily_close(self) -> None:
        raise NotImplementedError("daily_close method not implemented")

    def live(self) -> None:
        raise NotImplementedError("live method not implemented")
