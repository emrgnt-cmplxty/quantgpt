import logging
from datetime import datetime, timedelta
from glob import glob
from typing import Dict, List

import pandas as pd
import requests

from financial_tools import types as ft
from financial_tools.utils import build_symbols, home_path

from ..data_handler import DataHandler
from .data_scraper import DataScraper

logger = logging.getLogger(__name__)


class PolygonDataScraper(DataScraper):
    def __init__(
        self,
        data_handler: DataHandler,
        api_key: str,
        regenerate_tickers: bool = False,
        exclude_existing: bool = False,
        is_test_mode: bool = False,
    ):
        super().__init__(data_handler)
        self.api_key = api_key
        self.scale_to_ts_in_seconds = (
            True if data_handler.other_settings["scale_to_ts_in_seconds"] else False
        )
        self.is_test_mode = is_test_mode
        self.test_tickers = data_handler.other_settings["test_tickers"]
        self.ohlc_map = data_handler.other_settings["ohlc_map"]
        self.news_map = data_handler.other_settings["news_map"]
        self.financials_map = data_handler.other_settings["financials_map"]
        self.tickers_map = data_handler.other_settings["tickers_map"]
        self.regenerate_tickers = regenerate_tickers
        self.exclude_existing = exclude_existing

    def _handle_request_error(self, response: requests.Response) -> None:
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")

    def get_tickers(self, limit: int = 1000) -> List[Dict]:
        if self.is_test_mode:
            return self.test_tickers

        url = f"https://api.polygon.io/v3/reference/tickers?limit={limit}&apiKey={self.api_key}"
        all_tickers = []
        while url:
            response = requests.get(url)
            self._handle_request_error(response)
            data = response.json()
            all_tickers.extend(data["results"])
            url = data.get("next_url")
            if url:
                url = f"{url}&apiKey={self.api_key}"
        return all_tickers

    def get_news(self, ticker, limit=1000):
        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit={limit}&apiKey={self.api_key}"
        response = requests.get(url)
        self._handle_request_error(response)
        return response.json()["results"]

    def get_daily_ohlc(self, ticker, start_date, end_date):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?apiKey={self.api_key}"
        response = requests.get(url)
        self._handle_request_error(response)
        return response.json()["results"]

    def get_financials(self, ticker):
        url = f"https://api.polygon.io/vX/reference/financials/?{ticker}&apiKey={self.api_key}"
        response = requests.get(url)
        self._handle_request_error(response)
        return response.json()["results"]

    def backfill(self, remove_today=True, window_in_days=20 * 365):
        # Get tickers
        if self.regenerate_tickers:
            tickers = self.get_tickers()
            self.data_handler.save_data(
                "all", tickers, ft.DataType.SYMBOLS, self.is_test_mode, column_map=self.tickers_map
            )

        else:
            if self.is_test_mode:
                tickers = build_symbols(ft.AssetClass.US_EQUITY, "test_0")
            else:
                tickers = build_symbols(ft.AssetClass.US_EQUITY, "all")
            tickers = [{"ticker": ele.value} for ele in tickers]
        logger.info("Saving tickers...")

        existing_data = set(
            [
                ele.split("/")[-1].replace(".csv", "")
                for ele in glob(
                    "%s/data/%s/daily_ohlc/polygon/*"
                    % (home_path(), ft.AssetClass.US_EQUITY.value)
                )
            ]
        )

        def filter_tickers(tickers, existing_data):
            if self.exclude_existing and not self.is_test_mode:
                return [ticker for ticker in tickers if ticker["ticker"] not in existing_data]
            return tickers

        filtered_tickers = filter_tickers(tickers, existing_data)

        # Fetch the data for each ticker
        for ticker in filtered_tickers:
            symbol = ticker["ticker"]
            logger.info(f"Fetching data for {symbol}...")

            # Fetch the saved news for this ticker
            try:
                news = self.get_news(symbol)
                self.data_handler.save_data(
                    f"{symbol}",
                    news,
                    ft.DataType.NEWS,
                    self.is_test_mode,
                    column_map=self.news_map,
                )
                logger.info("News Saved Successfully")
            except Exception as e:
                logger.error(f"Error fetching news for {symbol}: {e}")

            # Fetch the daily OHLC
            try:
                if remove_today:
                    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    end_date = datetime.now().strftime("%Y-%m-%d")
                start_date = (datetime.now() - timedelta(days=window_in_days)).strftime("%Y-%m-%d")
                ohlc = self.get_daily_ohlc(symbol, start_date, end_date)
                if self.scale_to_ts_in_seconds:
                    self.scale_ohlc(ohlc)

                self.data_handler.save_data(
                    f"{symbol}",
                    ohlc,
                    ft.DataType.DAILY_OHLC,
                    self.is_test_mode,
                    column_map=self.ohlc_map,
                )
                logger.info("Daily OHLC Fetched Successfully")
            except Exception as e:
                logger.error(f"Error fetching daily OHLC for {symbol}: {e}")

            # Fetch the financials for this ticker
            try:
                financials = self.get_financials(symbol)
                self.data_handler.save_data(
                    f"{symbol}",
                    financials,
                    ft.DataType.FINANCIALS,
                    self.is_test_mode,
                    column_map=self.financials_map,
                )
                logger.info("Financials Fetched Successfully")
            except Exception as e:
                logger.error(f"Error fetching financials for {symbol}: {e}")
            logger.info("-" * 100 + "Done" + "-" * 100)

    def scale_ohlc(self, ohlc) -> pd.DataFrame:
        # Divide every entry in the timestamp column by 1000 to convert to seconds
        for ohlc_entry in ohlc:
            ohlc_entry["t"] = ohlc_entry["t"] // 1000
        return ohlc

    def daily_close(self) -> None:
        raise NotImplementedError("daily_close method not implemented")

    def live(self) -> None:
        raise NotImplementedError("live method not implemented")
