from abc import ABC, abstractmethod
from typing import Dict, List

from quantgpt.pipeline.data_handler import DataHandler


class DataScraper(ABC):
    def __init__(self, data_handler: DataHandler):
        self.data_handler = data_handler

    @abstractmethod
    def get_tickers(self) -> List[Dict]:
        pass

    @abstractmethod
    def get_news(self, ticker: str, limit: int = 1000) -> List[Dict]:
        pass

    @abstractmethod
    def get_daily_ohlc(
        self, ticker: str, start_date: str, end_date: str
    ) -> List[Dict]:
        pass

    @abstractmethod
    def get_financials(self, ticker: str) -> List[Dict]:
        pass

    @abstractmethod
    def backfill(self) -> None:
        pass

    @abstractmethod
    def daily_close(self) -> None:
        pass

    @abstractmethod
    def live(self) -> None:
        pass
