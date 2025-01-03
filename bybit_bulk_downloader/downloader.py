"""
bybit_bulk_downloader
"""

import gzip
# import standard libraries
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pandas as pd
# import third-party libraries
import requests
from bs4 import BeautifulSoup
from pybit.unified_trading import HTTP
from rich import print
from rich.progress import track


class BybitBulkDownloader:
    """
    Bulk downloader for Bybit data.
    """

    _CHUNK_SIZE = 20
    _BYBIT_DATA_DOWNLOAD_BASE_URL = "https://public.bybit.com"
    _DATA_TYPE = (
        "kline_for_metatrader4",
        "premium_index",
        "spot_index",
        "trading",
        "fundingRate",
        "klines",
    )

    def __init__(
        self, destination_dir=".", data_type="trading", klines_category="linear"
    ):
        """
        :param destination_dir: Directory to save the downloaded data.
        :param data_type: Data type to download. Available data types are: "kline_for_metatrader4", "premium_index", "spot_index", "trading", "fundingRate", "klines".
        :param klines_category: Klines category to download. Available categories are: "linear". ("spot", "inverse" is not supported yet.)
        """
        self._destination_dir = destination_dir
        self._data_type = data_type
        self._klines_category = klines_category
        self.session = HTTP()

    def _get_url_from_bybit(self):
        """
        Get the URL of the data to download from Bybit.
        :return: list of URLs to download.
        """
        url = self._BYBIT_DATA_DOWNLOAD_BASE_URL + "/" + self._data_type + "/"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        symbol_list = []
        for link in soup.find_all("a"):
            link_sym = link.get("href")
            if self._data_type == "kline_for_metatrader4":
                soup_year = BeautifulSoup(
                    requests.get(url + link.get("href")).text, "html.parser"
                )
                for link_year in soup_year.find_all("a"):
                    link_sym += link_year.get("href")
                    symbol_list.append(link_sym)
            else:
                symbol_list.append(link_sym)
        download_list = []
        for sym in track(symbol_list, description="Listing files"):
            soup_sym = BeautifulSoup(requests.get(url + sym).text, "html.parser")
            for link in soup_sym.find_all("a"):
                download_list.append(url + sym + link.get("href"))

        return download_list

    @staticmethod
    def make_chunks(lst: list, n: int) -> list:
        """
        Make chunks
        :param lst: Raw list
        :param n: size of chunk
        :return: list of chunks
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    def _download(self, url: str):
        """
        Execute the download.
        :param url: URL
        :return: None
        """
        print(f"Downloading: {url}")
        prefix_start = 3
        prefix_end = 6
        if self._data_type == "kline_for_metatrader4":
            prefix_end += 1
        # Create the destination directory if it does not exist
        parts = url.split("/")
        parts.insert(3, "bybit_data")
        prefix = "/".join(parts[prefix_start:prefix_end])

        # Download the file
        filepath = os.path.join(
            str(self._destination_dir) + "/" + "/".join(parts[prefix_start:])
        )
        filedir = os.path.dirname(filepath)
        # if not exists, create the directory
        if not os.path.exists(filedir):
            os.makedirs(filedir)

        print(f"[green]Downloading: {filepath}[/green]")
        response = requests.get(url, filepath)
        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        # Decompress the file
        print(f"[green]Unzipped: {filepath}[/green]")
        with gzip.open(filepath, mode="rb") as gzip_file:
            with open(filepath.replace(".gz", ""), mode="wb") as decompressed_file:
                shutil.copyfileobj(gzip_file, decompressed_file)

        # Delete the compressed file
        os.remove(filepath)
        print(f"[green]Deleted: {filepath}[/green]")

    def download(self, url: str):
        """
        Download the file.
        """
        self._download(url)

    @staticmethod
    def generate_dates_until_today(start_year: int, start_month: int) -> list:
        """
        Generate dates until today (2 months at a time)
        :param start_year:
        :param start_month:
        :return: list of dates
        """
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime.today()

        output = []
        while start_date <= end_date:
            next_date = start_date + timedelta(days=60)  # Roughly two months
            if next_date > end_date:
                next_date = end_date
            output.append(
                f"{start_date.strftime('%Y-%m-%d')} {next_date.strftime('%Y-%m-%d')}"
            )
            start_date = next_date + timedelta(days=1)

        return output

    def _download_fundingrate(self):
        """
        Download funding rate data from Bybit
        """
        s_list = [
            d["symbol"]
            for d in self.session.get_tickers(category="linear")["result"]["list"]
            if d["symbol"][-4:] == "USDT"
        ]
        if not os.path.exists(f"{self._destination_dir}/bybit_data/fundingRate"):
            os.makedirs(f"{self._destination_dir}/bybit_data/fundingRate")
        # Get all available symbols
        for sym in track(
            s_list, description="Downloading funding rate data from Bybit"
        ):
            # Get funding rate history
            df = pd.DataFrame(columns=["fundingRate", "fundingRateTimestamp", "symbol"])
            for dt in self.generate_dates_until_today(2021, 1):
                start_time, end_time = dt.split(" ")
                # Convert to timestamp (ms)
                start_time = int(
                    datetime.strptime(start_time, "%Y-%m-%d").timestamp() * 1000
                )
                end_time = int(
                    datetime.strptime(end_time, "%Y-%m-%d").timestamp() * 1000
                )
                for d in self.session.get_funding_rate_history(
                    category="linear",
                    symbol=sym,
                    limit=200,
                    startTime=start_time,
                    endTime=end_time,
                )["result"]["list"]:
                    df.loc[len(df)] = d

            df["fundingRateTimestamp"] = pd.to_datetime(
                df["fundingRateTimestamp"].astype(float) * 1000000
            )
            df["fundingRate"] = df["fundingRate"].astype(float)
            df = df.sort_values("fundingRateTimestamp")

            # Save to csv
            df.to_csv(f"{self._destination_dir}/bybit_data/fundingRate/{sym}.csv")

    @staticmethod
    def generate_dates_by_minutes_limited(
        start_date: datetime.date, interval_minutes=1000
    ) -> (list, list):
        """
        Generate dates by minutes (limited to 1000 minutes)
        :param start_date: The date of the start
        :param interval_minutes: Interval in minutes
        :return:
        """
        start_date = datetime(start_date.year, start_date.month, start_date.day)
        end_date = datetime.today()

        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(minutes=interval_minutes)

        return date_list

    def _download_klines(self, symbol: str, interval: str = "1"):
        """
        Download klines from Bybit
        :param symbol: symbol
        :param interval: interval
        Example of interval:
            1: 1m
            3: 3m
            5: 5m
            15: 15m
            30: 30m
            60: 1h
            120: 2h
            240: 4h
            360: 6h
            720: 12h
            D: 1d
            W: 1w
            M: 1M
        """
        if not os.path.exists(
            f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}"
        ):
            os.makedirs(
                f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}"
            )

        def __convert_to_int_interval(interval: str) -> int:
            if interval == "1":
                return 1
            elif interval == "3":
                return 3
            elif interval == "5":
                return 5
            elif interval == "15":
                return 15
            elif interval == "30":
                return 30
            elif interval == "60":
                return 60
            elif interval == "120":
                return 120
            elif interval == "240":
                return 240
            elif interval == "360":
                return 360
            elif interval == "720":
                return 720
            elif interval == "D":
                return 1440
            elif interval == "W":
                return 1440 * 7
            elif interval == "M":
                return 1440 * 30
            else:
                raise ValueError("Invalid interval")

        def __download(start_time: datetime):
            df_tmp = pd.DataFrame(
                columns=[
                    "startTime",
                    "openPrice",
                    "highPrice",
                    "lowPrice",
                    "closePrice",
                    "volume",
                    "turnover",
                ]
            )
            for d in self.session.get_kline(
                category=self._klines_category,
                symbol=symbol,
                interval=interval,
                limit=1000,
                startTime=int(start_time.timestamp()) * 1000,
                endTime=start_time
                + timedelta(minutes=__convert_to_int_interval(interval) * 1000),
            )["result"]["list"]:
                df_tmp.loc[len(df_tmp)] = d

            df_tmp["startTime"] = pd.to_datetime(
                df_tmp["startTime"].astype(float) * 1000000
            )
            df_tmp = df_tmp.sort_values("startTime")
            save_path = "/".join(
                [
                    self._destination_dir,
                    "bybit_data",
                    "klines",
                    self._klines_category,
                    symbol,
                    str(int(start_time.timestamp())) + ".csv",
                ]
            )
            print(f"[green]Saving: {save_path}[/green]")
            df_tmp.to_csv(save_path)

        print(f"[bold blue]Initial download: {symbol}[/bold blue]")
        __download(datetime(2019, 1, 1))
        df_init_path = sorted(
            [
                f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}/{file}"
                for file in os.listdir(
                    f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}"
                )
            ]
        )[0]
        df_init = pd.read_csv(df_init_path)
        start_date = df_init["startTime"].iloc[0]
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").date()

        for start_time_chunk in self.make_chunks(
            self.generate_dates_by_minutes_limited(
                start_date, __convert_to_int_interval(interval) * 1000
            ),
            self._CHUNK_SIZE,
        ):
            print(f"[bold blue]Downloading: {symbol}[/bold blue]")
            print(start_time_chunk)
            with ThreadPoolExecutor() as executor:
                executor.map(__download, start_time_chunk)

        # merge downloaded csv
        df = pd.DataFrame(
            columns=[
                "startTime",
                "openPrice",
                "highPrice",
                "lowPrice",
                "closePrice",
                "volume",
                "turnover",
            ]
        )
        for file in os.listdir(
            f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}"
        ):
            df_tmp = pd.read_csv(
                f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}/{file}"
            )
            df = pd.concat([df, df_tmp])
            os.remove(
                f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}/{file}"
            )
        df = df.sort_values("startTime")
        df = df.drop_duplicates(subset=["startTime"])
        df.to_csv(
            f"{self._destination_dir}/bybit_data/klines/{self._klines_category}/{symbol}/{interval}.csv"
        )

    def download_klines(self, symbol: str, interval: str = "1"):
        """
        Download klines from Bybit
        :param symbol: symbol
        :param interval: interval
        """
        self._download_klines(symbol, interval)

    def run_download(self, interval: str = "1"):
        """
        Execute download concurrently.
        :param interval: interval
        :return: None
        """
        print(
            f"[bold blue]Downloading {self._data_type} data from Bybit...[/bold blue]"
        )
        if self._data_type == "fundingRate":
            self._download_fundingrate()
        elif self._data_type == "klines":
            s_list = [
                d["symbol"]
                for d in self.session.get_tickers(category=self._klines_category)[
                    "result"
                ]["list"]
                if d["symbol"][-4:] == "USDT"
            ]
            for symbol in track(
                s_list, description="Downloading klines data from Bybit"
            ):
                self.download_klines(symbol, interval)
        else:
            for prefix_chunk in track(
                self.make_chunks(self._get_url_from_bybit(), self._CHUNK_SIZE),
                description="Downloading",
            ):
                with ThreadPoolExecutor() as executor:
                    executor.map(self._download, prefix_chunk)
