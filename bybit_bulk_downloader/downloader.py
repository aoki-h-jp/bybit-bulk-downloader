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

    def __init__(self, destination_dir=".", data_type="trading"):
        """
        :param destination_dir: Directory to save the downloaded data.
        :param data_type: Data type to download. Available data types are: "kline_for_metatrader4", "premium_index", "spot_index", "trading", "fundingRate", "klines".
        """
        self._destination_dir = destination_dir
        self._data_type = data_type
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
    def make_chunks(lst, n) -> list:
        """
        Make chunks
        :param lst: Raw list
        :param n: size of chunk
        :return: list of chunks
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    def _download(self, url):
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

    def download(self, url):
        self._download(url)

    @staticmethod
    def generate_dates_until_today(start_year, start_month) -> list:
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
        start_year, start_month, interval_minutes
    ) -> (list, list):
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime.today()

        # Generating the list
        date_list_1000min = []
        current_date = start_date
        while current_date <= end_date:
            date_list_1000min.append(current_date)
            current_date += timedelta(minutes=interval_minutes)

        start_dt = date_list_1000min[:-1]
        return start_dt

    def _download_klines(self, symbol):
        """
        Download klines from Bybit
        :param symbol: symbol
        """
        if not os.path.exists(f"{self._destination_dir}/bybit_data/klines/{symbol}"):
            os.makedirs(f"{self._destination_dir}/bybit_data/klines/{symbol}")

        def _download(start_time):
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
                category="linear",
                symbol=symbol,
                interval="1",
                limit=1000,
                startTime=int(start_time.timestamp()) * 1000,
                endTime=start_time + timedelta(minutes=1000),
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
                    symbol,
                    str(int(start_time.timestamp())) + ".csv",
                ]
            )
            print(f"[green]Saving: {save_path}[/green]")
            df_tmp.to_csv(save_path)

        # the oldest data is 2020-03-25
        for start_time_chunk in self.make_chunks(
            self.generate_dates_by_minutes_limited(2020, 3, 1000), self._CHUNK_SIZE
        ):
            print(f"[bold blue]Downloading: {symbol}[/bold blue]")
            print(start_time_chunk)
            with ThreadPoolExecutor() as executor:
                executor.map(_download, start_time_chunk)

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
        for file in os.listdir(f"{self._destination_dir}/bybit_data/klines/{symbol}"):
            df_tmp = pd.read_csv(
                f"{self._destination_dir}/bybit_data/klines/{symbol}/{file}"
            )
            df = pd.concat([df, df_tmp])
            os.remove(f"{self._destination_dir}/bybit_data/klines/{symbol}/{file}")
        df = df.sort_values("startTime")
        df = df.drop_duplicates(subset=["startTime"])
        df.to_csv(f"{self._destination_dir}/bybit_data/klines/{symbol}/1m.csv")

    def download_klines(self, symbol):
        self._download_klines(symbol)

    def run_download(self):
        """
        Execute download concurrently.
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
                for d in self.session.get_tickers(category="linear")["result"]["list"]
                if d["symbol"][-4:] == "USDT"
            ]
            for symbol in track(
                s_list, description="Downloading klines data from Bybit"
            ):
                self.download_klines(symbol)
        else:
            for prefix_chunk in track(
                self.make_chunks(self._get_url_from_bybit(), self._CHUNK_SIZE),
                description="Downloading",
            ):
                with ThreadPoolExecutor() as executor:
                    executor.map(self._download, prefix_chunk)
