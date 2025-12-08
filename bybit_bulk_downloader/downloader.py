"""
bybit_bulk_downloader
"""

import gzip

# import standard libraries
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urljoin

# import third-party libraries
import requests
from bs4 import BeautifulSoup
from pybit.unified_trading import HTTP
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)


class BybitBulkDownloader:
    """
    Bulk downloader for Bybit data.
    """

    _CHUNK_SIZE = 20
    _BYBIT_DATA_DOWNLOAD_BASE_URL = "https://public.bybit.com"
    _DATA_TYPE = (
        "kline_for_metatrader4",
        "premium_index",
        "spot",
        "spot_index",
        "trading",
    )

    def __init__(self, destination_dir=".", data_type="trading"):
        """
        :param destination_dir: Directory to save the downloaded data.
        :param data_type: Data type to download. Available data types are: "kline_for_metatrader4", "premium_index", "spot_index", "trading".
        """
        if data_type not in self._DATA_TYPE:
            raise ValueError(
                f"Invalid data_type: {data_type}. Available types are: {', '.join(self._DATA_TYPE)}"
            )

        self._destination_dir = destination_dir
        self._data_type = data_type
        self.session = HTTP()
        self.console = Console()
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

    def _get_url_from_bybit(self):
        """
        Get the URL of the data to download from Bybit.
        :return: list of URLs to download.
        """
        url = self._BYBIT_DATA_DOWNLOAD_BASE_URL + "/" + self._data_type + "/"
        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")
        symbol_list = []
        for link in soup.find_all("a"):
            link_sym = link.get("href")
            if self._data_type == "kline_for_metatrader4":
                soup_year = BeautifulSoup(
                    requests.get(url + link_sym, headers=self._headers).text, "html.parser"
                )
                for link_year in soup_year.find_all("a"):
                    symbol_list.append(link_sym + link_year.get("href"))
            else:
                symbol_list.append(link_sym)
        download_list = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[cyan]Listing files...", total=len(symbol_list))
            for sym in symbol_list:
                soup_sym = BeautifulSoup(requests.get(url + sym, headers=self._headers).text, "html.parser")
                for link in soup_sym.find_all("a"):
                    href = link.get("href")
                    # Note: Bybit server returns different href formats for different data types
                    # - spot: symbol without trailing slash (e.g., "0GUSDT"), requires explicit slash before filename
                    # - trading, premium_index, etc.: symbol with trailing slash (e.g., "10000COQUSDT/")
                    sym_path = urljoin(sym + "/" if not sym.endswith("/") else sym, href)
                    download_list.append(url + sym_path)
                progress.advance(task)

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
        try:
            # Extract filename and path components from URL
            url_parts = url.split("/")
            filename = url_parts[-1]

            # Construct the directory path based on data type
            if self._data_type == "kline_for_metatrader4":
                # For kline data, maintain year in the path: symbol/year/file
                symbol = url_parts[-3]
                year = url_parts[-2]
                target_dir = os.path.join(
                    str(self._destination_dir),
                    "bybit_data",
                    self._data_type,
                    symbol,
                    year,
                )
            elif self._data_type == "spot":
                # For spot data: symbol/file (URL format: /spot/SYMBOL/SYMBOL_date.csv.gz)
                symbol = url_parts[-2]
                target_dir = os.path.join(
                    str(self._destination_dir), "bybit_data", self._data_type, symbol
                )
            else:
                # For other data types: symbol/file
                symbol = url_parts[-2]
                target_dir = os.path.join(
                    str(self._destination_dir), "bybit_data", self._data_type, symbol
                )

            # Create directory if it doesn't exist
            os.makedirs(target_dir, exist_ok=True)

            # Set full file path
            filepath = os.path.join(target_dir, filename)

            decompressed_path = filepath.replace(".gz", "")
            if os.path.exists(decompressed_path):
                self.console.print(f"[yellow]Uncompressed path already exists and probably downloaded before, skipping:[/yellow] {decompressed_path}")
                return
            else:
                self.console.print(f"[bold green]Downloading:[/bold green] {filepath}")

            # Download the file
            response = requests.get(url, headers=self._headers)
            if response.status_code != 200:
                raise Exception(f"Failed to download {url}: {response.status_code}")

            # Save compressed file
            with open(filepath, "wb") as file:
                file.write(response.content)

            # Decompress the file
            self.console.print(f"[yellow]Unzipping:[/yellow] {filepath}")
            try:
                with gzip.open(filepath, mode="rb") as gzip_file:
                    with open(decompressed_path, mode="wb") as decompressed_file:
                        shutil.copyfileobj(gzip_file, decompressed_file)

                # Delete the compressed file
                os.remove(filepath)
                self.console.print(f"[dim]Cleaned up:[/dim] {filepath}")
            except Exception as e:
                self.console.print(
                    f"[red]Failed to decompress {filepath}: {str(e)}[/red]"
                )
                if os.path.exists(filepath):
                    os.remove(filepath)
                raise
        except Exception as e:
            self.console.print(f"[red]Error processing {url}: {str(e)}[/red]")
            raise

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

    def run_download(self):
        """
        Execute download concurrently.
        :return: None
        """
        self.console.print(
            Panel.fit(
                f"[bold blue]Downloading {self._data_type} data from Bybit[/bold blue]",
                border_style="blue",
            )
        )

        urls = self._get_url_from_bybit()
        chunks = self.make_chunks(urls, self._CHUNK_SIZE)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[cyan]Downloading chunks...", total=len(chunks))

            for prefix_chunk in chunks:
                with ThreadPoolExecutor() as executor:
                    executor.map(self._download, prefix_chunk)
                progress.advance(task)

        self.console.print(
            "[bold green]Download completed successfully! ✨[/bold green]"
        )

    def download_symbol(self, symbol: str):
        """
        Download data for a specific trading pair.
        Supports trading and spot data types
        :param symbol: Trading pair symbol (e.g., "BTCUSDT")
        """
        if self._data_type not in ("trading", "spot"):
            raise ValueError(
                "This method is only available for trading and spot data. Please use run_download() for other data types."
            )

        url = f"{self._BYBIT_DATA_DOWNLOAD_BASE_URL}/{self._data_type}/{symbol}/"
        self.console.print(f"[blue]Accessing URL: {url}[/blue]")

        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        download_list = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".gz"):
                full_url = url + href
                download_list.append(full_url)
                self.console.print(f"[dim]Found file: {href}[/dim]")

        if not download_list:
            self.console.print(
                f"[bold yellow]警告: {symbol}のデータが見つかりません[/bold yellow]"
            )
            return

        self.console.print(
            f"[green]Found {len(download_list)} files to download[/green]"
        )

        # Create the base directory structure
        base_dir = os.path.join(
            self._destination_dir, "bybit_data", self._data_type, symbol
        )
        os.makedirs(base_dir, exist_ok=True)
        self.console.print(f"[blue]Created directory: {base_dir}[/blue]")

        chunks = self.make_chunks(download_list, self._CHUNK_SIZE)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task(
                f"[cyan]Downloading {symbol}...", total=len(chunks)
            )

            for chunk in chunks:
                with ThreadPoolExecutor() as executor:
                    executor.map(self._download, chunk)
                progress.advance(task)
