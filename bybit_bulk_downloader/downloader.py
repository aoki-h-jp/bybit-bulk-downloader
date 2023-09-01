"""
bybit_bulk_downloader
"""
import gzip
# import standard libraries
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

# import third-party libraries
import requests
from bs4 import BeautifulSoup
from rich import print
from rich.progress import track


class BybitBulkDownloader:
    _CHUNK_SIZE = 20
    _BYBIT_DATA_DOWNLOAD_BASE_URL = "https://public.bybit.com/"
    _DATA_TYPE = ("kline_for_metatrader4", "premium_index", "spot_index", "trading")

    def __init__(self, destination_dir=".", data_type="trading"):
        """
        :param destination_dir: Directory to save the downloaded data.
        :param data_type: Data type to download. Available data types are: "kline_for_metatrader4", "premium_index", "spot_index", "trading".
        """
        self._destination_dir = destination_dir
        self._data_type = data_type
        self.downloaded_list = []

    def _get_url_from_bybit(self):
        """
        Get the URL of the data to download from Bybit.
        :return: list of URLs to download.
        """
        url = "https://public.bybit.com/" + self._data_type + "/"
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
        self.downloaded_list.append(prefix)

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

    def run_download(self):
        """
        Execute download concurrently.
        :return: None
        """
        print(
            f"[bold blue]Downloading {self._data_type} data from Bybit...[/bold blue]"
        )
        for prefix_chunk in track(
            self.make_chunks(self._get_url_from_bybit(), self._CHUNK_SIZE),
            description="Downloading",
        ):
            with ThreadPoolExecutor() as executor:
                executor.map(self._download, prefix_chunk)
            self.downloaded_list.extend(prefix_chunk)
