# bybit-bulk-downloader

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110//)
[![Run pytest on all branches](https://github.com/aoki-h-jp/bybit-bulk-downloader/actions/workflows/pytest.yaml/badge.svg)](https://github.com/aoki-h-jp/bybit-bulk-downloader/actions/workflows/pytest.yaml)

## Python library for bulk downloading bybit historical data

A Python library to efficiently and concurrently download historical data files from bybit. Supports all asset types (spot, USDT Perpetual, Inverse Perpetual &amp; Inverse Futures).

## Installation

```bash
pip install bybit-bulk-downloader
```

## Usage

### Download all kline_for_metatrader4 data

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

downloader = BybitBulkDownloader(data_type='kline_for_metatrader4')
downloader.run_download()
```

### Download all premium_index data

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

downloader = BybitBulkDownloader(data_type='premium_index')
downloader.run_download()
```

### Download all spot_index data

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

downloader = BybitBulkDownloader(data_type='spot_index')
downloader.run_download()
```

### Download all spot data

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

downloader = BybitBulkDownloader(data_type='spot')
downloader.run_download()
```

### Download specific trading pair data

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

# Download BTCUSDT trading data
downloader = BybitBulkDownloader(data_type='trading')
downloader.download_symbol('BTCUSDT')
```

### Download all available trading pairs

```python
from bybit_bulk_downloader.downloader import BybitBulkDownloader

# Download all trading pairs data
downloader = BybitBulkDownloader(data_type='trading')
downloader.run_download()  # This will download all available trading pairs
```

## pytest

```bash
python -m pytest
```

## Available data types

✅: Implemented and tested. ❌: Not available on bybit.

### by data_type

| data_type             | spot | futures |
| :-------------------- | :--: | :--: |
| kline_for_metatrader4 | ❌   | ✅   |
| premium_index         | ✅   | ❌   |
| spot                  | ✅   | ❌   |
| spot_index            | ✅   | ❌   |
| trading               | ❌   | ✅   |

## If you want to report a bug or request a feature

Please create an issue on this repository!

## Disclaimer

This project is for educational purposes only. You should not construe any such information or other material as legal,
tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, recommendation,
endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial
instruments in this or in any other jurisdiction in which such solicitation or offer would be unlawful under the
securities laws of such jurisdiction.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs,
or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
