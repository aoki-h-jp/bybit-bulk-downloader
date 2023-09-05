from setuptools import setup

setup(
    name="bybit-bulk-downloader",
    version="1.1.0",
    description=" Python library to efficiently and concurrently download historical data files from bybit. Supports all asset types (spot, USDT-M, COIN-M, options) and all data frequencies.",
    install_requires=["requests", "rich", "pytest", "beautifulsoup4", "pandas", "pybit"],
    author="aoki-h-jp",
    author_email="aoki.hirotaka.biz@gmail.com",
    license="MIT",
    packages=["bybit_bulk_downloader"],
)
