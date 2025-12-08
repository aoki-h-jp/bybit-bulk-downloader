from setuptools import setup

setup(
    name="bybit-bulk-downloader",
    version="1.2.4",
    description=" Python library to efficiently and concurrently download historical data files from Bybit. Supports all asset types (spot, USDT-M, COIN-M, options) and all data frequencies.",
    install_requires=["requests", "rich", "pytest", "beautifulsoup4", "pybit"],
    author="aoki-h-jp",
    author_email="hirotaka.aoki@solopreneurs.tech",
    license="MIT",
    packages=["bybit_bulk_downloader"],
)
