# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from bybit_bulk_downloader.downloader import BybitBulkDownloader

BYBIT_DATA = "bybit_data"


def dynamic_test_params():
    """
    Generate params for tests
    :return:
    """
    for data_type in [
        "kline_for_metatrader4",
        "premium_index",
        "spot",
        "spot_index",
        "trading",
    ]:
        yield pytest.param(data_type)


@pytest.mark.integration
@pytest.mark.parametrize("data_type", dynamic_test_params())
def test_download(tmpdir, data_type):
    """
    Test download with actual API
    :param tmpdir:
    :param data_type: data type
    :return:
    """
    downloader = BybitBulkDownloader(
        destination_dir=tmpdir,
        data_type=data_type,
    )
    if data_type == "kline_for_metatrader4":
        single_download_url = "https://public.bybit.com/kline_for_metatrader4/ADAUSDT/2022/ADAUSDT_15_2022-09-01_2022-09-30.csv.gz"
        downloader.download(single_download_url)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            os.path.join(
                tmpdir,
                BYBIT_DATA,
                "kline_for_metatrader4/ADAUSDT/2022/ADAUSDT_15_2022-09-01_2022-09-30.csv",
            )
        )

    elif data_type == "premium_index":
        single_download_url = "https://public.bybit.com/premium_index/ADAUSD/ADAUSD2022-03-24_premium_index.csv.gz"
        downloader.download(single_download_url)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            os.path.join(
                tmpdir,
                BYBIT_DATA,
                "premium_index/ADAUSD/ADAUSD2022-03-24_premium_index.csv",
            )
        )

    elif data_type == "spot_index":
        single_download_url = "https://public.bybit.com/spot_index/ADAUSD/ADAUSD2022-03-24_index_price.csv.gz"
        downloader.download(single_download_url)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            os.path.join(
                tmpdir, BYBIT_DATA, "spot_index/ADAUSD/ADAUSD2022-03-24_index_price.csv"
            )
        )

    elif data_type == "spot":
        single_download_url = "https://public.bybit.com/spot/BTCUSDT/BTCUSDT_2022-11-10.csv.gz"
        downloader.download(single_download_url)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            os.path.join(
                tmpdir,
                BYBIT_DATA,
                "spot/BTCUSDT/BTCUSDT_2022-11-10.csv",
            )
        )

    elif data_type == "trading":
        single_download_url = "https://public.bybit.com/trading/10000LADYSUSDT/10000LADYSUSDT2023-05-11.csv.gz"
        downloader.download(single_download_url)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            os.path.join(
                tmpdir,
                BYBIT_DATA,
                "trading/10000LADYSUSDT/10000LADYSUSDT2023-05-11.csv",
            )
        )

    else:
        raise ValueError("Invalid data type: {}".format(data_type))
