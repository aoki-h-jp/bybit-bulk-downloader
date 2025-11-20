import os
import gzip
from unittest import mock
import pytest
from bybit_bulk_downloader.downloader import BybitBulkDownloader

BYBIT_DATA = "bybit_data"


@pytest.fixture
def mock_response():
    """Mock response fixture for requests"""
    with mock.patch("requests.get") as mock_get:
        response = mock.Mock()
        # Create a valid gzip content
        with gzip.open(os.path.join("/tmp", "test.gz"), "wb") as f:
            f.write(b"mock_content")
        with open(os.path.join("/tmp", "test.gz"), "rb") as f:
            gzip_content = f.read()
        response.content = gzip_content
        response.iter_content = mock.Mock(return_value=[gzip_content])
        response.status_code = 200
        mock_get.return_value = response
        yield mock_get


@pytest.mark.unit
def test_download_success(tmpdir, mock_response):
    """Test successful download case"""
    downloader = BybitBulkDownloader(
        destination_dir=tmpdir,
        data_type="trading",
    )
    test_url = "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2023-01-01.csv.gz"
    downloader.download(test_url)

    # Verify the request was made
    assert mock_response.call_count == 1
    args, kwargs = mock_response.call_args
    assert args[0] == test_url

    # Verify the file was saved correctly
    expected_path = os.path.join(
        tmpdir,
        BYBIT_DATA,
        "trading/BTCUSDT/BTCUSDT2023-01-01.csv",
    )
    assert os.path.exists(expected_path)


@pytest.mark.unit
def test_download_network_error(tmpdir):
    """Test network error handling"""
    downloader = BybitBulkDownloader(
        destination_dir=tmpdir,
        data_type="trading",
    )
    with mock.patch("requests.get", side_effect=Exception("Network error")):
        with pytest.raises(Exception) as exc_info:
            downloader.download("https://example.com/test.csv.gz")
        assert "Network error" in str(exc_info.value)


@pytest.mark.unit
def test_invalid_data_type():
    """Test invalid data type handling"""
    with pytest.raises(ValueError) as exc_info:
        BybitBulkDownloader(
            destination_dir="test",
            data_type="invalid_type",
        )
    assert "Invalid data_type:" in str(exc_info.value)


@pytest.mark.unit
def test_spot_data_type(tmpdir, mock_response):
    """Test spot data type download"""
    downloader = BybitBulkDownloader(
        destination_dir=tmpdir,
        data_type="spot",
    )
    test_url = "https://public.bybit.com/spot/BTCUSDT/BTCUSDT2023-01-01.csv.gz"
    downloader.download(test_url)

    # Verify the request was made
    assert mock_response.call_count == 1
    args, kwargs = mock_response.call_args
    assert args[0] == test_url

    # Verify the file was saved correctly
    expected_path = os.path.join(
        tmpdir,
        BYBIT_DATA,
        "spot/BTCUSDT/BTCUSDT2023-01-01.csv",
    )
    assert os.path.exists(expected_path)


@pytest.mark.unit
def test_valid_spot_data_type():
    """Test that spot is a valid data type"""
    downloader = BybitBulkDownloader(
        destination_dir="test",
        data_type="spot",
    )
    assert downloader._data_type == "spot"

