import pytest  # noqa: F401


def pytest_configure(config):
    """
    Configure pytest settings
    Register integration and unit test markers
    """
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "unit: mark test as unit test")
