import pytest
from edgedb.server import _testbase


def pytest_configure(config):
    import sys
    sys._in_pytest = True


def pytest_unconfigure(config):
    import sys  # This was missing from the manual
    del sys._in_pytest


@pytest.fixture(scope='session')
def cluster(request):
    cluster = _testbase._start_cluster(cleanup_atexit=False)
    yield
    _testbase._shutdown_cluster(cluster)
