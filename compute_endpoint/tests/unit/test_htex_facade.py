from globus_compute_endpoint.engines import HighThroughputEngine
from globus_compute_endpoint.executors import HighThroughputExecutor


def test_deprecation_notice(capfd):
    """Instantiating HTEX should throw a WARNING notice about
    deprecation
    """
    HighThroughputExecutor()
    out, err = capfd.readouterr()
    assert "WARNING" in out


def test_htex_retuns_engine():
    """An instance of HighThroughputExecutor should now return
    a HighThroughputEngine object
    """
    htex = HighThroughputExecutor()
    assert isinstance(htex, HighThroughputEngine)
