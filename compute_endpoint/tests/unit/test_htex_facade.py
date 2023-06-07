from globus_compute_endpoint.engines import HighThroughputEngine
from globus_compute_endpoint.executors import HighThroughputExecutor


def test_deprecation_notice(mocker):
    """Instantiating HTEX should throw a WARNING notice about
    deprecation
    """
    mock_warn = mocker.patch(
        "globus_compute_endpoint.executors.high_throughput.executor.warnings"
    )
    HighThroughputExecutor()
    assert mock_warn.warn.called
    # call_args is weirdly nested
    assert "deprecated" in mock_warn.warn.call_args[0][0]


def test_htex_returns_engine():
    """An instance of HighThroughputExecutor should now return
    a HighThroughputEngine object
    """
    htex = HighThroughputExecutor()
    assert isinstance(htex, HighThroughputEngine)
