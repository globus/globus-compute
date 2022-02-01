import pytest


# mock logging config to do nothing
@pytest.fixture(autouse=True)
def _mock_logging_config(monkeypatch):
    def fake_setup_logging(*args, **kwargs):
        pass

    monkeypatch.setattr(
        "funcx_endpoint.logging_config.setup_logging", fake_setup_logging
    )
