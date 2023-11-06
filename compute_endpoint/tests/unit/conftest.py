def pytest_configure(config):
    config.addinivalue_line(
        "markers", "no_mock_pim: In test_endpointmanager_unit, disable autouse fixture"
    )
