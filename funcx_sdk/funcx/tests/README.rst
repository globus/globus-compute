Using the test-suite
====================

This test-suite is built using the `pytest` package. Please refer `here <https://docs.pytest.org/en/stable/>`_ for pytest specific docs.

How to run the tests
--------------------

1. Start an endpoint, make sure that the endpoint is set to point at the `funcx_service_address` for testing.

2. Update the `config` dict in the `funcx/funcx_sdk/funcx/tests/conftest.py` with the correct `funcx_service_address` and the `endpoint_uuid`.

3. Run individual tests with:

   >>> pytest test_basic.py --endpoint='<ENDPOINT_UUID>'

   or, run the whole test-suite with:

   >>> pytest . --endpoint='<ENDPOINT_UUID>'

