Stress Tests
============

These tests are designed to test the behavior of a deployment under various forms of stress
that it might face in production.

The goals of these tests are to:
* Test how well various components behave under stress
* Confirm that when limits are exceeded, proper exceptions are returned
* Indirectly test the internals

How-to
------

These test have to be updated minimally for each deployment, with updates to the `conftest.py`
similar to how it's done in the smoke_tests



Running tests
-------------

* Install requirements:

     >> pip install -r requirements.txt

* Run tests:

     >> pytest .

Running Against dev deployment
------------------------------

To run against the dev deployment, first start a local endpoint that points at the dev deployment.
Then use

.. code-block:: bash

    export FUNCX_LOCAL_ENDPOINT_ID=<SET_YOUR_FUNCX_ENDPOINT_ID>
    pytest . --funcx-config dev

You can configure it to use a different local endpoint name (not `default`) by
setting the `FUNCX_LOCAL_ENDPOINT_NAME` env var.

Additional ways of settings the endpoint ID to use are:

- set `FUNCX_LOCAL_ENDPOINT_ID` (higher precedence than `~/.funcx/` dir)

- pass the `--endpoint <UUID>` option to `pytest` (highest precedence)

All tests should pass when run locally. Tests which cannot work on a local
stack are marked with `pytest.skip` and will be skipped.
