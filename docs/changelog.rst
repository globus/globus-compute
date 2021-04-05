Changelog
=========


funcx & funcx-endpoint v0.0.6
-----------------------------

Tentative Release on April 10th, 2020

funcx v0.0.6 includes contributions (code, tests, reviews, and reports) from:

Ariel Rokem <arokem@gmail.com>, Ben Blaiszik <blaiszik@uchicago.edu>, Ben Galewsky <ben@peartreestudio.net>, Ben Glick <glick@glick.cloud>, Joshua Bryan <josh@globus.org>, Kirill Nagaitsev <knagaitsev1@gmail.com>, Kyle Chard <chard@uchicago.edu>, Loonride <22580625+Loonride@users.noreply.github.com>, pratikpoojary <pratik.poojary@somaiya.edu>, Ryan <rchard@anl.gov>, Yadu Nand Babuji <yadudoc1729@gmail.com>, yongyanrao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

Known Issues
^^^^^^^^^^^^

There is an ongoing stability issue with `pyzmq` wheels that causes endpoint crashes.
Read more about this `here <https://github.com/zeromq/libzmq/issues/3313>`.
To address this issue, we recommend the following:

.. code-block:: bash

   # Ensure you are using a GCC version older than v7
   gcc --version

   # Install pyzmq without the binaries from Pypi:
   pip install --no-binary :all: --force-reinstall pyzmq


New Functionality
^^^^^^^^^^^^^^^^^

* The security architecture has been overhauled. The current sequence of endpoint registration is as follows:
  * funcx-endpoint will connect to the funcx web-service and register itself
  * Upon registration, the endpoint receives server certificates and connection info.
  * funcx-endpoint connects to a forwarder service over an encrypted(Curve25519 elliptic curve) ZMQ channel using the server certificates.
  * If the connection is terminated this whole process repeats.

* Significant changes to the `Config object`. All options related to executors have been moved from the top level Config object to the executor object. Refer to the `configuration <configuration> section for more details. Here's an example of the config change:

  This is the old style config:

  .. code-block:: python

     from funcx_endpoint.endpoint.utils.config import Config
     from parsl.providers import LocalProvider

     config = Config(
         # Options at the top-level like provider and max_workers_per_node
         # are moved to the executor object
         scaling_enabled=True,
         provider=LocalProvider(
             init_blocks=1,
             min_blocks=1,
             max_blocks=1,
         ),
         max_workers_per_node=2,
     funcx_service_address='https://api.funcx.org/v1'
     )

  Here is a sample config based on the update Config object:

  .. code-block:: python

     from funcx_endpoint.endpoint.utils.config import Config
     from funcx_endpoint.executors import HighThroughputExecutor
     from parsl.providers import LocalProvider

     config = Config(
         executors=[HighThroughputExecutor(
             provider=LocalProvider(
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=1,
             ),
         )],
         detach_endpoint=True,
         funcx_service_address='https://api.funcx.org/v2'
     )

* The endpoint will now log to `~/.funcx/<ENDPOINT_NAME>/EndpointInterchange.log`.
  * Several updates to logging makes logs more concise and cleaner.

* The serialization mechanism has been updated to use multiple serialization libraries (dill, pickle)

* The funcx-endpoint cli-tool will raise an error message to screen if endpoint registration fails rather than log to a file

* Richer HTTP error codes and responses for failure conditions and reporting.

* The `/submit` route response format has changed. Previously, this route would return an error after the first failed task submission attempt. Now, the service will attempt to submit all tasks that the user sends via this route.

  This is the old response format, assuming all tasks submit successfully:

    {
      'status': 'Success',
      'task_uuids': ['task_id_1', 'task_id_2', ...],
      'task_uuid': ''
    }

  This is the new response format, where some task submissions have failed:

    {
      'response': 'batch',
      'results': [
        {
          'status': 'Success',
          'task_uuid': 'task_id_1',
          'http_status_code': 200
        },
        {
          'status': 'Failed',
          'code': 1,
          'task_uuid': 'task_id_2',
          'http_status_code': 4XX/5XX,
          'error_args': [...]
        },
        ...
      ]
    }


* `get_batch_status` has been renamed to `get_batch_result`


