Changelog
=========



funcx & funcx-endpoint v0.2.3
-----------------------------

Released on May 20th, 2021

funcx v0.2.3 is a minor release that includes contributions (code, tests, reviews, and reports) from:
Ben Galewsky <ben@peartreestudio.net>, Ryan Chard <rchard@anl.gov>, Weinan Si <siweinan@gmail.com>,
Yongyan Rao <yongyan.rao@gmail.com> Yadu Nand Babuji <yadudoc1729@gmail.com> and Zhuozhao Li <zhuozhao@uchicago.edu>


Bug Fixes
^^^^^^^^^

* Fixed a missing package in the ``requirements.txt`` file

* Updated version requirements in ``funcx-endpoint`` to match the ``funcx`` version

* ``funcx-endpoint`` commandline autocomplete has been fixed. See `issue#496 <https://github.com/funcx-faas/funcX/issues/496>`_

* ``funcx-endpoint restart`` failure is fixed. See `issue#488 <https://github.com/funcx-faas/funcX/issues/488>`_

* Several fixes and improvements to worker terminate messages which caused workers to crash silently `issue#462 <https://github.com/funcx-faas/funcX/pull/462>`_

* Fixed ``KubernetesProvider`` to use a default of ``init_blocks=0`` `issue#237 <https://github.com/funcx-faas/funcX/issues/237>`_



New Functionality
^^^^^^^^^^^^^^^^^


* ``FuncXClient.get_result(<TASK_ID>)`` will now raise a ``TaskPending`` exception if the task is not complete.

* Multiple improvement to function serialization: Refer `issue#479 <https://github.com/funcx-faas/funcX/pull/479>`_

  * ``FuncXSerializer`` has been updated to prioritize source based function serialization methods that offer
    more reliable behavior when the python version across the client and endpoint do not match.

  * ``FuncXSerializer`` now attempt deserialization on an isolated process to pre-empt failures on a remote worker.

* More consistent worker task message types `PR#462 <https://github.com/funcx-faas/funcX/pull/462>`_

* Better OS agnostic path joining `PR#458 <https://github.com/funcx-faas/funcX/pull/458>`_



funcx & funcx-endpoint v0.2.2
-----------------------------

Released on April 15th, 2021

funcx v0.2.2 is a hotfix release that includes contributions (code, tests, reviews, and reports) from:

Yadu Nand Babuji <yadudoc1729@gmail.com> and Zhuozhao Li <zhuozhao@uchicago.edu>


Bug Fixes
^^^^^^^^^

* Fixed a missing package in the `requirements.txt` file

* Updated version requirements in `funcx-endpoint` to match the `funcx` version


funcx & funcx-endpoint v0.2.1
-----------------------------

Released on April 15th, 2021

funcx v0.2.1 includes contributions (code, tests, reviews, and reports) from:

Daniel S. Katz <d.katz@ieee.org>, Yadu Nand Babuji <yadudoc1729@gmail.com>,
Yongyan Rao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

New Features
^^^^^^^^^^^^

* Cleaner reporting when an older non-compatible ``Config`` object is used. Refer: `issue 427 <https://github.com/funcx-faas/funcX/issues/427>`_

* Better automated checks at SDK initialization to confirm that the SDK and Endpoint versions are supported by the web-service.

* Updated Kubernetes docs and example configs.


Bug Fixes
^^^^^^^^^

* Fixed a bug in funcx-endpoint that caused the ZMQ connections to timeout and crash, terminating the endpoint.

* Fixed an unsafe string based version comparison check.

* Fixed an issue with poor error reporting when starting non-existent endpoints. Refer: `issue 432 <https://github.com/funcx-faas/funcX/issues/432>`_

* Fixed a bug in incorrectly passing the `funcx_service_address` to the EndpointInterchange.

* Several updates to the docs for clarity.

* JSON serializer is removed from the FuncXSeralizer mechanism due to issues with not preserving types over serialization (tuples/lists)


funcx & funcx-endpoint v0.2.0
-----------------------------

Released on April 8th, 2021

funcx v0.2.0 includes contributions (code, tests, reviews, and reports) from:

Ariel Rokem <arokem@gmail.com>, Ben Blaiszik <blaiszik@uchicago.edu>, Ben Galewsky <ben@peartreestudio.net>, Ben Glick <glick@glick.cloud>, Joshua Bryan <josh@globus.org>, Kirill Nagaitsev <knagaitsev1@gmail.com>, Kyle Chard <chard@uchicago.edu>, Loonride <22580625+Loonride@users.noreply.github.com>, pratikpoojary <pratik.poojary@somaiya.edu>, Ryan <rchard@anl.gov>, Yadu Nand Babuji <yadudoc1729@gmail.com>, yongyanrao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

Known Issues
^^^^^^^^^^^^

There is an ongoing stability issue with `pyzmq` wheels that causes endpoint crashes.
Read more about this `here <https://github.com/zeromq/libzmq/issues/3313>`_.
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
  * funcx-endpoint connects to a forwarder service over an encrypted (Curve25519 elliptic curve) ZMQ channel using the server certificates.
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

    Here is a sample config based on the updated Config object:

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
           funcx_service_address='https://api2.funcx.org/v2'
       )

* The endpoint will now log to `~/.funcx/<ENDPOINT_NAME>/EndpointInterchange.log`.

* Several updates to logging make logs more concise and cleaner.

* The serialization mechanism has been updated to use multiple serialization libraries (dill, pickle)

* The funcx-endpoint CLI tool will raise an error message to screen if endpoint registration fails rather than log to a file

* Richer HTTP error codes and responses for failure conditions and reporting.

* The `/submit` route response format has changed. Previously, this route would return an error after the first failed task submission attempt. Now, the service will attempt to submit all tasks that the user sends via this route.

    This is the old response format, assuming all tasks submit successfully:

    .. code-block:: json

        {
          'status': 'Success',
          'task_uuids': ['task_id_1', 'task_id_2', ...]
        }

    This is the new response format, where some task submissions have failed:

    .. code-block:: json

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
