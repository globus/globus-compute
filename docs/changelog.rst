Changelog
=========

.. scriv-insert-here

funcx & funcx-endpoint v0.3.7
-----------------------------

Bug Fixes
^^^^^^^^^

- When a provider raised an exception, that exception was then mishandled
  and presented as an AttributeError. This handling now no longer corrupts
  the exception. https://github.com/funcx-faas/funcX/issues/679

New Functionality
^^^^^^^^^^^^^^^^^

- Capture, log, and report execution time information. The time a function takes to execute is now logged in worker debug logs and reported to the funcX service.

- Added Helm options to specify Kuberenetes workerDebug, imagePullSecret and maxIdleTime values.

Changed
^^^^^^^

- Kubernetes worker pods will now be named funcx-worker-*
  instead of funcx-* to clarify what these pods are to
  observers of 'kubectl get pods'

- Logging for funcx-endpoint no longer writes to `~/.funcx/endpoint.log` at any point.
  This file is considered deprecated. Use `funcx-endpoint --debug <command>` to
  get debug output written to stderr.
- The output formatting of `funcx-endpoint` logging has changed slightly when
  writing to stderr.

funcx & funcx-endpoint v0.3.6
-----------------------------

Released on February 1, 2022.


Bug Fixes
^^^^^^^^^

- Updates the data size limit for WebSockets from 1MB to 11MB to
  address issue:https://github.com/funcx-faas/funcX/issues/677

- Fixed an issue in which funcx-endpoint commands expected the ``~/.funcx/``
  directory to exist, preventing the endpoint from starting on new installs

Changed
^^^^^^^

- The version of ``globus-sdk`` used by ``funcx`` has been updated to v3.x .

- ``FuncXClient`` is no longer a subclass of ``globus_sdk.BaseClient``, but
  instead contains a web client object which can be used to prepare and send
  requests to the web service

- ``FuncXClient`` will no longer raise throttling-related errors when too many
  requests are sent, and it may sleep and retry requests if errors are
  encountered

- The exceptions raised by the ``FuncXClient`` when the web service sends back
  an error response are now instances of ``funcx.FuncxAPIError``. This
  means that the errors no longer inherit from ``FuncxResponseError``. Update
  error handling code as follows:

In prior versions of the `funcx` package:

.. code-block:: python

    import funcx
    from funcx.utils.response_errors import (
        FuncxResponseError, ResponseErrorCode
    )

    client = funcx.FuncXClient()
    try:
        client.some_method(...)
    except FuncxResponseError as err:
        if err.code == ResponseErrorCode.INVALID_UUID:  # this is an enum
            ...

In the new version:

.. code-block:: python

    import funcx

    client = funcx.FuncXClient()
    try:
        client.some_method(...)
    except funcx.FuncxAPIError as err:
        if err.code_name == "invalid_uuid":  # this is a string
            ...

funcx & funcx-endpoint v0.3.5
-----------------------------


Released on January 12th, 2021

funcx v0.3.5 is a minor release that includes contributions (code, tests, reviews, and reports) from:
Ben Clifford <benc@hawaga.org.uk>, Ben Galewsky <bengal1@illinois.edu>,
Daniel S. Katz <d.katz@ieee.org>, Kirill Nagaitsev <knagaitsev@uchicago.edu>
Michael McQuade <michael@giraffesyo.io>, Ryan Chard <rchard@anl.gov>,
Stephen Rosen <sirosen@globus.org>, Wes Brewer <whbrew@gmail.com>
Yadu Nand Babuji <yadudoc1729@gmail.com>, Zhuozhao Li <zhuozhl@clemson.edu>

Bug Fixes
^^^^^^^^^

* ``MaxResultSizeExceeded`` is now defined in ``funcx.utils.errors``. Fixes `issue#640 <https://github.com/funcx-faas/funcX/issues/640>`_

* Fixed Websocket disconnect after idling for 10 mins. See `issue#562 <https://github.com/funcx-faas/funcX/issues/562>`_
  funcX SDK will not auto-reconnect on remote-side disconnects

* Cleaner logging on the ``funcx-endpoint``. See `PR#643 <https://github.com/funcx-faas/funcX/pull/643>`_
  Previously available ``set_stream_logger``, ``set_file_logger`` methods are now removed.
  For debugging the SDK use standard logging methods, as described in the
  `Python Logging HOWTO <https://docs.python.org/3/howto/logging.html>`_, on
  the logger named ``"funcx"``.

  For example:

  .. code-block::

    import logging

    logger = logging.getLogger("funcx")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    funcx_logger.addHandler(ch)

* Warn and continue on failure to load a results ack file. `PR#616 <https://github.com/funcx-faas/funcX/pull/616>`_


New Functionality
^^^^^^^^^^^^^^^^^

* Result size raised to 10MB from 512KB. See `PR#647 <https://github.com/funcx-faas/funcX/pull/647>`_

* Version match constraints between the ``funcx-endpoint`` and the ``funcx-worker`` are now relaxed.
  This allows containers of any supported python3 version to be used for running tasks.
  See `PR#637 <https://github.com/funcx-faas/funcX/pull/637>`_

* New example config for Polaris at Argonne Leadership Computing Facility

* Simplify instructions for installing endpoint secrets to cluster. `PR#623 <https://github.com/funcx-faas/funcX/pull/623>`_

* Webservice and Websocket service URLs are resolved by the names "production" and
  "dev". These values can be passed to FuncX client init as in ``environment="dev"``,
  or by setting the ``FUNCX_SDK_ENVIRONMENT`` environment variable.

* Support for cancelling tasks in ``funcx_endpoint.executors.HighThroughputExecutor``. To cancel a
  task, use the ``best_effort_cancel`` method on the task's ``future``. This method differs from the
  concurrent futures ``future.cancel()`` method in that a running task can be cancelled.
  ``best_effort_cancel`` returns ``True`` only if the task is cancellable with no guarantees that the
  task will not execute. If the task is already complete, it returns ``False``

  .. note:: Please note that this feature is not yet supported on the SDK.

  Example:

     .. code-block:: python

        from funcx_endpoint.executors import HighThroughputExecutor
        htex = HighThroughputExecutor(passthrough=False)
        htex.start()

        future = htex.submit(slow_function)
        future.best_effort_cancel()


funcx & funcx-endpoint v0.3.4
-----------------------------

Released on October 14th, 2021

funcx v0.3.4 is a minor release that includes contributions (code, tests, reviews, and reports) from:

Ben Galewsky <bengal1@illinois.edu>, Kyle Chard <chard@uchicago.edu>,
Stephen Rosen <sirosen@globus.org>, and Yadu Nand Babuji <yadudoc1729@gmail.com>

Bug Fixes
^^^^^^^^^

* Updated requirements to exclude `pyzmq==22.3.0` due to unstable wheel. `Issue#577 <https://github.com/funcx-faas/funcX/issues/611>`_

* Updated requirements specification to `globus-sdk<3.0`

New Functionality
^^^^^^^^^^^^^^^^^

* Docs have been restructured and updated to use a cleaner theme

* New smoke_tests added to test hosted services



funcx & funcx-endpoint v0.3.3
-----------------------------

Released on September 20th, 2021

funcx v0.3.3 is a minor release that includes contributions (code, tests, reviews, and reports) from:

Ben Galewsky <bengal1@illinois.edu>, Kyle Chard <chard@uchicago.edu>,
Kirill Nagaitsev <knagaitsev@uchicago.edu>, Stephen Rosen <sirosen@globus.org>,
Uriel Mandujano <uriel@globus.org>, and Yadu Nand Babuji <yadudoc1729@gmail.com>


Bug Fixes
^^^^^^^^^

* An exception is raised if results arrive over WebSocket result when no future is available to receive it `PR#590 <https://github.com/funcx-faas/funcX/pull/590>`_

* Example configs have been updated to use ``init_blocks=0`` as a default. `PR#583 <https://github.com/funcx-faas/funcX/pull/583>`_

* Log result passing to forwarder only for result messages `PR#577 <https://github.com/funcx-faas/funcX/pull/577>`_

* Fix zmq option setting bugs `PR#565 <https://github.com/funcx-faas/funcX/pull/565>`_

New Functionality
^^^^^^^^^^^^^^^^^

* Endpoints will now stay running and retry connecting to funcX hosted services in a disconnection event `PR#588 <https://github.com/funcx-faas/funcX/pull/588>`_, `PR#572 <https://github.com/funcx-faas/funcX/pull/572>`_

* Endpoints will now use ACK messages from the forwarder to confirm that results have been received `PR#571 <https://github.com/funcx-faas/funcX/pull/571>`_

* Endpoints will persist unacked results and resend them during disconnection events `PR#580 <https://github.com/funcx-faas/funcX/pull/580>`_

* Result size limits have been revised from 10MB to 512KB. If result size exceeds 512KB, a ``MaxResultSizeExceeded`` exception is returned. `PR#586 <https://github.com/funcx-faas/funcX/pull/586>`_

* Add additional platform info to registration message `PR#592 <https://github.com/funcx-faas/funcX/pull/592>`_

* All endpoint logs, (EndpointInterchange.log, interchange.stderr, interchange.stdout) will now be collated into a single log: ``endpoint.log`` `PR#582 <https://github.com/funcx-faas/funcX/pull/582>`_

funcx & funcx-endpoint v0.3.2
-----------------------------

Released on August 11th, 2021

funcx v0.3.2 is a minor release that includes contributions (code, tests, reviews, and reports) from:
Ben Galewsky <bengal1@illinois.edu>, Rafael Vescovi <ravescovi@gmail.com>, Ryan <rchard@anl.gov>,
Yadu Nand Babuji <yadudoc1729@gmail.com>, Zhuozhao Li <zhuozhl@clemson.edu>


New Functionality
^^^^^^^^^^^^^^^^^

* Streamlined release process `PR#569 <https://github.com/funcx-faas/funcX/pull/569>`_, `PR#568 <https://github.com/funcx-faas/funcX/pull/568>`_

* Added a new funcX config for ``Cooley`` at ALCF. `PR#566 <https://github.com/funcx-faas/funcX/pull/566>`_


funcx & funcx-endpoint v0.3.1
-----------------------------

Released on July 26th, 2021

funcx v0.3.1 is a minor release that includes contributions (code, tests, reviews, and reports) from:
Ben Galewsky <bengal1@illinois.edu>, Kirill Nagaitsev <knagaitsev@uchicago.edu>, Ryan Chard <rchard@anl.gov>, and Yadu Nand Babuji <yadudoc1729@gmail.com>

Bug Fixes
^^^^^^^^^

* Removed process check from endpoint status check for better cross platform support `PR#559 <https://github.com/funcx-faas/funcX/pull/559>`_

* Fixes to ensure that ``container_cmd_options`` propagate correctly `PR#555 <https://github.com/funcx-faas/funcX/pull/555>`_



funcx & funcx-endpoint v0.3.0
-----------------------------

Released on July 08th, 2021

funcx v0.3.0 is a major release that includes contributions (code, tests, reviews, and reports) from:
Ben Galewsky <bengal1@illinois.edu>, Kyle Chard <chard@uchicago.edu>,
Kirill Nagaitsev <knagaitsev@uchicago.edu>, Daniel S. Katz <d.katz@ieee.org>,
Stephen Rosen <sirosen@globus.org>, Yadu Nand Babuji <yadudoc1729@gmail.com>,
Yongyan Rao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

Bug Fixes
^^^^^^^^^

* ``FuncXClient.get_result(<TASK_ID>)`` will now raise a ``TaskPending`` with an expanded failure reason.  See `PR#502 <https://github.com/funcx-faas/funcX/pull/502>`_

* funcx-endpoint start and stop commands are now improved to report broken/disconnected states and handle them better. See `issue#327 <https://github.com/funcx-faas/funcX/issues/327>`_

* Fixed ManagerLost exceptions triggering failures.  See `issue#486 <https://github.com/funcx-faas/funcX/issues/486>`_

* Several fixes and tests for better error reporting. See `PR#523 <https://github.com/funcx-faas/funcX/pull/523>`_



New Functionality
^^^^^^^^^^^^^^^^^

* Support added for websockets to minimize result fetching latency.

* ``FuncXClient(asynchronous=True)`` now enables asynchronous result fetching using Asycio library.

  Here's an example:

    .. code-block:: python

        from funcx import FuncXClient

        def hello():
            return "Hello World!"

        fxc = FuncXClient(asynchronous=True)
        fn_id = fxc.register_function(hello, description="Hello")

        # In asynchronous mode, function run returns asyncio futures
        async_future = fxc.run(endpoint_id=<ENDPOINT_ID>, function_id=fn_id)
        print("Result : ", await async_future)

* A new ``FuncXExecutor`` class exposes funcX functionality using the familiar executor interface from the `concurrent.futures` library.

  Here's an example:

    .. code-block:: python

        from funcx import FuncXClient
        from funcx.sdk.executor import FuncXExecutor

        def hello():
            return "Hello World!"

        funcx_executor = FuncXExecutor(FuncXClient())

        # With the executor, functions are auto-registered
        future = funcx_executor.submit(hello, endpoint_id=<ENDPOINT_ID>)

        # You can check status of your task without blocking
        print(future.done())

        # Block and wait for the result:
        print("Result : ", future.result())


* Endpoint states have been renamed to ``running``, ``stopped``, and ``disconnected``. See `PR#525 <https://github.com/funcx-faas/funcX/pull/525>`_

* Container routing behavior has been improved to support `soft` and `hard` routing strategies. See `PR#324 <https://github.com/funcx-faas/funcX/pull/324>`_

funcx & funcx-endpoint v0.2.3
-----------------------------

Released on May 19th, 2021

funcx v0.2.3 is a minor release that includes contributions (code, tests, reviews, and reports) from:
Ben Galewsky <ben@peartreestudio.net>, Ryan Chard <rchard@anl.gov>, Weinan Si <siweinan@gmail.com>,
Yongyan Rao <yongyan.rao@gmail.com> Yadu Nand Babuji <yadudoc1729@gmail.com> and Zhuozhao Li <zhuozhao@uchicago.edu>


Bug Fixes
^^^^^^^^^

* Fixed a missing package in the ``requirements.txt`` file

* Updated version requirements in ``funcx-endpoint`` to match the ``funcx`` version

* ``funcx-endpoint`` commandline autocomplete has been fixed. See `issue#496 <https://github.com/funcx-faas/funcX/issues/496>`_

* ``funcx-endpoint restart`` failure is fixed. See `issue#488 <https://github.com/funcx-faas/funcX/issues/488>`_

* Several fixes and improvements to worker terminate messages which caused workers to crash silently. See `issue#462 <https://github.com/funcx-faas/funcX/pull/462>`_

* Fixed ``KubernetesProvider`` to use a default of ``init_blocks=0``. See `issue#237 <https://github.com/funcx-faas/funcX/issues/237>`_



New Functionality
^^^^^^^^^^^^^^^^^


* ``FuncXClient.get_result(<TASK_ID>)`` will now raise a ``TaskPending`` exception if the task is not complete.

* Multiple improvement to function serialization. See `issue#479 <https://github.com/funcx-faas/funcX/pull/479>`_

  * ``FuncXSerializer`` has been updated to prioritize source-based function serialization methods that offer
    more reliable behavior when the python version across the client and endpoint do not match.

  * ``FuncXSerializer`` now attempts deserialization on an isolated process to preempt failures on a remote worker.

* More consistent worker task message types. See `PR#462 <https://github.com/funcx-faas/funcX/pull/462>`_

* Better OS agnostic path joining. See `PR#458 <https://github.com/funcx-faas/funcX/pull/458>`_



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

Ariel Rokem <arokem@gmail.com>, Ben Blaiszik <blaiszik@uchicago.edu>, Ben Galewsky <ben@peartreestudio.net>, Ben Glick <glick@glick.cloud>, Joshua Bryan <josh@globus.org>, Kirill Nagaitsev <knagaitsev@uchicago.edu>, Kyle Chard <chard@uchicago.edu>, pratikpoojary <pratik.poojary@somaiya.edu>, Ryan <rchard@anl.gov>, Yadu Nand Babuji <yadudoc1729@gmail.com>, yongyanrao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

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

* Significant changes to the `Config object`. All options related to executors have been moved from the top level Config object to the executor object.
  Refer to the `configuration <configuration>`_ section for more details. Here's an example of the config change:

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
          "status": "Success",
          "task_uuids": ["task_id_1", "task_id_2", "..."]
        }

    This is the new response format, where some task submissions have failed:

    .. code-block:: json

        {
          "response": "batch",
          "results": [
            {
              "status": "Success",
              "task_uuid": "task_id_1",
              "http_status_code": 200
            },
            {
              "status": "Failed",
              "code": 1,
              "task_uuid": "task_id_2",
              "http_status_code": 400,
              "error_args": ["..."]
            },
            "..."
          ]
        }


* `get_batch_status` has been renamed to `get_batch_result`
