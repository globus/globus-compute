Changelog
=========

.. scriv-insert-here

.. _changelog-1.0.13:

funcx & funcx-endpoint v1.0.13
------------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Add two items to the ``Config`` object: ``idle_heartbeats_soft`` and
  ``idle_heartbeats_hard``.  If set, the endpoint will auto-shutdown after the
  specified number of heartbeats with no work to do.

Bug Fixes
^^^^^^^^^

- Address broken login-flow, introduced in v1.0.12 when attempting to start an
  endpoint.  This affected users with invalid or missing credentials.  (e.g.,
  new users or new installs).

Removed
^^^^^^^

- Removed all Search-related functionality.

Deprecated
^^^^^^^^^^

- Deprecated all Search-related arguments to ``FuncXClient`` methods.

.. _changelog-1.0.12:

funcx & funcx-endpoint v1.0.12
------------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Implement client credentials for Kubernetes Endpoint Helm chart

Changed
^^^^^^^

- Updated package dependencies.
- Simplified format of endpoint status reports.
- Streamlined API function registration

.. _changelog-1.0.11:

funcx & funcx-endpoint v1.0.11
------------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Created ``FuncxWebClient`` and ``FuncXClient`` methods to delete endpoints
  from the web service.
- Added a ``--force`` flag for the ``funcx-endpoint delete`` command, which
  ensures that the endpoint is deleted locally even if the web service
  returns an error or is unreachable.

Bug Fixes
^^^^^^^^^

- For new installs, handle unusual umask settings robustly.  Previously, a
  umask resulting in no execute or write permissions for the main configuration
  directory would result in an unexpected traceback for new users.  Now we
  ensure that the main configuration directory at least has the write and
  executable bits set.

- The ``funcx-endpoint delete`` command now deletes the endpoint both locally and
  from the web service.
- If a user attempts to start an endpoint that has already been marked as
  deleted in the web service, the process will exit with an error.

Security
^^^^^^^^

- Previously, the main configuraton directory (typically ``~/.funcx/``) would
  be created honoring the users umask, typically resulting in
  world-readability.  In a typical administration, this may be mitigated by
  stronger permissions on the user's home directory, but still isn't robust.
  Now, the group and other permissions are cleared.  Note that this does _not_
  change existing installs, and only address newly instantiated funcX endpoint
  setups.

.. _changelog-1.0.10:

funcx & funcx-endpoint v1.0.10
------------------------------

Bug Fixes
^^^^^^^^^

- Fix idle-executor handling in manager that was broken in v1.0.9

.. _changelog-1.0.9:

funcx & funcx-endpoint v1.0.9
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- 'whoami' has been added to the cli to show the current logged in
  identity and linked identities.
  - A --linked-identities optional argument shows all linked identities
  - ie. `funcx-endpoint whoami` or `funcx-endpoint whoami --linked-identities`

Bug Fixes
^^^^^^^^^

- FuncXExecutor no longer ignores the specified ``container_id``.  The same
  function may now be utilized in containers via the normal workflow:

  .. code-block:: python

      import funcx

      def some_func():
          return 1
      with funcx.FuncXExecutor() as fxe:
          fxe.endpoint_id = "some-endpoint-uuid"
          fxe.container_id = "some-container_uuid"
          fxe.submit(some_func)
          fxe.container_id = "some-other-container-uuid"
          fxe.submit(some_func)  # same function, different container!
          # ...

Changed
^^^^^^^

- Initiate shutdown of any currently running FuncXExecutor objects when the main
  thread ends (a.k.a., "end of script").  This follows the same behavior as
  both ``ThreadPoolExecutor`` and ``ProcessPoolExecutor``.

.. _changelog-1.0.8:

funcx & funcx-endpoint v1.0.8
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- The endpoint can now register metadata such as IP, hostname, and configuration values
  with the funcX services.

Changed
^^^^^^^

- Pin Parsl version required by the funcX Endpoint to v2023.1.23

.. _changelog-1.0.7:

funcx & funcx-endpoint v1.0.7
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- When an API auth error is raised by a ``FuncXClient`` method, a new auth flow
  will be initiated.

- The funcX Endpoint will now shutdown after 5 consecutive failures to
  initialize.  (The previous behavior was to try indefinitely, even if the
  error was unrecoverable.)

- Add API Calls to request a docker image build and to check on the status of a
  submitted build

Changed
^^^^^^^

- The exceptions raised by ``FuncXClient`` when the web service sends back an
  error response are now instances of ``globus_sdk.GlobusAPIError`` and the
  FuncX specific subclass FuncxAPIError has been removed.

  Previous code that checked for FuncxAPIError.code_name should now check for
  GlobusAPIError.code

In prior versions of the ``funcx`` package:

.. code-block:: python

    import funcx

    client = funcx.FuncXClient()
    try:
        client.some_method(...)
    except funcx.FuncxAPIError as err:
        if err.code_name == "invalid_uuid":
            ...

In the new version:

.. code-block:: python

    import funcx
    import globus_sdk

    client = funcx.FuncXClient()
    try:
        client.some_method(...)
    except globus_sdk.GlobusAPIError as err:
        if err.code == "INVALID_UUID":
            ...

- Renamed the ``FuncXClient`` method ``lock_endpoint`` to ``stop_endpoint``.

- Renamed the ``Endpoint.stop_endpoint()`` parameter ``lock_uuid`` to ``remote``.

- ``HighThroughputExecutor.address`` now accepts only IPv4 and IPv6. Example
  configs have been updated to use ``parsl.address_by_interface`` instead of
  ``parsl.address_by_hostname``.  Please note that following this change,
  endpoints that were previously configured with
  ``HighThroughputExecutor(address=address_by_hostname())`` will now raise a
  ``ValueError`` and will need updating.

- For better security, ``HighThroughputExecutor`` now listens only on a
  specific interface rather than all interfaces.

.. _changelog-1.0.6:

funcx & funcx-endpoint v1.0.6
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Add a '--remote' option when stopping endpoints to create a temporary lock such that any running endpoints with the same UUID will get a locked response and exit.

- Added `get_endpoints` methods to `FuncXWebClient` and `FuncXClient`, which retrieve
  a list of all endpoints owned by the current user

.. _changelog-1.0.5:

funcx & funcx-endpoint v1.0.5
-----------------------------

Bug Fixes
^^^^^^^^^

- Prevent Endpoint ID from wrapping in ``funcx-endpoint list`` output.

Changed
^^^^^^^

- Updated minimum Globus SDK requirement to v3.14.0

- Reorder ``funcx-endpoint list`` output: ``Endpoint ID`` column is now first
  and ``Endpoint Name`` is now last.

.. _changelog-1.0.5a0:

funcx & funcx-endpoint v1.0.5a0
-------------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Added ``.get_result_amqp_url()`` to ``FuncXClient`` to acquire user
  credentials to the AMQP service.  Globus credentials are first verified
  before user-specific AMQP credentials are (re)created and returned.  The only
  expected use of this method comes from ``FuncXExecutor``.

- Captures timing information throughout the endpoint by reporting
  TaskTransitions.

Bug Fixes
^^^^^^^^^

- General and specific attention to the ``FuncXExecutor``, especially around
  non-happy path interactions
  - Addressed the often-hanging end-of-script problem
  - Address web-socket race condition (GH#591)

Deprecated
^^^^^^^^^^

- ``batch_enabled`` argument to ``FuncXExecutor`` class; batch communication is
  now enforced transparently.  Simply use ``.submit()`` normally, and the class
  will batch the tasks automatically.  ``batch_size`` remains available.

- ``asynchronous``, ``results_ws_uri``, and ``loop`` arguments to
  ``FuncXClient`` class; use ``FuncXExecutor`` instead.

Changed
^^^^^^^

- Refactor ``funcx.sdk.batch.Batch.add`` method interface.  ``function_id`` and
  ``endpoint_id`` are now positional arguments, using language semantics to
  enforce their use, rather than (internal) manual ``assert`` checks.  The
  arguments (``args``) and keyword arguments (``kwargs``) arguments are no
  longer varargs, and thus no longer prevent function use of ``function_id``
  and ``endpoint_id``.

- ``FuncXExecutor`` no longer creates a web socket connection; instead it
  communicates directly with the backing AMQP service.  This removes an
  internal round trip and is marginally more performant.

- ``FuncXExecutor`` now much more faithfully implements the
  ``_concurrent.futures.Executor`` interface.  In particular, the
  ``endpoint_id`` and ``container_id`` items are specified on the executor
  _object_ and not per ``.submit()`` invocation.  See the class documentation
  for more information.

.. _changelog-1.0.4:

funcx & funcx-endpoint v1.0.4
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Add `.task_count_submitted` member to FuncXExecutor.  This value is useful
  for determining in client code how many tasks have *actually* made it to the
  funcX Web Services.

- Add a flag to avoid creating websocket queues on batch runs, the new default is not to create.
  Note that if the queue is not created, results will have to be retrieved directly instead of
  via background polling of the websocket

Bug Fixes
^^^^^^^^^

- gh#907 - Enable concurrent access to the token store by manually serializing
  access to the SQLite DB.

Deprecated
^^^^^^^^^^

- The `batch_interval` keyword argument to the FuncXExecutor is no longer
  utilized.  Internally, the executor no longer waits to coalesce tasks.
  Instead, it pulls them as fast as possible until either the input queue lags
  or the count of tasks in the batch reaches `batch_size`.

Changed
^^^^^^^

- The `funcx_client` argument to `FuncXExecutor()` has been made optional. If nothing
  is passed in, the `FuncXExecutor` now creates a `FuncXClient` for itself.

.. _changelog-1.0.3:

funcx & funcx-endpoint v1.0.3
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Add logic to support Globus Auth client credentials. This allows users to
  specify FUNCX_SDK_CLIENT_ID and FUNCX_SDK_CLIENT_SECRET environment variables
  to use a client credential.

- Endpoints now report their online status immediately on startup (previously,
  endpoints waited ``heartbeat_period`` seconds before reporting their status).

- In order to support the new endpoint status format, endpoints now report their
  heartbeat period as part of their status report package.

- Add `--log-to-console` CLI flag to the endpoint.  This is mostly to entertain
  additional development styles, but may also be useful for some end-user
  workflows.

- funcX Endpoint: Implement ANSI escape codes ("color") for log lines emitted
  to the console.  This is currently targeted to aid the development and
  debugging process, so color is strictly to the console, not to logs.  Use
  the `--log-to-console` and `--debug` flags together.

- Added logout command for funcx-endpoint to revoke cached tokens

Changed
^^^^^^^

- Changed the way that endpoint status is stored in the services - instead of storing a
  list of the most recent status reports, we now store the single most recent status
  report with a TTL set to the endpoint's heartbeat period. This affects the formatting
  of the return value of ``FuncXClient.get_endpoint_status``.

.. _changelog-1.0.0:

funcx & funcx-endpoint v1.0.2
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- New `ResultStore` class, that will store backlogged result messages to
  `<ENDPOINT_DIR>/unacked_results/`

- Upon disconnect from RabbitMQ, the endpoint will now retry connecting
  periodically while the executor continues to process tasks

Bug Fixes
^^^^^^^^^

- Fixed issue with `quiesce` event not getting set from the SIGINT handler,
  resulting in cleaner shutdowns

- DillCodeSource updated to use dill's lstrip option to serialize
  function definitions in nested contexts.

Removed
^^^^^^^

- `ResultsAckHandler` is removed, and `unacked_results.p` files are now
  obsolete.

Changed
^^^^^^^

- DillCodeSource will now be used ahead of DillCode

funcx & funcx-endpoint v1.0.1
-----------------------------

Bug Fixes
^^^^^^^^^

- Fix bug where stored credentials would fail to be loaded (manifesting in an
  EOF error for background processes while unnecessarily attempting to
  recollect credentials)

funcx & funcx-endpoint v1.0.0
-----------------------------

Bug Fixes
^^^^^^^^^

 - Now using the correct HighThroughputExecutor constructor arg to set the log dir for workers

New Functionality
^^^^^^^^^^^^^^^^^

- ``FuncXClient`` now warns you if it thinks you may have supplied ``funcx_service_address``
  and ``results_ws_uri`` that point to different environments. This behavior can be
  turned off by passing ``warn_about_url_mismatch=False``.

Removed
^^^^^^^

- The off_process_checker, previously used to test function serialization methods, was removed

Changed
^^^^^^^

- [Breaking] funcx and funcx-endpoint both require v1.0.0+ to connect to cloud-hosted
  services, and older versions will no longer be supported.

- [Breaking] funcx-endpoint now connects to the cloud-hosted services with RabbitMQ
  over port:5671 instead of ZeroMQ which previously used ports (55001-55003).

- [Breaking] Communication with the services are now encrypted and go over AMQPS
  (TLS/SSL encrypted AMQP).

- Pickle module references were replaced with dill

- The order of serialization method attempts has been changed to try dill.dumps first

- Alter the FuncXEndpoint to include a timestamp with each task state change.
  This is mostly for the development team so as to support retrospective log
  analyses of where tasks get stuck in the pipeline.

- The Parsl dependency has been upgraded to a more recent
  parsl master, from the older parsl 1.1 release.
  This allows recent changes to provider functionality to
  be accessed by funcX endpoint administrators.

.. _changelog-0.4.0a2:

funcx & funcx-endpoint v0.4.0a2
-------------------------------

Added
^^^^^

- The ``FuncXWebClient`` now sends version information via ``User-Agent`` headers
  through the ``app_name`` property exposed by ``globus-sdk``

  - Additionally, users can send custom metadata alongside this version
    information with ``user_app_name``

- The funcx-endpoint service now interfaces with RabbitMQ.

  - As previously, the endpoint registers with the FuncX web service upon
    startup, but now receives endpoint-specific RabbitMQ connection
    configuration.

Removed
^^^^^^^

- The config file in ``~/.funcx/config.py`` has been removed from any
  application logic. The file will not be automatically cleaned up but is
  ignored by the funcx-endpoint application.

Changed
^^^^^^^

- The CLI interface for ``funcx-endpoint`` has been updated in several ways:

  - ``-h`` is supported as a help option

  - ``funcx-endpoint --version`` has been replaced with ``funcx-endpoint version``

- The ``funcx`` error module has been renamed from ``funcx.utils.errors`` to
  ``funcx.errors``

funcx & funcx-endpoint v0.4.0a1
-------------------------------

Added
^^^^^

* ``TaskQueueSubscriber`` class added that allows receiving tasks over RabbitMQ
* ``ResultQueuePublisher`` class added that allows publishing results and status over RabbitMQ
* ``TaskQueuePublisher`` class added for testing
* ``ResultQueueSubscriber`` class added for testing
* A bunch of tests are added that test the above classes described above

- Implement Task Group reloading on the FuncXExecutor.  Look for ``.reload_tasks()``

- FuncXExecutor.submit returns futures with a .task_id attribute
  that will contain the task ID of the corresponding FuncX task.
  If that task has not been submitted yet, then that attribute
  will contain None.

- The ``FuncXClient`` may now be passed ``do_version_check=False`` on init,
  which will lead to faster startup times

- The ``FuncXClient`` now accepts a new argument ``login_manager``, which is
  expected to implement a protocol for providing authenticated http client
  objects, login, and logout capabilities.

- The login manager and its protocol are now defined and may be imported as in
  ``from funcx.sdk.login_manager import LoginManager, LoginManagerProtocol``.
  They are internal components but may be used to force a login or to implement
  an alternative ``LoginManagerProtocol`` to customize authentication

Removed
^^^^^^^

- The following arguments to ``FuncXClient`` are no longer supported:
  ``force_login``

- The ``SearchHelper`` object no longer exposes a method for searching for
  endpoints, as this functionality was never fully implemented.

- The custom response type provided by the SearchHelper object has been
  removed. Instead, callers to function search will get the Globus Search
  response object directly

Deprecated
^^^^^^^^^^

- The following arguments to ``FuncXClient`` are deprecated and will emit
  warnings if used: ``fx_authorizer``, ``search_authorizer``,
  ``openid_authorizer``. The use-cases for these arguments are now satisfied by
  the ability to pass a custom ``LoginManager`` to the client class, if desired.

- The ``openid_authorizer`` argument to FuncXClient is now deprecated. It can
  still be passed, but is ignored and will emit a ``DeprecationWarning`` if
  used

Changed
^^^^^^^

- The endpoint has a new log level, TRACE, which is more verbose than DEBUG

- The ``FuncXClient`` constructor has been refactored. It can no longer be
  passed authorizers for various sub-services. Instead, a new component, the
  ``LoginManager``, has been introduced which makes it possible to pass
  arbitrary globus-sdk client objects for services (by passing a customized
  login manager). The default behavior remains the same, checking login and
  doing a new login on init.

- Tokens are now stored in a new location, in a sqlite database, using
  ``globus_sdk.tokenstorage``. Users will need to login again after upgrading
  from past versions of ``funcx``.

- Remove support for python3.6

- Endpoint logs have been reduced in verbosity. A number of noisy log lines have been
  lowered to TRACE level. [PREFIXES] have been removed from many messages as they
  contain information more reliably availale in log metadata.

- `FuncXExecutor <https://funcx.readthedocs.io/en/latest/executor.html>`_
  now uses batched submission by default.  This typically significantly
  improves the task submission rate when using the executor interface (for
  example, 3 seconds to submit 500 tasks vs 2 minutes, in an informal test).
  However, individual task submission latency may be increased.

  To use non-batched submission mode, set `batch_mode=False` when instantiating
  the `FuncXExecutor <https://funcx.readthedocs.io/en/latest/executor.html>`_
  object.

.. _changelog-0.3.9:

funcx & funcx-endpoint v0.3.9
-----------------------------

Bug Fixes
^^^^^^^^^

- Improve performance in endpoint interchange->manager dispatch,
  by fixing a race condition in worker status processing.
  In an example kubernetes setup, this can double throughput of
  5 second tasks on 6 workers.

- Pin the version of ``click`` used by ``funcx-endpoint``. This resolves issues
  stemming from ``typer`` being incompatible with the latest ``click`` release.

Removed
^^^^^^^

- FuncXFuture was removed. This functionality has been superseded by
  code in FuncXExecutor which uses plain Futures.

Changed
^^^^^^^

- Endpoint logs now have richer metadata on each log line

- Endpoint threads and processes now have human readable names, for logging metadata

funcx & funcx-endpoint v0.3.8
-----------------------------

New Functionality
^^^^^^^^^^^^^^^^^

- Added option for pinning workers to different accelerators
- Log standard error and output from workers to disk

Changed
^^^^^^^

- ``FuncXExecutor`` is now importable from the top-level namespace, as in
  ``from funcx import FuncXExecutor``

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

- Logging for funcx-endpoint no longer writes to ``~/.funcx/endpoint.log`` at any point.
  This file is considered deprecated. Use ``funcx-endpoint --debug <command>`` to
  get debug output written to stderr.
- The output formatting of ``funcx-endpoint`` logging has changed slightly when
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

In prior versions of the ``funcx`` package:

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

* Updated requirements to exclude ``pyzmq==22.3.0`` due to unstable wheel. `Issue#577 <https://github.com/funcx-faas/funcX/issues/611>`_

* Updated requirements specification to ``globus-sdk<3.0``

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

* A new ``FuncXExecutor`` class exposes funcX functionality using the familiar executor interface from the ``concurrent.futures`` library.

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

* Container routing behavior has been improved to support ``soft`` and ``hard`` routing strategies. See `PR#324 <https://github.com/funcx-faas/funcX/pull/324>`_

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

* Fixed a missing package in the ``requirements.txt`` file

* Updated version requirements in ``funcx-endpoint`` to match the ``funcx`` version


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

* Fixed a bug in incorrectly passing the ``funcx_service_address`` to the EndpointInterchange.

* Several updates to the docs for clarity.

* JSON serializer is removed from the FuncXSeralizer mechanism due to issues with not preserving types over serialization (tuples/lists)


funcx & funcx-endpoint v0.2.0
-----------------------------

Released on April 8th, 2021

funcx v0.2.0 includes contributions (code, tests, reviews, and reports) from:

Ariel Rokem <arokem@gmail.com>, Ben Blaiszik <blaiszik@uchicago.edu>, Ben Galewsky <ben@peartreestudio.net>, Ben Glick <glick@glick.cloud>, Joshua Bryan <josh@globus.org>, Kirill Nagaitsev <knagaitsev@uchicago.edu>, Kyle Chard <chard@uchicago.edu>, pratikpoojary <pratik.poojary@somaiya.edu>, Ryan <rchard@anl.gov>, Yadu Nand Babuji <yadudoc1729@gmail.com>, yongyanrao <yongyan.rao@gmail.com>, and Zhuozhao Li <zhuozhao@uchicago.edu>

Known Issues
^^^^^^^^^^^^

There is an ongoing stability issue with ``pyzmq`` wheels that causes endpoint crashes.
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


* ``get_batch_status`` has been renamed to ``get_batch_result``
