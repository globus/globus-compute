New Functionality
^^^^^^^^^^^^^^^^^

- Added ``.get_result_amqp_url()`` to ``FuncXClient`` to acquire user
  credentials to the AMQP service.  Globus credentials are first verified
  before user-specific AMQP credentials are (re)created and returned.  The only
  expected use of this method comes from ``FuncXExecutor``.

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

- ``FuncXExecutor`` no longer creates a web socket connection; instead it
  communicates directly with the backing AMQP service.  This removes an
  internal round trip and is marginally more performant.

- ``FuncXExecutor`` now much more faithfully implements the
  ``_concurrent.futures.Executor`` interface.  In particular, the
  ``endpoint_id`` and ``container_id`` items are specified on the executor
  _object_ and not per ``.submit()`` invocation.  See the class documentation
  for more information.
