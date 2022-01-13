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
  an error response are now instances of ``globus_sdk.GlobusAPIError``. This
  means that the errors no longer inherit from ``FuncxResponseError``. Update
  error handling code as follows:


In prior versions of the `funcx` package:

.. code-block:: python

    from funcx.utils.response_errors import FuncxResponseError

    client = FuncXClient(...)
    try:
        client.some_method(...)
    except FuncxResponseError as err:
        code = err.code  # this is an enum member
        ...


In the new version:

.. code-block:: python

    import globus_sdk

    client = FuncXClient(...)
    try:
        client.some_method(...)
    except globus_sdk.GlobusAPIError as err:
        code = err["code"]  # this is an integer
        ...
