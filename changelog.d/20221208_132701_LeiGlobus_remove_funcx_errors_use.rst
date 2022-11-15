.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).

Changed
^^^^^^^
- The exceptions raised by the ``FuncXClient`` when the web service sends back
  an error response are now instances of ``globus_sdk.GlobusAPIError``, and
  the FuncX specific subclass FuncxAPIError has been removed.

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

