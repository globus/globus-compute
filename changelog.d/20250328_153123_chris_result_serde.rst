New Functionality
^^^^^^^^^^^^^^^^^

- When using the :class:`~globus_compute_sdk.Executor`, users can now specify what
  serialization strategies they would like their results to be serialized with.
  For example:

  .. code-block:: python

    from globus_compute_sdk import Executor
    from globus_compute_sdk.serialize import JSONData

    with Executor("<your-endpoint-uuid>") as gcx:
      # tell the endpoint to serialize the result of the function using JSONData:
      gcx.result_serializers = [JSONData()]
      result = gcx.submit(...)
