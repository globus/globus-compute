New Functionality
^^^^^^^^^^^^^^^^^

- Serialization strategies can now be specified at the Executor level, without needing
  to construct a Client:

  .. code:: python

    from globus_compute_sdk import Executor
    from globus_compute_sdk.serialize import ComputeSerializer, CombinedCode, JSONData

    with Executor("blah") as gcx:
      gcx.serializer = ComputeSerializer(
        strategy_code=CombinedCode(), strategy_data=JSONData()
      )
      # do something with gcx
