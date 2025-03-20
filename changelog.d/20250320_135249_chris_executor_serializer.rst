New Functionality
^^^^^^^^^^^^^^^^^

- The :class:`~globus_compute_sdk.Executor` can now access the underlying
  :class:`~globus_compute_sdk.serialize.ComputeSerializer` directly through the
  ``serializer`` constructor argument and property. Eg:

  .. code-block:: python

    from globus_compute_sdk import Client, Executor
    from globus_compute_sdk.serialize import ComputeSerializer, CombinedCode

    # this code block:
    gcc = Client(code_serialization_strategy=CombinedCode())
    gce = Executor("<some uuid>", client=gcc)

    # is equivalent to this:
    serde = ComputeSerializer(strategy_code=CombinedCode())
    gce = Executor("<some uuid>", serializer=serde)

    # or this:
    with Executor("<some uuid>") as gce:
        gce.serializer = ComputeSerializer(strategy_code=CombinedCode())
