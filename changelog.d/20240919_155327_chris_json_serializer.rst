New Functionality
^^^^^^^^^^^^^^^^^

- Added a new data serialization strategy, ``JSONData``, which serializes/deserializes
  function args and kwargs via JSON. Usage example:

  .. code-block:: python

    from globus_compute_sdk import Client, Executor
    from globus_compute_sdk.serialize import JSONData

    gcc = Client(
        data_serialization_strategy=JSONData()
    )

    with Executor(<your endpoint UUID>, client=gcc) as gcx:
        # do something with gcx
