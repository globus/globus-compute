New Functionality
^^^^^^^^^^^^^^^^^

- Added two new serialization strategies,
  :class:`~globus_compute_sdk.serialize.PureSourceTextInspect` and
  :class:`~globus_compute_sdk.serialize.PureSourceDill`, which serialize functions as
  raw source code strings (avoiding ``dill`` bytecode serialization entirely).
