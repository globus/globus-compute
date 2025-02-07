Bug Fixes
^^^^^^^^^

- The default engine is now ``GlobusComputeEngine`` when the engine ``type`` field is not specified.
  Previously, the default was ``HighThroughputEngine``, which was removed in version ``3.0.0``.