Changed
^^^^^^^

- Changed ``heartbeat_period`` type from float to int for ``GlobusComputeEngine``,
  ``ProcessPoolEngine``, and ``ThreadPoolEngine`` to maintain parity with the
  ``HighThroughputEngine`` and Parsl's ``HighThroughputExecutor``.