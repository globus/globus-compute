Bug Fixes
^^^^^^^

- The ``GlobusComputeEngine``, ``ProcessPoolEngine``, and ``ThreadPoolEngine``
  now respect the ``heartbeat_period`` variable, as defined in ``config.yaml``.

Changed
^^^^^^^

- Renamed the ``heartbeat_period_s`` attribute to ``heartbeat_period`` for
  ``GlobusComputeEngine``, ``ProcessPoolEngine``, and ``ThreadPoolEngine``
  to maintain parity with the ``HighThroughputEngine`` and Parsl's
  ``HighThroughputExecutor``.
