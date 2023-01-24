Changed
^^^^^^^

- Initiate shutdown of any currently running FuncXExecutor objects when the main
  thread ends (a.k.a., "end of script").  This follows the same behavior as
  both ``ThreadPoolExecutor`` and ``ProcessPoolExecutor``.
