Deprecated
^^^^^^^^^^

- The `batch_interval` keyword argument to the FuncXExecutor is no longer
  utilized.  Internally, the executor no longer waits to coalesce tasks.
  Instead, it pulls them as fast as possible until either the input queue lags
  or the count of tasks in the batch reaches `batch_size`.
