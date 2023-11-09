Changed
^^^^^^^

  Setting `heartbeat_period` on the engines no longer change the
  interval at which endpoint status messages are sent to the service.

  Setting `GlobusComputeEngine(heartbeat_period=1)` now only sets the
  frequency of heartbeats between the internal components of the Engine.
