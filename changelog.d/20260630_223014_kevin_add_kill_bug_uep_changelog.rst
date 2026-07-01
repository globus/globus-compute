Bug Fixes
^^^^^^^^^

- Update shutdown logic to more gracefully bring down user endpoint processes
  that use the |GlobusComputeEngine|.  In some scenarios, the underlying
  ``Parsl`` interchange would hang during shutdown, leading to unnecessary
  delays and warnings in the log.
