New Functionality
^^^^^^^^^^^^^^^^^

- Added a new runtime check to ``globus_compute_endpoint.engines`` that will raise a `RuntimeError`
  if a task is submitted before ``engine.start()`` was called.
