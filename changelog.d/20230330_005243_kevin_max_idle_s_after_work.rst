New Functionality
^^^^^^^^^^^^^^^^^

- Add two items to the ``Config`` object: ``idle_heartbeats_soft`` and
  ``idle_heartbeats_hard``.  If set, the endpoint will auto-shutdown after the
  specified number of heartbeats with no work to do.

