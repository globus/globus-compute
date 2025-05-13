New Functionality
^^^^^^^^^^^^^^^^^

- Implement hot-restart functionality for Multi-user endpoint.  See
  :ref:`hot-restart` for full documentation, but the synopsis is send the
  ``SIGHUP`` signal to the MEP (parent) process.  Currently, there is no
  equivalent built-in sub-command to ``globus-compute-endpoint``.
