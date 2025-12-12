New Functionality
^^^^^^^^^^^^^^^^^

- Added two options to ``globus-compute-endpoint configure``, ``--manager-config``
  and ``--template-config``, which allow specifying alternate configuration files
  to be copied into the new endpoint directory during configuration.

Deprecated
^^^^^^^^^^

- Deprecated the ``--endpoint-config`` option of ``globus-compute-endpoint configure``,
  in favor of the new ``--manager-config`` and ``--template-config`` options.
