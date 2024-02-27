Bug Fixes
^^^^^^^^^

- Users can now define the ``max_workers_per_node`` configuration variable
  for the ``GlobusComputeEngine``, which is equivalent to the ``max_workers``
  variable. When both are defined, ``max_workers_per_node`` will take precedence.