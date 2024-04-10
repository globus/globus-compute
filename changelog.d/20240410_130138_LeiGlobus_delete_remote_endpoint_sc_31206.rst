.. New Functionality
.. ^^^^^^^^^^^^^^^^^
..
- The ``delete`` command can now delete endpoints by name or UUID from the
   Compute service remotely when local config files are not available.  Note
   that without the ``--force`` option the command may exit early if the
   endpoint is currently running or local config files are corrupted.
