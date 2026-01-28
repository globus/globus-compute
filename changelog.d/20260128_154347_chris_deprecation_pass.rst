Removed
^^^^^^^

- The following deprecated features were removed:

    - The ``executors`` config option (use ``engine`` instead)
    - Support for environment variables prefixed with ``FUNCX_``, namely
      ``FUNCX_SDK_CLIENT_ID``, ``FUNCX_SDK_CLIENT_SECRET``, and ``FUNCX_SCOPE``
      (use ``GLOBUS_COMPUTE_``-prefixed environment variables instead)
    - The ``globus-compute-endpoint self-diagnostic`` command (use
      ``globus-compute-diagnostic`` instead)