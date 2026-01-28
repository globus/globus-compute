Removed
^^^^^^^

- The following deprecated features were removed:

    - The ``executors`` config option (use ``engine`` instead)
    - Support for environment variables prefixed with ``FUNCX_``, namely
      ``FUNCX_SDK_CLIENT_ID``, ``FUNCX_SDK_CLIENT_SECRET``, and ``FUNCX_SCOPE``
      (use ``GLOBUS_COMPUTE_``-prefixed environment variables instead)