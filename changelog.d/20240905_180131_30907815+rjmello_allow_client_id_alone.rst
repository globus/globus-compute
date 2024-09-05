Bug Fixes
^^^^^^^^^

- We no longer raise an exception if a user defines the ``GLOBUS_COMPUTE_CLIENT_ID``
  environment variable without defining ``GLOBUS_COMPUTE_SECRET_KEY``. The reverse,
  however, will still raise an exception.
