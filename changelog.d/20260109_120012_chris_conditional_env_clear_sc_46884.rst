New Functionality
^^^^^^^^^^^^^^^^^

- When running an endpoint under a non-root user, environment variables from the parent
  process are now passed to the user endpoint process. (Endpoints run as root still
  start with a clean environment.)
