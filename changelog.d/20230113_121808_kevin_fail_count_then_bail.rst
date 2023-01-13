New Functionality
^^^^^^^^^^^^^^^^^

- The funcX Endpoint will now shutdown after 5 consecutive failures to
  initialize.  (The previous behavior was to try indefinitely, even if the
  error was unrecoverable.)
