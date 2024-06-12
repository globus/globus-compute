New Functionality
^^^^^^^^^^^^^^^^^

- Implement ``debug`` as a top-level config boolean for a Compute Endpoint.
  This flag determines whether debug-level logs are emitted -- the same
  functionality as the ``--debug`` command line argument to the
  ``globus-compute-endpoint`` executable.  Note: if this flag is set to
  ``False`` when the ``--debug`` CLI flag is specified, the CLI wins.
