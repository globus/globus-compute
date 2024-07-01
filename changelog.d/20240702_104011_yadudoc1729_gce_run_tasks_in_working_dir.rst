Changed
^^^^^^^

- ``GlobusComputeEngine.working_dir`` now defaults to ``tasks_working_dir``
   * When ``working_dir=relative_path``, tasks run in a path relative to the endpoint.run_dir.
     The default is ``tasks_working_dir`` set relative to endpoint.run_dir.
   * When ``working_dir=absolute_path``, tasks run in the specified absolute path