New Functionality
^^^^^^^^^^^^^^^^^

- ``GlobusComputeEngine`` now supports a ``working_dir`` keyword argument that sets the directory in which
  all functions will be executed. Relative paths, if set, will be considered relative to the endpoint directory
  (``~/.globus_compute/<endpoint_name>``). If this option is not set, ``GlobusComputeEngine`` will use the
  endpoint directory as the working directory. Set this option using ``working_dir: <working_dir_path>``
  Example config:

  .. code-block:: yaml

    display_name: WorkingDirExample
    engine:
      type: GlobusComputeEngine
      # Run functions in ~/.globus_compute/<EP_NAME>/TASKS
      working_dir: TASKS

- ``GlobusComputeEngine`` now supports function sandboxing, where each function is executed within a
  sandbox directory for better isolation. When this option is enabled by setting ``run_in_sandbox: True``
  a new directory with the function UUID as the name is created in the working directory (configurable with
  the ``working_dir`` kwarg). Example config:

  .. code-block:: yaml

    display_name: WorkingDirExample
    engine:
      type: GlobusComputeEngine
      # Set working dir to /projects/MY_PROJ
      working_dir: /projects/MY_PROJ
      # Enable sandboxing to have functions run under /projects/MY_PROJ/<function_uuid>/
      run_in_sandbox: True


Bug Fixes
^^^^^^^^^

- Fixed bug where ``GlobusComputeEngine`` set the current working directory to the directory
  from which the endpoint was started. Now, ``GlobusComputeEngine`` will set the working directory
  to the endpoint directory (``~/.globus_compute/<endpoint_name>``) by default. This can be configured
  via the endpoint config.