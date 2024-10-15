New Functionality
^^^^^^^^^^^^^^^^^

- ``GlobusComputeEngine``, ``ThreadPoolEngine``, and ``ProcessPoolEngine`` can
  now be configured with ``working_dir`` to specify the tasks working directory.
  If a relative path is specified, it is set in relation to the endpoint
  run directory (usually ``~/.globus_compute/<endpoint_name>``). Here's an example
  config file:

  .. code-block:: yaml

    engine:
      type: GlobusComputeEngine
      working_dir: /absolute/path/to/tasks_working_dir

Bug Fixes
^^^^^^^^^

 - Fixed a bug where functions run with ``ThreadPoolEngine`` and ``ProcessPoolEngine``
   create and switch into the ``tasks_working_dir`` creating endless nesting.
