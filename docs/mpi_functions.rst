MPI Functions
-------------

|MPIFunction|_ is the solution for executing MPI applications remotely using Globus Compute.
As an extension to |ShellFunction|_, the |MPIFunction|_ supports the same interface
to specify the command to invoke on the endpoint as well as the capture of output
streams. However, |MPIFunction|_ diverges from |ShellFunction|_ in the following ways:

1. An |MPIFunction|_ requires a specification of the resources for its execution. This
   specification must be set on the executor on the client-side. The ``resource_specification: dict``
   takes the following options:

   .. code-block:: python

        executor.resource_specification = {
            'num_nodes': <int>,        # Number of nodes required for the application instance
            'ranks_per_node': <int>,   # Number of ranks / application elements to be launched per node
            'num_ranks': <int>,        # Number of ranks in total
        }

2. |MPIFunction|_ is designed to be used with ``GlobusMPIEngine`` on the endpoint.
   ``GlobusMPIEngine`` is required for the partitioning of a batch job (blocks) dynamically
   based on the ``resource_specification`` of the |MPIFunction|_.


3. |MPIFunction|_ automatically prefixes the supplied command with ``$PARSL_MPI_PREFIX``
   which resolves to an appropriate mpi launcher prefix (for e.g, ``mpiexec -n 4 -host <NODE1,NODE2>``).


Multi-line commands
^^^^^^^^^^^^^^^^^^^

|MPIFunction|_ allows for multi-line commands, however the MPI launcher prefix is applied only to
the entire command. If multiple commands need to be launched, explicitly use the ``$PARSL_MPI_PREFIX``.
Here's an example:

.. code-block:: python

      MPIFunction("""true; # force the default prefix to launch a no-op
      $PARSL_MPI_PREFIX <command_1>
      $PARSL_MPI_PREFIX <command_2>
      """)


Here is an example configuration for an HPC system that uses PBSPro scheduler:

.. code-block:: yaml

    # Example configuration for a PBSPro based HPC system
    display_name: PBSProHPC
    engine:
      type: GlobusMPIEngine
      mpi_launcher: mpiexec

      provider:
        type: PBSProProvider

        # Specify # of nodes per batch job that will be
        # shared by multiple MPIFunctions
        nodes_per_block: 4

        launcher:
          type: SimpleLauncher


Here's another trimmed example for an HPC system that uses Slurm as the scheduler:

.. code-block:: yaml

    # Example configuration for a Slurm based HPC system
    display_name: SlurmHPC
    engine:
      type: GlobusMPIEngine
      mpi_launcher: srun

      provider:
        type: SlurmProvider

        launcher:
          type: SimpleLauncher

        # Specify # of nodes per batch job that will be
        # shared by multiple MPIFunctions
        nodes_per_block: 4


.. code-block:: python

   from globus_compute_sdk import MPIFunction

    ep_id = <SPECIFY_ENDPOINT_ID>
    func = MPIFunction("hostname")
    for nodes in range(1,4):
        executor.resource_specification = {
             "num_nodes": 2,
             "ranks_per_node": nodes
        }
        fu = executor.submit(func)
        mpi_result = fu.result()
        print(mpi_result.stdout)

        # The above line prints:
        exp-14-08
        exp-14-20

        exp-14-08
        exp-14-20
        exp-14-08
        exp-14-20

        exp-14-08
        exp-14-20
        exp-14-08
        exp-14-08
        exp-14-20
        exp-14-20

The |ShellResult|_ object captures outputs relevant to simplify debugging when execution
failures. By default, |MPIFunction|_ captures 1000 lines of stdout and stderr, but this
can be changed via the ``MPIFunction(snippet_lines:int = <NUM_LINES>)`` kwarg.

Results
^^^^^^^

|MPIFunction|_ encapsulates its output in a |ShellResult|_. Please refer to
our `documentation section on shell results <#shell-results>`_ for more information.


.. |MPIFunction| replace:: ``MPIFunction``
.. _MPIFunction: reference/mpi_function.html

.. |ShellFunction| replace:: ``ShellFunction``
.. _ShellFunction: reference/shell_function.html

.. |ShellResult| replace:: ``ShellResult``
.. _ShellResult: reference/shell_function.html#globus_compute_sdk.sdk.shell_function.ShellResult
