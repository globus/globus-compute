Working With MPI
****************

MPI, short for `Message Passing Interface`__, is a communication protocol for parallel
applications that is widely used in HPC. This tutorial is intended for administrators
who want to enable MPI support on their endpoint, as well as users who want to submit
MPI jobs to such an endpoint.

__ https://en.wikipedia.org/wiki/Message_Passing_Interface

.. note::

    Compute's MPI system is a wrapper over Parsl's MPI system. For more details on
    how the latter works, see the `Parsl documentation`__.

    __ https://parsl.readthedocs.io/en/stable/userguide/apps/mpi_apps.html


Configure an Endpoint
=====================

If you are starting from scratch, you will need to initialize an endpoint with
the ``configure`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint configure my-ep


Modify the Configuration Template
---------------------------------

.. note::

    For more details on MPI-related configuration options, see :ref:`configuring-mpi`.

Start with a clean :ref:`user-config-template-yaml-j2`:

.. code-block:: yaml+jinja

    engine:
        type: GlobusComputeEngine
        max_workers_per_node: 1

        provider:
            type: LocalProvider

            min_blocks: 0
            max_blocks: 1
            init_blocks: 1


Update the config to use |GlobusMPIEngine| and |SimpleLauncher|_:

.. code-block:: yaml+jinja
    :emphasize-lines: 2, 8-9

    engine:
        type: GlobusMPIEngine
        max_workers_per_node: 1

        provider:
            type: LocalProvider

            launcher:
                type: SimpleLauncher

            min_blocks: 0
            max_blocks: 1
            init_blocks: 1


Depending on the target system, set the correct provider and ``mpi_launcher``. For this
tutorial, we'll use Slurm; check :doc:`/endpoints/endpoint_examples` for examples based
on other schedulers.

.. code-block:: yaml+jinja
    :emphasize-lines: 3, 7

    engine:
        type: GlobusMPIEngine
        mpi_launcher: srun
        max_workers_per_node: 1

        provider:
            type: SlurmProvider

            launcher:
                type: SimpleLauncher

            min_blocks: 0
            max_blocks: 1
            init_blocks: 1


Finally, configure the shape of the resources available to MPI tasks. Set
``nodes_per_block`` to configure the size of the block Parsl will reserve for MPI
tasks, and set ``max_workers_per_block`` to limit how many MPI tasks can be run per
block.

.. code-block:: yaml+jinja
    :emphasize-lines: 11-12

    engine:
        type: GlobusMPIEngine
        mpi_launcher: srun

        provider:
            type: SlurmProvider

            launcher:
                type: SimpleLauncher

            max_workers_per_block: 4
            nodes_per_block: 8


For this example we give each block 8 nodes, and allow up to 4 MPI jobs at once on a
single block.


Start the Endpoint
------------------

Once the endpoint is configured, we can start it up.

.. code-block:: console

   $ globus-compute-endpoint start my-ep

Take note of the endpoint ID emitted to the console; we will use it later in the
tutorial.


Submit Tasks from the SDK
=========================

.. note::

    For more details on MPI support on the SDK, see :ref:`submitting-mpi`.

We'll use the |Executor| to submit our MPI tasks, but first, we need to define our MPI
function. In this case, we'll just run ``hostname`` on every MPI node:

.. code-block:: python

    from globus_compute_sdk import MPIFunction
    mpi_func = MPIFunction("hostname")

An |MPIFunction| can be submitted like any other Python function. When submitted, it
runs bash commands on the endpoint, with the appropriate MPI executable and arguments
handled by Parsl.

In order to run MPI tasks we need to give our |Executor| a ``resource_specification``,
which tells the endpoint how to distribute the nodes amongst MPI workers:

.. code-block:: python

    ep_id =  "..."  # Endpoint ID from before
    with Executor(endpoint_id=ep_id) as ex:
        ex.resource_specification = {
            "num_ranks": 1,  # run 1 MPI task
            "num_nodes": 8   # and give it 8 nodes
        }
        f = ex.submit(mpi_func)
        mpi_result = f.result()
        print(mpi_result.stdout)

|MPIFunction| submissions return |ShellResult| objects, hence the ``.stdout``.

Finally, each task can have its own ``resource_specification``:

.. code-block:: python

    with Executor(endpoint_id=ep_id) as ex:
        for ranks in range(1, 4):  # reminder: (1, 2, 3). 4 not included
            # run "ranks" MPI tasks on each node
            ex.resource_specification = {
                "ranks_per_node": ranks,
                "num_nodes": 2
            }
            f = ex.submit(mpi_func)
            mpi_result = f.result()
            print(mpi_result.stdout)

This should result in output that looks something like the following:

.. code-block:: text

    # 2 nodes, 1 rank
    my-node-1
    my-node-2

    # 2 nodes, 2 ranks
    my-node-2
    my-node-1
    my-node-1
    my-node-2

    # 2 nodes, 3 ranks
    my-node-1
    my-node-2
    my-node-1
    my-node-2
    my-node-2
    my-node-1

.. |MPIFunction| replace:: :class:`~globus_compute_sdk.sdk.mpi_function.MPIFunction`
.. |ShellFunction| replace:: :class:`~globus_compute_sdk.sdk.shell_function.ShellFunction`
.. |ShellResult| replace:: :class:`~globus_compute_sdk.sdk.shell_function.ShellResult`
.. |GlobusMPIEngine| replace:: :class:`~globus_compute_endpoint.engines.GlobusMPIEngine`
.. |GlobusComputeEngine| replace:: :class:`~globus_compute_endpoint.engines.GlobusComputeEngine`
.. |Executor| replace:: :class:`~globus_compute_sdk.Executor`
.. |MPIExecutor| replace:: ``MPIExecutor``
.. _MPIExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.MPIExecutor.html
.. |HighThroughputExecutor| replace:: ``HighThroughputExecutor``
.. _HighThroughputExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html
.. |SimpleLauncher| replace:: ``SimpleLauncher``
.. _SimpleLauncher: https://parsl.readthedocs.io/en/stable/stubs/parsl.launchers.SimpleLauncher.html
