Single User Compute Endpoints
=============================

  A Globus Compute endpoint is a user-launched process on a computing resource to
  act as a conduit for executing functions.  The Compute endpoint manages the
  site-specific interactions for executing functions, leaving users with only a single
  API necessary for running code on their laptop, campus cluster, or even
  leadership‑class HPC machines.

A single-user Compute endpoint (CEP) is a user-level service (i.e., run by the
user, not an administrator) that receives tasks from the Globus Compute web services,
ferries those tasks to site-specific execution engines, and forwards the results back to
the web-services.  The defining characteristic of a CEP over a :doc:`multi-user Compute
Endpoint (MEP) <multi_user>` is that the user is responsible for the configuration and
lifecycle.

For those just looking for the quickstart commands:

.. code-block:: console

   $ python3 -m pipx install globus-compute-endpoint

   $ globus-compute-endpoint configure my_first_endpoint

   $ globus-compute-endpoint start my_first_endpoint

   $ globus-compute-endpoint stop my_first_endpoint

Installation
------------

If the site administrator has not already installed the Globus Compute Endpoint
software, users typically install it from `PyPI
<https://pypi.org/project/globus-compute-endpoint/>`_.  We **strongly** recommend
installing the Globus Compute endpoint into an isolated virtual environment.  We
recommend use of |pipx for library isolation|_:

.. code-block:: console

   $ python3 -m pipx install globus-compute-endpoint

.. note::

   **Currently, the Globus Compute Endpoint is only supported on Linux.**  Some have
   reported success running Compute endpoints on macOS, but we do not currently support
   it.  If running on macOS is required, consider doing so in a Docker container.

At this point, the ``globus-compute-endpoint`` executable should exist on the local
``$PATH``:

.. code-block:: console

   $ globus-compute-endpoint version
   Globus Compute endpoint version: 2.19.0


Configuration
-------------

With the software installed and accessible, create a new endpoint directory and default
files in ``$HOME/.globus_compute/`` via the ``configure`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint configure my_first_endpoint
   Created profile for endpoint named <my_first_endpoint>

   	Configuration file: /.../.globus_compute/my_first_endpoint/config.yaml

The Compute Endpoint will also be in the output of the ``list`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint list
   +--------------------------------------+--------------+--------------------+
   |             Endpoint ID              |    Status    |   Endpoint Name    |
   +======================================+==============+====================+
   | None                                 | Initialized  | my_first_endpoint  |
   +--------------------------------------+--------------+--------------------+

The Compute endpoint will receive an endpoint identifier from the Globus Compute web
service upon first connect.  Until then, it exists solely as a subdirectory of
``$HOME/.globus_compute/``.

.. _cea_configuration:

As Globus Compute endpoints may be run on a diverse set of computational resources
(i.e., the gamut from laptops to supercomputers), it is important to configure each
CEP instance to match the underlying capabilities and restrictions of the resource.  The
default configuration is functional |nbsp| -- |nbsp| it will process tasks |nbsp| --
|nbsp| but it is intentionally limited to only use processes on the Endpoint host.  In
it's entirety, the default configuration is:

.. code-block:: yaml
   :caption: ``$HOME/.globus_compute/my_first_endpoint/config.yaml``

   amqp_port: 443
   display_name: null
   engine:
     type: GlobusComputeEngine
     provider:
       type: LocalProvider
       init_blocks: 1
       max_blocks: 1
       min_blocks: 0

For now, the key items to observe are the structure (e.g., ``provider`` as a child of
``engine``), the engine type, ``GlobusComputeEngine``, and the provider type,
``LocalProvider``.

The *engine* pulls tasks from incoming queue and conveys them to the *provider* for
execution.  Globus Compute implements three engines: ``ThreadPoolEngine``,
``ProcessPoolEngine``, and ``GlobusComputeEngine``.  The first two are Compute endpoint
wrappers of Python's `thread pool`_ and `process pool`_ executors, but most users will
rely on |GlobusComputeEngine|_, which is a wrapper over Parsl's
|HighThroughputExecutor|_.

In contrast to the engine, the *provider* speaks to the site's available resources.  For
example, if an endpoint is on the local workstation, the configuration might use the
|LocalProvider|_, but for running jobs on a Slurm cluster, the endpoint would need the
|SlurmProvider|_.  (LocalProvider and SlurmProvider are an arbitrary selection for this
discussion; Parsl implements `a number of other providers`_.)

Using the full power of the underlying resources requires site-specific setup, and can
be tricky to get right.  For instance, configuring the CEP to submit tasks to a batch
scheduler might require a scheduler account id, awareness of which queues are
accessible for the account id and the job size at hand (that can change!), knowledge of
which network interface cards to use, administrator-chosen setup steps, and so forth ...
the :doc:`list of example configurations <endpoint_examples>` is a good first resource as
these are known working configurations.

.. _thread pool: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
.. _process pool: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
.. |pipx for library isolation| replace:: ``pipx`` for library isolation
.. _pipx for library isolation: https://pipx.pypa.io/stable/
.. |GlobusComputeEngine| replace:: ``GlobusComputeEngine``
.. _GlobusComputeEngine: ../reference/engine.html#globus_compute_endpoint.engines.GlobusComputeEngine
.. |HighThroughputExecutor| replace:: ``HighThroughputExecutor``
.. _HighThroughputExecutor: https://parsl.readthedocs.io/en/latest/stubs/parsl.executors.HighThroughputExecutor.html
.. |LocalProvider| replace:: ``LocalProvider``
.. _LocalProvider: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.LocalProvider.html
.. |SlurmProvider| replace:: ``SlurmProvider``
.. _SlurmProvider: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.SlurmProvider.html
.. _a number of other providers: https://parsl.readthedocs.io/en/latest/reference.html#providers

Starting the Endpoint
---------------------

After configuration, start the CEP instance with the ``start`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint start my_first_endpoint
   Starting endpoint; registered ID: <...registered UUID...>

The CEP instance will first register with the Globus Compute web services, open two AMQP
connections to the Globus Compute AMQP service (one to receive tasks, one to submit
results), print the web service-provided Endpoint ID to the console, then daemonize.
Though the prompt returns, the process is still running:

.. code-block:: console

        ; ### (output edited for brevity)

   $ globus-compute-endpoint list
   +--------------------------------------+--------------+--------------------+
   |             Endpoint ID              |    Status    |   Endpoint Name    |
   +======================================+==============+====================+
   |   <...the same registered UUID...>   | Running      | my_first_endpoint  |
   +--------------------------------------+--------------+--------------------+

   $ ps x --forest | grep -A 2 my_first_endpoint
     [...]   \_ Globus Compute Endpoint (<THE_ENDPOINT_UUID>, my_first_endpoint) [...]
     [...]       \_ parsl: HTEX interchange
     [...]       \_ Globus Compute Endpoint (<THE_ENDPOINT_UUID>, my_first_endpoint) [...]

The Globus Compute endpoint requires outbound access to the Globus Compute services over
HTTPS (port 443) and AMQPS (port 5671).

.. note::

   All Compute endpoints run on behalf of a user.  At the Unix level, the processes run
   as a particular username (c.f., ``$USER``, ``uid``), but to connect to the Globus
   Compute web services (and thereafter receive tasks and transmit results), the
   endpoint must be associated with a Globus Auth identity.  The Globus Compute web
   services will validate incoming tasks for this CEP against this identity.  Further,
   once registered, the CEP instance cannot be run by another Globus Auth identity.

.. note::

   On the first invocation, the CEP will emit a long link to the console and ask for a
   Globus Auth code in return.  As part of this step, the Globus Compute web services
   will request access to your Globus Auth identity and Globus Groups.   (Subsequent
   runs will not need to perform this login step as the credentials are cached.)

Stopping the Compute Endpoint
-----------------------------

There are a couple of ways to stop the Compute endpoint.  At the process-level, the
service responds to the Unix signals SIGTERM and SIGQUIT, so if the PID of the parent
process is handy, then either will work:

.. code-block:: console

   $ kill -SIGQUIT <the_cep_pid>    # equivalent to -SIGTERM
   $ kill -SIGTERM <the_cep_pid>    # equivalent to -SIGQUIT

More ergonomic, however, is the ``stop`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint stop my_first_endpoint
   > Endpoint <my_first_endpoint> is now stopped

Listing Endpoints
-----------------

To list available endpoints on the current system, run:

.. code-block:: console

   $ globus-compute-endpoint list
   +--------------------------------------+--------------+-----------------------+
   |             Endpoint ID              |    Status    |   Endpoint Name       |
   +======================================+==============+=======================+
   |   <...111111 a registered UUID...>   | Initialized  | just_configured       |
   +--------------------------------------+--------------+-----------------------+
   |   <...the same registered UUID...>   | Stopped      | my_first_endpoint     |
   +--------------------------------------+--------------+-----------------------+
   |   <...22 other registered UUID...>   | Running      | debug_queue           |
   +--------------------------------------+--------------+-----------------------+
   |   <...33 another endpoint UUID...>   | Disconnected | unexpected_disconnect |
   +--------------------------------------+--------------+-----------------------+

Endpoints will be in one of the following states:

* **Initialized**: The endpoint has been created, but not started following
  configuration and is not registered with the `Globus Compute service`.
* **Running**: The endpoint is active and available for executing functions.
* **Stopped**: The endpoint was stopped by the user.  It is not running and therefore,
  cannot service any functions.  It can be started again without issues.
* **Disconnected**: The endpoint disconnected unexpectedly.  It is not running
  and therefore cannot service any functions.  Starting this endpoint will first invoke
  necessary endpoint cleanup, since it was not stopped correctly previously.

Ensuring execution environment
------------------------------

When running a function, endpoint worker processes expect to have all the necessary
dependencies readily available to them.  For example, if a function uses ``numpy`` and a
worker is running on a machine without ``numpy`` installed, attempts to execute that
function using that worker will result in an error.

In HPC contexts, the endpoint process |nbsp| -- |nbsp| which receives tasks from the
Compute central services and queues them up for execution |nbsp| -- |nbsp| generally
runs on a separate node from the workers which actually do the computation.  As a
result, it is often necessary to load in some kind of pre‑initialized environment.  In
general there are two approaches:

1. Python based environment isolation such as ``conda`` environment or ``venv``,
2. Containers: containerization with ``docker`` or ``apptainer`` (``singularity``)

.. note::
   Please note that worker environment is required to have the
   ``globus-compute-endpoint`` python module installed.  We recommend matching the
   Python version and ``globus-compute-endpoint`` module version on the worker
   environment to that on the endpoint itself.

Python based environment isolation
----------------------------------

To use python based environment management, use the |worker_init|_ config option:

.. code-block:: yaml

   engine:
     provider:
       worker_init: |
         conda activate my-conda-env

The exact behavior of ``worker_init`` depends on the |Provider|_ being used.

In some cases, it may also be helpful to run some setup during the startup process of
the endpoint itself, before any workers start.  This can be achieved using the top-level
``endpoint_setup`` config option:

.. code-block:: yaml

  endpoint_setup: |
    conda create -n my-conda-env
    conda activate my-conda-env
    pip install -r requirements.txt

.. note::
   Note that ``endpoint_setup`` runs in a shell, as a child of the CEP process, and must
   finish successfully before the start up process continues.  In particular, *note that
   it is not possible to use this hook to set or change environment variables for the
   CEP.*

Similarly, artifacts created by ``endpoint_setup`` can be cleaned up with
``endpoint_teardown``:

.. code-block:: yaml

  endpoint_teardown: |
    conda remove -n my-conda-env --all


Advanced Setups
---------------

Client Identities
^^^^^^^^^^^^^^^^^
To start an endpoint using a client identity, rather than as a user, export the
``GLOBUS_COMPUTE_CLIENT_ID`` and ``GLOBUS_COMPUTE_CLIENT_SECRET`` environment variables.
This is explained in detail in :ref:`client credentials with globus compute clients`.

Containerized Environments
^^^^^^^^^^^^^^^^^^^^^^^^^^

Container support is limited to ``GlobusComputeEngine``.  To configure the endpoint the
following options are supported:

* `container_type` : Specify container type from one of ``('docker', 'apptainer',
  'singularity', 'custom', 'None')``
* `container_uri`: Specify container uri, or file path if specifying sif files
* `container_cmd_options`: Specify custom command options to pass to the container
  launch command, such as filesystem mount paths, network options etc.

.. code-block:: yaml

    display_name: Docker
    engine:
      type: GlobusComputeEngine
      container_type: docker
      container_uri: funcx/kube-endpoint:main-3.10
      container_cmd_options: -v /tmp:/tmp
      provider:
        init_blocks: 1
        max_blocks: 1
        min_blocks: 0
        type: LocalProvider

For more custom use-cases where either an unsupported container technology is required
or building the container string programmatically is preferred use
``container_type='custom'``.  In this case, ``container_cmd_options`` is treated as a
string template, in which the following two strings are expected:

* ``{EXECUTOR_RUNDIR}`` : Used to specify mounting of the RUNDIR to share logs
* ``{EXECUTOR_LAUNCH_CMD}`` : Used to specify the worker launch command within the
  container.

Here's an example:

.. code-block:: yaml

   display_name: Docker Custom
   engine:
     type: GlobusComputeEngine
     container_type: custom
     container_cmd_options: docker run -v {EXECUTOR_RUNDIR}:{EXECUTOR_RUNDIR} funcx/kube-endpoint:main-3.10 {EXECUTOR_LAUNCH_CMD}
     provider:
       init_blocks: 1
       max_blocks: 1
       min_blocks: 0
       type: LocalProvider

.. _enable_on_boot:

Starting the Compute Endpoint on Host Boot
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run ``globus-compute-endpoint enable-on-boot`` to install a systemd unit file:

.. code-block:: console

   $ globus-compute-endpoint enable-on-boot my_first_endpointendpoint
   Systemd service installed. Run
      sudo systemctl enable globus-compute-endpoint-my_first_endpoint.service --now
   to enable the service and start the endpoint.

Run ``globus-compute-endpoint disable-on-boot`` for commands to disable and uninstall
the service:

.. code-block:: console

   $ globus-compute-endpoint disable-on-boot my-endpoint
   Run the following to disable on-boot-persistence:
      systemctl stop globus-compute-endpoint-my-endpoint
      systemctl disable globus-compute-endpoint-my-endpoint
      rm /etc/systemd/system/globus-compute-endpoint-my-endpoint.service


AMQP Port
^^^^^^^^^

Endpoints receive tasks and communicate task results via the AMQP messaging protocol.
As of v2.11.0, newly configured endpoints use AMQP over port 443 by default, since
firewall rules usually leave that port open. In case 443 is not open on a particular
cluster, the port to use can be changed in the endpoint config via the ``amqp_port``
option, like so:

.. code-block:: yaml

  amqp_port: 5671
  display_name: My Endpoint
  engine: ...

Note that only ports 5671, 5672, and 443 are supported with the Compute hosted services.
Also note that when ``amqp_port`` is omitted from the config, the port is based on the
connection URL the endpoint receives after registering itself with the services, which
typically means port 5671.

.. |worker_init| replace:: ``worker_init``
.. _worker_init: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider#:~:text=worker_init%20%28str%29,env%E2%80%99

.. |Provider| replace:: ``ExecutionProvider``
.. _Provider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.base.ExecutionProvider.html

.. |nbsp| unicode:: 0xA0
   :trim:
