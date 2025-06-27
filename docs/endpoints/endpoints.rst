Globus Compute Endpoints
************************

A Globus Compute Endpoint is a process launched by the user to serve as a conduit for
executing functions on a computing resource.  The Compute Endpoint process manages the
site‑specific interactions for executing functions, leaving users with only a single API
necessary for running code on their laptop, campus cluster, or even leadership‑class HPC
machines.

In the mental model of Globus Compute, Endpoints are the remote instance:

- Globus Compute SDK |nbsp| --- |nbsp| i.e., the script a researcher writes to submit
  functions to ...

- Globus Compute Web Services |nbsp| --- |nbsp| the Globus‑provided cloud‑services that
  authenticate and ferry functions and tasks

- **Globus Compute Endpoint** |nbsp| --- |nbsp| a site-specific process, typically on a
  cluster head node, that manages user‑accessible compute resources to run tasks
  submitted by the SDK

The Compute Endpoint may be installed via PyPI in the usual manner, as well as via
system packages.  See :doc:`installation` for more information.


Quickstart
==========

For those just looking for the quickstart commands:

.. code-block:: console

   $ python3 -m pipx install globus-compute-endpoint

   $ globus-compute-endpoint configure my_first_endpoint

   $ globus-compute-endpoint start my_first_endpoint

   $ globus-compute-endpoint stop my_first_endpoint


Getting Started
===============

Creating New Compute Endpoints
------------------------------

Create a new endpoint directory and default files in ``$HOME/.globus_compute/`` via the
``configure`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint configure my_first_endpoint
   Created profile for endpoint named <my_first_endpoint>

       Configuration file: /.../.globus_compute/my_first_endpoint/config.yaml

This new Compute Endpoint will also be in the output of the ``list`` subcommand:

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
instance to match the underlying capabilities and restrictions of the resource.  The
default configuration is functional |nbsp| --- |nbsp| it will process tasks |nbsp| ---
|nbsp| but it is intentionally limited to only use processes on the Endpoint host; in
particular, on an HPC host, it will not use any additional computational nodes.  In
its entirety, the default configuration is:

.. code-block:: yaml
   :caption: ``$HOME/.globus_compute/my_first_endpoint/config.yaml``

   display_name: null
   engine:
     max_workers_per_node: 1
     type: GlobusComputeEngine
     provider:
       type: LocalProvider
       init_blocks: 1
       max_blocks: 1
       min_blocks: 0

For now, the key items to observe are the structure (e.g., ``provider`` as a child of
``engine``), the engine type, ``GlobusComputeEngine``, and the provider type,
``LocalProvider``.

The *engine* pulls tasks from the incoming queue and conveys them to the *provider* for
execution.  Globus Compute implements three engines: ``ThreadPoolEngine``,
``ProcessPoolEngine``, and ``GlobusComputeEngine``.  The first two are Compute endpoint
wrappers of Python's |ThreadPoolExecutor|_ and |ProcessPoolExecutor|_.  These engines
are most appropriate for single‑host installations (e.g., a personal workstation).  For
scheduler‑based clusters, |GlobusComputeEngine|_, as a wrapper over Parsl's
|HighThroughputExecutor|_, enables access to multiple computation nodes.

In contrast to the engine, the *provider* speaks to the site's available resources.  For
example, if an endpoint is on the local workstation, the configuration might use the
|LocalProvider|_, but for running jobs on a Slurm cluster, the endpoint would need the
|SlurmProvider|_.  (|LocalProvider|_ and |SlurmProvider|_ are an arbitrary selection for
this discussion; Parsl implements `a number of other providers`_.)

Using the full power of the underlying resources requires site‑specific setup, and can
be tricky to get right.  For instance, configuring the endpoint to submit tasks to a
batch scheduler might require a scheduler account id, awareness of which queues are
accessible for the account id and the job size at hand (that can change!), knowledge of
which network interface cards to use, administrator‑chosen setup steps, and so forth ...
the :doc:`list of example configurations <endpoint_examples>` is a good first resource
as these are known working configurations.

.. |ThreadPoolExecutor| replace:: ``concurrent.futures.ThreadPoolExecutor``
.. _ThreadPoolExecutor: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
.. |ProcessPoolExecutor| replace:: ``concurrent.futures.ProcessPoolExecutor``
.. _ProcessPoolExecutor: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
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

After configuration, start the endpoint instance with the ``start`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint start my_first_endpoint
   Starting endpoint; registered ID: <...registered UUID...>

.. _endpoint-process-tree:

The endpoint instance will first register with the Globus Compute web services, open two
AMQP connections to the Globus Compute AMQP service (one to receive tasks, one to submit
results; :ref:`both over port 443 <compute-endpoint-pre-requisites>`), print the web
service‑provided Endpoint ID to the console, then daemonize.  Though the prompt returns,
the process is still running:

.. code-block:: console

        --- output edited for brevity ---

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
:ref:`HTTPS (port 443) and AMQPS (port 443) <compute-endpoint-pre-requisites>`.

.. note::

   All Compute endpoints run on behalf of a user.  At the Unix level, the processes run
   as a particular username (c.f., ``$USER``, ``uid``), but to connect to the Globus
   Compute web services (and thereafter receive tasks and transmit results), the
   endpoint must be associated with a `Globus Auth identity`_.  The Globus Compute web
   services will validate incoming tasks for this endpoint against this identity.
   Further, once registered, the endpoint instance cannot be run by another Globus Auth
   identity.

.. _Globus Auth identity: https://www.globus.org/platform/services/auth

.. note::

   On the first invocation, the endpoint will emit a long link to the console and ask
   for a Globus Auth code in return.  As part of this step, the Globus Compute web
   services will request access to your Globus Auth identity and Globus Groups.
   (Subsequent runs will not need to perform this login step as the credentials are
   cached.)

The default configuration will fork the endpoint process to the background, returning
control to the shell.  To debug, or for general insight into the status, look in the
endpoint's ``endpoint.log``.  This log is also part of the corpus of information
collected by the ``globus-compute-diagnostic`` utility:

.. code-block:: console

   $ tail ~/.globus_compute/my_first_endpoint/endpoint.log
   ========== Endpoint begins: <THE_ENDPOINT_UUID>
   ... INFO MainProcess-3650227 MainThread-136228654940160 globus_compute_endpoint.endpoint.interchange:95 __init__ Initializing EndpointInterchange process with Endpoint ID: <THE_ENDPOINT_UUID>
   [... snipped for documentation brevity ...]
   ... INFO MainProcess-3650227 Thread-2-136228444812864 globus_compute_endpoint.endpoint.rabbit_mq.result_publisher:135 run ResultPublisher<✗; o:0; t:0> Opening connection to AMQP service.

If all is well, then using the endpoint is just as described in :ref:`Quickstart
<quickstart-run-function>`:

.. code-block:: python
   :caption: ``does_it_work.py``

   from globus_compute_sdk import Executor

   def dot_product(a, b):
       return sum(a_i * b_i for a_i, b_i in zip(a, b))

   inp = ((1, 2, 3), (4, 5, 6))
   with Executor(endpoint_id="<THE_ENDPOINT_UUID>") as ex:
       f = ex.submit(dot_product, *inp)
       print(f"  {'⸳'.join(map(str, inp))} ==> {f.result()}")

.. code-block:: console

   $ python does_it_work.py
     (1, 2, 3)⸳(4, 5, 6) ==> 32

Stopping the Compute Endpoint
-----------------------------

There are a couple of ways to stop the Compute endpoint.  The CLI offers the ``stop``
subcommand:

.. code-block:: console

   $ globus-compute-endpoint stop my_first_endpoint
   > Endpoint <my_first_endpoint> is now stopped

Sometimes, a Unix signal may be more ergonomic for a workflow.  At the process‑level,
the service responds to the Unix signals SIGTERM and SIGQUIT, so if the PID of the
parent process is handy, then either will work:

.. code-block:: console

   $ kill -SIGQUIT <the_cea_pid>    # equivalent to -SIGTERM
   $ kill -SIGTERM <the_cea_pid>    # equivalent to -SIGQUIT


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

.. note::

   The ``list`` subcommand presents the endpoint status in tabular form, but note
   that the table is generated by iterating the subdirectories of
   ``$HOME/.globus_compute/``.


Fine-Tuning Endpoint Setups
===========================

GlobusComputeEngine
-------------------

|GlobusComputeEngine|_ is the execution backend that Globus Compute uses
to execute functions.  To execute functions at scale, Globus Compute can be
configured to use a range of |Providers|_ which allows it to connect to Batch schedulers
like Slurm and PBSTorque to provision compute nodes dynamically in response to workload.
These capabilities are largely borrowed from Parsl's |HighThroughputExecutor|_ and
therefore all of |HighThroughputExecutor|_'s parameter options are supported as
passthrough.

.. note::

   As of ``globus-compute-endpoint==2.12.0``, |GlobusComputeEngine|_ is the default
   engine type.  The ``HighThroughputEngine`` is deprecated.

Here are |GlobusComputeEngine|_ specific features:


Retries
^^^^^^^

Functions submitted to the |GlobusComputeEngine|_ can fail due to infrastructure
failures, for example, the worker executing the task might terminate due to it running
out of memory, or all workers under a batch job could fail due to the batch job
exiting as it reaches the walltime limit. |GlobusComputeEngine|_ can be configured
to automatically retry these tasks by setting ``max_retries_on_system_failure=N``
where N is the number of retries allowed. The endpoint config sets default retries
to 0 since functions can be computationally expensive, not idempotent, or leave
side effects that affect subsequent retries.

Example config snippet:

.. code-block:: yaml

   amqp_port: 443
   display_name: Retry_2_times
   engine:
       type: GlobusComputeEngine
       max_retries_on_system_failure: 2  # Default=0


Auto-Scaling
^^^^^^^^^^^^

|GlobusComputeEngine|_ by default automatically scales workers in response to workload.

``Strategy`` configuration is limited to two options:

#. ``max_idletime``: Maximum duration in seconds that workers are allowed to idle before
   they are marked for termination

#. ``strategy_period``: Set the # of seconds between strategy attempting auto-scaling
   events

The bounds for scaling are determined by the options to the ``Provider``
(``init_blocks``, ``min_blocks``, ``max_blocks``). Please refer to the `Parsl docs
<https://parsl.readthedocs.io/en/stable/userguide/execution.html#elasticity>`_ for more
info.

Here's an example configuration:

.. code-block:: yaml

   engine:
       type: GlobusComputeEngine
       job_status_kwargs:
           max_idletime: 60.0      # Default = 120s
           strategy_period: 120.0  # Default = 5s


Ensuring Execution Environment
------------------------------

When executing a function, endpoint *worker processes* expect to have all dependencies
installed.  For example, if a function requires ``numpy`` and a worker environment does
not have that package installed, attempts to execute that function on that worker will
fail.

The process tree as shown in :ref:`starting the endpoint <endpoint-process-tree>` is the
Compute Endpoint interchange.  This is distinct from the *worker* processes, which are
managed by the Provider.  For example, the ProcessPoolEngine |nbsp| --- |nbsp| with a
(conceptually) built‑in provider |nbsp| --- |nbsp| will create multiple new processes on
the same host as the endpoint itself, whereas the GlobusComputeEngine might start
processes (via the system‑specific batch scheduler) on entirely different hosts.  In
:abbr:`HPC (High Performance Computing)` contexts, the latter is typically the case.

As a result, it is often necessary to load in some kind of pre‑initialized environment
for each worker.  In general there are two approaches:

.. note::

   The worker environment must have the ``globus-compute-endpoint`` Python module
   installed.  We recommend matching the Python version and ``globus-compute-endpoint``
   module version on the worker environment and on the endpoint interchange.

1. Python-Based Environment Isolation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Python‑based environment management uses the |worker_init|_ config option:

.. code-block:: yaml

   engine:
     provider:
       worker_init: |
         conda activate my-conda-env  # or venv, or virtualenv, or ...
         source /some/other/config

Though the exact behavior of ``worker_init`` depends on the specific |Provider|_, this
is typically run in the same process as (or the parent process of) the worker, allowing
environment modification (i.e., environment variables).

In some cases, it may also be helpful to run some setup during the startup process of
the endpoint itself, before any workers start.  This can be achieved using the top‑level
``endpoint_setup`` config option:

.. code-block:: yaml

   endpoint_setup: |
     conda create -n my-conda-env
     conda activate my-conda-env
     pip install -r requirements.txt

.. warning::

   The script specified by ``endpoint_setup`` runs in a shell (usually ``/bin/sh``), as
   a child process, and must finish successfully before the endpoint will continue
   starting up.  In particular, *note that it is not possible to use this hook to set or
   change environment variables for the endpoint*, and is a separate thought‑process
   from ``worker_init`` which *can* set environment variables for the workers.

Similarly, artifacts created by ``endpoint_setup`` may be cleaned up with
``endpoint_teardown``:

.. code-block:: yaml

   endpoint_teardown: |
     conda remove -n my-conda-env --all


.. _containerized-environments:


2. Containerized Environments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. important::
   Container images must include the ``globus-compute-endpoint`` package.

   .. code-block:: dockerfile

      # Example Dockerfile
      FROM python:3.13
      RUN pip install globus-compute-endpoint

.. hint::
   See the :doc:`../tutorials/dynamic_containers` tutorial for instructions on how
   to specify container configuration when submitting tasks.

Container support is limited to the |GlobusComputeEngine|_, and accessible via the
following options:

* ``container_type``
    Specify container type from one of:

    * ``apptainer``
    * ``docker``
    * ``singularity``
    * ``podman``
    * ``podman-hpc``
    * ``custom``
    * ``None``

* ``container_uri``
    Specify container URI, or file path if specifying ``sif`` files

* ``container_cmd_options``
    Specify custom command options to pass to the container launch command, such as
    filesystem mount paths, network options etc.

.. code-block:: yaml
   :caption: Example ``config.yaml``, showing container type, uri, and cmd options to
      run tasks inside a Docker instance.
   :emphasize-lines: 4-6

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

For more custom use‑cases where either an unsupported container technology is required
or building the container string programmatically is preferred use
``container_type: custom``.  In this case, ``container_cmd_options`` is treated as a
string template, with the following two strings replaced:

* ``{EXECUTOR_RUNDIR}``: All occurrences will be replaced with the engine run path
* ``{EXECUTOR_LAUNCH_CMD}``: All occurrences will be replaced with the worker launch
  command within the container.

The Docker YAML example from above could be approached via ``custom`` and the
``container_cmd_options`` as:

.. code-block:: yaml
   :caption: Example ``config.yaml``, showing how to use the custom container type.
   :emphasize-lines: 4-5

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

.. |worker_init| replace:: ``worker_init``
.. _worker_init: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider#:~:text=worker_init%20%28str%29,env%E2%80%99

.. |Provider| replace:: ``ExecutionProvider``
.. _Provider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.base.ExecutionProvider.html


Client Identities
-----------------

The usual workflow involves a human manually starting an endpoint.  After the first‑run
and the ensuing "long‑url" login‑process, the credentials are cached in
``$HOME/.globus_compute/storage.db``, but a human must still manually invoke the `start`
subcommand |nbsp| --- |nbsp| for example, after system maintenance or a reboot.  There
are times, however, where it is neither convenient nor appropriate to run an endpoint
that requires human‑interaction and authentication.  For these cases, start an endpoint
using a client identity by exporting the following two environment variables when
running the endpoint:

* ``GLOBUS_COMPUTE_CLIENT_ID``
* ``GLOBUS_COMPUTE_CLIENT_SECRET``

.. code-block:: console

   $ GLOBUS_COMPUTE_CLIENT_ID=... GLOBUS_COMPUTE_CLIENT_SECRET=... globus-compute-endpoint start ...

      # Alternatively
   $ export GLOBUS_COMPUTE_CLIENT_ID=...
   $ export GLOBUS_COMPUTE_CLIENT_SECRET=...
   $ globus-compute-endpoint start ...

This will authenticate the endpoint with the Compute web‑services as the exported client
identifier |nbsp| --- |nbsp| and means that this endpoint cannot also be registered to
another identity.  (Like what would happen if one forgot to export these variables when
starting the same endpoint at a later date.)

.. note::

   If these environment variables are set, they take precedence over the logged‑in
   identity, making it possible to run both client |nbsp| id- and manually |nbsp|
   authenticated- endpoints from the same host and at the same time (albeit from two
   different terminals).

We explain how to acquire the environment variable values in detail in
:ref:`client credentials with globus compute clients`.


.. _restrict-submission-serialization-methods:

Restricting Submission Serialization Methods
--------------------------------------------

When submitting to an endpoint, users may :ref:`select alternate strategies to
serialize their code and data. <specifying-serde-strategy>` When that happens, the
payload is serialized with the specified strategy in such a way that the executing
worker knows to deserialize it with the same strategy.

There are some cases where an admin might want to limit the strategies that users select
|nbsp| --- |nbsp| :ref:`Python version errors <avoiding-serde-errors>` can be reduced by
using a non-bytecode strategy for data such as :class:`~globus_compute_sdk.serialize.JSONData`,
and there can be security concerns with `deserializing untrusted data via pickle,`_
which is a dependency of the default serialization strategies used by Compute.

The mechanism for restricting serialization strategies is the ``allowed_serializers``
option under the ``engine`` section of the config, which accepts a list of fully-qualified
import paths to :doc:`Globus Compute serialization strategies </reference/serialization_strategies>`:

.. code-block:: yaml

   engine:
      type: GlobusComputeEngine
      allowed_serializers:
         - globus_compute_sdk.serialize.CombinedCode
         - globus_compute_sdk.serialize.JSONData
      ...

With this config set, any time a worker encounters a payload that was not serialized
by one of the allowed strategies, that worker raises an error which is sent back to
the user who submitted that payload:

.. code-block:: python

   from globus_compute_sdk import Executor
   # without any specified serializer, this will use the defaults
   Executor("<restricted serializer endpoint>").submit(<some function>).result()
   # TaskExecutionFailed:
   #  Traceback (most recent call last):
   # ...
   #  globus_compute_sdk.errors.error_types.DeserializationError: Deserialization failed:
   #   Code serializer DillCode disabled by current configuration.
   #   The current configuration requires the *function* to be serialized with one of the allowed Code classes:
   #
   #       Allowed serializers: CombinedCode, JSONData

.. tip:: For an up-to-date list of all available serialization strategies, see
   the :doc:`serialization strategy reference. </reference/serialization_strategies>`

If ``allowed_serializers`` is specified, it must contain at least one ``Code``-based
strategy and one ``Data``-based strategy:

.. code-block:: yaml

   engine:
      allowed_serializers: [globus_compute_sdk.serialize.DillCodeSource]

.. code-block:: console

   $ globus-compute-endpoint start not-enough-allowed-serializers
   Error: 1 validation error for UserEndpointConfigModel
   engine
      Deserialization allowlists must contain at least one code and one data deserializer/wildcard (got: ['globus_compute_sdk.serialize.DillCodeSource']) (type=value_error)

There are additionally two special values that the list accepts to allow all
serializers of a certain type |nbsp| --- |nbsp| ``globus_compute_sdk.*Code`` allows all
Globus-provided Compute Code serializers, and ``globus_compute_sdk.*Data`` allows all
Globus-provided Compute Data serializers. For example, the following config is
functionally equivalent to a config that omits ``allowed_serializers``:

.. code-block:: yaml

   engine:
      allowed_serializers:
         - globus_compute_sdk.*Code
         - globus_compute_sdk.*Data

.. note:: These values are *not* interpreted as globs |nbsp| --- |nbsp| they are
   hard-coded values with special meaning in the Compute serialization system. No other
   glob-style options are supported.


.. _enable_on_boot:

Starting the Compute Endpoint on Host Boot
------------------------------------------

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
---------

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

.. _endpoints_templating_configuration:

Templating Endpoint Configuration
---------------------------------

A common experience for Compute users is a proliferation of their Compute Endpoints.
After starting with a basic configuration, changing or conflicting requirements
necessitate running multiple endpoints |nbsp| --- |nbsp| sometimes simultaneously |nbsp|
--- |nbsp| with different configurations.  For example, attributing work to different
accounts, changing the size of the provisioned compute resource to match the demands of
the problem, or changing the submission queue to the batch‑system.

This becomes a bit of an administrative mess for these users, who must constantly update
their endpoint configurations, bring up and bring down different endpoints, and be aware
of which endpoints have which configuration.

As of May, 2024, Compute Endpoints may now be run as "multi‑user" endpoints.  Please
ignore the name (:ref:`see the note, below <pardon-the-mess>`) and instead think of it
as "template‑able".  This type of 'multi'‑user endpoint specifies a configuration
*template* that will be filled in by SDK‑supplied user‑variables.  This configuration is
then applied to sub‑processes of the multi‑user endpoint.  To disambiguate, we call the
parent process the Multi‑User Endpoint and abbreviate it as MEP, and the child‑processes
*of* the MEP the User Endpoints, or UEPs.

The UEP is exactly the same process and logic as discussed in previous sections.  The
only difference is that the UEP always has a parent MEP process.  Conversely, MEPs *do
not run tasks*.  They have exactly one job: start UEPs based on the passed
configuration.

.. _create-templatable-endpoint:

Create a MEP configuration by passing the ``--multi-user`` flag to the ``configure``
subcommand:

.. code-block:: console

   $ globus-compute-endpoint configure --multi-user my_second_endpoint
   Created multi-user profile for endpoint named <my_second_endpoint>

       Configuration file: /.../.globus_compute/my_second_endpoint/config.yaml

       Example identity mapping configuration: /.../.globus_compute/my_second_endpoint/example_identity_mapping_config.json

       User endpoint configuration template: /.../.globus_compute/my_second_endpoint/user_config_template.yaml.j2
       User endpoint configuration schema: /.../.globus_compute/my_second_endpoint/user_config_schema.json
       User endpoint environment variables: /.../.globus_compute/my_second_endpoint/user_environment.yaml

     Use the `start` subcommand to run it:

       $ globus-compute-endpoint start my_second_endpoint

The default configuration of the MEP, in its entirety, is:

.. code-block:: console

   $ cat /.../.globus_compute/my_second_endpoint/config.yaml
   display_name: null
   identity_mapping_config_path: /.../.globus_compute/my_second_endpoint/example_identity_mapping_config.json
   multi_user: true

Unless this MEP will be run as a privileged user (e.g., ``root``) |nbsp| --- |nbsp| in
which case, please read :doc:`the next section <multi_user>` |nbsp| --- |nbsp| the
Identity Mapping pieces may be removed.  (If left in place, they will be ignored and a
warning message will be emitted to the log.)

.. code-block:: console

   $ rm /.../.globus_compute/my_second_endpoint/example_identity_mapping_config.json
   $ sed -i '/identity_mapping/d' /.../.globus_compute/my_second_endpoint/config.yaml

The template file is ``user_config_template.yaml.j2``.  As implied by the ``.j2``
extension, this file will be processed by `Jinja <https://jinja.palletsprojects.com/>`_
before being used to start a child UEP.  For example, if the MEP might be utilized to
send jobs to different allocations, one might write the template as:

.. code-block:: yaml
   :caption: ``/.../.globus_compute/my_second_endpoint/user_config_template.yaml.j2``

   engine:
     type: GlobusComputeEngine
     provider:
       type: SlurmProvider
       partition: {{ PARTITION }}

       launcher:
           type: SrunLauncher

       account: {{ ACCOUNT_ID }}

   idle_heartbeats_soft: 2
   idle_heartbeats_hard: 4

After starting the MEP, this template will use the specified ``PARTITION`` and
``ACCOUNT_ID`` variables to create the final configuration (i.e., ``config.yaml``) to
start the UEP.  On the SDK-side, this uses the ``user_endpoint_config`` on the Executor:

.. code-block:: python
   :emphasize-lines: 2,5,8,9

   mep_id = "<UUID_FOR_MY_SECOND_ENDPOINT>"
   user_endpoint_config = {"ACCOUNT_ID": "ABCD-1234", "PARTITION": "debug"}
   with Executor(
       endpoint_id=mep_id,
       user_endpoint_config=user_endpoint_config
   ) as ex:
       print(ex.submit(some_task, 1).result())
       user_endpoint_config["ACCOUNT_ID"] = "WXYZ-7890"
       ex.user_endpoint_config = user_endpoint_config
       print(ex.submit(some_task, 2).result())

Both ``.submit()`` calls will send tasks to the *same* endpoint, the one specified by
``mep_id``, but the MEP will spawn two different UEPs, one for each unique
``user_endpoint_config`` sent to the web services.

.. _pardon-the-mess:

.. note::

   Pardon "the mess" while we build the product, but "multi‑user" is perhaps a misnomer
   stemming from the initial development thrust of this feature.  For now, the name and
   flag has stuck, but we will very likely evolve the implementation and thinking here
   to be a more general concept.

Validating Template Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the file ``user_config_schema.json`` exists, then the MEP will validate the
``user_endpoint_config`` input against the JSON schema.  The default schema is quite
permissive, allowing the two defined variables to be strings, and then also allowing any
other user‑specified properties directly:

.. code-block:: json

   {
     "$schema": "https://json-schema.org/draft/2020-12/schema",
     "type": "object",
     "properties": {
       "endpoint_setup": { "type": "string" },
       "worker_init": { "type": "string" }
     },
     "additionalProperties": true
   }

Configuring a JSON schema is out of scope for this documentation, but this tool is
available to restrict what the MEP will accept for interpolation.  If the only person
using this endpoint is you, then this schema might be considered overkill.  On the other
hand, using it properly can help ferret out typos and thinkos, so one item to call out
specifically is ``additionalProperties: true``, which is what allows
non‑specified (i.e., "arbitrary") properties.  Please consult the `JSON Schema
documentation <https://json-schema.org/>`_ for more information.

.. |nbsp| unicode:: 0xA0
   :trim:

.. |Providers| replace:: ``Providers``
.. _Providers: https://parsl.readthedocs.io/en/stable/reference.html#providers

.. _deserializing untrusted data via pickle,: https://github.com/swisskyrepo/PayloadsAllTheThings/blob/4.1/Insecure%20Deserialization/Python.md
