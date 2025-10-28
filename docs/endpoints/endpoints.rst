Endpoint User Guide
*******************

This guide will walk you through most of the features of Globus Compute
endpoints.

The Compute Endpoint may be installed via PyPI in the usual manner, as well as
via system packages.  See :doc:`installation` for more information.


Overview
========

A Globus Compute Endpoint serves as a conduit for executing functions on
computing resources.  The Compute Endpoint manages site‑specific interactions
for function execution, providing users with a unified API for running code
across diverse environments -- from laptops to campus clusters to
leadership‑class HPC systems.

A Globus Compute Endpoint consists of three main process types:

- **Manager endpoint process (MEP)** - Renders a configuration template to
  launch and manage user endpoint processes.

- **User endpoint process (UEP)** - Establishes secure communication with Globus
  Compute web services to receive user-submitted tasks, launch worker processes,
  and publish results.

- **Worker process** - Executes submitted tasks on the target computing
  resources (e.g., HPC cluster nodes) according to the rendered configuration
  template.

For a detailed look at how a task makes its way to a user endpoint process, see
:ref:`tracing-a-task`.


Quickstart
==========

For those just looking for the quickstart commands:

.. code-block:: console

   $ python3 -m pipx install globus-compute-endpoint

   $ globus-compute-endpoint configure my_first_endpoint

   $ globus-compute-endpoint start my_first_endpoint

   $ globus-compute-endpoint stop my_first_endpoint


Configuring an Endpoint
=======================

Create a new endpoint directory and default files in ``$HOME/.globus_compute/``
via the ``configure`` subcommand:

.. code-block:: console

   $ globus-compute-endpoint configure my_endpoint
   Created multi-user profile for endpoint named <my_endpoint>

       Configuration file: /home/user/.globus_compute/my_endpoint/config.yaml

       User endpoint configuration template: /home/user/.globus_compute/my_endpoint/user_config_template.yaml.j2
       User endpoint configuration schema: /home/user/.globus_compute/my_endpoint/user_config_schema.json
       User endpoint environment variables: /home/user/.globus_compute/my_endpoint/user_environment.yaml

   Use the `start` subcommand to run it:

   globus-compute-endpoint start my_endpoint

.. hint::

   If run as a privileged user (e.g., root), the ``configure`` subcommand will
   generate an additional :ref:`example-idmap-config` file.  See
   :ref:`multi-user-configuration` for more information.


``config.yaml``
---------------

The ``config.yaml`` file controls the manager endpoint process.  Please refer
to :ref:`endpoint-manager-config` for details on each field.

.. code-block:: yaml
   :caption: Default ``config.yaml`` configuration

   amqp_port: 443
   display_name: null

.. note::
   ``display_name`` is an optional field that determines how the endpoint will
   appear in the `Web UI`_.


.. _user-config-template-yaml-j2:

``user_config_template.yaml.j2``
--------------------------------

The manager endpoint process will render this template with user defined
variables to launch a user endpoint process.  More than simple interpolation,
the endpoint treats this file as a `Jinja template`_, enabling a good bit of
flexibility.

The template file lives in the manager endpoint directory by default, but users
can specify a different template path using the ``user_config_template_path``
setting in the manager's :ref:`config.yaml <endpoint-manager-config>`.

The default template includes a basic single-worker configuration and defines
two variables: ``endpoint_setup`` and ``worker_init``.  Both variables default
to empty strings when not specified by the user, as indicated by the
``...|default()`` filter.

.. code-block:: yaml+jinja
   :caption: Default ``user_config_template.yaml.j2``

   endpoint_setup: {{ endpoint_setup|default() }}

   engine:
      type: GlobusComputeEngine
      max_workers_per_node: 1

      provider:
         type: LocalProvider
         min_blocks: 0
         max_blocks: 1
         init_blocks: 1
         worker_init: {{ worker_init|default() }}

   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760

The default configuration is functional |nbsp| --- |nbsp| it will process tasks
|nbsp| --- |nbsp| but the `LocalProvider`_ will only use processes on the
endpoint host.  Thus, on an HPC head node, the endpoint will not use any
additional computational nodes.

Our docs showcase many :doc:`example configurations <endpoint_examples>`, but
please refer to :ref:`uep-conf` for an in-depth explanation of configuration
features and :doc:`templates` for details on template capabilities and
peculiarities.


.. _user-config-schema-json:

``user_config_schema.json``
---------------------------

Admins can define a `JSON schema <https://json-schema.org/>`_ to validate
user-defined variables.  The schema file lives in the manager endpoint directory
by default, but users can specify a different schema path using the
``user_config_schema_path`` variable in the manager's :ref:`config.yaml
<endpoint-manager-config>`.

The default schema is quite permissive, enforcing that the two default template
variables are strings, then allowing any other user-defined properties:

.. code-block:: json
   :caption: Default ``user_config_schema.json``

   {
     "$schema": "https://json-schema.org/draft/2020-12/schema",
     "type": "object",
     "properties": {
       "endpoint_setup": { "type": "string" },
       "worker_init": { "type": "string" }
     },
     "additionalProperties": true
   }

.. important::

   The default schema sets ``additionalProperties`` to ``true``, allowing
   properties not explicitly defined in the schema.  This enables the default
   template to work without customization.

   Setting ``additionalProperties`` to ``false`` will reject unexpected
   properties and enable more transparent failures.


.. _user-environment-yaml:

``user_environment.yaml``
-------------------------

Use this file to specify site-specific environment variables to export to the
user endpoint process.  Though this is a YAML file, it is interpreted internally
as a simple top-level-only set of key-value pairs.  Nesting of data structures
will probably not behave as expected.  Example:

.. code-block:: yaml

   COMPILER_PATH: /opt/compiler/3.2.9/bin
   LD_LIBRARY_PATH: /opt/compiler/lib:/opt/cray/lib64:/opt/nvidia/Linux_x86_64/nccl/lib
   LD_PRELOAD: /opt/lib64/libsite.so

These will be injected into the user endpoint process as environment variables.


.. _starting-the-endpoint:

Starting the Endpoint
=====================

After configuration, start the endpoint instance with the ``start`` subcommand.
Once started, the endpoint stays attached to the console, with a timer that
updates every second as a hint that the manager endpoint process is running:

.. code-block:: console

   $ globus-compute-endpoint start my_endpoint
         >>> Endpoint ID: [endpoint_uuid] <<<
   ----> Wed Aug  6 20:03:02 2025

On the first invocation, the endpoint will emit a link to the console and ask
for a temporary code in return.  As part of this step, the Globus Compute web
services will request access to your `Globus Auth`_ identity and
`Globus Groups`_.  Your Globus Auth identity will register as the endpoint owner
and cannot be changed.

If you start the endpoint as a non-privileged local user, then the manager and
user endpoint processes will run as the same local user, and the endpoint will
only accept tasks submitted by the endpoint owner.  We refer to this as a
single-user endpoint:

.. code-block:: text
   :caption: Single-user endpoint process hierarchy

   Manager Endpoint Process (alice, UID: 1001)
   └── User Endpoint Process (alice, UID: 1001)

.. hint::

   If you start the endpoint as a privileged user (e.g., root), then the
   endpoint will require additional configuration to enable mapping Globus Auth
   identities to local user accounts.  Please refer to :doc:`multi_user` for
   more information.


Stopping the Endpoint
=====================

To stop the endpoint from the same terminal process, type ``Ctrl+\`` (SIGQUIT)
or ``Ctrl+C`` (SIGINT).

To stop the endpoint from a different process, the CLI offers the ``stop``
subcommand:

.. code-block:: console

   $ globus-compute-endpoint stop my_endpoint
   > Endpoint <my_endpoint> is now stopped

Alternatively, if the PID of the manager endpoint process is handy, then either
will work:

.. code-block:: console

   $ kill -SIGQUIT <manager_pid>    # equivalent to -SIGTERM
   $ kill -SIGTERM <manager_pid>    # equivalent to -SIGQUIT


Basic User Workflow
===================

After starting the endpoint, users can specify values for the template variables
when submitting a task:

.. code-block:: python
   :emphasize-lines: 8,11

   from globus_compute_sdk import Executor

   def add(a, b):
      return a + b

   with Executor(
       endpoint_id="...",
       user_endpoint_config={"worker_init": "source /path/to/venv1/bin/activate"}
   ) as ex:
       print(ex.submit(add, 30, 12).result())
       ex.user_endpoint_config["worker_init"] = "source /path/to/venv2/bin/activate"
       print(ex.submit(add, 21, 21).result())

.. note::
   This example code highlights the ``user_endpoint_config`` attribute of the
   ``Executor`` class; please consult the :doc:`../sdk/executor_user_guide`
   documentation for more information.

The manager endpoint process renders the configuration template with the
user-defined variables, then launches a user endpoint process.  When these
values change, a new user endpoint process is launched and can run concurrently
with existing processes.


Listing Endpoints
=================

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
* **Stopped**: The endpoint was stopped by the user.  It is not running and
  therefore, cannot service any functions.  It can be started again without
  issues.
* **Disconnected**: The endpoint disconnected unexpectedly.  It is not running
  and therefore cannot service any functions.  Starting this endpoint will first
  invoke necessary endpoint cleanup, since it was not stopped correctly
  previously.

.. note::

   The ``list`` subcommand presents the endpoint status in tabular form, but
   note that the table is generated by iterating the subdirectories of
   ``$HOME/.globus_compute/``.


Ensuring Execution Environment
==============================

Endpoint *worker processes*, which perform the actual function execution, expect
to have all dependencies installed.  For example, if a function requires
``numpy`` and a worker environment does not have that package installed, then
attempts to execute that function on that worker will fail.

As a result, it is often necessary to load in some kind of pre‑initialized
environment for each worker.  In general, there are two approaches:

.. important::

   The worker environment must have the ``globus-compute-endpoint`` Python
   module installed.  We recommend matching the Python version and
   ``globus-compute-endpoint`` module version on the worker environment and on
   the endpoint interchange.


Python-Based Environments
-------------------------

Python‑based environment management uses the |worker_init|_ config option:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
     provider:
       worker_init: |
         conda activate my-conda-env  # or venv, or virtualenv, or ...
         source /some/other/config

.. hint::
   See the :doc:`../tutorials/dynamic_python_environments` tutorial for instructions on how
   to dynamically change the worker's environment based on the Python version of the submitting
   user.

Though the exact behavior of ``worker_init`` depends on the specific
|Provider|_, this is run in the same process as the worker, allowing environment
modification (i.e., environment variables).

In some cases, it may also be helpful to run some setup while starting the user
endpoint process, before any workers start.  This can be achieved using the
top‑level ``endpoint_setup`` config option:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   endpoint_setup: |
     conda create -n my-conda-env
     conda activate my-conda-env
     pip install -r requirements.txt

.. warning::

   The script specified by ``endpoint_setup`` runs in a shell (usually
   ``/bin/sh``), as a child process, and must finish successfully before the
   user endpoint  process will continue starting up.  In particular, *note that
   it is not possible to use this hook to set or change environment variables
   for the endpoint*, and is a separate thought process from ``worker_init``,
   which *can* set environment variables for the workers.

Similarly, artifacts created by ``endpoint_setup`` may be cleaned up with
``endpoint_teardown``:

.. code-block:: yaml

   endpoint_teardown: |
     conda remove -n my-conda-env --all


.. _containerized-environments:

Containerized Environments
--------------------------

.. important::
   Container images must include the ``globus-compute-endpoint`` package.

   .. code-block:: dockerfile

      # Example Dockerfile
      FROM python:3.13
      RUN pip install globus-compute-endpoint

Container support is limited to the |GlobusComputeEngine|_, and accessible via
the following options:

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
    Specify custom command options to pass to the container launch command, such
    as filesystem mount paths, network options etc.

.. code-block:: yaml
   :caption: Example ``user_config_template.yaml.j2`` showing container type,
     uri, and cmd options to run tasks inside a Docker instance.
   :emphasize-lines: 4-6

   display_name: Docker
   engine:
     type: GlobusComputeEngine
     container_type: docker
     container_uri: compute-worker:4.0.0
     container_cmd_options: -v /tmp:/tmp

.. hint::

   See the :doc:`../tutorials/dynamic_containers` tutorial for instructions on
   how to specify container configuration when submitting tasks.

For custom use cases requiring unsupported container technologies or
programmatic container string construction, set ``container_type: custom``.
With this configuration, ``container_cmd_options`` functions as a string
template where the following placeholders are replaced:

* ``{EXECUTOR_RUNDIR}``: All occurrences are replaced with the engine run path
* ``{EXECUTOR_LAUNCH_CMD}``: All occurrences are replaced with the worker launch
  command within the container.

The Docker YAML example from above could be approached via ``custom`` and the
``container_cmd_options`` as:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 4-5

   display_name: Docker Custom
   engine:
     type: GlobusComputeEngine
     container_type: custom
     container_cmd_options: docker run -v {EXECUTOR_RUNDIR}:{EXECUTOR_RUNDIR} compute-worker:4.0.0 {EXECUTOR_LAUNCH_CMD}

.. |worker_init| replace:: ``worker_init``
.. _worker_init: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider#:~:text=worker_init%20%28str%29,env%E2%80%99

.. |Provider| replace:: ``ExecutionProvider``
.. _Provider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.base.ExecutionProvider.html


Advanced Environment Customization
==================================

.. note::
   This section is only relevant for :ref:`repository-based installations
   <repo-based-installation>`.

There are some instances where static configuration is not enough.  For example,
setting a user-specific environment variable or running arbitrary scripts prior
to handing control over to the user endpoint process.  For these cases, observe
that ``/usr/sbin/globus-compute-endpoint`` is actually a shell script wrapper:

.. code-block:: shell

   #!/bin/sh

   VENV_DIR="/opt/globus-compute-agent/venv-py39"

   if type deactivate 1> /dev/null 2> /dev/null; then
   deactivate
   fi

   . "$VENV_DIR"/bin/activate

   exec "$VENV_DIR"/bin/globus-compute-endpoint "$@"

While we don't suggest modifying this wrapper (for ease of future maintenance),
one might inject another wrapper into the process, by modifying the process PATH
and writing a custom ``globus-compute-endpoint`` wrapper:

.. code-block:: yaml
   :caption: ``user_environment.yaml``

   PATH: /usr/local/admin_scripts/

.. code-block:: sh
   :caption: ``/usr/local/admin_scripts/globus-compute-endpoint``

   #!/bin/sh

   /some/other/executable
   . import/some/vars/script

   # remove the `/usr/local/admin_scripts` entry from the PATH
   export PATH=/usr/local/bin:/usr/bin:/REST/OF/PATH

   exec /usr/sbin/globus-compute-endpoint "$@"

(The use of ``exec`` is not critical, but keeps the process tree tidy.)


Debugging
=========

If actively debugging or iterating, the two command line arguments
``--log-to-console`` and ``--debug`` may be helpful as they increase the
verbosity and color of the text to the console.  Meanwhile, the log is always
available at ``.globus_compute/my_endpoint/endpoint.log`` and is the first place
to look upon an unexpected behavior.  In a healthy endpoint setup, there will be
many lines about processes starting and stopping:

.. code-block:: text

   [...] Creating new user endpoint (pid: 3867325) [(harper, uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a) globus-compute-endpoint start uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a --die-with-parent]
   [...] Command process successfully forked for 'harper' (Globus effective identity: b072d17b-08fd-4ada-8949-1fddca189b5e).
   [...] Command stopped normally (3867325) [(harper, uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a) globus-compute-endpoint start uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a --die-with-parent]


Debugging User Endpoint Processes
---------------------------------

The ``--debug`` flag will not carry-over to the child user endpoint processes.
In particular, the command executed by the manager endpoint process is:

.. code-block:: python
   :caption: arguments to ``os.execvpe``

   proc_args = ["globus-compute-endpoint", "start", ep_name, "--die-with-parent"]

Note the lack of the ``--debug`` flag; by default, user endpoint processes will
not emit DEBUG level logs.  To place them into debug mode, use the ``debug``
top-level configuration directive:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 1

   debug: true
   display_name: Debugging template
   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760
   engine:
      ...

Note that this is *also* how to get the user endpoint process to emit its
configuration to the log, which may be helpful in determining which set of logs
are associated with which configuration or just generally while implementing and
debugging.  The configuration is written to the logs before the user endpoint
process boots; look for the following sentinel lines::

   [TIMESTAMP] DEBUG ... Begin Compute endpoint configuration (5 lines):
      ...
   End Compute endpoint configuration

To this end, the authors have found the following command line helpful for
pulling out the configuration from the logs:

.. code-block:: console

   $ sed -n "/Begin Compute/,/End Compute/p" ~/.globus_compute/uep.[...]/endpoint.log | less


Client Identities
=================

The usual workflow involves a human manually starting an endpoint.  After the
first‑run and the ensuing "long‑url" login‑process, the credentials are cached
in ``$HOME/.globus_compute/storage.db``, but a human must still manually invoke
the ``start`` subcommand |nbsp| --- |nbsp| for example, after system maintenance
or a reboot.  There are times, however, where it is neither convenient nor
appropriate to run an endpoint that requires human‑interaction and
authentication.  For these cases, start an endpoint using a client identity by
exporting the following two environment variables when running the endpoint:

* ``GLOBUS_COMPUTE_CLIENT_ID``
* ``GLOBUS_COMPUTE_CLIENT_SECRET``

.. code-block:: console

   $ GLOBUS_COMPUTE_CLIENT_ID=... GLOBUS_COMPUTE_CLIENT_SECRET=... globus-compute-endpoint start ...

      # Alternatively
   $ export GLOBUS_COMPUTE_CLIENT_ID=...
   $ export GLOBUS_COMPUTE_CLIENT_SECRET=...
   $ globus-compute-endpoint start ...

This will authenticate the endpoint with the Compute web‑services as the
exported client identifier |nbsp| --- |nbsp| and means that this endpoint cannot
also be registered to another identity.  (Like what would happen if one forgot
to export these variables when starting the same endpoint at a later date.)

.. note::

   If these environment variables are set, they take precedence over the
   logged‑in identity, making it possible to run both client |nbsp| id- and
   manually |nbsp| authenticated- endpoints from the same host and at the same
   time (albeit from two different terminals).

We explain how to acquire the environment variable values in detail in
:ref:`client credentials with globus compute clients`.


.. _restrict-submission-serialization-methods:

Restricting Submission Serialization Methods
============================================

When submitting to an endpoint, users may :ref:`select alternate strategies to
serialize their code and data<specifying-serde-strategy>`.  When that happens,
the payload is serialized with the specified strategy in such a way that the
executing worker knows to deserialize it with the same strategy.

There are some cases where an admin might want to limit the strategies that
users select |nbsp| --- |nbsp|
:ref:`Python version errors <avoiding-serde-errors>` can be reduced by using a
non-bytecode strategy for data such as
:class:`~globus_compute_sdk.serialize.JSONData`, and there can be security
concerns with `deserializing untrusted data via pickle,`_ which is a dependency
of the default serialization strategies used by Compute.

The mechanism for restricting serialization strategies is the
``allowed_serializers`` option under the ``engine`` section of the config
template, which accepts a list of fully-qualified import paths to
:doc:`Globus Compute serialization strategies </reference/serialization_strategies>`:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
      type: GlobusComputeEngine
      allowed_serializers:
         - globus_compute_sdk.serialize.PureSourceTextInspect
         - globus_compute_sdk.serialize.JSONData
      ...

With this config set, any time a worker encounters a payload that was not
serialized by one of the allowed strategies, that worker raises an error which
is sent back to the user who submitted that payload:

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
   #       Allowed serializers: PureSourceTextInspect, JSONData

.. tip::

   For an up-to-date list of all available serialization strategies, see
   the :doc:`serialization strategy reference
   </reference/serialization_strategies>`.

If ``allowed_serializers`` is specified, it must contain at least one
``Code``-based strategy and one ``Data``-based strategy:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
      allowed_serializers: [globus_compute_sdk.serialize.DillCodeSource]

.. code-block:: console

   $ globus-compute-endpoint start not-enough-allowed-serializers
   Error: 1 validation error for UserEndpointConfigModel
   engine
      Deserialization allowlists must contain at least one code and one data deserializer/wildcard (got: ['globus_compute_sdk.serialize.DillCodeSource']) (type=value_error)

There are additionally two special values that the list accepts to allow all
serializers of a certain type |nbsp| --- |nbsp| ``globus_compute_sdk.*Code``
allows all Globus-provided Compute Code serializers, and
``globus_compute_sdk.*Data`` allows all Globus-provided Compute Data
serializers.  For example, the following config is functionally equivalent to a
config that omits ``allowed_serializers``:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
      allowed_serializers:
         - globus_compute_sdk.*Code
         - globus_compute_sdk.*Data

.. note::

   These values are *not* interpreted as globs |nbsp| --- |nbsp| they are
   hard-coded values with special meaning in the Compute serialization system.
   No other glob-style options are supported.


.. _enable_on_boot:

Installing as a Service
=======================

Run ``globus-compute-endpoint enable-on-boot`` to install a systemd unit file:

.. code-block:: console

   $ globus-compute-endpoint enable-on-boot my_endpoint
   Systemd service installed. Run
      sudo systemctl enable globus-compute-endpoint-my_endpoint.service --now
   to enable the service and start the endpoint.

Run ``globus-compute-endpoint disable-on-boot`` for commands to disable and
uninstall the service:

.. code-block:: console

   $ globus-compute-endpoint disable-on-boot my-endpoint
   Run the following to disable on-boot-persistence:
      systemctl stop globus-compute-endpoint-my-endpoint
      systemctl disable globus-compute-endpoint-my-endpoint
      rm /etc/systemd/system/globus-compute-endpoint-my-endpoint.service


.. _auth-policies:

Authentication Policies
=======================

Administrators can use a `Globus authentication policy`_ to limit access to an
endpoint and enable HA functionality.  Access control is only relevant to
:doc:`multi-user endpoints <multi_user>`, and HA features require an active HA
subscription.

Authentication policies are centrally managed within the Globus Auth service and
can be shared across multiple endpoints.  For detailed information on policy
configuration and available fields, see the
`Authentication Policies documentation`_.


Create a New Authentication Policy
----------------------------------

Administrators can create new authentication policies via the `Globus Auth API
<https://docs.globus.org/api/auth/reference/#create_policy>`_, or via the
following ``configure`` subcommand options:

.. note::

   The resulting policy will be automatically applied to the endpoint's
   ``config.yaml``.

``--auth-policy-project-id``
  The id of a Globus Auth project that this policy will belong to.  If not
  provided, the user will be prompted to create one.

``--auth-policy-display-name``
  A user friendly name for the policy.

``--allowed-domains``
  A comma separated list of domains that can satisfy the policy.  These may
  include wildcards.  For example, ``*.edu, globus.org``.  For more details, see
  ``domain_constraints_include`` in the
  `Authentication Policies documentation`_.

``--excluded-domains``
  A comma separated list of domains that will fail the policy.  These may
  include wildcards.  For example, ``*.edu, globus.org``.  For more details, see
  ``domain_constraints_exclude`` in the
  `Authentication Policies documentation`_.

``--auth-timeout``
  The maximum amount of time in seconds that a previous authentication must have
  occurred to satisfy the policy.  Setting this will also set ``high_assurance``
  to ``true``.

  .. attention::

     For performance reasons, the web-service caches lookups for 60s.
     Pragmatically, this means that smallest timeout that Compute supports is 1
     minute, even though it is possible to set required authorizations for high
     assurance policies to smaller time intervals.


Apply an Existing Authentication Policy
---------------------------------------

Administrators can apply an authentication policy directly in the endpoint's
``config.yaml``:

.. code-block:: yaml
   :caption: ``config.yaml``

   authentication_policy: 2340174a-1a0e-46d8-a958-7c3ddf2c834a

... or via the ``--auth-policy`` option with the ``configure`` subcommand, which
will make the necessary changes to ``config.yaml``:

.. code-block:: bash

   $ globus-compute-endpoint configure my_endpoint --auth-policy 2340174a-1a0e-46d8-a958-7c3ddf2c834a


.. _high-assurance:

High-Assurance
--------------

Globus Compute endpoints may be designated as High-Assurance (HA) to meet
stricter security, compliance, and operational requirements.  HA endpoints
differ from non-HA endpoints in a few key ways:

- in addition to running regular functions, HA endpoints may also run HA
  functions (described below).

- HA endpoints enable audit-logging, whereby the states of all tasks (whether HA
  or not) are logged to a file.

Consider deploying a High-Assurance endpoint where there is need for stronger
identity verification and authentication controls, and for workloads that
require elevated assurance levels, such as sensitive research or regulated data
processing.

.. note::

   Once an endpoint has registered, the High-Assurance setting may not be
   toggled.  An endpoint may not become HA at a later date if it is not
   initially registered as such.  The reverse ("downgrading" from HA) is
   similarly disallowed.


HA Configuration
^^^^^^^^^^^^^^^^

High-Assurance functionality requires a HA-enabled `Globus subscription`_, to be
explicitly marked as HA, and to be associated with a HA
`Globus authentication policy`_.  In configuration form, that translates to
``subscription_id``, ``high_assurance``, and ``authentication_policy``:

.. code-block:: yaml
   :caption: Example ``config.yaml`` of a HA endpoint

   ...
   subscription_id: 2c94f030-d346-11e9-939f-02ff96a5aa76
   high_assurance: true
   authentication_policy: 8e6529c8-a7ce-4310-b895-244e1b33702a
   ...

In this example, the ``subscription_id`` is associated with a HA-enabled
subscription, and the ``authentication_policy`` has similarly been setup as HA.
Both of these values may be found in the Globus App `Web UI`_.  Find
subscriptions available to you via **Settings** |rarr| **Subscriptions**.

Policies are associated with projects, so first navigate to **Settings**
|rarr| **Developers** and select or create a project.  Within a project,
navigate to the **Policies** tab.  If creating a policy, be sure to check the
"High Assurance" checkbox.

Alternatively, a new HA policy may be created at configuration time with an
overly-specified command line:

.. code-block:: console
   :linenos:

   $ globus-compute-endpoint configure \
       --multi-user \
       --display-name "Example High-Assurance Compute Endpoint" \
       --high-assurance \
       --subscription-id 00000000-1111-...ffff \
       --auth-policy-project-id 11111111-2222-...5555 \
       --allowed-domains "example.edu,*.example.edu" \
       --auth-timeout 1800 \
      example_ha_compute_endpoint

This example command line will

- (Line 4) *create* a new HA (``--high-assurance``) policy ...
- (Line 5) associated with the ``0000...`` subscription ...
- (Line 6) under the ``1111...`` project ...
- (Line 7) that will require users be from the ``example.edu`` domain or any
  subdomain ...
- (Line 8) and that they authenticate every ``1800`` seconds.

This example does not show all options; use ``configure --help`` to see what is
available:

.. code-block:: console

   $ globus-compute-endpoint configure --help
   Usage: globus-compute-endpoint configure [OPT...


High-Assurance Functions
^^^^^^^^^^^^^^^^^^^^^^^^

A High-Assurance function is one that has been registered with the Globus
Compute API as associated with a HA endpoint.  In other words, a HA function
requires exactly one HA endpoint.  A HA function may not be run on any other
endpoint and will be removed from all Globus-operated storage after 3 months of
inactivity.

To register a High-Assurance function, specify the ``ha_endpoint_id`` argument
to ``register_function()``:

.. code-block:: python

   from globus_compute_sdk import Client, Executor

   def ha_func(pii: str):
      from datetime import datetime
      return f"ha_func: {datetime.now()} - processed pii: {pii}"

   gc = Client()
   ex = Executor()  # for illustrative purposes; strongly consider `with Executor() as ex:` style

   # Registration works the same, whether via the Client or the Executor
   ha_func_id_via_client = gc.register_function(ha_func, ha_endpoint_id='...')
   ha_func_id_via_ex = ex.register_function(ha_func, ha_endpoint_id='...')

   # Now save one of the registered ids (from this example, they point to the same
   # function logic) for later use with `.submit_to_registered_function()`
   print(ha_func_id_via_ex)

Further, SDK usage of a HA function will require the SDK interaction to follow
the HA requirements of the associated policy.  For example, a long running HA
task (a task that uses an HA function) might require the SDK to login again
before downloading the associated result:

.. code-block:: python
   :emphasize-lines: 6,15
   :linenos:

   >>> from globus_compute_sdk import Executor
   >>> ha_endpoint_id = '...'
   >>> ha_function_id = '...'
   >>> with Executor(endpoint_id=ha_endpoint_id) as ex:
   ...     fut = ex.submit_to_registered_function(ha_function_id, "ha-worthy-argument-to-function")
   ...     print("\nResult:", fut.result())

   Please authenticate with Globus here:
   -------------------------------------
   https://auth.globus.org/v2/oauth2/authorize?...
   -------------------------------------

   Enter the resulting Authorization Code here: ...

   It is your responsibility to ensure disclosure of regulated data, such as Protected Health Information, resulting from the submission of this function and its arguments is legally authorized.

   ha_func: 2025-04-29 13:39:57.888666 - processed pii: ha-worthy-argument-to-function

On line 6, the script will wait until it receives notification from the AMQP
server that the task result is ready.  Crucially, as the function is designated
HA, the service does **not** send the result directly to the ``Executor``
instance, but instead simply sends notification that the task has completed.  To
retrieve the result, the ``Executor`` will make a request to the Globus Compute
API which will verify, at the time of retrieval, that all HA requirements are
met.  If they are not, then the ``Executor`` will initiate the required login
flow.

Line 15 shows the standard disclaimer when working with an HA function.


Audit Logging
^^^^^^^^^^^^^

Audit logging is available only to High-Assurance endpoints, and is enabled by
the ``audit_log_path`` |ManagerEndpointConfig| item:

.. code-block:: yaml
   :caption: Example ``config.yaml`` showing the ``audit_log_path``
     configuration key

   high_assurance: true
   audit_log_path: /.../audit.log

If this file does not exist, then it will be created with user-secure
permissions (``umask=0o077``) when the endpoint starts.  It will not be checked
thereafter, so it is incumbent on the administrator to ensure the file remains
appropriately secured.

When enabled, task events, if available, will be emitted one record per line:

.. code-block:: text
   :caption: Example ``audit.log`` content; the ``...`` fields are omitted for
      documentation clarity

   2025-04-10T14:44:58.742079-05:00 uid=0 pid=... eid=... Begin MEP session =====
   ...
   2025-04-10T14:45:30.906170-05:00 uid=... pid=... uep=... fid=... tid=... bid= RECEIVED
   2025-04-10T14:45:30.906661-05:00 uid=... pid=... uep=... fid=... tid=... bid= EXEC_START
   2025-04-10T14:45:37.401833-05:00 uid=... pid=... uep=... fid=... tid=... bid=4 jid=2477 RUNNING
   2025-04-10T14:45:37.969570-05:00 uid=... pid=... uep=... fid=... tid=... bid=4 jid=2477 EXEC_END
   ...
   2025-04-10T14:46:39.689716-05:00 uid=0 pid=... eid=... End MEP session -----

Each session begins when the MEP starts, and ends when the MEP stops (denoted
with the sentinel lines ``Begin MEP session =====`` and
``End MEP session -----``).  Each record contains for the emitting process:

- a local-timezone timestamp in ISO 8601 format
- ``uid`` -- the POSIX user id
- ``pid`` -- the POSIX process id
- ``uep`` -- the internal UEP identifier, a UUID
- ``fid`` -- the function identifier registered with the Compute service, a UUID
- ``tid`` -- the task identifier registered with the Compute service, a UUID
- ``bid`` -- the block identifier, if available, where the task was scheduled
- ``jid`` -- the scheduler job identifier, if available
- the task state (one of ``RECEIVED``, ``EXEC_START``, ``RUNNING``,
  ``EXEC_END``)

The four task states describe where the task was in the execution process when
the audit record was emitted:

- ``RECEIVED`` -- denotes that the endpoint has received the task from the AMQP
  service

- ``EXEC_START`` -- emitted when the engine has handed the task to the internal
  executor

- ``RUNNING`` -- emitted if the engine shares this event; notably, the
  GlobusComputeEngine does while the :ref:`ProcessPoolEngine and
  ThreadPoolEngines <uep-conf>` do not.

- ``EXEC_END`` -- emitted when the task has completed, just prior to sending the
  result to the Compute AMQP service


Additional High-Assurance Resources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Globus Subscriptions:

  https://www.globus.org/subscriptions

- High Assurance Security Overview:

  https://docs.globus.org/guides/overviews/security/high-assurance-overview/


.. _function-allowlist:

Function Allow Listing
======================

To require that user endpoint processes only allow certain functions, specify
the ``allowed_functions`` top-level configuration item in ``config.yaml``:

.. code-block:: yaml
   :caption: ``config.yaml``

   allowed_functions:
      - 6d0ba55f-de15-4af2-827d-05c50c338aa7
      - e552e7f2-c007-4671-8ca4-3a4fd84f3805

At registration, the web service will be apprised of these function identifiers,
and only tasks that invoke these functions will be sent to the user endpoint
processes.  Any submission that specifies non-approved function identifiers will
be rebuffed with HTTP 403 response like:

.. code-block:: text
   :caption: *Example HTTP invalid function error response via the SDK; edited for clarity*

   403
   FUNCTION_NOT_PERMITTED
   Function <function_id> not permitted on endpoint <endpoint_id>

Additionally, user endpoint processes will verify that tasks only use functions
from the allow list.  Given the guarantees of the API, this is a redundant
verification, but is performed locally as a precautionary measure.

There are some instances where an administrator may want to restrict different
users to different functions.  In this scenario, the administrator must specify
the restricted functions within the :ref:`Jinja template logic for the user
endpoint configuration <user-config-template-yaml-j2>`, and *specifically not
specify any restrictions* in the parent ``config.yaml`` file.  In this setup,
the web-service will not verify task-requested functions as this check will be
done locally by the user endpoint process.  An example configuration template
snippet might be:

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``

   engine:
      ...
   allowed_functions:
   {% if '3.13' in user_runtime.python_version %}
     - c01ebede-06f5-4647-9712-e5649d0f573a
     - 701fc11a-69b5-4e97-899b-58c3cb56334d
   {% elif '3.12' in user_runtime.python_version %}
     - 8dea796f-67cd-49ba-92b9-c9763d76a21d
     - 0a6e8bed-ae93-4fd5-bb60-11c45bc1f42d
   {% endif %}

Rejections to the SDK from the user endpoint process look slightly different:

.. code-block:: text

   Function <function_id> not permitted on endpoint <user_endpoint_id>

In the web-response, the task is not sent to the endpoint at all.  In this
second error message, the user endpoint process was started, received the task
and rejected it; the mentioned endpoint is the internal user endpoint process
identifier, not the parent endpoint identifier.

.. attention::

   If the template configuration sets ``allowed_functions`` and ``config.yaml``
   also specifies ``allowed_functions``, then the template configuration is
   ignored.  The only exception to this is if ``config.yaml`` does *not*
   restrict the functions, as discussed above.


AMQP Port
=========

Endpoints communcaite with the Globus Compute web services via the AMQP
messaging protocol.  As of v2.11.0, newly configured endpoints use AMQP over
port 443 by default, since firewall rules usually leave that port open.  In case
443 is not open on a particular cluster, the port can be changed in
``config.yaml`` via the ``amqp_port`` option:

.. code-block:: yaml
   :caption: ``config.yaml``

   amqp_port: 5671
   display_name: My Endpoint

Note that only ports 5671, 5672, and 443 are supported with the Compute hosted
services.  Also note that when ``amqp_port`` is omitted from the config, the
port is based on the connection URL the endpoint receives after registering
itself with the services, which typically means port 5671.


.. _tracing-a-task:

Tracing a Task to the Endpoint
==============================

The workflow for a task sent to an endpoint roughly follows these steps:

#. The user obtains an endpoint ID either by starting (see
   :ref:`starting-the-endpoint`) or from the administrator of a public endpoint.

#. The user submits a task to the endpoint with the SDK, specifying the
   ``endpoint_id`` and ``user_endpoint_config``:

   .. code-block:: python
      :emphasize-lines: 7, 8

      from globus_compute_sdk import Executor

      def some_task(*a, **k):
          return 1

      with Executor() as ex:
          ex.endpoint_id = "..."  # as acquired from step 1
          ex.user_endpoint_config = {"worker_init": "source /path/to/venv/bin/activate"}
          fut = ex.submit(some_task)
          print("Result:", fut.result())  # Reminder: blocks until result received

#. After the ``ex.submit()`` call, the SDK `POSTs a REST request`_ to the Globus
   Compute web service.

#. The Compute web-service generates a unique identifier for a new user endpoint
   process using the following:

   - the ``endpoint_id``
   - the Globus Auth identity ID of the user making the request
   - the endpoint configuration in the request
   - various user runtime information

   This identifier is simultaneously stable and unique.  In other words,
   changing any of these values *will result in a new user endpoint process*.

#. The Compute web-service sends a message to the endpoint (via `AMQP`_) asking
   it to start a user endpoint process as the user that initiated the REST
   request.

#. If running a :doc:`multi-user endpoint <multi_user>`, the manager endpoint
   process maps the Globus Auth identity in the start request to a local POSIX
   username.  If not, the manager endpoint process skips identity mapping
   altogether.

#. The manager endpoint process calls |fork(2)|_ to ceate a new process.

#. If running a :doc:`multi-user endpoint <multi_user>`, the manager endpoint
   process ascertains the host-specific UID based on a |getpwnam(3)|_ call with
   the local username from the previous step, then drops privileges.

#. The manager endpoint process validates the user-defined variables against the
   JSON schema, if present, then renders the user endpoint configuration
   template.

#. The manager endpoint process calls ``exec()`` to launch the user endpoint
   process and passes the generated configuration over ``stdin``.  Each user
   endpoint process creates a dedicated directory in the local user's
   ``$HOME/.globus_compute/`` directory to store logs and other essential files.

#. The just-started user endpoint process checks in with the Globus Compute
   web-services.

#. The web services detect the check-in, complete the original SDK request by
   accepting the task, then send the task to the now-running user endpoint
   process.

Troubleshooting
===============

If you are having issues starting an endpoint or submitting a task to Globus
Compute, you can run self-diagnostics via the `globus-compute-diagnostic`
script that is installed as part of the Globus Compute SDK.

We recommend running the diagnostic on the same machine that the Globus Compute
endpoint is running, so that it can gather log snippets from each of the installed
endpoints.  If you are having trouble only when sending a task, you can run
diagnostics from the submitting side (VM, laptop, etc) to confirm connectivity,
preferably via the `-e ENDPOINT_UUID` option to send a simple sample task.

Without parameters, the diagnostic will generate a .gz compressed file in the
current directory named `globus_compute_diagnostic_YYYY-MM-DD-HH-mm-ssZ.txt.gz`
with all test results (use `gunzip` to uncompress it).


.. code-block:: console

   $ globus-compute-diagnostic -h

   usage: globus-compute-diagnostic [-h] [-p] [-k number] [-v] [-e ENDPOINT_UUID] [-c CONFIG_DIR]

   Run diagnostics for Globus Compute

   options:
     -h, --help            show this help message and exit
     -p, --print-only      Do not generate a Gzip-compressed file. Print diagnostic results to the console instead.
     -k number, --log-kb number
                           Specify the number of kilobytes (KB) to read from log files. Defaults to 1024 KB (1 MB)
                           per file.
     -v, --verbose         When writing diagnostic output to local compressed file, also print the name of each test
                           to stdout as they are being run, to help monitor diagnostic progress.
     -e ENDPOINT_UUID, --endpoint-uuid ENDPOINT_UUID
                           Test an endpoint by registering a sample function and sending a task to it using the newly
                           registered function. An endpoint UUID is required.
     -c CONFIG_DIR, --config-dir CONFIG_DIR
                           Gather endpoint configuration and log info from the specified parent directory instead of
                           the default ~/.globus_compute or what is set in $GLOBUS_COMPUTE_USER_DIR

   This utility gathers local hardware specifications, tests connectivity to the Globus Compute web services, and
   collates portions of local Compute Endpoint log files, if present, to a local compressed file.


For additional help with Globus Compute that is not addressed here, please
reach out to our Team directly by submitting a
[support ticket](https://www.globus.org/contact-us).


.. |nbsp| unicode:: 0xA0
   :trim:

.. |rarr| unicode:: 0x2192
   :trim:

.. |Providers| replace:: ``Providers``
.. _Providers: https://parsl.readthedocs.io/en/stable/reference.html#providers
.. _LocalProvider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LocalProvider.html
.. |GlobusComputeEngine| replace:: ``GlobusComputeEngine``
.. _GlobusComputeEngine: ../reference/engine.html#globus_compute_endpoint.engines.GlobusComputeEngine
.. |HighThroughputExecutor| replace:: ``HighThroughputExecutor``
.. _HighThroughputExecutor: https://parsl.readthedocs.io/en/latest/stubs/parsl.executors.HighThroughputExecutor.html
.. |ManagerEndpointConfig| replace:: :class:`ManagerEndpointConfig <globus_compute_endpoint.endpoint.config.config.ManagerEndpointConfig>`
.. _Web UI: https://app.globus.org/compute
.. _Jinja template: https://jinja.palletsprojects.com/en/stable/
.. _Globus Auth: https://www.globus.org/platform/services/auth
.. _Globus Groups: https://www.globus.org/platform/services/groups
.. _Globus authentication policy: https://docs.globus.org/api/auth/developer-guide/#authentication-policies
.. _Authentication Policies documentation: https://docs.globus.org/api/auth/developer-guide/#authentication_policy_fields
.. _Globus subscription: https://www.globus.org/subscriptions
.. _conda: https://docs.conda.io/en/latest/
.. _AMQP: https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol
.. |getpwnam(3)| replace:: ``getpwnam(3)``
.. _getpwnam(3): https://www.man7.org/linux/man-pages/man3/getpwnam.3.html
.. |fork(2)| replace:: ``fork(2)``
.. _fork(2): https://www.man7.org/linux/man-pages/man2/fork.2.html
.. _deserializing untrusted data via pickle,: https://github.com/swisskyrepo/PayloadsAllTheThings/blob/4.1/Insecure%20Deserialization/Python.md
.. _POSTs a REST request: https://compute.api.globus.org/redoc#tag/Endpoints/operation/submit_batch_v3_endpoints__endpoint_uuid__submit_post
