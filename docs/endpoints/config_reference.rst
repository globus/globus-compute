Configuration Reference
***********************

Globus Compute endpoints require two configuration files:

- ``config.yaml`` for the manager endpoint process
- ``user_config_template.yaml.j2`` for user endpoint processes

These two YAML files serve as convenience interfaces to the Python configuration
classes used internally by Compute endpoints.  Anything specified here is
conveyed during the endpoint startup to those backing classes; consequently, for
a complete list of options, please see the :ref:`internal class documentation
<config-class-doc>` at the bottom of this page.  Meanwhile, the YAML interface
is generally much easier to understand, easier to ``diff``, and, with less
required boilerplate code, easier to maintain.

.. _uep-conf:

User Endpoint Configuration
===========================

The ``user_config_template.yaml.j2`` file is a Jinja template used to generate
YAML configurations for user endpoint processes that execute tasks.  Under the
hood, all configuration options are used to create an instance of the
|UserEndpointConfig| class.

For information on template capabilities and peculiarities, see
:doc:`templates`.

Idle Timeout
------------

User endpoint processes automatically shut down after a configurable idle
timeout to conserve resources:

- ``idle_heartbeats_soft``: if there are no outstanding tasks still processing,
  and the user endpoint process has been idle for this many heartbeats, shut it
  down

- ``idle_heartbeats_hard``: if the user endpoint process is *apparently* idle
  (e.g., there are outstanding tasks, but they have not moved) for this many
  heartbeats, then shut down anyway

By default, a heartbeat occurs every 30s.  If ``idle_heartbeats_hard`` is set to
7, and no tasks or results move (i.e., tasks received from the web service or
results received from workers), then the user endpoint process will shut down
after 3m30s (7 × 30s).

Engine
------

The only required configuration item is ``engine``, with three available types:
``ThreadPoolEngine``, ``ProcessPoolEngine``, and ``GlobusComputeEngine``.  The
first two are Compute endpoint wrappers of Python's |ThreadPoolExecutor|_ and
|ProcessPoolExecutor|_, respectively.  These engines are appropriate for
single‑host installations (e.g., a personal workstation).  For scheduler‑based
clusters, |GlobusComputeEngine|_, as a wrapper over Parsl's
|HighThroughputExecutor|_, enables access to multiple computation nodes.  The
:ref:`default configuration <uep-conf>` specifies |GlobusComputeEngine|_.

The simplest configuration would use the ``ThreadPoolEngine``:

.. code-block:: yaml
   :caption: ``~/.globus_compute/simple_threadpool/user_config_template.yaml.j2``

   engine:
     type: ThreadPoolEngine

Per Python's default of ``max_workers=None``, this configuration will create as
many threads as the host has processor cores (up to 32, per Python 3.8+).  Any
argument to fine-tune the underlying executor's behavior must be placed inside
the ``engine`` stanza.  For example, to limit the worker to 3 threads:

.. code-block:: yaml
   :caption: ``~/.globus_compute/three_threads/user_config_template.yaml.j2``

   engine:
     type: ThreadPoolEngine
     max_workers: 3

Similarly, if using the ``ProcessPoolEngine``, one might implement a policy of
workers only running 100 tasks before workers are respawned:

.. code-block:: yaml
   :caption: ``~/.globus_compute/four_workers_100_tasks/user_config_template.yaml.j2``

   engine:
     type: ProcessPoolEngine
     max_tasks_per_child: 100
     max_workers: 4

Given the above two endpoint configurations, the ``ps`` utility on the host
console can verify the setup.  The *process* pool has 4 worker nodes (per the
``max_workers`` configuration), while the thread pool endpoint has the same
concept in threads:

.. code-block:: console
   :caption: ``ps`` of different engine configurations on the host; output
     edited for clarity

   $ ps w --forest | grep "Globus Compute Endpoint"
   25713 ... \_ Globus Compute Endpoint (..., four_workers_100_tasks)
   25726 ...     \_ Globus Compute Endpoint (..., four_workers_100_tasks)
   25727 ...     \_ Globus Compute Endpoint (..., four_workers_100_tasks)
   25728 ...     \_ Globus Compute Endpoint (..., four_workers_100_tasks)
   25729 ...     \_ Globus Compute Endpoint (..., four_workers_100_tasks)
   26339 ... \_ Globus Compute Endpoint (..., three_threads)

   $ ps wm --forest | grep -A2 three_threads
   26339 ... Globus Compute Endpoint (..., three_threads)
       - ... -
       - ... -

Per usual Python semantics, the ``ThreadPoolEngine`` (``ThreadPoolExecutor``
under the hood) is typically best for I/O oriented workflows, while the
``ProcessPoolEngine`` (``ProcessPoolExecutor`` under the hood) will be a better
fit for CPU-intensive tasks.  But both engines will only run tasks *on the
endpoint host machine*.  If the endpoint is strictly limited to a single host
(e.g., a home desktop, an idle workstation), then these engines may be the
simplest option.

For running in a multi-node setup (e.g., clusters, with scheduling software like
`PBS`_ or `Slurm`_), the ``GlobusComputeEngine`` enables much more concurrency.
This engine has more options and is similarly more complicated to configure.  A
rough equivalent to the ``ProcessPoolEngine`` example would be:

.. code-block:: yaml
   :caption: ``~/.globus_compute/my_first_cluster_setup/user_config_template.yaml.j2``

   engine:
     type: GlobusComputeEngine
     provider:
       type: LocalProvider
       max_blocks: 4

Retries
^^^^^^^

Functions submitted to the |GlobusComputeEngine|_ can fail due to infrastructure
failures.  For example, the worker executing the task might terminate due to it
running out of memory, or all workers under a batch job could fail due to the
batch job exiting as it reaches the walltime limit.  |GlobusComputeEngine|_ can
be configured to automatically retry these tasks by setting
``max_retries_on_system_failure=N``, where N is the number of retries allowed.
The default config sets retries to 0 since functions can be computationally
expensive, not idempotent, or leave side effects that affect subsequent retries.

Example config snippet:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
       type: GlobusComputeEngine
       max_retries_on_system_failure: 2  # Default=0


Auto-Scaling
^^^^^^^^^^^^

|GlobusComputeEngine|_ by default automatically scales workers in response to
workload.

``Strategy`` configuration is limited to two options:

#. ``max_idletime``: Maximum duration in seconds that workers are allowed to
   idle before they are marked for termination

#. ``strategy_period``: Set the # of seconds between strategy attempting
   auto-scaling events

The bounds for scaling are determined by the options to the ``Provider``
(``init_blocks``, ``min_blocks``, ``max_blocks``).  Please refer to the `Parsl
docs <https://parsl.readthedocs.io/en/stable/userguide/execution.html#elasticity>`_
for more info.

Here's an example configuration:

.. code-block:: yaml
   :caption: ``user_config_template.yaml.j2``

   engine:
       type: GlobusComputeEngine
       job_status_kwargs:
           max_idletime: 60.0      # Default = 120s
           strategy_period: 120.0  # Default = 5s

Provider
^^^^^^^^

Whereas the ``ThreadPoolEngine`` and ``ProcessPoolEngine`` wrappers have an
implicit approach to managing the compute resources (the `process model`_), the
``GlobusComputeEngine`` requires explicit knowledge of the local topology.  The
``provider`` stanza chooses the Parsl mechanism by which to communicate with the
local site.  If the workers will be on the same host as the endpoint,
|LocalProvider|_ is appropriate.  But if workers will be on cluster nodes, those
resources will be accessed via the site-specific scheduler, and communication
between the endpoint and the workers will occur via a site-specific network
interface.

`Parsl implements a number of providers`_, so we will not describe them here.
Instead, we present a single example, and then suggest reading through the rest
of the :doc:`endpoint configuration examples <endpoint_examples>` and Parsl's
documentation while keeping your project needs in mind.

The University of Chicago's Midway cluster uses `Slurm`_ as the batch scheduler,
so the following example configuration chooses Parsl's |SlurmProvider|_.  The
Slurm batch scheduler requires an allocation to debit for each job submission,
specified for |SlurmProvider|_ via ``account``.  On each acquired cluster node,
the ``worker_init`` is a set of shell-script lines that will be run prior to
starting the worker; this example loads the site-specific module named
``Anaconda``, and then activates the ``compute-env`` environment.  When sending
jobs to the batch scheduler, this requests only a single node at a time
(``nodes_per_block``), from the partition of nodes named ``caslake``
(``partition``), does not have more than 1 active or pending job
(``max_blocks``), and has the scheduler enforce a time limit for each job to no
more than 5 minutes (``walltime``).

For communication between the endpoint and the worker nodes, tell the endpoint
to open up communication ports on the *internal* interface, named ``bond0``.

.. code-block:: yaml+jinja
   :caption: Example ``user_config_template.yaml.j2`` of an endpoint on UChicago
     RCC's Midway

   engine:
       type: GlobusComputeEngine
       max_workers_per_node: 2
       provider:
           type: SlurmProvider
           account: {{ account }}
           partition: caslake
           worker_init: "module load Anaconda; source activate compute-env"
           nodes_per_block: 1
           max_blocks: 1
           walltime: 00:05:00
       address:
           type: address_by_interface
           ifname: bond0

Again, this is only a basic example.  For more inspiration, please consult the
:doc:`list of examples <endpoint_examples>` and peruse Parsl's documentation on
both the |HighThroughputExecutor|_ and the `available providers`_.

.. note::

   **How does one determine the appropriate interface (ifname) to use for each
   system?**

   There is no one answer to this.  One route is to simply ask a more
   knowledgeable person (e.g., a colleague or system administrator).  Another
   route might be a combination of educated guesses and trial and error.

   To see what interfaces the host machine has configured, one can use the
   ``ip`` utility to look for the UP interfaces.  From a hypothetical machine:

   .. code-block:: console

      $ ip addr  # (typically 'ip' is in /usr/sbin/)
      ...
      2: enp2s0f0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc fq_codel state DOWN group default qlen 1000
          link/ether 88:a4:c2:12:a8:6d brd ff:ff:ff:ff:ff:ff
      ...
      4: enp2s0f1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
          link/ether 10:C5:95:49:b0:a1 brd ff:ff:ff:ff:ff:ff
          inet 10.110.23.47/24 brd 10.119.23.255 scope global dynamic noprefixroute wlp3s0
             valid_lft 2669sec preferred_lft 2669sec
          inet6 fe80::6a05:caff:fee0:320a/64 scope link noprefixroute
             valid_lft forever preferred_lft forever
      ...
      7: wlp3s0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
          link/ether 50:2f:9b:12:3e:bb brd ff:ff:ff:ff:ff:ff
          inet 192.168.167.219/24 brd 192.168.167.255 scope global dynamic noprefixroute wlp3s0
             valid_lft 6007sec preferred_lft 6007sec
          inet6 fe80::1d48:c378:42d3:d031/64 scope link noprefixroute
             valid_lft forever preferred_lft forever
      ...

   (If you like color, can use ``ip -c addr`` to get the DOWN and UP interfaces
   in red and green output.)

   From the above, ``enp2s0f1`` and ``wlp3s0`` would be likely candidates (c.f.,
   the capitalized ``UP`` in the first line of those records).  The next step is
   to recognize that most setups attach their cluster nodes to *internal*
   networks.  Put differently, a ping destined for the outside world but placed
   on the internal interface, should fail.  Programmatically, then, one can look
   for a *failing* ``ping`` invocation as an indication of the inward-facing
   interface.  From the above output, we can use the ``-I`` argument to ``ping``:

   .. code-block:: console

      $ ping -c 1 -I wlp3s0 google.com 1>/dev/null 2>&1; echo $?
      0

   Zero.  That means "Successfully pinged ``google.com`` and got a response."
   But since we are looking for ping to *fail*, that is likely not the correct
   interface for the endpoint to utilize.

   .. code-block:: console

      $ ping -c 1 -I enp2s0f1 google.com 1>/dev/null 2>&1; echo $?
      1

   Nonzero -- "Failed to communicate with ``google.com``".  Therefore, that is
   likely an internal interface, and a good bet for the interface that is
   plumbed up to talk with the cluster's internal nodes.

   At which point, try it "and see."

.. |LocalProvider| replace:: ``LocalProvider``
.. _LocalProvider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LocalProvider.html
.. |SlurmProvider| replace:: ``SlurmProvider``
.. _SlurmProvider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html

.. _PBS: https://openpbs.org/
.. _Slurm: https://slurm.schedmd.com/overview.html
.. _Parsl implements a number of providers: https://parsl.readthedocs.io/en/stable/reference.html#providers
.. _available providers: https://parsl.readthedocs.io/en/stable/reference.html#providers

.. _endpoint-manager-config:

Manager Endpoint Configuration
==============================

The ``config.yaml`` file contains the YAML configuration for the manager
endpoint process, which manages user endpoint processes.  Under the hood, all
configuration options in this file are used to create an instance of the
|ManagerEndpointConfig| class.

- ``identity_mapping_config_path``

  A path to an identity mapping configuration, per the Globus Connect Server
  `Identity Mapping Guide`_.  The configuration file must be a JSON-list of
  identity mapping configurations.  The multi-user endpoint  documentation
  :ref:`discusses the content<example-idmap-config>` of this file in detail.

  .. code-block:: yaml
     :caption: Example ``config.yaml`` with an identity mapping path

     identity_mapping_config_path: /path/to/idmap_config.json

- ``user_config_template_path``

  The path to the user endpoint configuration Jinja2 template YAML file.  If not
  specified, the default template path will be used:
  ``~/.globus_compute/my-ep/user_config_template.yaml.j2``.

  See :ref:`user-config-template-yaml-j2` for more information.

  .. code-block:: yaml
     :caption: Example ``config.yaml`` with a custom user config template path

     user_config_template_path: /path/to/my_template.yaml.j2

- ``user_config_schema_path``

  The path to the user endpoint configuration JSON schema file.  If not
  specified, the default schema path will be used:
  ``~/.globus_compute/my-mep/user_config_schema.json``.

  See :ref:`user-config-schema-json` for more information.

  .. code-block:: yaml
     :caption: Example ``config.yaml`` with a custom user config schema path

     user_config_schema_path: /path/to/my_schema.json

- ``public``

  A boolean value, dictating whether other users can discover this endpoint in
  the Globus Compute web API and Globus `Web UI`_.  It defaults to ``false``.

  .. warning::

     This field does **not** prevent access to the endpoint.  It determines only
     whether this endpoint is easily discoverable |nbsp| --- |nbsp| do not use
     this field as a security control.

  .. code-block:: yaml
     :caption: ``config.yaml`` -- example public multi-user endpoint

     public: true

- ``admins``

  A list of Globus Auth identity IDs that have administrative access to the
  endpoint, in addition to the owner.

  .. important::

     This field requires an active Globus subscription (i.e.,
     ``subscription_id``).

  .. code-block:: yaml
     :caption: ``config.yaml`` -- specifying endpoint administrators

     subscription_id: 600ba9ac-ef16-4387-30ad-60c6cc3a6853
     admins:
       # Peter Gibbons (software engineer)
       - 10afcf74-b041-4439-8e0d-eab371767440
       # Samir Nagheenanajar (sysadmin, HPC services)
       - a6a7b9ee-be04-4e45-9832-d3737c2fafa2

- ``display_name``

  If not specified, the endpoint will show up in the `Web UI`_ as the given
  local name.  (In other words, the same name as used to create the endpoint
  with the ``configure`` subcommand, and as used for the directory name inside
  of ``~/.globus_compute/``.)  This field is free-form (accepting space
  characters, for example).

  .. code-block:: yaml
     :caption: ``config.yaml`` -- naming a public endpoint

     display_name: Debug queue, 10m max job time (RCC, Midway, UChicago)
     public: true

- ``allowed_functions``

  This field specifies an allow-list of functions that may be run by a user
  endpoint process.  As this list is available at endpoint registration time,
  not only do the user endpoint processes verify that each task requests a valid
  function, but the web-service enforces the allowed functions list at task
  submission as well.  For more information, see :ref:`Function Allow Listing
  <function-allowlist>`.

  .. code-block:: yaml
     :caption: ``config.yaml`` -- only allow certain functions

     allowed_functions:
       - 00911703-e76b-4d0b-7b98-6f2e25ab9943
       - e552e7f2-c007-4671-6ca4-3a4fd84f3805

- ``authentication_policy``

  Use a Globus `Authentication Policy`_ to restrict who can use a multi-user
  endpoint at the web service.  See :ref:`Authentication Policies
  <auth-policies>` for more information.

  .. code-block:: yaml
     :caption: ``config.yaml`` -- allowing only valid identities

     authentication_policy: 498c7327-9c6a-4847-c954-1eafa923da8e
     subscription_id: 600ba9ac-ef16-4387-30ad-60c6cc3a6853

- ``pam``

  Use `Pluggable Authentication Modules`_ (PAM) for site-specific authorization
  requirements.  A structure with ``enable`` and ``service_name`` options.
  Defaults to disabled and ``globus-compute-endpoint``.  See :ref:`Multi-User §
  PAM <pam>` for more information.

  .. code-block:: yaml
     :caption: ``config.yaml`` -- enabling PAM

     pam:
       enable: true


.. _Identity Mapping Guide: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/
.. _Web UI: https://app.globus.org/compute
.. _Authentication Policy: https://docs.globus.org/api/auth/developer-guide/#authentication-policies
.. _Pluggable Authentication Modules: https://en.wikipedia.org/wiki/Linux_PAM

----

.. _config-class-doc:

Python Class Documentation
==========================

The YAML configurations discussed above are facades over the following Python
classes.  Though the vast majority of users will only use the YAML
configurations, we present the following class documentation to show all of the
available options.

.. autoclass:: globus_compute_endpoint.endpoint.config.config.UserEndpointConfig
   :members:
   :member-order: bysource
   :inherited-members:
   :show-inheritance:

.. autoclass:: globus_compute_endpoint.endpoint.config.config.ManagerEndpointConfig
   :members:
   :member-order: bysource
   :inherited-members:
   :show-inheritance:

.. autoclass:: globus_compute_endpoint.endpoint.config.config.BaseConfig
   :members:
   :member-order: bysource

.. autoclass:: globus_compute_endpoint.endpoint.config.pam.PamConfiguration
   :members:
   :member-order: bysource

.. |nbsp| unicode:: 0xA0
   :trim:

.. _process model: https://en.wikipedia.org/wiki/Process_(computing)

.. |ThreadPoolExecutor| replace:: ``concurrent.futures.ThreadPoolExecutor``
.. _ThreadPoolExecutor: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
.. |ProcessPoolExecutor| replace:: ``concurrent.futures.ProcessPoolExecutor``
.. _ProcessPoolExecutor: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
.. |GlobusComputeEngine| replace:: ``GlobusComputeEngine``
.. _GlobusComputeEngine: ../reference/engine.html#globus_compute_endpoint.engines.GlobusComputeEngine
.. |HighThroughputExecutor| replace:: ``HighThroughputExecutor``
.. _HighThroughputExecutor: https://parsl.readthedocs.io/en/latest/stubs/parsl.executors.HighThroughputExecutor.html
