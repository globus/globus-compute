Multi-User Compute Endpoints
****************************

.. attention::

  This chapter describes Multi-user Compute Endpoints (MEP) for site administrators.
  For those not running MEPs with ``root`` privileges, please instead consult the
  :ref:`endpoints_templating_configuration` section in the previous chapter.


.. tip::

   For those just looking to get up and running, see the `Administrator Quickstart`_,
   below.


Multi-user Compute Endpoints (MEP) enable administrators to securely offer compute
resources to users without mandating the typical shell access (i.e., ssh).  The basic
thrust is that the administrator of a MEP creates a configuration template that is
populated by user data and passed to a user endpoint process, or UEP.  When a user
sends a task to the MEP, it will start a UEP on their behalf as a local POSIX user
account mapped from their Globus Auth identity.  In this manner, administrators may
preset endpoint configuration options (e.g., Torque, PBS, Slurm) and offer
user‑configurable items (e.g., account id, problem‑specific number of blocks), while at
the same time lowering the barrier to use of the resource.

.. note::

   The only difference between a "normal" endpoint process and a UEP is a semantic one
   for discussion so as to differentiate the two different starting contexts.  An
   endpoint is started manually by a human, while a UEP will always have a
   parent MEP process.


User Endpoint Startup Overview
==============================

UEPs are initiated by tasks sent to the MEP id.  In REST-speak, that means that one or
more tasks were |POSTed to the /v3/endpoints/<mep_uuid>/submit|_ Globus Compute
route.  When the web service determines that the ``endpoint_uuid`` is for a MEP, it
generates a UEP identifier specific to the tuple of the ``endpoint_uuid``, the Globus
Auth identity of the user making the request, and the endpoint configuration in the
request (e.g., ``generate_identifier_from(site_id, user_id, conf)``) |nbsp| --- |nbsp|
this identifier is simultaneously stable and unique.  After verifying that the generated
ID is either new, or already belongs to the user, the web service then sends a start-UEP
message to the MEP (via `AMQP
<https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol>`_), asking it to start
an endpoint on behalf of the Globus Auth identity making the REST request identified by
the generated UEP id.

At the other end of the AMQP queue, the MEP receives the start-UEP message, validates
the basic structure, then attempts to map the Globus Auth identity.  If the mapping is
successful and the POSIX username exists, the MEP will proceed to ``fork()`` a new
process.  The child process will immediately and irreversibly become the user, and then
``exec()`` a new ``globus-compute-endpoint`` instance.

The new child process |nbsp| --- |nbsp| the UEP |nbsp| --- |nbsp| will receive AMQP
connection credentials from the MEP (which received them as part of the start-UEP
request), and immediately let the web service know it is ready to receive tasks.

(For those who prefer lists over prose, please see :ref:`tracing-a-task-to-mep`.)


Key Benefits
============

For Administrators
------------------

This biggest benefit of a MEP setup is a lowering of the barrier for legitimate users of
a site.  To date, knowledge of the command line has been critical to most users of High
Performance Computing (HPC) systems, though only as a necessity of infrastructure rather
than a legitimate scientific purpose.  A MEP allows a user to ignore many of the
important-but-not-really details of plumbing, like logging in through SSH, restarting
user-only daemons, or, in the case of Globus Compute, fine-tuning scheduler options by
managing multiple endpoint configurations.  The only thing they need to do is run their
scripts locally on their own workstation, and the rest "just works."

Another boon for administrators is the ability to fine-tune and pre-configure what
resources UEPs may utilize.  For example, many users struggle to discover which
interface is routed to a cluster's internal network; the administrator can preset that,
completely bypassing the question.  Using `ALCF's Polaris
<https://www.alcf.anl.gov/polaris>`_ as an example, the administrator could use the
following user configuration template (``user_config_template.yaml.j2``) to place all
jobs sent to this MEP on the ``debug-scaling`` queue, and pre-select the obvious
defaults (`per the documentation <https://docs.alcf.anl.gov/polaris/running-jobs/>`_):

.. code-block:: yaml+jinja
   :caption: ``/root/.globus_compute/mep_debug_scaling/user_config_template.yaml.j2``

   display_name: Polaris at ALCF - debug-scaling queue
   engine:
     type: GlobusComputeEngine
     address:
       type: address_by_interface
       ifname: bond0

     strategy:
       type: SimpleStrategy
       max_idletime: 30

     provider:
       type: PBSProProvider
       queue: debug-scaling

       account: {{ ACCOUNT_ID }}

       # Command to be run before starting a worker
       # e.g., "module load Anaconda; source activate parsl_env"
       worker_init: {{ WORKER_INIT_COMMAND|default() }}

       init_blocks: 0
       min_blocks: 0
       max_blocks: 1
       nodes_per_block: {{ NODES_PER_BLOCK|default(1) }}

       walltime: 1:00:00

       launcher:
         type: MpiExecLauncher

   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760

The user must specify the ``ACCOUNT_ID``, and could optionally specify the
``WORKER_INIT_COMMAND`` and ``NODES_PER_BLOCK`` variables.  If the user's jobs finish
and no more work comes in after ``max_idletime`` seconds (30s), the UEP will scale down
and consume no more wall time.

Another benefit is a cleaner process table on the login nodes.  Rather than having user
endpoints sit idle on a login-node for days after a run has completed (perhaps until the
next machine reboot), a MEP setup automatically shuts down idle UEPs (as defined in
``user_config_template.yaml.j2``).  When the UEP has had no movement for 48h (by
default; see ``idle_heartbeat_hard``), or has no outstanding work for 5m (by default;
see ``idle_heartbeats_soft``), it will shut itself down.

For Users
---------

Under the MEP paradigm, users largely benefit from not having to be quite so aware of an
endpoint and its configuration.  As the administrator will have taken care of most of
the smaller details (c.f., installation, internal interfaces, queue policies), the user
is able to write a consuming script, knowing only the endpoint id and their system
accounting username:

.. code-block:: python

   import concurrent.futures
   from globus_compute_sdk import Executor

   def jitter_double(task_num):
       import random
       return task_num, task_num * (1.5 + random.random())

   polaris_site_id = "..."  # as acquired from the admin in the previous section
   with Executor(
       endpoint_id=polaris_site_id,
       user_endpoint_config={
           "ACCOUNT_ID": "user_allocation_account_id",
           "NODES_PER_BLOCK": 2,
       }
   ) as ex:
       futs = [ex.submit(jitter_double, task_num) for task_num in range(100)]
       for fut in concurrent.futures.as_completed(futs):
           print("Result:", fut.result())

It is a boon for the researcher to see the relevant configuration variables immediately
adjacent to the code, as opposed to hidden in the endpoint configuration and behind an
opaque endpoint id.  An MEP removes almost half of the infrastructure plumbing that the
user must manage |nbsp| --- |nbsp| many users will barely even need to open their own
terminal, much less an SSH terminal on a login node.


Configuration
=============

Creating a MEP starts with the ``--multi-user`` :ref:`command line flag
<create-templatable-endpoint>` to the ``configure`` subcommand, which will generate the
below five configuration files:

.. code-block:: console

   # globus-compute-endpoint configure --multi-user mep_debug
   Created multi-user profile for endpoint named <mep_debug>

       Configuration file: /root/.globus_compute/mep_debug/config.yaml

       Example identity mapping configuration: /root/.globus_compute/mep_debug/example_identity_mapping_config.json

       User endpoint configuration template: /root/.globus_compute/mep_debug/user_config_template.yaml.j2
       User endpoint configuration schema: /root/.globus_compute/mep_debug/user_config_schema.json
       User endpoint environment variables: /root/.globus_compute/mep_debug/user_environment.yaml

   Use the `start` subcommand to run it:

   globus-compute-endpoint start mep_debug


``config.yaml``
---------------

The default MEP ``config.yaml`` file is:

.. code-block:: yaml
   :caption: The default multi-user ``config.yaml`` configuration

   amqp_port: 443
   display_name: null
   identity_mapping_config_path: /root/.globus_compute/mep_debug/example_identity_mapping_config.json
   multi_user: true

The ``multi_user`` flag is required, but the ``identity_mapping_config_path`` is only
required if the MEP process will have privileges to change users (e.g., if ``$USER =
root``).  ``display_name`` is optional, but if set, determines how the MEP will appear
in the `Web UI`_.  (And as the MEP does *not execute tasks*, :ref:`there is no engine
block <cea_configuration>`.)

.. _example-idmap-config:

``example_identity_mapping_config.json``
----------------------------------------

This is a valid-syntax-but-will-never-successfully-map example identity mapping
configuration file.  It is a JSON list of identity mapping configurations that will be
tried in order.  By implementation within the MEP code base, the first configuration to
return a match "wins."  In this example, the first configuration is a call out to an
external tool, as specified by the |idmap_external|_ DATA_TYPE.  The command is a list
of arguments, with the first element as the actual executable.  In this case, the flags
are strictly illustrative, as ``/bin/false`` always returns with a non-zero exit code
and so will be ignored by the |globus-identity-mapping|_ logic.  However, if the site
requires custom or special logic to acquire the correct local username, this executable
must accept a |idmap_input|_ JSON document via ``stdin`` and output a |idmap_output|_
JSON document to ``stdout``.

The second configuration in this example is an |idmap_expression|_, which means it uses
a subset of regular expression syntax to search for a suitable POSIX username.  This
configuration searches the ``username`` field from the passed identity set for a value
that ends in ``@example.com``.  The library appends the ``^`` and ``$`` anchors to the
regex before searching, so the actual regular expression used would be
``^(.*)@example.com$``.  Finally, if a match is found, the first saved group is the
output (i.e., ``{0}``).  If the ``username`` field contained ``mickey97@example.com``,
then this configuration would return ``mickey97``, and the MEP would then use
|getpwnam(3)|_ to look up ``mickey97``.  But if the username field(s) did not end with
``@example.com``, then it would not match and the start-UEP request would fail.

.. code-block:: json
   :caption: The default example identity mapping configuration; technically functional
       but pragmatically useless

   [
     {
       "comment": "For more examples, see: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/",
       "DATA_TYPE": "external_identity_mapping#1.0.0",
       "command": ["/bin/false", "--some", "flag", "-a", "-b", "-c"]
     },
     {
       "comment": "For more examples, see: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/",
       "DATA_TYPE": "expression_identity_mapping#1.0.0",
       "mappings": [
         {
           "source": "{username}",
           "match": "(.*)@example.com",
           "output": "{0}"
         }
       ]
     }
   ]

The syntax of this document is defined in the `Globus Connect Server Identity Mapping
<https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/>`_
documentation.  It is a JSON-list of mapping configurations, and there are two
implemented strategies to determine a mapping:

* ``expression_identity_mapping#1.0.0`` |nbsp| --- |nbsp| Regular Expression based
  mapping applies an administrator-defined regular expression against any field in the
  input identity documents, returning ``None`` or the matched string.  (Example below.)

* ``external_identity_mapping#1.0.0`` |nbsp| --- |nbsp| Invoke an administrator-defined
  external process, passing the input identity documents via ``stdin``, and reading the
  response from ``stdout``.

.. note::

   While developing this file, administrators may appreciate using the
   ``globus-idm-validator`` tool.  This script is installed as part of the
   |globus-identity-mapping|_ dependency.

The MEP process watches this file for changes.  If an administrator needs to make a
live change, simply update the content of the identity mapping file specified by the
``config.yaml`` configuration.  The MEP server will note the change, and atomically
apply it: if the new identity mapping configuration is invalid, the previously loaded
configuration will remain in place.  In both cases (valid or invalid), the MEP will emit
a message to the log.

``expression_identity_mapping#1.0.0``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For example, a simple policy might require that users of a system have an email address
at your institution or department.  The identity mapping configuration might be:

.. code-block:: json
   :caption: ``only_allow_my_institution.json``

   [
     {
       "DATA_TYPE": "expression_identity_mapping#1.0.0",
       "mappings": [
         {"source": "{email}", "output": "{0}", "match": "(.*)@your_institution.com"},
         {"source": "{email}", "output": "{0}", "match": "(.*)@cs.your_institution.com"}
       ]
     }
   ]


A Globus Auth identity (input) document might look something like:

.. code-block:: json
   :caption: An example identity set, containing two linked identities for the same
      person.

   [
     {
       "id": "00000000-0000-4444-8888-111111111111",
       "email": "alicia@legal.your_institution.com",
       "identity_provider": "abcd7238-f917-4eb2-9ace-c523fa9b1234",
       "identity_type": "login",
       "name": "Alicia",
       "organization": null,
       "status": "used",
       "username": "alicia@legal.your_institution.com"
     },
     {
       "id": "00000000-0000-4444-8888-222222222222",
       "email": "roberto@cs.your_institution.com",
       "identity_provider": "ef345063-bffd-41f7-b403-24f97e325678",
       "identity_type": "login",
       "name": "Roberto",
       "organization": "Your Institution, GmbH",
       "status": "used",
       "username": "roberto@your_institution.com"
     }
   ]

This user has linked both identities, so both identities are in the identity set.  Per
the configuration, the first identity will not match either regex, but the second
(``roberto@your_institution.com``) will, and the returned username would be
``roberto``.  Note that any field could be tested, but this example used ``email``.

``external_identity_mapping#1.0.0``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes, more complicated logic may be required (e.g., LDAP lookups), in which case
consider the ``external_identity_mapping#1.0.0`` configuration stanza.  The
administrator may write a script (or generally, an executable) for the required custom
logic.  The script will be passed a ``identity_mapping_input#1.0.0`` JSON document via
``stdin``, and must output a ``identity_mapping_output#1.0.0`` JSON document on
``stdout``.

.. code-block:: json
   :caption: An example ``identity_mapping_input#1.0.0`` document

   {
     "DATA_TYPE": "identity_mapping_input#1.0.0",
     "identities": [
       {
         "id": "00000000-0000-4444-8888-111111111111",
         "email": "alicia@legal.your_institution.com",
         "identity_provider": "abcd7238-f917-4eb2-9ace-c523fa9b1234",
         "identity_type": "login",
         "name": "Alicia",
         "organization": null,
         "status": "used",
         "username": "alicia@legal.your_institution.com"
       },
       {
         "id": "00000000-0000-4444-8888-222222222222",
         "email": "roberto@cs.your_institution.com",
         "identity_provider": "ef345063-bffd-41f7-b403-24f97e325678",
         "identity_type": "login",
         "name": "Roberto",
         "organization": "Your Institution, GmbH",
         "status": "used",
         "username": "roberto@your_institution.com"
       }
     ]
   }

The executable must identify the successfully mapped identity in the output document by
the ``id`` field.  For example, if an LDAP lookup of ``alicia@legal.your_institution.com``
were to result in ``Alicia`` for this MEP host, then the output document might read:

.. code-block:: json
   :caption: Hypothetical ``identity_mapping_output#1.0.0`` document from an external
      script

   {
     "DATA_TYPE": "identity_mapping_output#1.0.0",
     "result": [
       {"id": "1234567c-cf51-4032-afb8-05986708abcd", "output": "alicia"}
     ]
   }


.. note::

   Reminder that the identity mapping configuration is a JSON *list*.  Multiple mappings
   may be defined, and each will be tried in order until one maps the identity
   successfully or no mappings are possible.

For a much more thorough dive into identity mapping configurations, please consult
the Globus Connect Server's `Identity Mapping documentation`_.

.. |idmap_external| replace:: ``external_identity_mapping#1.0.0``
.. _idmap_external: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#external_program_reference
.. |idmap_expression| replace:: ``expression_identity_mapping#1.0.0``
.. _idmap_expression: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#expression_reference
.. |idmap_input| replace:: ``identity_mapping_input#1.0.0``
.. _idmap_input: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#input_document
.. |idmap_output| replace:: ``identity_mapping_output#1.0.0``
.. _idmap_output: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#output_document

.. _user-config-template-yaml-j2:

``user_config_template.yaml.j2``
--------------------------------

This file is the template that will be interpolated with user-specific variables for
successful start-UEP requests.  More than simple interpolation, the MEP treats this file
as a `Jinja template`_, so there is a good bit of flexibility available to the motivated
administrator.  The initial user config template implements two user-specifiable
variables, ``endpoint_setup`` and ``worker_init``.  Both of these default to the empty
string if not specified by the user (i.e., ``...|default()``).

.. code-block:: yaml+jinja

   endpoint_setup: {{ endpoint_setup|default() }}
   engine:
     ...
     provider:
       ...
       worker_init: {{ worker_init|default() }}

   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760

Given the above template, users submitting to this MEP would be able to specify the
``endpoint_setup`` and ``worker_init`` values.  All other values will remain unchanged
when the UEP starts up.

As linked on the left, :doc:`there are a number of example configurations
<endpoint_examples>` to showcase the available options, but ``idle_heartbeats_soft`` and
``idle_heartbeats_hard`` bear describing.

- ``idle_heartbeats_soft``: if there are no outstanding tasks still processing, and the
  endpoint has been idle for this many heartbeats, shutdown the endpoint

- ``idle_heartbeats_hard``: if endpoint is *apparently* idle (e.g., there are
  outstanding tasks, but they have not moved) for this many heartbeats, then shutdown
  anyway.

A heartbeat occurs every 30s; if ``idle_heartbeats_hard`` is set to 7, and no tasks
or results move (i.e., tasks received from the web service or results received from
workers), then the endpoint will shutdown after 3m30s (7 × 30s).

Every template also has access to the following variables:

- ``parent_config``: Contains the configuration values of the parent MEP. Can be helpful
  in situations involving Python-based configuration files.

- ``user_runtime``: Contains information about the runtime that the user used when
  submitting the task request, such as Python version. See |UserRuntime| for a complete
  list of available information.

These are reserved words and their values cannot be overridden by the user or admin,
and an error is thrown if a user tries to send it as a user option:

.. code-block:: python

   mep_id = "..."
   with Executor(
       endpoint_id=mep_id,
       user_endpoint_config={
           "parent_config": "not allowed"
       },
   ) as ex:
       ex.submit(some_task).result()

   # the following exception is thrown:
   # GlobusAPIError: ('POST', 'http://compute.api.globus.org/v3/endpoints/<mep_id>/submit',
   #   'Bearer', 422, 'SEMANTICALLY_INVALID', "Request payload failed validation:
   #   Unable to start user endpoint process for <user> [exit code: 77; (ValueError)
   #   'parent_config' is a reserved word and cannot be passed in via user config]")

``user_config_schema.json``
---------------------------

If this file exists, then the MEP will validate the user's input against the JSON
schema.  The default schema is quite permissive, allowing strings for the two defined
variables to be strings, and then any other properties. Example:

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

While configuring a JSON schema is out of scope for this documentation, one item to call
out specifically is ``additionalProperties: true``.  If set to true, then the schema
will allow any key not already-specified in ``properties`` |nbsp| --- |nbsp| in other
words, any arbitrary keys and values specified by the user at task submission time,
whether or not they are utilized in ``user_config_template.yaml.j2``.  Please consult
the `JSON Schema documentation <https://json-schema.org/>`_ for more information.

``user_environment.yaml``
-------------------------

Use this file to specify site-specific environment variables to export to the UEP
process.  Though this is a YAML file, it is interpreted internally as a simple
top-level-only set of key-value pairs.  Nesting of data structures will probably not
behave as expected.  Example:

.. code-block:: yaml

   SITE_SPECIFIC_VAR: --additional_flag_for_frobnicator

That will be injected into the UEP process as an environment variable.


Security Posture
================

The current security model of the MEP relies heavily upon Identity Mapping and POSIX
user support.  The only job of the MEP is to start UEPs for users on request from the
Globus Compute web service.  The actual processing of tasks is left to the individual
UEPs.  This is accomplished through the well-known ``fork()`` |rarr| *drop privileges*
|rarr| ``exec()`` Unix workflow, mimicking the approach of many other services
(including Secure Shell [ssh], Globus GridFTP, and the Apache Web server).  In this
manner, all of the standard Unix administrative user controls can be enforced.

Additionally, administrators may further limit access to MEP installations via Globus
authentication policies (which can verify that users have site-appropriate identities
linked to their Globus account with recent authentications), and limiting tasks to an
approved list of functions.


.. _identity-mapping:

Identity Mapping
----------------

"Mapping an identity" is the site-specific process of verifying that one identity is
equivalent to another for the purposes of a given action.  In the Globus Compute case,
this means translating a Globus Auth identity set to a local POSIX user account on the
MEP host for each start-UEP message.  For an administrator-run MEP (i.e., running as the
``root`` user), an identity mapping configuration is required, and is the main
difference from a :ref:`non-root MEP <endpoints_templating_configuration>` |nbsp| ---
|nbsp| a ``root``-owned MEP first maps the Globus Auth identity set from each start-UEP
message to a local POSIX user (i.e., a local username), before ``fork()``-ing a new
process, dropping privileges to that user, and starting the requested UEP.

Please reference the discussion with :ref:`example-idmap-config` (above) for specifics
and examples.


Authentication Policies
-----------------------

In addition to the identity mapping access control, administrators may also use Globus
authentication policies to narrow which identities can even send tasks to a MEP.  An
authentication policy can enforce details such as that a user has an identity from a
specific domain or has authenticated with the Globus Auth recently.  Refer to the
`Authentication Policies documentation`_ for more background and specifics on what
Globus authentication policies can do and how they fit in to a site's security posture.

Please reference the larger :ref:`auth-policies` section (below) for more information.


Function Allow Listing
----------------------

Administrators can further narrow MEP usage by limiting what functions may be invoked
by tasks.  At task submission time, the web-service will reject any submissions that
request functions not in the MEP's configured ``allowed_functions`` list.  Then, child
UEPs will again verify each task against the same list |nbsp| --- |nbsp| a check at the
web-service and a check on-site.

Please reference :ref:`function-allowlist` (below) for more detailed information.


Running the MEP
===============

The MEP starts in the exact same way as a regular endpoint |nbsp| --- |nbsp| with the
``start`` subcommand.  However, the MEP has no notion of the ``detach_endpoint``
configuration item.  Once started, the MEP stays attached to the console, with a timer
that updates every second:

.. code-block:: text

    globus-compute-endpoint start debug_queue
        >>> Multi-User Endpoint ID: [endpoint_uuid] <<<
    ----> Fri Apr 19 11:56:27 2024

The timer is only displayed if the process is connected to the terminal, and is intended
as a hint to the administrator that the MEP process is running, even if no start UEP
requests are yet incoming.

And |hellip| that's it.  The Multi-user endpoint is running, waiting for start UEP
requests to come in.  (But see :ref:`mep-as-a-service` for automatic starting.)

To stop the MEP, type ``Ctrl+\`` (SIGQUIT) or ``Ctrl+C`` (SIGINT).  Alternatively, the
process also responds to SIGTERM.

Checking the Logs
-----------------

If actively debugging or iterating, the two command line arguments ``--log-to-console``
and ``--debug`` may be helpful as they increase the verbosity and color of the text to
the console.  Meanwhile, the log is always available at
``.globus_compute/<mt_endpoint_name>/endpoint.log``, and is the first place to look
upon an unexpected behavior.  In a healthy MEP setup, there will be lots of lines about
processes starting and stopping:

.. code-block:: text

   [...] Creating new user endpoint (pid: 3867325) [(harper, uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a) globus-compute-endpoint start uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a --die-with-parent]
   [...] Command process successfully forked for 'harper' (Globus effective identity: b072d17b-08fd-4ada-8949-1fddca189b5e).
   [...] Command stopped normally (3867325) [(harper, uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a) globus-compute-endpoint start uep.4ade2ce0-9c00-4d8c-b996-4dff8fbb4bd0.e9097f8f-dcfc-3bc0-1b42-0b4ad5e3922a --die-with-parent]


Advanced Environment Customization
==================================

There are some instances where static configuration is not enough.  For example, setting
a user-specific environment variable or running arbitrary scripts prior to handing
control over to the UEP.  For these cases, observe that
``/usr/sbin/globus-compute-endpoint`` is actually a shell script wrapper:

.. code-block:: shell

   #!/bin/sh

   VENV_DIR="/opt/globus-compute-agent/venv-py39"

   if type deactivate 1> /dev/null 2> /dev/null; then
   deactivate
   fi

   . "$VENV_DIR"/bin/activate

   exec "$VENV_DIR"/bin/globus-compute-endpoint "$@"

While we don't suggest modifying this wrapper (for ease of future maintenance), one
might inject another wrapper into the process, by modifying the process PATH and writing
a custom ``globus-compute-endpoint`` wrapper:

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


.. _configure-multiple-python-versions:

Configuring to Accept Multiple Python Versions
==============================================

Due to issues with cross-version serialization, we recommend :ref:`keeping the Python
version running on Endpoint workers in sync <avoiding-serde-errors>` with the version
that functions are first submitted from. However, this can be limiting for
workflows where admins have little control over their users' SDK environments, such as
locally run Jupyter notebooks.  This can sometimes be alleviated with :ref:`an alternate
serialization strategy <specifying-serde-strategy>` (e.g. :class:`~globus_compute_sdk.serialize.JSONData`,
which doesn't rely on bytecode), but not all serialization strategies work in all
environments.  A more robust workaround is to use the ``user_runtime`` config template
variable to detect what Python version was used to submit the task.

Suppose an admin wants to accept the four most recent Python versions (3.10-3.13).
Using `conda`_, they can create an environment for each Python version they want to
support, and launch the UEP's workers with the correct environment depending on the
user's Python version.  A config template for that might look like:

.. code-block:: yaml+jinja

   endpoint_setup: {{ endpoint_setup|default() }}
   engine:
     type: GlobusComputeEngine
     provider:
        type: LocalProvider
     {% if '3.13' in user_runtime.python_version %}
        worker_init: conda activate py313
     {% elif '3.12' in user_runtime.python_version %}
        worker_init: conda activate py312
     {% elif '3.11' in user_runtime.python_version %}
        worker_init: conda activate py311
     {% else %}
        worker_init: conda activate py310
     {% endif %}

This of course requires that there are conda environments named ``py313, ``py312``,
``py311``, and ``py310`` with the appropriate Python versions and
``globus-compute-endpoint`` installed.

For more information on what an MEP knows about the user's runtime environment, see
|UserRuntime|.


Debugging User Endpoints
========================

During implementation, most users are accustomed to using the ``--debug`` flag (or
equivalent) to get more information.  (And usually, caveat emptor, as the amount of
information can be overwhelming.)  The ``globus-compute-endpoint`` executable similarly
implements that flag.  However, if applied to the MEP, that flag will not carry-over to
the child UEP instances.  In particular, the command executed by the MEP is:

.. code-block:: python
   :caption: arguments to ``os.execvpe``

   proc_args = ["globus-compute-endpoint", "start", ep_name, "--die-with-parent"]

Note the lack of the ``--debug`` flag; by default UEPs will not emit DEBUG level logs.
To place UEPs into debug mode, use the ``debug`` top-level configuration directive:

.. code-block:: yaml
   :caption: ``user_config_template.yaml``
   :emphasize-lines: 1

   debug: true
   display_name: Debugging template
   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760
   engine:
      ...

Note that this is *also* how to get the UEP to emit its configuration to the log, which
may be helpful in determining which set of logs are associated with which configuration
or just generally while implementing and debugging.  The configuration is written to the
logs before the UEP boots; look for the following sentinel lines::

   [TIMESTAMP] DEBUG ... Begin Compute endpoint configuration (5 lines):
      ...
   End Compute endpoint configuration

To this end, the authors have found the following command line helpful for pulling out
the configuration from the logs:

.. code-block:: console

   $ sed -n "/Begin Compute/,/End Compute/p" ~/.globus_compute/uep.[...]/endpoint.log | less

.. _mep-as-a-service:

Installing the MEP as a Service
===============================

Installing the MEP as a service is the same :ref:`procedure as with a regular endpoint
<enable_on_boot>`: use the ``enable-on-boot``.  This will dynamically create and
install a systemd unit file.


.. _pam:

Pluggable Authentication Modules (PAM)
======================================

`Pluggable Authentication Modules`_ (PAM) allows administrators to configure
site-specific authentication schemes with arbitrary requirements.  For example, where
one site might require users to use `MFA`_, another site could disallow use of the
system for some users at certain times of the day.  Rather than rewrite or modify
software to accommodate each site's needs, administrators can simply change their site
configuration.

As a brief intro to PAM, the architecture is designed with four phases:

- authentication
- account management
- session management
- password management

The MEP implements *account* and *session management*.  If enabled, then the child
process will create a PAM session, check the account (|pam_acct_mgmt(3)|_), and then
open a session (|pam_open_session(3)|_).  If these two steps succeed, then the MEP will
continue to drop privileges and become the UEP.  But in these two steps is where the
administrator can implement custom configuration.

PAM is configured in two parts.  For the MEP, use the ``pam`` field:

.. code-block:: yaml
   :caption: ``config.yaml`` to show PAM
   :emphasize-lines: 3,4

   multi_user: true
   identity_mapping_config_path: .../some/idmap.json
   pam:
     enable: true

This configuration will choose the default PAM service name,
``globus-compute-endpoint`` (see |PamConfiguration|).  The service name is the name of
the PAM configuration file in ``/etc/pam.d/``.  Use ``service_name`` to tell the MEP
to authorize users against a different PAM configuration:

.. code-block:: yaml
   :caption: ``config.yaml`` with a custom PAM service name
   :emphasize-lines: 7

   multi_user: true
   identity_mapping_config_path: .../some/idmap.json
   pam:
     enable: true

     # the PAM routines will look for `/etc/pam.d/gce-mep123-specific-requirements`
     service_name: gce-mep123-specific-requirements

For clarity, note that the service name is simply passed to |pam_start(3)|_, to tell
PAM which service configuration to apply.

.. important::

  If PAM is not enabled, then before starting user endpoints, the child process drops
  all capabilities and sets the no-new-privileges flag with the kernel.  (See
  |prctl(2)|_ and reference ``PR_SET_NO_NEW_PRIVS``).  In particular, this will
  preclude use of SETUID executables, which can break some schedulers.  If your site
  requires use of SETUID executables, then PAM must be enabled.

Though configuring PAM itself is outside the scope of this document (e.g., see
|PAM_SAG|_), we briefly discuss a couple of modules to share a taste of what PAM can
do.  For example, if the administrator were to implement a configuration of:

.. code-block:: text
   :caption: ``/etc/pam.d/globus-compute-endpoint``

   account   requisite     pam_shells.so
   session   required      pam_limits.so

then, per |pam_shells(8)|_, any UEP for a user whose shell is not listed in
``/etc/shells`` will not start and the logs will have a line like:

.. code-block:: text

   ... (error code: 7 [PAM_AUTH_ERR]) Authentication failure

On the other end, the user's SDK would receive a message like:

.. code-block:: text

   Request payload failed validation: Unable to start user endpoint process for jessica [exit code: 71; (PermissionError) see your system administrator]

Similarly, for users who are administratively allowed (i.e., have a valid shell), the
|pam_limits(8)|_ module will install the admin-configured process limits.

.. hint::

   The Globus Compute Endpoint software implements the account management and session
   phases of PAM.  As authentication is enacted via Globua Auth and
   :ref:`Identity Mapping <identity-mapping>`, it does not use PAM's authentication
   (|pam_authenticate(3)|_) phase, nor does it attempt to manage the user's password.
   Functionally, this means that only PAM configuration lines that begin with
   ``account`` and ``session`` will be utilized.

Look to PAM for a number of tasks (which we tease here, but are similarly out of scope
of this documentation):

- Setting UEP process capabilities (|pam_cap(8)|_)
- Setting UEP process limits (|pam_limits(8)|_)
- Setting environment variables (|pam_env(8)|_)
- Enforcing ``/var/run/nologin`` (|pam_nologin(8)|_)
- Updating ``/var/log/lastlog`` (|pam_lastlog(8)|_)
- Create user home directory on demand (|pam_mkhomedir(8)|_)

(If the available PAM modules do not fit the bill, it is also possible to write a
custom module!  But sadly, that is also out of scope of this documentation; please see
|PAM_MWG|_.)

.. _MFA: https://en.wikipedia.org/wiki/Multi-factor_authentication
.. |PAM_SAG| replace:: The Linux-PAM System Administrators' Guide
.. _PAM_SAG: https://www.chiark.greenend.org.uk/doc/libpam-doc/html/Linux-PAM_SAG.html
.. |PAM_MWG| replace:: The Linux-PAM Module Writers' Guide
.. _PAM_MWG: https://www.chiark.greenend.org.uk/doc/libpam-doc/html/Linux-PAM_MWG.html
.. |pam_acct_mgmt(3)| replace:: ``pam_acct_mgmt(3)``
.. _pam_acct_mgmt(3): https://www.man7.org/linux/man-pages/man3/pam_acct_mgmt.3.html
.. |pam_open_session(3)| replace:: ``pam_open_session(3)``
.. _pam_open_session(3): https://www.man7.org/linux/man-pages/man3/pam_open_session.3.html
.. |pam_authenticate(3)| replace:: ``pam_authenticate(3)``
.. _pam_authenticate(3): https://www.man7.org/linux/man-pages/man3/pam_authenticate.3.html
.. |pam_start(3)| replace:: ``pam_start(3)``
.. _pam_start(3): https://www.man7.org/linux/man-pages/man3/pam_start.3.html
.. |pam_shells(8)| replace:: ``pam_shells(8)``
.. _pam_shells(8): https://www.man7.org/linux/man-pages/man8/pam_shells.8.html
.. |pam_limits(8)| replace:: ``pam_limits(8)``
.. _pam_limits(8): https://www.man7.org/linux/man-pages/man8/pam_limits.8.html
.. |pam_cap(8)| replace:: ``pam_cap(8)``
.. _pam_cap(8): https://www.man7.org/linux/man-pages/man8/pam_cap.8.html
.. |pam_env(8)| replace:: ``pam_env(8)``
.. _pam_env(8): https://www.man7.org/linux/man-pages/man8/pam_env.8.html
.. |pam_nologin(8)| replace:: ``pam_nologin(8)``
.. _pam_nologin(8): https://www.man7.org/linux/man-pages/man8/pam_nologin.8.html
.. |pam_lastlog(8)| replace:: ``pam_lastlog(8)``
.. _pam_lastlog(8): https://www.man7.org/linux/man-pages/man8/pam_lastlog.8.html
.. |pam_mkhomedir(8)| replace:: ``pam_mkhomedir(8)``
.. _pam_mkhomedir(8): https://www.man7.org/linux/man-pages/man8/pam_mkhomedir.8.html

.. |prctl(2)| replace:: ``prctl(2)``
.. _prctl(2): https://www.man7.org/linux/man-pages/man2/prctl.2.html

.. _auth-policies:

Authentication Policies
=======================

Administrators can limit access to a MEP via a Globus authentication policy, which
verifies that the user has appropriate identities linked to their Globus account and
that the required identities have recent authentications. Authentication policies are
stored within the Globus Auth service and can be shared among multiple MEPs.

Please refer to the `Authentication Policies documentation`_ for a description of each
policy field and other useful information.

.. note::
   The ``high_assurance`` and ``authentication_assurance_timeout`` policies are only
   supported on MEPs with HA subscriptions.


Create a New Authentication Policy
----------------------------------

Administrators can create new authentication policies via the `Globus Auth API
<https://docs.globus.org/api/auth/reference/#create_policy>`_, or via the following
``configure`` subcommand options:

.. note::
  The resulting policy will be automatically applied to the MEP's ``config.yaml``.

``--auth-policy-project-id``
  The id of a Globus Auth project that this policy will belong to. If not provided,
  the user will be prompted to create one.

``--auth-policy-display-name``
  A user friendly name for the policy.

``--allowed-domains``
  A comma separated list of domains that can satisfy the policy. These may include
  wildcards.  For example, ``*.edu, globus.org``.  For more details, see
  ``domain_constraints_include`` in the `Authentication Policies documentation`_.

``--excluded-domains``
  A comma separated list of domains that will fail the policy.  These may include
  wildcards.  For example, ``*.edu, globus.org``.  For more details, see
  ``domain_constraints_exclude`` in the `Authentication Policies documentation`_.

``--auth-timeout``
  The maximum amount of time in seconds that a previous authentication must have
  occurred to satisfy the policy.  Setting this will also set ``high_assurance`` to
  ``true``.

  .. attention::

     For performance reasons, the web-service caches lookups for 60s.  Pragmatically,
     this means that smallest timeout that Compute supports is 1 minute, even though it
     is possible to set required authorizations for high assurance policies to smaller
     time intervals.


Apply an Existing Authentication Policy
---------------------------------------

Administrators can apply an authentication policy directly in the MEP's ``config.yaml``:

.. code-block:: yaml

   multi_user: true
   authentication_policy: 2340174a-1a0e-46d8-a958-7c3ddf2c834a

... or via the ``--auth-policy`` option with the ``configure`` subcommand, which will
make the necessary changes to ``config.yaml``:

.. code-block:: bash

   $ globus-compute-endpoint configure my-mep --multi-user --auth-policy 2340174a-1a0e-46d8-a958-7c3ddf2c834a


.. _function-allowlist:

Function Allow Listing
======================

To require that UEPs only allow certain functions, specify the ``allowed_functions``
top-level configuration item:

.. code-block:: yaml

   multi_user: true
   allowed_functions:
      - 6d0ba55f-de15-4af2-827d-05c50c338aa7
      - e552e7f2-c007-4671-8ca4-3a4fd84f3805

At registration, the web service will be apprised of these function identifiers, and
only tasks that invoke these functions will be sent to the UEPs of the MEP.  Any
submission that specifies non-approved function identifiers will be rebuffed with
HTTP 403 response like:

.. code-block:: text
   :caption: *Example HTTP invalid function error response via the SDK; edited for clarity*

   403
   FUNCTION_NOT_PERMITTED
   Function <function_identifier> not permitted on endpoint <MEP_identifier>

Additionally, UEPs of a function-restricted MEP will verify that tasks only use
functions from the allow list.  Given the guarantees of the API, this is a redundant
verification, but is performed locally as a precautionary measure.

There are some instances where an administrator may want to restrict different users to
different functions.  In this scenario, the administrator must specify the restricted
functions within the :ref:`Jinja template logic for the UEP configuration
<user-config-template-yaml-j2>`, and *specifically not specify any restrictions in the
parent MEP*.  In this setup, the web-service will not verify task-requested functions
as this check will be done locally by the UEP.  An example UEP configuration template
snippet might be:

.. code-block:: yaml+jinja

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

Rejections to the SDK from the UEP look slightly different:

.. code-block:: text

   Function <function_identifier> not permitted on endpoint <UEP_internal_identifier>

In the web-response, the task is not sent to the MEP site at all and the listed
endpoint identifier belongs to the MEP.  In this second error message, the UEP was
started, received the task and rejected it; the mentioned endpoint is the internal UEP
identifier, not the parent MEP identifier.

.. attention::

   By design, it is not possible to further restrict a UEP's set of allowed functions
   if the MEP has specified ``allowed_functions``.  If the template configuration
   sets ``allowed_functions`` and the MEP's ``config.yaml`` also specifies
   ``allowed_functions``, then the UEP's configuration is ignored.  The only exception
   to this is if the MEP does *not* restrict the functions, as discussed above.


.. _tracing-a-task-to-mep:

Tracing a Task to a MEP
=======================

A MEP might be thought of as an endpoint manager.  In a typical non-MEP paradigm, a
user would log in (e.g., via SSH) to a compute resource (e.g., a cluster's login-node),
create a Python virtual environment (e.g., `virtualenv`_, `pipx`_, `conda`_), and then
install and run ``globus-compute-endpoint`` from their user-space.  By contrast, a MEP
is a ``root``-installed and ``root``-run process that manages child processes for
regular users.  Upon receiving a "start endpoint" request from the Globus Compute AMQP
service, a MEP creates a user-process via the ``fork()`` |rarr| *drop privileges* |rarr|
``exec()`` pattern, and then watches that child process until it stops.  At no point
does the MEP attempt to execute tasks, nor does the MEP even see tasks |nbsp| --- |nbsp|
those are handled the same as they have been to-date, by the UEPs.  The material
difference between an endpoint started by a human and a UEP is a semantic one for
clarity of discussion: MEPs start UEPs.

The workflow for a task sent to a MEP roughly follows these steps:

#. The user acquires a MEP endpoint id (perhaps as shared by the administrator via an
   internal email, web page, or bulletin).

#. The user uses the SDK to send the task to the MEP with the ``endpoint_id``:

   .. code-block:: python
      :emphasize-lines: 6, 8

      from globus_compute_sdk import Executor

      def some_task(*a, **k):
          return 1

      mep_site_id = "..."  # as acquired from step 1
      with Executor() as ex:
          ex.endpoint_id = mep_site_id
          fut = ex.submit(some_task)
          print("Result:", fut.result())  # Reminder: blocks until result received

#. After the ``ex.submit()`` call, the SDK POSTs a REST request to the Globus Compute
   web service.

#. The Compute web-service identifies the endpoint in the request as belonging to a MEP.

#. The Compute web-service generates a UEP id specific to the tuple of the
   ``mep_site_id``, the id of the user making the request, and the endpoint
   configuration in the request (e.g., ``tuple(site_id, user_id, conf)``) |nbsp| ---
   |nbsp| this identifier is simultaneously stable and unique.

#. The Compute web-service sends a start-UEP message to the MEP (via AMQP), asking it to
   start an endpoint as the user that initiated the REST request and identified by the
   id generated in the previous step.

#. The MEP maps the Globus Auth identity in the start-UEP-request to a local (POSIX)
   username.

#. The MEP ascertains the host-specific UID based on a |getpwnam(3)|_ call with the
   local username from the previous step.

#. The MEP starts a UEP as the UID from the previous step.

#. The just-started UEP checks in with the Globus Compute web-services.

#. The web-services will see the check-in and then complete the original request to the
   SDK, accepting the task and submitting it to the now-started UEP.

The above workflow may be of interest to system administrators from a "How does this
work in theory?" point of view, but will be of little utility to most users.  The part
of interest to most end users is the on-the-fly custom configuration.  If the
administrator has provided any hook-in points in ``user_config_template.yaml.j2`` (e.g.,
an account id), then a user may specify that via the ``user_endpoint_config`` argument
to the Executor constructor or for later submissions:

.. code-block:: python
   :caption: Utilizing the ``.user_endpoint_config`` via both a constructor call, and
      an ad-hoc change
   :emphasize-lines: 9, 13

   from globus_compute_sdk import Executor

   def jittery_multiply(a, b):
       return a * b + (1 - random.random()) * (1 + abs(a - b))

   mep_site_id = "..."  # as acquired from step 1
   with Executor(
       endpoint_id=mep_site_id,
       user_endpoint_config={"account_id": "user_allocation_account_id"},
   ) as ex:
       futs = [ex.submit(jittery_multiply, 2, 7)]

       ex.user_endpoint_config["account_id"] = "different_allocation_id"
       futs = [ex.submit(jittery_multiply, 13, 11)]

       # Reminder: .result() blocks until result received
       results = list[f.result() for f in futs]
       print("Result:", results)

N.B. this is example code highlighting the ``user_endpoint_config`` attribute of the
``Executor`` class; please generally consult the :doc:`../executor` documentation.


Administrator Quickstart
========================

#. :ref:`Install the Globus Compute Agent package <repo-based-installation>`

#. Quickly verify that installation succeeded and the shell environment points to the
   correct path:

   .. code-block:: console

      # command -v globus-compute-endpoint
      /usr/sbin/globus-compute-endpoint

#. Create a Multi-User Endpoint configuration with the ``--multi-user`` flag
   to the ``configure`` subcommand:

   .. code-block:: console

      # globus-compute-endpoint configure --multi-user prod_gpu_large
      Created multi-user profile for endpoint named <prod_gpu_large>

          Configuration file: /root/.globus_compute/prod_gpu_large/config.yaml

          Example identity mapping configuration: /root/.globus_compute/prod_gpu_large/example_identity_mapping_config.json

          User endpoint configuration template: /root/.globus_compute/prod_gpu_large/user_config_template.yaml.j2
          User endpoint configuration schema: /root/.globus_compute/prod_gpu_large/user_config_schema.json
          User endpoint environment variables: /root/.globus_compute/prod_gpu_large/user_environment.yaml

      Use the `start` subcommand to run it:

          $ globus-compute-endpoint start prod_gpu_large

#. Setup the identity mapping configuration |nbsp| --- |nbsp| this depends on your
   site's specific requirements and may take some trial and error.  The key point is to
   be able to take a Globus Auth Identity set, and map it to a local username *on this
   resource* |nbsp| --- |nbsp| this resulting username will be passed to |getpwnam(3)|_
   to ascertain a UID for the user.  This file is linked in ``config.yaml`` (from the
   previous step's output), and, per initial configuration, is set to
   ``example_identity_mapping_config.json``.  While the configuration is syntactically
   valid, it references ``example.com`` so will not work until modified.   Please refer
   to the `Globus Connect Server Identity Mapping Guide`_ for help updating this file.

#. Modify ``user_config_template.yaml.j2`` as appropriate for the resources to make
   available.  This file will be interpreted as a `Jinja template`_ and will be rendered
   with user-provided variables to generate the final UEP configuration.  The default
   configuration (as created in step 4) has a basic working configuration, but uses the
   ``LocalProvider``.

   Please look to :doc:`endpoint_examples` (all written for single-user use) as a
   starting point.

#. Optionally modify ``user_config_schema.json``; the file, if it exists, defines the
   `JSON schema`_ against which user-provided variables are validated.  Writing JSON
   schemas is out of scope for this documentation, but we do specifically recognize
   ``additionalProperties: true`` which makes the default schema very permissive: any
   key not specifically specified in the schema *is treated as valid*.

#. Modify ``user_environment.yaml`` for any environment variables that should be
   injected into the user endpoint process space:

   .. code-block:: yaml

      SOME_SITE_SPECIFIC_ENV_VAR: a site specific value
      PATH: /site/specific:/path:/opt:/usr:/some/other/path

#. Run MEP manually for testing and easier debugging, as well as to collect the
   (Multi‑User) endpoint ID for sharing with users.  The first time through, the Globus
   Compute endpoint will initiate a Globus Auth login flow, and present a long URL:

   .. code-block:: console

      # globus-compute-endpoint start prod_gpu_large
      > Endpoint Manager initialization
      Please authenticate with Globus here:
      ------------------------------------
      https://auth.globus.org/v2/oauth2/authorize?clie...&prompt=login
      ------------------------------------

      Enter the resulting Authorization Code here: <PASTE CODE HERE AND PRESS ENTER>

#. While iterating, the ``--log-to-console`` flag may be useful to emit the log lines to
   the console (also available at ``.globus_compute/prod_gpu_large/endpoint.log``).

   .. code-block:: console

      # globus-compute-endpoint start prod_gpu_large --log-to-console
      >

      ========== Endpoint Manager begins: 1ed568ab-79ec-4f7c-be78-a704439b2266
              >>> Multi-User Endpoint ID: 1ed568ab-79ec-4f7c-be78-a704439b2266 <<<

   Additionally, for even noisier output, there is ``--debug``.

#. When ready to install as an on-boot service, install it with a ``systemd`` unit file:

   .. code-block:: console

      # globus-compute-endpoint enable-on-boot prod_gpu_large
      Systemd service installed at /etc/systemd/system/globus-compute-endpoint-prod_gpu_large.service. Run
          sudo systemctl enable globus-compute-endpoint-prod_gpu_large --now
      to enable the service and start the endpoint.

   And enable via the usual interaction:

   .. code-block:: console

      # systemctl enable globus-compute-endpoint-prod_gpu_large --now

.. |nbsp| unicode:: 0xA0
   :trim:

.. |rarr| unicode:: 0x2192
   :trim:

.. |hellip| unicode:: 0x2026

.. _`same Linux distributions as does Globus Connect Server`: https://docs.globus.org/globus-connect-server/v5/#supported_linux_distributions

.. |POSTed to the /v3/endpoints/<mep_uuid>/submit| replace:: POSTed to the ``/v3/endpoints/<mep_uuid>/submit``
.. _POSTed to the /v3/endpoints/<mep_uuid>/submit: https://compute.api.globus.org/redoc#tag/Endpoints/operation/submit_batch_v3_endpoints__endpoint_uuid__submit_post

.. _Web UI: https://app.globus.org/compute
.. _Identity Mapping documentation: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/
.. _Authentication Policies documentation: https://docs.globus.org/api/auth/developer-guide/#authentication_policy_fields
.. |globus-identity-mapping| replace:: ``globus-identity-mapping``
.. _globus-identity-mapping: https://pypi.org/project/globus-identity-mapping/
.. |getpwnam(3)| replace:: ``getpwnam(3)``
.. _getpwnam(3): https://www.man7.org/linux/man-pages/man3/getpwnam.3.html
.. _Jinja template: https://jinja.palletsprojects.com/en/3.1.x/
.. _Globus Connect Server Identity Mapping Guide: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#mapping_recipes
.. _#help on the Globus Compute Slack: https://funcx.slack.com/archives/C017637NZFA
.. |UserRuntime| replace:: :class:`UserRuntime <globus_compute_sdk.sdk.batch.UserRuntime>`
.. _JSON schema: https://json-schema.org/

.. |PamConfiguration| replace:: :class:`PamConfiguration <globus_compute_endpoint.endpoint.config.pam.PamConfiguration>`

.. _virtualenv: https://pypi.org/project/virtualenv/
.. _pipx: https://pypa.github.io/pipx/
.. _conda: https://docs.conda.io/en/latest/
.. _dill: https://pypi.org/project/dill/
.. _Pluggable Authentication Modules: https://en.wikipedia.org/wiki/Linux_PAM
