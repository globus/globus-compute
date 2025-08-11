Multi-User Endpoints
********************

This guide builds on the :doc:`endpoints` to cover the additional features and
configuration steps needed to set up multi-user endpoints.

Multi-user endpoints use the same installation process as regular endpoints.
See :doc:`installation` for more information.

.. tip::

   For those just looking to get up and running, see the
   `Administrator Quickstart`_, below.


Overview
========

Multi-user endpoints enable administrators to securely offer compute resources
to users without requiring shell access (e.g., SSH).  These endpoints support
all features described in the :doc:`endpoints` page, plus the ability to map
Globus Auth identities of function-submitting users to local POSIX user
accounts, then launch user endpoint processes as those mapped users.

For a detailed look at how a task makes its way to a user endpoint process, see
:ref:`tracing-a-task`.


Key Benefits
============

For Administrators
------------------

The biggest benefit of a multi-user endpoint setup is a lowering of the barrier
for legitimate users of a site.  To date, knowledge of the command line has been
critical to most users of High Performance Computing (HPC) systems, though only
as a necessity of infrastructure rather than a legitimate scientific purpose.  A
multi-user endpoint allows a user to ignore many of the important-but-not-really
details of plumbing, like logging in through SSH, restarting user-only daemons.
The only thing they need to do is run their scripts locally on their own
workstation, and the rest "just works."

Another boon for administrators is the ability to fine-tune and pre-configure
what resources users may utilize.  For example, many users struggle to discover
which interface is routed to a cluster's internal network; the administrator can
preset that, completely bypassing the question.  Using `ALCF's Polaris
<https://www.alcf.anl.gov/polaris>`_ as an example, the administrator could use
the following user configuration template (``user_config_template.yaml.j2``) to
place all jobs sent to this multi-user endpoint on the ``debug-scaling`` queue,
and pre-select the obvious defaults
(`per the documentation <https://docs.alcf.anl.gov/polaris/running-jobs/>`_):

.. code-block:: yaml+jinja
   :caption: ``/root/.globus_compute/debug_scaling_ep/user_config_template.yaml.j2``

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
``WORKER_INIT_COMMAND`` and ``NODES_PER_BLOCK`` variables.  If the user's jobs
finish and no more work comes in after ``max_idletime`` seconds (30s), the user
endpoint process will scale down and consume no more wall time.

For Users
---------

Under the multi-user paradigm, users largely benefit from not having to be quite
so aware of an endpoint and its configuration.  As the administrator will have
taken care of most of the smaller details (c.f., installation, internal
interfaces, queue policies), the user is able to write a consuming script,
knowing only the endpoint ID and their system accounting username:

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

It is a boon for the researcher to see the relevant configuration variables
immediately adjacent to the code, as opposed to hidden in the endpoint
configuration and behind an opaque endpoint ID.  A multi-user endpoint removes
almost half of the infrastructure plumbing that the user must manage |nbsp| ---
|nbsp| many users will barely even need to open their own terminal, much less an
SSH terminal on a login node.


.. _multi-user-configuration:

Configuring a Multi-User Endpoint
=================================

The ``configure`` subcommand must be run as a privileged user (e.g., root) to
properly generate the :ref:`multi-user-config-yaml` and
:ref:`example-idmap-config` files, along with other default files in
``$HOME/.globus_compute/``:

.. code-block:: console

   # globus-compute-endpoint configure my_mu_ep
   Created multi-user profile for endpoint named <my_mu_ep>

       Configuration file: /root/.globus_compute/my_mu_ep/config.yaml

       Example identity mapping configuration: /root/.globus_compute/my_mu_ep/example_identity_mapping_config.json

       User endpoint configuration template: /root/.globus_compute/my_mu_ep/user_config_template.yaml.j2
       User endpoint configuration schema: /root/.globus_compute/my_mu_ep/user_config_schema.json
       User endpoint environment variables: /root/.globus_compute/my_mu_ep/user_environment.yaml

   Use the `start` subcommand to run it:

   globus-compute-endpoint start my_mu_ep


.. _multi-user-config-yaml:

``config.yaml``
---------------

The default multi-user endpoint ``config.yaml`` file contains one additional
field to specify the identity mapping file path:

.. code-block:: yaml
   :caption: The default multi-user ``config.yaml`` configuration
   :emphasize-lines: 3

   amqp_port: 443
   display_name: null
   identity_mapping_config_path: /root/.globus_compute/my_mu_ep/example_identity_mapping_config.json

Please refer to :ref:`endpoint-manager-config` for details on each field.


.. _example-idmap-config:

``example_identity_mapping_config.json``
----------------------------------------

This is a valid-syntax-but-will-never-successfully-map example identity mapping
configuration file.  It is a JSON list of identity mapping configurations that
will be tried in order.  By implementation within the endpoint code base, the
first configuration to return a match "wins."  In this example, the first
configuration is a call out to an external tool, as specified by the
|idmap_external|_ DATA_TYPE.  The command is a list of arguments, with the first
element as the actual executable.  In this case, the flags are strictly
illustrative, as ``/bin/false`` always returns with a non-zero exit code and so
will be ignored by the |globus-identity-mapping|_ logic.  However, if the site
requires custom or special logic to acquire the correct local username, this
executable must accept a |idmap_input|_ JSON document via ``stdin`` and output a
|idmap_output|_ JSON document to ``stdout``.

The second configuration in this example is an |idmap_expression|_, which means
it uses a subset of regular expression syntax to search for a suitable POSIX
username.  This configuration searches the ``username`` field from the passed
identity set for a value that ends in ``@example.com``.  The library appends the
``^`` and ``$`` anchors to the regex before searching, so the actual regular
expression used would be ``^(.*)@example.com$``.  Finally, if a match is found,
the first saved group is the output (i.e., ``{0}``).  If the ``username`` field
contained ``mickey97@example.com``, then this configuration would return
``mickey97``, and the MEP would then use |getpwnam(3)|_ to look up ``mickey97``.
But if the username field(s) did not end with ``@example.com``, then it would
not match and the start request would fail.

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

* ``expression_identity_mapping#1.0.0`` |nbsp| --- |nbsp| Regular Expression
  based mapping applies an administrator-defined regular expression against any
  field in the input identity documents, returning ``None`` or the matched
  string.  (Example below.)

* ``external_identity_mapping#1.0.0`` |nbsp| --- |nbsp| Invoke an
  administrator-defined external process, passing the input identity documents
  via ``stdin``, and reading the response from ``stdout``.

.. tip::

   While developing this file, administrators may appreciate using the
   ``globus-idm-validator`` tool.  This script is installed as part of the
   |globus-identity-mapping|_ dependency.

The manager endpoint process watches this file for changes.  If an administrator
needs to make a live change, simply update the content of the identity mapping
file specified by the ``config.yaml`` configuration.  The manager endpoint
process will note the change, and atomically apply it: if the new identity
mapping configuration is invalid, the previously loaded configuration will
remain in place.  In both cases (valid or invalid), the endpoint will emit a
message to the log.

``expression_identity_mapping#1.0.0``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For example, a simple policy might require that users of a system have an email
address at your institution or department.  The identity mapping configuration
might be:

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

This user has linked both identities, so both identities are in the identity
set.  Per the configuration, the first identity will not match either regex, but
the second (``roberto@your_institution.com``) will, and the returned username
would be ``roberto``.  Note that any field could be tested, but this example
used``email``.

``external_identity_mapping#1.0.0``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes, more complicated logic may be required (e.g., LDAP lookups), in which
case consider the ``external_identity_mapping#1.0.0`` configuration stanza.  The
administrator may write a script (or generally, an executable) for the required
custom logic.  The script will be passed a ``identity_mapping_input#1.0.0`` JSON
document via ``stdin``, and must output a ``identity_mapping_output#1.0.0`` JSON
document on ``stdout``.

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

The executable must identify the successfully mapped identity in the output
document by the ``id`` field.  For example, if an LDAP lookup of
``alicia@legal.your_institution.com`` were to result in ``Alicia`` for this
endpoint host, then the output document might read:

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

   Reminder that the identity mapping configuration is a JSON *list*.  Multiple
   mappings may be defined, and each will be tried in order until one maps the
   identity successfully or no mappings are possible.

For a much more thorough dive into identity mapping configurations, please
consult the Globus Connect Server's `Identity Mapping documentation`_.

.. |idmap_external| replace:: ``external_identity_mapping#1.0.0``
.. _idmap_external: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#external_program_reference
.. |idmap_expression| replace:: ``expression_identity_mapping#1.0.0``
.. _idmap_expression: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#expression_reference
.. |idmap_input| replace:: ``identity_mapping_input#1.0.0``
.. _idmap_input: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#input_document
.. |idmap_output| replace:: ``identity_mapping_output#1.0.0``
.. _idmap_output: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#output_document


Starting the Multi-User Endpoint
================================

A multi-user endpoint requires a privileged local user account (e.g., ``root``)
to start, enabling the manager endpoint process to perform identity mapping and
drop privileges to mapped user accounts.  Apart from this initial setup
requirement, multi-user endpoints operate identically to regular endpoints for
starting and stopping:

.. code-block:: console

   # globus-compute-endpoint start my_mu_ep
         >>> Endpoint ID: [endpoint_uuid] <<<
   ----> Wed Aug  6 20:03:02 2025

Each user endpoint process runs as the mapped local user, ensuring secure
isolation of execution environments:

.. code-block:: text
   :caption: Multi-user endpoint process hierarchy

   Manager Endpoint Process (root)
   ├── User Endpoint Process (alice, UID: 1001)
   ├── User Endpoint Process (bob, UID: 1002)
   └── User Endpoint Process (eve, UID: 1003)


.. _mep-as-a-service:

Installing as a Service
=======================

Installing the multi-user endpoint as a service is the same :ref:`procedure as
with a regular endpoint <enable_on_boot>`: use the ``enable-on-boot``.  This
will dynamically create and install a systemd unit file.


.. _pam:

Pluggable Authentication Modules (PAM)
======================================

`Pluggable Authentication Modules`_ (PAM) allows administrators to configure
site-specific authentication schemes with arbitrary requirements.  For example,
where one site might require users to use `MFA`_, another site could disallow
use of the system for some users at certain times of the day.  Rather than
rewrite or modify software to accommodate each site's needs, administrators can
simply change their site configuration.

As a brief intro to PAM, the architecture is designed with four phases:

- authentication
- account management
- session management
- password management

The multi-user endpoint implements *account* and *session management*.  If
enabled, then the child process will create a PAM session, check the account
(|pam_acct_mgmt(3)|_), and then open a session (|pam_open_session(3)|_).  If
these two steps succeed, then the manager endpoint process will continue to drop
privileges.  But in these two steps is where the administrator can implement
custom configuration.

PAM is configured in two parts.  For the ``config.yaml`` file, use the ``pam``
field:

.. code-block:: yaml
   :caption: ``config.yaml`` to show PAM
   :emphasize-lines: 2,3

   identity_mapping_config_path: .../some/idmap.json
   pam:
     enable: true

This configuration will choose the default PAM service name,
``globus-compute-endpoint`` (see |PamConfiguration|).  The service name is the
name of the PAM configuration file in ``/etc/pam.d/``.  Use ``service_name`` to
tell the endpoint to authorize users against a different PAM configuration:

.. code-block:: yaml
   :caption: ``config.yaml`` with a custom PAM service name
   :emphasize-lines: 6

   identity_mapping_config_path: .../some/idmap.json
   pam:
     enable: true

     # the PAM routines will look for `/etc/pam.d/gce-ep123-specific-requirements`
     service_name: gce-ep123-specific-requirements

For clarity, note that the service name is simply passed to |pam_start(3)|_, to
tell PAM which service configuration to apply.

.. important::

  If PAM is not enabled, then before starting user endpoint processes, the child
  process drops all capabilities and sets the no-new-privileges flag with the
  kernel.  (See |prctl(2)|_ and reference ``PR_SET_NO_NEW_PRIVS``).  In
  particular, this will preclude use of SETUID executables, which can break some
  schedulers.  If your site requires use of SETUID executables, then PAM must be
  enabled.

Though configuring PAM itself is outside the scope of this document (e.g., see
|PAM_SAG|_), we briefly discuss a couple of modules to share a taste of what PAM
can do.  For example, if the administrator were to implement a configuration of:

.. code-block:: text
   :caption: ``/etc/pam.d/globus-compute-endpoint``

   account   requisite     pam_shells.so
   session   required      pam_limits.so

then, per |pam_shells(8)|_, any user endpoint process for a user whose shell is
not listed in ``/etc/shells`` will not start and the logs will have a line like:

.. code-block:: text

   ... (error code: 7 [PAM_AUTH_ERR]) Authentication failure

On the other end, the user's SDK would receive a message like:

.. code-block:: text

   Request payload failed validation: Unable to start user endpoint process for jessica [exit code: 71; (PermissionError) see your system administrator]

Similarly, for users who are administratively allowed (i.e., have a valid
shell), the |pam_limits(8)|_ module will install the admin-configured process
limits.

.. hint::

   The Globus Compute Endpoint software implements the account management and
   session phases of PAM.  As authentication is enacted via Globus Auth and
   :ref:`Identity Mapping <identity-mapping>`, it does not use PAM's
   authentication (|pam_authenticate(3)|_) phase, nor does it attempt to manage
   the user's password.  Functionally, this means that only PAM configuration
   lines that begin with ``account`` and ``session`` will be utilized.

Look to PAM for a number of tasks (which we tease here, but are similarly out of
scope of this documentation):

- Setting user endpoint process capabilities (|pam_cap(8)|_)
- Setting user endpoint process limits (|pam_limits(8)|_)
- Setting environment variables (|pam_env(8)|_)
- Enforcing ``/var/run/nologin`` (|pam_nologin(8)|_)
- Updating ``/var/log/lastlog`` (|pam_lastlog(8)|_)
- Create user home directory on demand (|pam_mkhomedir(8)|_)

(If the available PAM modules do not fit the bill, it is also possible to write
a custom module!  But sadly, that is also out of scope of this documentation;
please see |PAM_MWG|_.)

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


Authentication Policies
=======================

Administrators can use a `Globus authentication policy`_ to limit access to a
multi-user endpoint by enforcing that the user has appropriate identities linked
to their Globus account and that the required identities have recent
authentications.

Please refer to :ref:`auth-policies` for more information.


Administrator Quickstart
========================

#. :ref:`Install the Globus Compute Agent package <repo-based-installation>`

#. Quickly verify that installation succeeded and the shell environment points
   to the correct path:

   .. code-block:: console

      # command -v globus-compute-endpoint
      /usr/sbin/globus-compute-endpoint

#. Create a Multi-User Endpoint configuration by running the ``configure``
   subcommand as a privileged user (e.g., root):

   .. code-block:: console

      # globus-compute-endpoint configure prod_gpu_large
      Created multi-user profile for endpoint named <prod_gpu_large>

          Configuration file: /root/.globus_compute/prod_gpu_large/config.yaml

          Example identity mapping configuration: /root/.globus_compute/prod_gpu_large/example_identity_mapping_config.json

          User endpoint configuration template: /root/.globus_compute/prod_gpu_large/user_config_template.yaml.j2
          User endpoint configuration schema: /root/.globus_compute/prod_gpu_large/user_config_schema.json
          User endpoint environment variables: /root/.globus_compute/prod_gpu_large/user_environment.yaml

      Use the `start` subcommand to run it:

          $ globus-compute-endpoint start prod_gpu_large

#. Set up the identity mapping configuration |nbsp| --- |nbsp| this depends on
   your site's specific requirements and may take some trial and error.  The key
   point is to be able to take a Globus Auth Identity set, and map it to a local
   username *on this resource* |nbsp| --- |nbsp| this resulting username will be
   passed to |getpwnam(3)|_ to ascertain a UID for the user.  This file is
   linked in ``config.yaml`` (from the previous step's output), and, per initial
   configuration, is set to ``example_identity_mapping_config.json``.  While the
   configuration is syntactically valid, it references ``example.com`` so will
   not work until modified.   Please refer to the
   `Globus Connect Server Identity Mapping Guide`_ for help updating this file.

#. Modify ``user_config_template.yaml.j2`` as appropriate for the resources to
   make available.  This file will be interpreted as a `Jinja template`_ and
   will be rendered with user-provided variables to generate the final user
   endpoint process configuration.  The default configuration (as created in
   step 4) has a basic working configuration, but uses the ``LocalProvider``.

   Please look to :doc:`endpoint_examples` as a starting point.

#. Optionally modify ``user_config_schema.json``; the file, if it exists,
   defines the `JSON schema`_ against which user-provided variables are
   validated.  Writing JSON schemas is out of scope for this documentation, but
   we do specifically recognize ``additionalProperties: true`` which makes the
   default schema very permissive: any key not specifically specified in the
   schema *is treated as valid*.

#. Modify ``user_environment.yaml`` for any environment variables that should be
   injected into the user endpoint process space:

   .. code-block:: yaml

      SOME_SITE_SPECIFIC_ENV_VAR: a site specific value
      PATH: /site/specific:/path:/opt:/usr:/some/other/path

#. Run multi-user endpoint manually for testing and easier debugging, as well as
   to collect the endpoint ID for sharing with users.  The first time through,
   the endpoint will initiate a Globus Auth login flow, and present a long URL:

   .. code-block:: console

      # globus-compute-endpoint start prod_gpu_large
      > Endpoint Manager initialization
      Please authenticate with Globus here:
      ------------------------------------
      https://auth.globus.org/v2/oauth2/authorize?clie...&prompt=login
      ------------------------------------

      Enter the resulting Authorization Code here: <PASTE CODE HERE AND PRESS ENTER>

#. While iterating, the ``--log-to-console`` flag may be useful to emit the log
   lines to the console (also available at
   ``.globus_compute/prod_gpu_large/endpoint.log``).

   .. code-block:: console

      # globus-compute-endpoint start prod_gpu_large --log-to-console
      >

      ========== Endpoint Manager begins: 1ed568ab-79ec-4f7c-be78-a704439b2266
              >>> Multi-User Endpoint ID: 1ed568ab-79ec-4f7c-be78-a704439b2266 <<<

   Additionally, for even noisier output, there is ``--debug``.

#. When ready to install as an on-boot service, install it with a ``systemd``
   unit file:

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

.. _Identity Mapping documentation: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/
.. |globus-identity-mapping| replace:: ``globus-identity-mapping``
.. _globus-identity-mapping: https://pypi.org/project/globus-identity-mapping/
.. |getpwnam(3)| replace:: ``getpwnam(3)``
.. _getpwnam(3): https://www.man7.org/linux/man-pages/man3/getpwnam.3.html
.. _Jinja template: https://jinja.palletsprojects.com/en/3.1.x/
.. _Globus authentication policy: https://docs.globus.org/api/auth/developer-guide/#authentication-policies
.. _Globus Connect Server Identity Mapping Guide: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#mapping_recipes
.. _JSON schema: https://json-schema.org/

.. |PamConfiguration| replace:: :class:`PamConfiguration <globus_compute_endpoint.endpoint.config.pam.PamConfiguration>`

.. _Pluggable Authentication Modules: https://en.wikipedia.org/wiki/Linux_PAM
