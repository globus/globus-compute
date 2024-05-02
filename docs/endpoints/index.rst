Globus Compute Endpoints
========================

Overview
--------

A Globus Compute Endpoint is an agent (i.e., a service; a process) launched by the user
to serve as a conduit for executing functions on a computing resource.  The Compute
Endpoint agent manages the site-specific interactions for executing functions, leaving
users with only a single API necessary for running code on their laptop, campus cluster,
or even leadership‑class HPC machines.

The Globus Compute Endpoint agent may be configured as a

* single-user, single-configuration service, where the user manages the daemon lifecycle
  and a single configuration
* multi-user service, where an administrator defines a configuration template for
  users and the per-user daemon life-cycle management is automatic
* single-user, templated configuration where a regular user (non-administrator) defines
  a configuration template; the daemon life-cycle must be manually managed, but
  individual instances are managed automatically.

At a high-level, the single-user Compute Endpoint agent (CEA) is the simplest to
conceptually understand.  Most users of the CEA will install the
``globus-compute-endpoint`` package from PyPI into their home directory and simply start
(and stop) separate instances as their needs warrant.  For example:

.. code-block:: console

   $ pip install globus-compute-endpoint
   $ globus-compute-endpoint configure debug_queue
        # [ ... configure the endpoint ...]
   $ globus-compute-endpoint start debug_queue

At step 3, the agent will open two connections to the Globus Compute AMQP service (one
to receive tasks, one to transmit results) and will emit an "Endpoint UUID" to the
console.  (See the :doc:`../sdk` for details on how to submit tasks.)  The
:doc:`following section <single_user>` discusses CEA installations in more detail.

Conversely, the multi-user Compute Endpoint (MEP) does not process user tasks.  Instead,
the MEP agent is a ``root``-owned process that starts individual user Compute Endpoint
(UEP) agents upon request from the Globus Compute web-services.  The MEP approach
revolves around an administrator-provided template (so that users only need supply
fundamental metadata, like scheduler account identifiers), and mapping the "start UEP"
requests from the web-service to the correct local users.  See the
:doc:`Multi User <multi_user>` section for more information.

The MEP agent can also be run by non-``root`` users; in this setup, the agent still
receives "start UEP" commands from the web-service to start templated instances of the
configuration, but only for the registration identity.  In other words, a single-user
templated endpoint.

The following sections dive deeper into each mode of use.

.. toctree::
    :maxdepth: 1

    single_user
    multi_user
    endpoint_examples
