Security Posture
================

There are multiple avenues to secure a Multi-User Compute Endpoint (MEP) installation.
Administrators familiar with `Globus Connect Server`_ will recognize the concepts of
identity mapping and authentication policies to limit access to resources.  (Notably,
Globus Compute also implements the :ref:`High-Assurance <posture-ha>` aspect of Globus
Auth policies.)  But more specifically for Compute, there is also the possibility to
limit exactly which functions may be invoked, and how arguments may be serialized.

Each of these concepts is outlined below.


.. _identity-mapping:

Identity Mapping
----------------

The current security model of the Multi-User Compute Endpoint (MEP) relies heavily upon
Identity Mapping and POSIX user support.  The only job of a MEP is to start user
endpoint processes (UEP) for users on request from the Globus Compute web service.  The
actual processing of tasks is left to the individual UEPs.  This is accomplished
through the well-known ``fork()`` |rarr| *drop privileges* |rarr| ``exec()`` Unix
workflow, mimicking the approach of many other services (including Secure Shell [ssh],
Globus GridFTP, and the Apache Web server).  In this manner, all of the standard Unix
administrative user controls can be enforced.

"Mapping an identity" is the site-specific process of verifying that one identity is
equivalent to another for the purposes of a given action.  In the Globus Compute case,
this means translating a Globus Auth identity set to a local POSIX user account on the
MEP host for each start-UEP message.  For an administrator-run MEP (i.e., running as the
``root`` user), an identity mapping configuration is required, and is the main
difference from a :ref:`non-root MEP <endpoints_templating_configuration>` |nbsp| ---
|nbsp| a ``root``-owned MEP first maps the Globus Auth identity set from each start-UEP
message to a local POSIX user (i.e., a local username), before ``fork()``-ing a new
process, dropping privileges to that user, and starting the requested UEP.

See :ref:`MEP § Configuration <example-idmap-config>` for specifics and examples.


Authentication Policies
-----------------------

In addition to the identity mapping access control, administrators may also use Globus
authentication policies to narrow which identities can even send tasks to a MEP.  An
authentication policy can enforce details such as that a user has an identity from a
specific domain or has authenticated with the Globus Auth recently.  Refer to the
`Authentication Policies documentation`_ for more background and specifics on what
Globus authentication policies can do and how they fit in to a site's security posture.

Reference :ref:`MEP § Authentication Policies <auth-policies>` for more information.


Function Allow Listing
----------------------

Administrators can narrow MEP usage by limiting what functions may be requested by
tasks.  The web-service will reject any submissions that request functions not in the
MEP's configured ``allowed_functions`` list, and user endpoint processes will again
verify each task against the same list |nbsp| --- |nbsp| a check at the web-service and
a check on-site.

Please reference :ref:`MEP § Function Allow Listing <function-allowlist>` for more
detailed information.


Function Argument Serialization
-------------------------------

Administrators may also fine-tune how task arguments are deserialised by user
endpoints.  While callables (such as functions) must be serialized by |dill|_ (an
extension of Python's native |pickle|_ serializer), arguments to functions may be
restricted to the safer |JSONData| serializer.  This limits what arguments functions
may receive |nbsp| --- |nbsp| for example, a callable like another function could not
be passed |nbsp| --- |nbsp| but is safe from arbitrary code execution during
deserialization.

See :ref:`Endpoint § Restricting Submission Serialization Methods
<restrict-submission-serialization-methods>` for more information.


.. _posture-ha:

High-Assurance (HA)
-------------------

Endpoints may be designated High-Assurance (HA), which enact a couple of behavior
differences:

- audit logging is enabled, and (until configured otherwise) logs task events to
  ``audit.log`` in the endpoint directory
- enable registration of HA functions with the endpoint; HA functions may only run on
  the HA endpoint to which they were registered
- SDK uses of HA functions will similarly incur the HA policy requirements
- HA functions are cleared from all Globus-related storage (e.g., function name,
  definition, description) after 3 months of inactivity.

Reference :ref:`MEP § High-Assurance <high-assurance-mep>` for details.


Multiple Administrators of a MEP
--------------------------------

MEPs associated with a subscription may be administered by multiple Globus Auth
identities.  The identities are stated in the ``admins`` key of the ``config.yaml``:

.. code-block:: yaml
   :caption: ``config.yaml``

   multi_user: true
   subscription_id: 600ba9ac-ef16-4387-30ad-60c6cc3a6853
   admins:
     # Peter Gibbons (software engineer)
     - 10afcf74-b041-4439-7e0d-eab371767440
     # Samir Nagheenanajar (sysadmin, HPC services)
     - a6a7b9ee-be04-4e45-7832-d3737c2fafa2


These administrators are in addition to the owner of the endpoint, so the example
provided would effectively have 3 administrators, each with the ability to remotely
manage and view the endpoint's status page in the `Globus Web app`_.

.. important::

   Note that changes to this list will not go into effect until the MEP is restarted
   and registers afresh with the Globus Compute web services.


.. |nbsp| unicode:: 0xA0
   :trim:

.. |rarr| unicode:: 0x2192
   :trim:

.. |dill| replace:: ``dill``
.. _dill: https://dill.readthedocs.io/
.. |pickle| replace:: ``pickle``
.. _pickle: https://docs.python.org/3/library/pickle.html

.. |JSONData| replace:: :class:`JSONData <globus_compute_sdk.serialize.JSONData>`

.. _Authentication Policies documentation: https://docs.globus.org/api/auth/developer-guide/#authentication_policy_fields
.. _Globus Connect Server: https://www.globus.org/globus-connect-server
.. _Globus Web app: https://app.globus.org/compute
