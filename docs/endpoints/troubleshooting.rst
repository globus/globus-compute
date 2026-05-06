.. _troubleshooting:

Troubleshooting / FAQ
*********************

This page provides troubleshooting tips and answers to frequently asked questions about
Globus Compute Endpoints.

"Identity failed to map to a local user name" error
===================================================

This happens when a multi-user endpoint doesn't recognize the submitting user identity,
because the identity mapping configuration either is incorrect or intentionally excludes
that identity.

.. code-block:: text
    :caption: Example error response, edited for clarity

    ComputeAPIError: (
        'POST',
        'http://compute.api.globus.org/v3/endpoints/[ENDPOINT_ID]/submit',
        'Bearer',
        422, 'SEMANTICALLY_INVALID',
        'Request payload failed validation: Identity failed to map to a local user name.  (LookupError) \n  Globus effective identity: 3c085472-314d-4f22-abc6-591f2767af2b\n  Globus username: alice@example.edu
    )

**Users**: contact the administrator of the endpoint in question; if you should have
access and see this error, the endpoint is likely misconfigured.

**Administrators**: if the user should be allowed to submit to the endpoint, there is
likely a bug in the identity mapping configuration. The `Globus Connect Server Identity
Mapping guide`__ is a useful reference to brush up on. While iterating, the
|globus-idm-validator|_ tool, installed along with the endpoint package, can be used to
quickly test the output of an endpoint's identity mapping configuration without
submitting any tasks.

__ https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/

It's also useful to check if the user is actually associated with an identity the
endpoint supports. If the endpoint has debug logging enabled, the submitting user's
identity set can be found in the logs by searching for lines containing
``"globus_identity_set"``. This will point to a JSON list that includes the user's main
identity and any additional identities that have been linked through Globus Auth.
Without debug logging enabled, the user's main identity can still be found in log lines
containing ``Globus effective identity``, which are emitted whether or not the identity
mapping succeeded.

.. code-block:: text
    :caption: Log line examples

    # the debug-level line containing globus_identity_set, trimmed for brevity
    2026-05-08 18:49:56,320323 DEBUG MainProcess-13 CQS-281472820769184 globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber:262 _on_message CommandQueueSubscriber<✓; pid=13> Received message from 1: None, b'{"command": "cmd_start_endpoint", "globus_username": "alice@example.edu", "globus_effective_identity": "40322b9a-ccff-4e94-a6f6-4aa684bb3121", "globus_identity_set": [{"sub": "40322b9a-ccff-4e94-a6f6-4aa684bb3121", "organization": "Example University", "name": "Alice", "username": "alice@example.edu", "identity_provider": "8a896088-a03f-4f1c-88cc-ca5c7e9144e8", "identity_provider_display_name": "Example University IDP", "email": "alice@example.edu", "last_authentication": 1775771457, "identity_type": "login", "id": "40322b9a-ccff-4e94-a6f6-4aa684bb3121", "status": "used"}], ... }'

    # the JSON list of identity sets from above:
    "globus_identity_set": [
        {
            "sub": "40322b9a-ccff-4e94-a6f6-4aa684bb3121",
            "organization": "Example University",
            "name": "Alice",
            "username": "alice@example.edu",
            "identity_provider": "8a896088-a03f-4f1c-88cc-ca5c7e9144e8",
            "identity_provider_display_name": "Example University IDP",
            "email": "alice@example.edu",
            "last_authentication": 1775771457,
            "identity_type": "login",
            "id": "40322b9a-ccff-4e94-a6f6-4aa684bb3121",
            "status": "used"
        }
    ]

    # an info-level line containing "Globus effective identity"
    2026-04-10 21:54:39,313397 INFO MainProcess-112 MainThread-281472913129504 globus_compute_endpoint.endpoint.endpoint_manager:777 _event_loop Command process successfully forked for 'alice' (Globus effective identity: 40322b9a-ccff-4e94-a6f6-4aa684bb3121).



"Worker lost" error
===================

In the vast majority of cases, "worker lost" exceptions mean that endpoint workers are
running in a different version of Python than the submitting user.

**Users**: beside contacting their administrator, users can try installing the same
Python version that the endpoint is running in a virtual environment, and using that
virtual environment to submit tasks.

**Administrators**: configure the endpoint to accept :doc:`multiple Python versions
<../tutorials/dynamic_python_environments>`.


"SystemExit" error
==================

This happens when a manager endpoint process is unable to start a user endpoint process
for various reasons. The table below combines information from § :ref:`exit_code_table`
and § :ref:`exit_code_table_admins`; see those sections for additional context.

.. code-block:: text
   :caption: Example error response, edited for clarity

   globus_sdk.services.compute.errors.ComputeAPIError: (
     'POST',
     'https://compute.api.globus.org/v3/endpoints/[ENDPOINT_ID]/submit',
     'Bearer',
     422, 'SEMANTICALLY_INVALID',
     'Request payload failed validation: Failed to start or unexpected error:\n  (SystemExit) 73'
   )


.. table:: Possible user endpoint process exit codes
   :widths: auto
   :align: left

   +-----------------------------+---------------+-------------------------------------+
   | Python ``os`` constant name | Integer value | Likely Reason                       |
   +=============================+===============+=====================================+
   | ``os.EX_DATAERR``           | 65            | Registration of the endpoint was    |
   |                             |               | blocked by the Compute API with an  |
   |                             |               | HTTP 400 (bad request), HTTP 422    |
   |                             |               | (unprocessable entity) response;    |
   |                             |               | look to the logs for more           |
   |                             |               | information.                        |
   +-----------------------------+---------------+-------------------------------------+
   | ``os.EX_UNAVAILABLE``       | 69            | Registration of the endpoint was    |
   |                             |               | blocked by the Compute API with an  |
   |                             |               | HTTP 404 (not found), HTTP 409      |
   |                             |               | (conflict), or HTTP 423 (locked)    |
   |                             |               | response; look to the logs for more |
   |                             |               | information.                        |
   +-----------------------------+---------------+-------------------------------------+
   | ``os.EX_SOFTWARE``          | 70            | The endpoint received an unexpected |
   |                             |               | response from the Compute API.  This|
   |                             |               | might happen with a very outdated   |
   |                             |               | endpoint install.                   |
   +-----------------------------+---------------+-------------------------------------+
   | ``os.EX_CANTCREAT``         | 73            | Cannot create a PID file; another   |
   |                             |               | instance of the user process may    |
   |                             |               | still be running.                   |
   +-----------------------------+---------------+-------------------------------------+
   | ``os.EX_NOPERM``            | 77            | Missing required permissions to read|
   |                             |               | the identity mapping configuration  |
   |                             |               | file.                               |
   +-----------------------------+---------------+-------------------------------------+
   | ``os.EX_CONFIG``            | 78            | Unknown problem reading or parsing  |
   |                             |               | the identity mapping configuration  |
   |                             |               | file.                               |
   +-----------------------------+---------------+-------------------------------------+


.. |globus-idm-validator| replace:: ``globus-idm-validator``
.. _globus-idm-validator: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#validating-identity-mappings
