New Functionality
^^^^^^^^^^^^^^^^^

- Added the following arguments to ``globus-compute-endpoint configure``, which allow
  on-the-fly creation of Globus authentication policies while configuring Compute
  endpoints. See ``globus-compute-endpoint configure --help`` for more details.

  - ``--auth-policy-project-id``
  - ``--auth-policy-display-name``
  - ``--auth-policy-description``
  - ``--allowed-domains``
  - ``--excluded-domains``
  - ``--auth-timeout``

Changed
^^^^^^^

- Endpoint ``LoginManager`` s now request the ``AuthScopes.manage_projects`` scope, in
  order to create auth projects during the auth policy creation flow.

- The minimum version of ``globus-sdk`` that is compatible with ``globus-compute-sdk``
  and ``globus-compute-endpoint`` is now 3.35.0.
