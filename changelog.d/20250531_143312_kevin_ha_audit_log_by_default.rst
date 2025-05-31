Changed
^^^^^^^

- Under the assumption that newly configured `High Assurance`_ (HA) endpoints
  will typically require audit logging, the new endpoint configuration (i.e.,
  the :ref:`configure <multi-user-configuration>` subcommand) routine now sets
  the |audit_log_path| configuration item to a default value.  (In effect,
  audit log functionality is now opt-out, not opt-in.)
