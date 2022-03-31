New Functionality
^^^^^^^^^^^^^^^^^

- The ``FuncXClient`` may now be passed ``do_version_check=False`` on init,
  which will lead to faster startup times

Removed
^^^^^^^

- The ``SearchHelper`` object no longer exposes a method for searching for
  endpoints, as this functionality was never fully implemented.

Deprecated
^^^^^^^^^^

- The ``openid_authorizer`` argument to FuncXClient is now deprecated. It can
  still be passed, but is ignored and will emit a ``DeprecationWarning`` if
  used
