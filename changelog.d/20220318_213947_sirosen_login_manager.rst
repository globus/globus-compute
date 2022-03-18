Added
^^^^^

- The ``FuncXClient`` now accepts a new boolean argument ``do_version_check``,
  which can be used to suppress a call to the FuncX API on initialization. Use
  as in ``FuncXClient(do_version_check=False)``. This may lead to faster
  instantiation of clients in some cases.

Removed
^^^^^^^

- The following arguments to ``FuncXClient`` are no longer supported:
  ``force_login``, ``fx_authorizer``, ``search_authorizer``,
  ``openid_authorizer``. The use-cases for these arguments are now satisfied by
  the ability to pass a custom ``LoginManager`` to the client class, if desired.

Changed
^^^^^^^

- The ``FuncXClient`` constructor has been refactored. It can no longer be
  passed authorizers for various sub-services. Instead, a new component, the
  ``LoginManager``, has been introduced which makes it possible to pass
  arbitrary globus-sdk client objects for services (by passing a customized
  login manager). The default behavior remains the same, checking login and
  doing a new login on init.

- Tokens are now stored in a new location, in a sqlite database, using
  ``globus_sdk.tokenstorage``. Users will need to login again after upgrading
  from past versions of ``funcx``.
