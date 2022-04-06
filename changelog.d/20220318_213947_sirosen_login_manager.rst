Added
^^^^^

- The ``FuncXClient`` now accepts a new argument ``login_manager``, which is
  expected to implement a protocol for providing authenticated http client
  objects, login, and logout capabilities.

- The login manager and its protocol are now defined and may be imported as in
  ``from funcx.sdk.login_manager import LoginManager, LoginManagerProtocol``.
  They are internal components but may be used to force a login or to implement
  an alternative ``LoginManagerProtocol`` to customize authentication

Deprecated
^^^^^^^^^^

- The following arguments to ``FuncXClient`` are deprecated and will emit
  warnings if used: ``fx_authorizer``, ``search_authorizer``,
  ``openid_authorizer``. The use-cases for these arguments are now satisfied by
  the ability to pass a custom ``LoginManager`` to the client class, if desired.

Removed
^^^^^^^

- The following arguments to ``FuncXClient`` are no longer supported:
  ``force_login``

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
