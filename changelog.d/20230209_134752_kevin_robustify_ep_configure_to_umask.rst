Bug Fixes
^^^^^^^^^

- For new installs, handle unusual umask settings robustly.  Previously, a
  umask resulting in no execute or write permissions for the main configuration
  directory would result in an unexpected traceback for new users.  Now we
  ensure that the main configuration directory at least has the write and
  executable bits set.

Security
^^^^^^^^

- Previously, the main configuraton directory (typically ``~/.funcx/``) would
  be created honoring the users umask, typically resulting in
  world-readability.  In a typical administration, this may be mitigated by
  stronger permissions on the user's home directory, but still isn't robust.
  Now, the group and other permissions are cleared.  Note that this does _not_
  change existing installs, and only address newly instantiated funcX endpoint
  setups.
