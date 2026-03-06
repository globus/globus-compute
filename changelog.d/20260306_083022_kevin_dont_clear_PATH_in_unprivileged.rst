Bug Fixes
^^^^^^^^^

- Correct oversight in :ref:`4.5.0 <changelog-4.5.0>`_ that taught user-run EPs
  to passthrough all environment variables *except* the important ``PATH``
  variable.  The ``PATH`` environment variable is now only set if it is not
  already in the environment.
