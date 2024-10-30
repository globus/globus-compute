Bug Fixes
^^^^^^^^^

- In cases where stdin is closed or not a TTY, we now only raise an error
  if the user requires an interactive login flow (i.e., does not have cached
  credentials).