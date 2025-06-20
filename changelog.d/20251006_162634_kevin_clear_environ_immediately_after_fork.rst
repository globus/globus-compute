Bug Fixes
^^^^^^^^^

- Clear environment immediately after ``fork()``, prior even to dropping
  privileges.  (The environment was already cleared prior to ``exec()`` ing the
  child UEP, but there's no sense in waiting during the pre-exec phase.)  For
  the motivating behavior, ``GLOBUS_COMPUTE_USER_DIR`` will no longer be
  transparently passed to the child process.  All UEP environment variables
  must be set statically via the :ref:`user-environment-yaml` UEP configuration
  file or using :ref:`pam`.
