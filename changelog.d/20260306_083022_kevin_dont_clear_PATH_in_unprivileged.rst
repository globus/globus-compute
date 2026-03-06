Bug Fixes
^^^^^^^^^

- The environment variables passthrough feature for non-``root`` users
  (introduced in :ref:`4.5.0 <changelog-4.5.0>`) incorrectly handled
  the ``PATH`` variable.  The behavior is corrected in line with all other
  variables (a variable is now only set if it is not already present in the
  environment).
