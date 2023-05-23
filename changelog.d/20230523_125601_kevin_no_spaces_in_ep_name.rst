Bug Fixes
^^^^^^^^^

- Add validation logic to the endpoint ``configure`` subcommand to prevent
  certain classes of endpoint names.  That is, Compute Endpoints may have
  arbitrary _display_ names, but the name for use on the filesystem works best
  without, for example, spaces.  Now, the ``configure`` step will exit early
  with a (hopefully!) helpul error message explaining the problem.
