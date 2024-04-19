Bug Fixes
^^^^^^^^^

- We now raise an informative error when a user sets the ``strategy`` configuration field
  to an incorrect value type for a given engine. For example, the ``GlobusComputeEngine``
  expects ``strategy`` to be a string or null, not an object.