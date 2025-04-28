Security
^^^^^^^^^

- Apply JSON escaping to the values of the ``user_runtime`` Jinja variable
  passed to the user endpoint configuration template. This matches our handling
  of user-provided template variables and helps endpoint administrators prevent
  YAML injection attacks.
