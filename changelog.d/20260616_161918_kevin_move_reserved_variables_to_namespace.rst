Deprecated
^^^^^^^^^^

- Moved the reserved template variables ``parent_config``, ``user_runtime``,
  and ``mapped_identity`` into the variable ``_R``.  Access to these variables
  as standalone entities is now deprecated, and a warning is emitted to the
  parent endpoint log when a template uses them.

Changed
^^^^^^^

- Introduced the reserved template variable ``_R`` to hold all reserved
  variables.  The provided template variables ``parent_config``,
  ``user_runtime``, and ``mapped_identity`` should now be accessed via the
  ``_R`` namespace (e.g., ``{{ _R.mapped_identity.local.uname }}``) within the
  ``user_config_template.yaml.j2`` template.
