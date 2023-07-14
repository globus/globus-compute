Bug Fixes
^^^^^^^^^

- The ``provider`` field was required in the endpoint YAML configuration but is
  not accepted by the ``ThreadPoolEngine``, rendering it unusable. The ``provider``
  field is now optional.
