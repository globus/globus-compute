New Functionality
^^^^^^^^^^^^^^^^^

- Added ``Client.get_allowed_functions`` for retrieving the list of functions that are
  allowed to be executed on an endpoint.

Removed
^^^^^^^

- The ``add_to_whitelist``, ``delete_from_whitelist``, and ``get_whitelist`` functions
  have been removed from the ``Client``. Use the ``allowed_functions`` endpoint config
  option instead of the add/remove functions, and ``Client.get_allowed_functions``
  instead of ``get_whitelist``.
