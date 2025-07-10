New Functionality
^^^^^^^^^^^^^^^^^

- Add a new ``role`` keyword argument to the ``Client.get_endpoints`` method
  that enables querying for endpoints that are accessble to, but not owned by,
  the user (e.g., MEPs). Valid options for ``role`` are ``owner`` and ``any``.
