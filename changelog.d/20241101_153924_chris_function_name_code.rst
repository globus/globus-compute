Deprecated
^^^^^^^^^^

- Before this version, the ``function_name`` argument to ``Client.register_function``
  was not used, so it has now been deprecated. As before, function names are
  determined by the function's ``__name__`` and cannot be manually specified.
