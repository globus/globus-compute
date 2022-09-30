Changed
^^^^^^^

- Refactor ``funcx.sdk.batch.Batch.add`` method interface.  ``function_id`` and
  ``endpoint_id`` are now positional arguments, using language semantics to
  enforce their use, rather than (internal) manual ``assert`` checks.  The
  arguments (``args``) and keyword arguments (``kwargs``) arguments are no
  longer varargs, and thus no longer prevent function use of ``function_id``
  and ``endpoint_id``.
