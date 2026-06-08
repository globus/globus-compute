Changed
^^^^^^^

- DeprecationWarning is now always emitted by default, instead of being hidden.  As before,
  ``warnings.warn()`` output can be `customized <https://docs.python.org/3/library/warnings.html#warning-filter>`_.
  e.g. ``PYTHONWARNINGS="ignore::DeprecationWarning" globus-compute-endpoint start my_endpoint``
