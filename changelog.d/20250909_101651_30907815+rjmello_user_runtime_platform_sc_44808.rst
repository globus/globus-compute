New Functionality
^^^^^^^^^^^^^^^^^

- We've deprecated the ``user_runtime.python_version`` reserved template variable in favor
  of more granular variables to facilitate leveraging Python installation information in
  the configuration template:

  - ``user_runtime.python.version``: Python version string (e.g., ``"3.13.7"``)
  - ``user_runtime.python.version_tuple``: Python version as a tuple (e.g., ``(3, 13, 7)``)
  - ``user_runtime.python.version_info``: Python version info from ``tuple(sys.version_info)``
    (e.g., ``(3, 13, 7, "final", 0)``)
  - ``user_runtime.python.compiler``: Python implementation (e.g., ``"CPython"``)
  - ``user_runtime.python.implementation``: String identifying the compiler used for compiling
    Python (e.g., ``"Clang 14.0.6"``)

  The ``user_runtime.python_version`` variable provides the complete ``sys.version`` string,
  which contains useful information but is hard to utilize in the template.

- Added reserved template variables containing general platform information from the user's
  installation:

  - ``user_runtime.platform.architecture``: Host architecture tuple (e.g., ``("64bit", "ELF")``)
  - ``user_runtime.platform.machine``: Host machine type (e.g., ``"x86_64"``)
  - ``user_runtime.platform.node``: Host node name (e.g., ``"login03"``)
  - ``user_runtime.platform.platform``: Host platform (e.g., ``"Linux-6.14.0-29-generic-x86_64-with-glibc2.39"``)
  - ``user_runtime.platform.processor``: Host processor name (e.g., ``"x86_64"``)
  - ``user_runtime.platform.release``: Host OS release (e.g., ``"6.16.5-2-generic"``)