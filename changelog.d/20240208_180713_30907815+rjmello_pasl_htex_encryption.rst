New Functionality
^^^^^^^^^^^^^^^^^

- Upgraded Parsl to version ``2024.02.05`` to enable encryption for the ``GlobusComputeEngine``.
  Under the hood, Parsl uses CurveZMQ to encrypt all communication channels between the engine
  and related nodes.

  We enable encryption by default, but users can disable it by setting the ``encrypted``
  configuration variable under the ``engine`` stanza to ``false``.

  E.g.,

  .. code-block:: yaml

    engine:
      type: GlobusComputeEngine
      encrypted: false

  Depending on the installation, encryption might noticeably degrade throughput performance.
  If this is an issue for your workflow, please refer to `Parsl's documentation on encryption
  performance <https://parsl.readthedocs.io/en/stable/userguide/execution.html#encryption-performance>`_
  before disabling encryption.