New Functionality
^^^^^^^^^^^^^^^^^

- Added support for the new Globus subscription management service. An endpoint can be
  associated with a subscription group via the ``--subscription-id`` flag to
  ``globus-compute-endpoint configure``, or via the ``subscription_id`` option in
  ``config.yaml``:

  .. code-block:: yaml

    subscription_id: 12345678-9012-3456-7890-123456789012
    engine:
      type: GlobusComputeEngine
      ...
