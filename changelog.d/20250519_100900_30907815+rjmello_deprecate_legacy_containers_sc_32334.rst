Deprecated
^^^^^^^^^^

- Deprecated the following legacy container features:

  - :attr:`Executor.container_id <globus_compute_sdk.Executor.container_id>` attribute
  - :class:`ContainerSpec <globus_compute_sdk.sdk.container_spec.ContainerSpec>` class
  - ``container_uuid`` argument to :meth:`Client.register_function <globus_compute_sdk.Client.register_function>` method
  - :meth:`Client.get_container <globus_compute_sdk.Client.get_container>` method
  - :meth:`Client.register_container <globus_compute_sdk.Client.register_container>` method
  - :meth:`Client.build_container <globus_compute_sdk.Client.build_container>` method
  - :meth:`Client.get_container_build_status <globus_compute_sdk.Client.get_container_build_status>` method

  Container functionality has moved to the endpoint configuration. For more information,
  see the Compute endpoint `containerized environments documentation
  <https://globus-compute.readthedocs.io/en/latest/endpoints/endpoints.html#containerized-environments>`_.
