New Functionality
^^^^^^^^^^^^^^^^^

- Implement ability to launch workers in containerized environments, with support for
  Docker, Singularity, and Apptainer.  Use by setting ``container_type``, ``container_uri``
  and  additional options may be specified via ``container_cmd_options``.
  Sample configuration:

  .. code-block:: yaml

    display_name: Docker
    engine:
      type: GlobusComputeEngine
      container_type: docker
      container_uri: funcx/kube-endpoint:main-3.10
      container_cmd_options: -v /tmp:/tmp
