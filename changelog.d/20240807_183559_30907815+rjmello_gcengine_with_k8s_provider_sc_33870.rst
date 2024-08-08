Bug Fixes
^^^^^^^^^

- The endpoint CLI will now raise an error if the endpoint configuration includes
  both the ``container_uri`` field and a provider that manages containers internally
  (``AWSProvider``, ``GoogleCloudProvider``, or ``KubernetesProvider``). This prevents
  conflicts in container management.