Globus Compute Client
=====================

The Compute SDK |Client| wraps the `Compute web services API`_.  While the |Client| is
the main interface to the web services, most users will want the higher-level
:doc:`../reference/executor`.

For usage examples and explanations, please see the :doc:`../sdk/client_user_guide`.

----

.. autoclass:: globus_compute_sdk.Client
    :members:
    :member-order: bysource

.. autoclass:: globus_compute_sdk.sdk.container_spec.ContainerSpec
    :members:
    :member-order: bysource

.. autoclass:: globus_compute_sdk.sdk.batch.Batch
    :members:
    :member-order: bysource

.. autoclass:: globus_compute_sdk.sdk.batch.UserRuntime
    :members:
    :member-order: bysource

.. |Client| replace:: :class:`Client <globus_compute_sdk.sdk.client.Client>`

.. _Compute web services API: https://compute.api.globus.org/redoc