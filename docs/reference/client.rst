Globus Compute Client
=====================

The Compute SDK |Client| wraps the `Compute web services API`_.  While the |Client| is
the main interface to the web services, most users will want the higher-level
:doc:`../reference/executor`.

For usage examples and explanations, please see the :doc:`../sdk/client_user_guide`.

Accessing the Compute web services API requires the Globus Auth scope string:

.. code-block:: text

   https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all

If using the Compute |Client|, this scope is automatically handled.  But for those
doing manual interactions, the scope identifier is
``58ce1893-b1fe-4753-a697-7138ceb95adb``:

.. code-block:: console
   :emphasize-lines: 22

   $ globus api auth get /v2/api/scopes -Q 'scope_strings=https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all' | jq
   {
     "scopes": [
       {
         "dependent_scopes": [
           {
             "scope": "69a73d8f-cd45-4e37-bb3b-43678424aeb7",
             "optional": false,
             "requires_refresh_token": true
           },
           {
             "scope": "73320ffe-4cb4-4b25-a0a3-83d53d59ce4f",
             "optional": false,
             "requires_refresh_token": false
           }
         ],
         "scope_string": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
         "client": "facd7ccc-c5f4-42aa-916b-a0e270e2c2a9",
         "advertised": true,
         "description": "Register compute endpoints and run functions",
         "required_domains": [],
         "id": "58ce1893-b1fe-4753-a697-7138ceb95adb",
         "allows_refresh_token": true,
         "name": "Manage Globus Compute"
       }
     ]
   }

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
.. |Executor| replace:: :class:`Executor <globus_compute_sdk.sdk.executor.Executor>`

.. _Compute web services API: https://compute.api.globus.org/redoc