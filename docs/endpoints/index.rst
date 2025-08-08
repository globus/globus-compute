Globus Compute Endpoints
************************

In the mental model of Globus Compute, Endpoints are the remote instance:

- Globus Compute SDK |nbsp| --- |nbsp| i.e., the script a researcher writes to submit
  functions to ...

- Globus Compute Web Services |nbsp| --- |nbsp| the Globus‑provided cloud‑services that
  authenticate and ferry functions and tasks

- **Globus Compute Endpoint** |nbsp| --- |nbsp| a site-specific process, typically on a
  cluster head node, that manages user‑accessible compute resources to run tasks
  submitted by the SDK

.. When an administrator starts a Compute endpoint as a non-privileged local user, the
.. manager and user endpoint processes all run as the same local user, and the endpoint
.. will only accept tasks submitted by the endpoint owner (i.e., the Globus identity
.. active when first starting the endpoint). We refer to this as a single-user endpoint:

.. .. code-block:: text
..    :caption: Starting endpoint as alice

..    Endpoint
..    └── Manager Endpoint Process (alice, UID: 1001)
..       └── User Endpoint Process (alice, UID: 1001)

.. If an administrator starts a Compute endpoint as a privileged local user (e.g., root),
.. the manager endpoint process employs an `identity mapping script <example-idmap-config>`_
.. to map task-submitting Globus identities to local user accounts, then launches the user
.. endpoint process as the mapped local user. We refer to this as a :doc:`multi-user endpoint <multi_user>`:

.. .. code-block:: text
..    :caption: Starting endpoint as root

..    Endpoint
..    └── Manager Endpoint Process (root)
..        ├── User Endpoint Process (alice, UID: 1001)
..        ├── User Endpoint Process (bob, UID: 1002)
..        └── User Endpoint Process (eve, UID: 1003)

.. toctree::
   :maxdepth: 1

   installation
   endpoints
   multi_user
   security_posture
   templates
   endpoint_examples
   config_reference


.. |nbsp| unicode:: 0xA0
   :trim: