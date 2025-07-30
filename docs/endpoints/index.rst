Globus Compute Endpoints
************************

A Globus Compute Endpoint serves as a conduit for executing functions on a computing
resource.  The Compute Endpoint manages the site‑specific interactions for executing
functions, leaving users with only a single API necessary for running code on their
laptop, campus cluster, or even leadership‑class HPC machines.

In the mental model of Globus Compute, Endpoints are the remote instance:

- Globus Compute SDK |nbsp| --- |nbsp| i.e., the script a researcher writes to submit
  functions to ...

- Globus Compute Web Services |nbsp| --- |nbsp| the Globus‑provided cloud‑services that
  authenticate and ferry functions and tasks

- **Globus Compute Endpoint** |nbsp| --- |nbsp| a site-specific process, typically on a
  cluster head node, that manages user‑accessible compute resources to run tasks
  submitted by the SDK

Process Hierarchy
=================

When a user submits tasks to a Globus Compute endpoint, the manager endpoint process uses
:doc:`configuration templates <templates>` to launch a user endpoint process for the user,
which ultimately launches workers to execute the tasks.

If an administrator starts a Compute endpoint as a non-privileged local user, the manager
endpoint process and user endpoint processes all run as same local user:

.. code-block:: text
   :caption: Starting endpoint as alice

   Endpoint
   └── Manager Endpoint Process (alice, UID: 1001)
      └── User Endpoint Process (alice, UID: 1001)

If an administrator starts a Compute endpoint as a privileged local user (e.g., root), the
manager endpoint process will run as the privileged local user and employ an `identity mapping
script <example-idmap-config>`_ to determine which local user account to use when launching each
user endpoint process:

.. code-block:: text
   :caption: Starting endpoint as root

   Endpoint
   └── Manager Endpoint Process (root)
       ├── User Endpoint Process (alice, UID: 1001)
       ├── User Endpoint Process (bob, UID: 1002)
       └── User Endpoint Process (eve, UID: 1003)


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