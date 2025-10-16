Globus Compute Endpoints
************************

In the mental model of Globus Compute, Endpoints are the remote instance:

- Globus Compute SDK |nbsp| --- |nbsp| i.e., the script a researcher writes to
  submit functions to ...

- Globus Compute Web Services |nbsp| --- |nbsp| the Globus‑provided
  cloud‑services that authenticate and ferry functions and tasks

- **Globus Compute Endpoint** |nbsp| --- |nbsp| a site-specific process,
  typically on a cluster head node, that manages user‑accessible compute
  resources to run tasks submitted by the SDK

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