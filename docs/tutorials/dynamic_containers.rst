Dynamic Container Configuration
*******************************

This tutorial demonstrates how to enable dynamic container configuration at the
point of task submission.

For general information on how to specify container environments in an endpoint
configuration, see :ref:`Containerized Environments <containerized-environments>`.


Configure an Endpoint
=====================

If you are starting from scratch, you will need to initialize an endpoint with
the ``configure`` subcommand:


.. code-block:: console

   $ globus-compute-endpoint configure my-ep


Modify the Configuration Template
---------------------------------

Since the user endpoint configuration template is a Jinja template rendered with
user-provided values, we can use Jinja variables for container-related fields.
See :ref:`Containerized Environments <containerized-environments>` for more
information on supported container fields.

.. code-block:: yaml+jinja
   :caption: Example :ref:`user_config_template.yaml.j2 <user-config-template-yaml-j2>`
   :emphasize-lines: 4-6

   display_name: My Containerized Endpoint
   engine:
     type: GlobusComputeEngine
     container_type: {{ container_type }}
     container_uri: {{ container_uri }}
     container_cmd_options: {{ container_cmd_options|default() }}
     provider:
       init_blocks: 1
       max_blocks: 1
       min_blocks: 0
       type: LocalProvider

.. important::

   Container support is limited to the |GlobusComputeEngine|_.


Modify the Configuration Schema
-------------------------------

Optionally, we can modify the configuration JSON schema to validate the user-provided
values. For example, we might ensure that the container type is a valid option.

.. code-block:: json
   :caption: Example :ref:`user_config_schema.json <user-config-schema-json>`
   :emphasize-lines: 5-17

   {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "properties": {
         "container_type": {
            "type": "string",
            "enum": [
                "apptainer",
                "docker",
                "singularity",
                "podman",
                "podman-hpc",
                "custom"
            ]
         },
         "container_uri": { "type": "string" },
         "container_cmd_options": { "type": "string" }
      }
   }


Start the Endpoint
------------------

Once the endpoint is configured, we can start it up.

.. code-block:: console

   $ globus-compute-endpoint start my-ep

Take note of the endpoint ID emitted to the console; we will use it later in the tutorial.


Build a Container Image
=======================

.. important::
   The container image must include the ``globus-compute-endpoint`` package.

Below is a simple Dockerfile that inherits from the ``python:3.13`` image and
accepts ``ENDPOINT_VERSION`` as a build argument.

.. code-block:: dockerfile
   :caption: Example Dockerfile

   FROM python:3.13
   ARG ENDPOINT_VERSION
   RUN pip install globus-compute-endpoint==${ENDPOINT_VERSION}

For this tutorial, we will build two images with different endpoint versions.

.. code-block:: console

   $ docker build --build-arg ENDPOINT_VERSION=3.7.0 -t compute-worker:3.7.0 .
   $ docker build --build-arg ENDPOINT_VERSION=3.8.0 -t compute-worker:3.8.0 .


Submit Tasks from the SDK
=========================

We will submit tasks to the endpoint with the |Executor|_ from the SDK. Specifically,
we will utilize the ``user_endpoint_config`` argument and attribute to define values
for the user endpoint configuration template.

In the example below, we submit the ``get_endpoint_pkg_version()`` function multiple
times to the endpoint that we previously configured. This function returns the version
of the ``globus-compute-endpoint`` package installed in the container, which will vary
depending on the ``container_uri`` specified.

.. code-block:: python
   :emphasize-lines: 10-14, 18, 23

   from globus_compute_sdk import Executor


   def get_endpoint_pkg_version():
      import globus_compute_endpoint
      return globus_compute_endpoint.__version__


   ep_id = "..."  # Endpoint ID from previous step
   config = {
      "container_type": "docker",
      "container_uri": "compute-worker:3.7.0",
      "container_cmd_options": "-v /tmp:/tmp"
   }

   with Executor(
      endpoint_id=ep_id,
      user_endpoint_config=config
   ) as ex:
      f = ex.submit(get_endpoint_version)
      assert f.result() == "3.7.0"

      ex.user_endpoint_config["container_uri"] = "compute-worker:3.8.0"
      f = ex.submit(get_endpoint_version)
      assert f.result() == "3.8.0"


.. |GlobusComputeEngine| replace:: ``GlobusComputeEngine``
.. _GlobusComputeEngine: ../reference/engine.html#globus_compute_endpoint.engines.GlobusComputeEngine
.. |Executor| replace:: ``Executor``
.. _Executor: ../reference/executor.html#globus_compute_sdk.Executor



