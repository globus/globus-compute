Globus Compute Endpoints
========================

A Globus Compute Endpoint is a persistent service launched by the user on a compute system to serve as a conduit for
executing functions on that computer. Globus Compute supports a range of target systems, enabling
an endpoint to be deployed on a laptop, the login node of a campus cluster, a cloud instance,
or a Kubernetes cluster, for example.

The endpoint requires outbound network connectivity. That is, it must be able to connect to
Globus Compute at `funcx.org <https://compute.api.globus.org/v2>`_.

The Globus Compute Endpoint package `is available on PyPI <https://pypi.org/project/globus-compute-endpoint/>`_
(and thus available via ``pip``). However, *we strongly recommend installing the
Globus Compute endpoint into an isolated virtual environment*.
`Pipx <https://pypa.github.io/pipx/installation/>`_ automatically manages
package-specific virtual environments for command line applications, so install Globus Compute endpoint via::

     $ python3 -m pipx install globus-compute-endpoint

.. note::

   Please note that the Globus Compute endpoint is only supported on Linux.


After installing the Globus Compute endpoint, use the ``globus-compute-endpoint`` command to manage existing endpoints.


First time setup
----------------

You will be required to authenticate the first time you run ``globus-compute-endpoint``.
If you have authenticated previously, the endpoint will cache access tokens in
the local configuration file.

Globus Compute requires authentication in order to associate
endpoints with users and ensure only authorized users can run tasks on that endpoint. As part of this step,
we request access to your identity and Globus Groups.

To get started, you will first want to configure a new endpoint.  ::

  $ globus-compute-endpoint configure

Once you've run this command, a directory will be created at ``$HOME/.globus_compute`` and a set of default configuration files will be generated.

You can also set up auto-completion for the ``globus-compute-endpoint`` commands in your shell, by using the command ::

  $ globus-compute-endpoint --install-completion [zsh bash fish ...]


Configuring an Endpoint
-----------------------

Globus Compute endpoints act as gateways to diverse computational resources, including clusters, clouds,
supercomputers, and even your laptop. To make the best use of your resources, the endpoint must be
configured to match the capabilities of the resource on which it is deployed.

Globus Compute provides a Python class-based configuration model that allows you to specify the shape of the
resources (number of nodes, number of cores per worker, walltime, etc.) as well as allowing you to place
limits on how Globus Compute may scale the resources in response to changing workload demands.

To generate the appropriate directories and default configuration template, run the following command::

  $ globus-compute-endpoint configure <ENDPOINT_NAME>

This command will create a profile for your endpoint in ``$HOME/.globus_compute/<ENDPOINT_NAME>/`` and will instantiate a
``config.py`` file. This file should be updated with the appropriate configurations for the computational system you are
targeting before you start the endpoint.
Globus Compute is configured using a :class:`~compute_endpoint.endpoint.utils.config.Config` object.
Globus Compute uses `Parsl <https://parsl-project.org>`_ to manage resources. For more information,
see the :class:`~compute_endpoint.endpoint.utils.config.Config` class documentation and the
`Parsl documentation <https://parsl.readthedocs.io/en/stable/userguide/overview.html>`_ .

.. note:: If the ENDPOINT_NAME is not specified, a default endpoint named "default" is configured.


Starting an Endpoint
--------------------

To start a new endpoint run the following command::

  $ globus-compute-endpoint start <ENDPOINT_NAME>

.. note:: If the ENDPOINT_NAME is not specified, a default endpoint named "default" is started.

Starting an endpoint will perform a registration process with Globus Compute.
The registration process provides Globus Compute with information regarding the endpoint.
The endpoint also establishes an outbound connection to RabbitMQ to retrieve tasks, send results, and communicate command information.
Thus, the Globus Compute endpoint requires outbound access to the Globus Compute services over HTTPS (port 443) and AMQPS (port 5671).

Once started, the endpoint uses a daemon process to run in the background.

.. note:: If the endpoint was not stopped correctly previously (e.g., after a computer restart when the endpoint was running), the endpoint directory will be cleaned up to allow a fresh start

.. warning::

    Only the owner of an endpoint is authorized to start an endpoint. Thus if you register an endpoint
    using one identity and try to start an endpoint owned by another identity, it will fail.


To start an endpoint using a client identity, rather than as a user, you can export the FUNCX_SDK_CLIENT_ID and FUNCX_SDK_CLIENT_SECRET
environment variables. This is explained in detail in :ref:`client credentials with globus compute clients`.


Stopping an Endpoint
--------------------

To stop an endpoint, run the following command::

  $ globus-compute-endpoint stop <ENDPOINT_NAME>

If the endpoint is not running and was stopped correctly previously, this command does nothing.

If the endpoint is not running but was not stopped correctly previously (e.g., after a computer restart
when the endpoint was running), this command will clean up the endpoint directory such that the endpoint
can be started cleanly again.

.. note:: If the ENDPOINT_NAME is not specified, the default endpoint is stopped.

.. warning:: Run the ``globus-compute-endpoint stop`` command **twice** to ensure that the endpoint is shutdown.

Listing Endpoints
-----------------

To list available endpoints on the current system, run::

  $ globus-compute-endpoint list
  +---------------+-------------+--------------------------------------+
  | Endpoint Name |   Status    |             Endpoint ID              |
  +===============+=============+======================================+
  | default       | Active      | 1e999502-b434-49a2-a2e0-d925383d2dd4 |
  +---------------+-------------+--------------------------------------+
  | KNL_test      | Inactive    | 8c01d13c-cfc1-42d9-96d2-52c51784ea16 |
  +---------------+-------------+--------------------------------------+
  | gpu_cluster   | Initialized | None                                 |
  +---------------+-------------+--------------------------------------+

Endpoints can be the following states:

* **Initialized**: The endpoint has been created, but not started
  following configuration and is not registered with the `Globus Compute service`.
* **Running**: The endpoint is active and available for executing  functions.
* **Stopped**: The endpoint was stopped by the user. It is not running
  and therefore, cannot service any functions. It can be started again without issues.
* **Disconnected**: The endpoint disconnected unexpectedly. It is not running
  and therefore, cannot service any functions. Starting this endpoint will first invoke
  necessary endpoint cleanup, since it was not stopped correctly previously.


Container behaviors and routing
-------------------------------

The Globus Compute endpoint can run functions using independent Python processes or optionally
inside containers. Globus Compute supports various container technologies (e.g., docker and singularity)
and different routing mechanisms for different use cases.

Raw worker processes (``worker_mode=no_container``):

* Hard routing: All worker processes are of the same type "RAW". It this case, the Globus Compute endpoint simply routes tasks to any available worker processes. This is the default mode of a Globus Compute endpoint.
* Soft routing: It is the same as hard routing.

Kubernetes (docker):

* Hard routing: Both the manager and the worker are deployed within a pod and thus the manager cannot change the type of worker container. In this case, a set of managers are deployed with specific container images and the Globus Compute endpoint simply routes tasks to corresponding managers (matching their types).
* Soft routing: NOT SUPPORTED.

Native container support (docker, singularity, shifter):

* Hard routing: In this case, each manager (on a compute node) can only launch worker containers of a specific type and thus each manager can serve only one type of function.
* Soft routing: When receiving a task for a specific container type, the Globus Compute endpoint attempts to send the task to a manager that has a suitable warm container to minimize the total number of container cold starts. If there are not any warmed containers in any connected managers, the Globus Compute endpoint chooses one manager randomly to dispatch the task.


Example configurations
----------------------

.. include:: configuring.rst
