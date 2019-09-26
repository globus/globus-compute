Endpoints
=========

An endpoint is a persistent service launched by the user on their compute system that serves as a conduit for routing
and executing functions to their compute system. This could be their laptop, the login node of a campus cluster,
grid or supercomputing facility.

The endpoint can be configured to connect to the funcX webservice at `funcx.org <https://funcx.org>`_
or to your private service. Once the endpoint is connected, the web portal presents available
endpoints to which you can direct functions.


First time setup
----------------

For general use, running the initialize command via the funcx-endpoint is the quickest way to
setup your `funcx-endpoint` installation::

  $ funcx-endpoint init

Once you've run this command, a set of configuration files would be stored in your $HOME directories.
A default endpoint profile is also created that will be used, whenever you do not explicitly
specify a profile to use for endpoint actions.


Configuring an Endpoint
-----------------------

FuncX endpoints are designed to act as gateways to computational resources such as Clusters, Clouds,
Supercomputers and even your laptop. To make the best use of your resources, the endpoint must be
configured to match the needs of the workloads that you plan to execute, and the capacity of the
computational resource available to you. For eg, you may want to limit the number of cloud nodes
to your budget.

FuncX provides you a rich class based configuration model that allows you to specify the shape of the
resources (# of nodes, # of cores per worker, duration etc) as well as place limits on how FuncX may
scale the resources in response to changing workload demands.


To generate the appropiate directories and default config template, run the following command::

  $ funcx-endpoint configure <ENDPOINT_NAME>

The above command will create a profile for your endpoint in `$HOME/.funcx/<ENDPOINT_NAME>/config.py`.
This file should be updated with the appropriate configurations for the computational system you are
targeting before you start the endpoint. To launch the endpoint, see the next section.

.. note:: If the ENDPOINT_NAME is not specified, a defaut endpoint named "default" is configured.


Starting an Endpoint
--------------------

To start a new endpoint run the following command::

  $ funcx-endpoint start <ENDPOINT_NAME>

The above command will create a profile for your endpoint in `$HOME/.funcx/<ENDPOINT_NAME>/config.py`.
This file should be updated with the appropriate configurations for the computational system you are
targeting before you start the endpoint. To launch the endpoint, simply rerun the above command.

.. note:: If the ENDPOINT_NAME is not specified, a defaut endpoint named "default" is started.

Stopping an Endpoint
--------------------

To stop an endpoint, run the following command::

  $ funcx-endpoint stop <ENDPOINT_NAME>

.. note:: If the ENDPOINT_NAME is not specified, the default endpoint is stopped.

.. warning:: Run the ``funcx-endpoint stop`` command **twice** to ensure that the endpoint is shutdown.

Listing Endpoints
-----------------

To list available endpoints on the current system, run::

  $ funcx-endpoint list
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

* **Initialized**: This status means that the endpoint has been created, but not started
  following configuration and not registered with the `funcx service`
* **Active**: This status means that the endpoint is active and available for executing
  functions
* **Inactive**: This status means that endpoint is not running right now and therefore,
  cannot service any functions.
