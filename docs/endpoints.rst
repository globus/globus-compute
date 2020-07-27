Endpoints
=========

An endpoint is a persistent service launched by the user on their compute system that serves as a conduit for routing
and executing functions to their compute system. This could be their laptop, the login node of a campus cluster,
grid, or supercomputing facility.

The endpoint can be configured to connect to the funcX webservice at `funcx.org <https://funcx.org>`_.
Once the endpoint is registered you can invoke functions to be executed on it.


First time setup
----------------

The first time you run any of the `funcx-endpoint` commands, if there is no existing configuration found at
`$HOME/.funcx`, you will be prompted to authenticate.  For example, you will likely want to configure an endpoint
so you may as well start by

  $ funcx-endpoint configure

You will be asked to authenticate with Globus Auth. We require authentication in order to associate
endpoints with users and enforce authentication and access control on the endpoint. As part of this step
we request access to your identity information (to retrieve your email address) and Globus Groups management.
We use Groups information to facilitate sharing of functions and endpoints by checking the Group membership
of a group associated with a function.

Once you've run this command, a directory will be created at `$HOME/.funcx` and a set of configuration files will be generated.
A default endpoint profile is also created that will be used, whenever you do not explicitly
specify a profile to use for endpoint actions.


Configuring funcX
-----------------

A configuration file will be created at `$HOME/.funcx/config.py`. This contains
base information regarding the endpoint, such as a username and email. By default, this includes
an address and port for a local broker, which is used if a local broker is deployed.
You can also set the address of the endpoint for your workers to connect to.
This is necessary if your workers are not deployed on the
same resource as the endpoint (e.g., when using a batch submission system, or cloud workers).

.. note:: If your funcX workers are not deployed on the same resource as the endpoint you must set the endpoint address for the workers to find the endpoint. This is done by setting the endpoint_address.

For example

.. code-block:: python

  import getpass
  from parsl.addresses import address_by_route, address_by_hostname

  global_options = {
    'username': getpass.getuser(),
    'email': 'USER@USERDOMAIN.COM',
    'broker_address': '127.0.0.1',
    'broker_port': 8088,
    'endpoint_address': address_by_hostname(),
  }

Configuring an Endpoint
-----------------------

FuncX endpoints are designed to act as gateways to computational resources such as Clusters, Clouds,
Supercomputers, and even your laptop. To make the best use of your resources, the endpoint must be
configured to match the resources' capabilities and reflect the needs of the workloads you plan to execute.
For example, you may want to limit the number of cores available to your endpointt.

FuncX provides you a rich class based configuration model that allows you to specify the shape of the
resources (# of nodes, # of cores per worker, walltime etc) as well as place limits on how funcX may
scale the resources in response to changing workload demands.

To generate the appropriate directories and default config template, run the following command::

  $ funcx-endpoint configure <ENDPOINT_NAME>

The above command will create a profile for your endpoint in `$HOME/.funcx/<ENDPOINT_NAME>/` and will instantiate a
`config.py` file. This file should be updated with the appropriate configurations for the computational system you are
targeting before you start the endpoint. The funcX builds on `Parsl <https://parsl-project.org>`_ and is
configured using a :class:`~funcx.config.Config` object.
For more information, see the :class:`~funcx.config.Config` class documentation.

.. note:: If the ENDPOINT_NAME is not specified, a default endpoint named "default" is configured.


Starting an Endpoint
--------------------

To start a new endpoint run the following command::

  $ funcx-endpoint start <ENDPOINT_NAME>

The above command will create a profile for your endpoint in `$HOME/.funcx/<ENDPOINT_NAME>/config.py`.
This file should be updated with the appropriate configurations for the computational system you are
targeting before you start the endpoint. To launch the endpoint, simply rerun the above command.

.. note:: If the ENDPOINT_NAME is not specified, a default endpoint named "default" is started.

Starting an endpoint will perform a registration process with the funcX Web Service.
The registration process provides funcX with information regarding the endpoint. The Web Service then creates a
Forwarder process for the endpoint and returns a UUID and connection information to the Forwarder.
The endpoint will use this connection information to connect to the Forwarder. The endpoint establishes three outbound
ZeroMQ channels to the forwarder (on the three ports returned during registration) to retrieve tasks, send results,
and communicate command information.

Once started, the endpoint uses a daemon process to run in the background.

.. warning:: Only the owner of an endpoint is authorized to start an endpoint. Thus if you register with a different Globus Auth identity and try to start an endpoint owned by another identity, it will fail.


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
