Endpoints
=========

An endpoint is a persist service launched by the user on their compute facility's login
that serves as a conduit for routing tasks to their compute facility.

The endpoint can be configured to connect up to the funcX webservice at `funcx.org <https://funcx.org>`_
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


Starting an Endpoint
--------------------

To start a new endpoint run the following command::

  $ funcx-endpoint start <ENDPOINT_NAME>
  A default profile has been create for <testing> at ~/.funcx/<ENDPOINT_NAME>/config.py
  Configure this file and try restarting with:
       $ funcx-endpoint start <ENDPOINT_NAME>

The above command will create a profile for your endpoint in `$HOME/.funcx/<ENDPOINT_NAME>/config.py`.
This file should be updated with the appropriate configurations for the computational system your are
targeting before you start the endpoint. To launch the endpoint, simply rerun the above command.

.. note:: If the ENDPOINT_NAME is not specified, the default endpoint is started.

Stopping an Endpoint
--------------------

To stop an endpoint, run the following command::

  $ funcx-endpoint stop <ENDPOINT_NAME>

.. note:: If the ENDPOINT_NAME is not specified, the default endpoint is started.

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

The endpoints can be the following states:

* Initialized : This status means that the endpoint has been created, but not started
  following configuration and not registered with the `funcx service`
* Active : This status means that the endpoint is active and available for executing
  functions
* Inactive : This status means that endpoint is not running right now and therefore,
  cannot service any functions.
