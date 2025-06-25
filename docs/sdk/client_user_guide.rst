Client User Guide
=================

The SDK provides a |Client| class for interacting with Globus Compute. The |Client|
abstracts authentication and provides an interface to make Globus Compute
API calls without needing to know the Globus Compute REST endpoints for those operations.
You can instantiate a Globus Compute client as follows:

.. code-block:: python

  from globus_compute_sdk import Client
  gcc = Client()

Instantiating a |Client| will start an authentication process where you will be asked to authenticate via Globus Auth.
We require every interaction with Globus Compute to be authenticated, as this enables enforced
access control on both functions and endpoints.
Globus Auth is an identity and access management platform that provides authentication brokering
capabilities enabling users to login using one of several hundred supported identities.
It also provides group and profile management for user accounts.
As part of the authentication process, Globus Compute will request access
to your identity (to retrieve your email address) and Globus Groups. Globus Compute uses
Groups to facilitate sharing and to make authorization decisions.
Globus Compute allows functions to be shared by associating a Globus Group.

.. note:: Globus Compute internally caches function, endpoint, and authorization lookups. Caches are based on user authentication tokens. To force refresh cached
          entries, you can re-authenticate your client with ``force_login=True``.

Registering Functions
---------------------

You can register a Python function with Globus Compute via ``register_function()``. Function registration serializes the
function body and transmits it to Globus Compute. Once the function is registered with Globus Compute, it is assigned a
UUID that can be used to manage and invoke the function.

.. note:: You must import any dependencies required by the function inside the function body.


The following example shows how to register a function. In this case, the function simply
returns the platform information of the system on which it is executed. The function
is defined in the same way as any Python function before being registered with Globus Compute.

.. code-block:: python

  def platform_func():
    """Get platform information about this system."""

    import platform
    return platform.platform()

  func_uuid = gcc.register_function(platform_func)


Running Functions
-----------------

You can invoke a function using the UUID returned when registering the function. The ``run()`` function
requires that you specify the function (``function_id``) and endpoint (``endpoint_id``) on which to execute
the function. Globus Compute will return a UUID for the executing function (called a task) via which you can
monitor status and retrieve results.

.. code-block:: python

  tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
  task_id = gcc.run(endpoint_id=tutorial_endpoint, function_id=func_uuid)

.. note::
   Globus Compute places limits on the size of the functions and the rate at which functions can be submitted.
   Please refer to the :doc:`../limits` section for details.


Retrieving Results
-------------------
The result of your function's invocation can be retrieved using the ``get_result()`` function. This will either
return the deserialized result of your invocation or raise an exception indicating that the
task is still pending.

.. note:: If your function raises an exception, get_result() will reraise it.

.. code-block:: python

  try:
    print(gcc.get_result(task_id))
  except Exception as e:
    print("Exception: {}".format(e))

.. note:: Globus Compute caches results in the cloud until they have been retrieved. The SDK also caches results
          during a session. However, calling ``get_result()`` from a new session will not be able to access the results.


Arguments and data
------------------

Globus Compute functions operate the same as any other Python function. You can pass arguments \*args and \**kwargs
and return values from functions. The only constraint is that data passed to/from a Globus Compute function must be
serializable (e.g., via Pickle) and fall within service limits.
Input arguments can be passed to the function using the ``run()`` function.
The following example shows how strings can be passed to and from a function.

.. code-block:: python

  def hello(firstname, lastname):
    """Say hello to someone."""

    return 'Hello {} {}'.format(firstname, lastname)

  func_id = gcc.register_function(hello)

  task_id = gcc.run("Bob", "Smith", endpoint_id=tutorial_endpoint, function_id=func_id)

  try:
    print(gcc.get_result(task_id))
  except Exception as e:
    print("Exception: {}".format(e))


Sharing Functions
-----------------
You may share functions publicly (with anyone) or a set of users via a Globus Group.
You can also add a function description such that it can be discovered by others.

To share with a group, set ``group=<globus_group_id>`` when registering a function.

.. code-block:: python

  gcc.register_function(func, group=<globus_group_id>)


Upon execution, Globus Compute will check group membership to ensure that the user is authorized to execute the function.

You can also set a function to be publicly accessible by setting ``public=True`` when registering the function.

.. code-block:: python

  gcc.register_function(func, public=True)


To add a description to a function, you can either set ``description=<my_description>``
when calling ``register_function``, or add a docstring to the function. Note that the
latter also works with the ``Executor`` class.

.. code-block:: python

  gcc.register_function(func, description="My function")

  def function_with_docstring():
    """My function, with a docstring"""
    return "foo"

  gcc.register_function(func)  # description is automatically read from the docstring

  gcx = Executor()
  fut = gcx.submit(function_with_docstring)  # automatically registers the function with its docstring

  # if both are specified, the argument wins
  gcc.register_function(function_with_docstring, description="this has priority over docstrings")


.. _batching:

Batching
--------

The SDK includes a batch interface to reduce the overhead of launching a function many
times.  To use this interface, first create a |Batch| object and then pass it to the
|batch_run()| method.  The |batch_run()| method only blocks for the duration of the API
call, returning a dictionary structure that contains the task identifiers.  (Note that
the tasks may not yet have been received by the endpoint when the call returns.)

.. code-block:: python
  :caption: Example use of ``Batch`` objects


  def noisy_multiply(x, y, noise=1, offset=0) -> float:
      import random
      return x * y + random.uniform(offset - noise, offset + noise)

  noisy_fn_id = gcc.register_function(noisy_multiply)
  batch = gcc.create_batch()

  for val in range(7):
    fn_args = (val + 3, val)
    fn_kwargs = {"noise": val // 5, offset=val + 1}

    batch.add(noisy_fn_id, args=fn_args, kwargs=fn_kwargs)

  # See what data structure will be sent to the API with .prepare()
  # pprint.pprint(batch.prepare())

  batch_res = gcc.batch_run(endpoint_id, batch)

  # The endpoint may not have even received the tasks yet, much less returned any
  # results to the Compute web services, so sit for a spell before checking on the
  # status:
  time.sleep(60)  # or otherwise an appropriate time for your use-case

  # Now collate the task_ids.  The `batch_res` data structure is, analogous to what
  # `Batch.prepare` sent to the API, so iterate it accordingly:

  task_ids = []
  for fn_task_list in batch_res["tasks"].values():
    task_ids.extend(fn_task_list)

  # then pass that list to `get_batch_result()`
  batch_status = gcc.get_batch_result(task_ids)
  # pprint.pprint(batch_status)  # A dictionary, keyed by the batch task ids

.. _globus apps:

GlobusApps
-----------

The Compute |Client| uses |GlobusApp|_ objects to handle authentication and
authorization.  By default, the |Client| will instantiate a |UserApp|_ to facilitate a
native app login flow.  For headless setups that :ref:`export client credentials
<client credentials with globus compute clients>`, the |Client| will instantiate a
|ClientApp|_.

You can also create a custom ``GlobusApp`` object then pass it to the ``Client`` constructor. For example, to specify
the client ID for a custom thick client, you could do the following:

.. code:: python

  import globus_sdk
  from globus_compute_sdk import Executor, Client

  my_client_id = "..."
  my_endpoint_id = "..."

  app = globus_sdk.UserApp("MyNativeApp", client_id=my_client_id)
  gcc = Client(app=app)
  gce = Executor(endpoint_id=my_endpoint_id, client=gcc)


.. _client credentials with globus compute clients:

Client Credentials with Clients
-------------------------------

Client credentials can be useful if you need an endpoint to run in a service account or to be started automatically with a process manager.

The Globus Compute SDK supports use of Globus Auth client credentials for login, if you have `registered a client. <https://docs.globus.org/api/auth/developer-guide/#register-app>`_

To use client credentials, you must set the environment variables
**GLOBUS_COMPUTE_CLIENT_ID** to your client ID, and **GLOBUS_COMPUTE_CLIENT_SECRET** to
your client secret.

When these environment variables are set they will take priority over any other
credentials on the system and the Client will assume the identity of the client app.
This also applies when starting a Globus Compute endpoint.

.. code:: bash

  $ export GLOBUS_COMPUTE_CLIENT_ID="b0500dab-ebd4-430f-b962-0c85bd43bdbb"
  $ export GLOBUS_COMPUTE_CLIENT_SECRET="ABCDEFGHIJKLMNOP0123456789="

.. note:: Globus Compute clients and endpoints will use the client credentials if they are set, so it is important to ensure the client submitting requests has access to an endpoint.


.. _existing-token:

Using an Existing Token
-----------------------

To create a |Client| from an existing access token and skip the interactive login flow, you can pass an |AccessTokenAuthorizer|_
via the ``authorizer`` parameter:

.. code-block:: python

  import globus_sdk
  from globus_compute_sdk import Executor, Client

  authorizer = globus_sdk.AccessTokenAuthorizer(access_token="...")
  gcc = Client(authorizer=authorizer)
  gce = Executor(endpoint_id="...", client=gcc)

.. note::
    Accessing the Globus Compute API requires the Globus Compute scope:

    .. code-block:: python

      >>> from globus_sdk.scopes import ComputeScopes
      >>> ComputeScopes.all
      'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'


Endpoint Operations
-------------------

You can retrieve information about endpoints including status and
information about how the endpoint is configured.

.. code:: python

  >>> from globus_compute_sdk import Client
  >>> gcc = Client()
  >>> ep_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
  >>> gcc.get_endpoint_status(ep_id)
  {'status': 'online'}
  >>> gcc.get_endpoint_metadata(ep_id)
  {'uuid': '4b116d3c-1703-4f8f-9f6f-39921e5864df', 'name': 'tutorial', ...}

.. |Batch| replace:: :class:`Batch <globus_compute_sdk.sdk.batch.Batch>`
.. |Client| replace:: :class:`Client <globus_compute_sdk.sdk.client.Client>`
.. |Executor| replace:: :class:`Executor <globus_compute_sdk.sdk.executor.Executor>`

.. |batch_run()| replace:: :func:`~globus_compute_sdk.Client.batch_run`

.. |AccessTokenAuthorizer| replace:: ``AccessTokenAuthorizer``
.. _AccessTokenAuthorizer: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_authorizers.html#globus_sdk.AccessTokenAuthorizer

.. |GlobusApp| replace:: ``GlobusApp``
.. _GlobusApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html
.. |UserApp| replace:: ``UserApp``
.. _UserApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html#globus_sdk.UserApp
.. |ClientApp| replace:: ``ClientApp``
.. _ClientApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html#globus_sdk.ClientApp