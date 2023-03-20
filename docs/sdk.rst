funcX SDK User Guide
====================

The **funcX SDK** provides a programmatic interface to funcX from Python.
The SDK provides a convenient Pythonic interface to:

1. Register functions
2. Register containers and execution environments
3. Launch registered functions on accessible endpoints
4. Check the status of launched functions
5. Retrieve outputs from functions

The SDK provides a client class for interacting with funcX. The client
abstracts authentication and provides an interface to make funcX
API calls without needing to know the funcX REST endpoints for those operations.
You can instantiate a funcX client as follows:

.. code-block:: python

  from funcx import FuncXClient
  fxc = FuncXClient()

Instantiating a client will start an authentication process where you will be asked to authenticate via Globus Auth.
We require every interaction with funcX to be authenticated, as this enables enforced
access control on both functions and endpoints.
Globus Auth is an identity and access management platform that provides authentication brokering
capablities enabling users to login using one of several hundred supported identities.
It also provides group and profile management for user accounts.
As part of the authentication process, funcX will request access
to your identity (to retrieve your email address) and Globus Groups. funcX uses
Groups to facilitate sharing and to make authorization decisions.
funcX allows endpoints and functions to be shared by associating a Globus Group.

.. note:: funcX internally caches function, endpoint, and authorization lookups. Caches are based on user authentication tokens. To force refresh cached
          entries, you can re-authenticate your client with ``force_login=True``.

Registering Functions
---------------------

You can register a Python function with funcX via ``register_function()``. Function registration serializes the
function body and transmits it to funcX. Once the function is registered with funcX, it is assigned a
UUID that can be used to manage and invoke the function.

.. note:: You must import any dependencies required by the function inside the function body.


The following example shows how to register a function. In this case, the function simply
returns the platform information of the system on which it is executed. The function
is defined in the same way as any Python function before being registered with funcX.

.. code-block:: python

  def platform_func():
    import platform
    return platform.platform()

  func_uuid = fxc.register_function(platform_func)


Running Functions
-----------------

You can invoke a function using the UUID returned when registering the function. The ``run()`` function
requires that you specify the function (``function_id``) and endpoint (``endpoint_id``) on which to execute
the function. funcX will return a UUID for the executing function (called a task) via which you can
monitor status and retrieve results.

.. code-block:: python

  tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
  task_id = fxc.run(endpoint_id=tutorial_endpoint, function_id=func_uuid)

.. note::
   funcX places limits on the size of the functions and the rate at which functions can be submitted.
   Please refer to the limits section for TODO:YADU


Retrieving Results
-------------------
The result of your function's invocation can be retrieved using the ``get_result()`` function. This will either
return the deserialized result of your invocation or raise an exception indicating that the
task is still pending.

.. note:: If your function raises an exception, get_result() will reraise it.

.. code-block:: python

  try:
    print(fxc.get_result(task_id))
  except Exception as e:
    print("Exception: {}".format(e))

.. note:: funcX caches results in the cloud until they have been retrieved. The SDK also caches results
          during a session. However, calling ``get_result()`` from a new session will not be able to access the results.


Arguments and data
------------------

funcX functions operate the same as any other Python function. You can pass arguments \*args and \**kwargs
and return values from functions. The only constraint is that data passed to/from a funcX function must be
serializable (e.g., via Pickle) and fall within service limits.
Input arguments can be passed to the function using the ``run()`` function.
The following example shows how strings can be passed to and from a function.

.. code-block:: python

  def funcx_hello(firstname, lastname):
    return 'Hello {} {}'.format(firstname, lastname)

  func_id = fxc.register_function(funcx_hello)

  task_id = fxc.run("Bob", "Smith", endpoint_id=tutorial_endpoint, function_id=func_id)

  try:
    print(fxc.get_result(task_id))
  except Exception as e:
    print("Exception: {}".format(e))


Sharing Functions
-----------------
You may share functions publicly (with anyone) or a set of users via a Globus Group.
You can also add a function description such that it can be discovered by others.

To share with a group, set ``group=<globus_group_id>`` when registering a function.

.. code-block:: python

  fxc.register_function(funcx, description="My function", group=<globus_group_id>)


Upon execution, funcX will check group membership to ensure that the user is authorized to execute the function.

You can also set a function to be publicly accessible by setting ``public=True`` when registering the function.

.. code-block:: python

  fxc.register_function(funcx, description="My function", public=True)


Discovering Functions
----------------------

funcX maintains an access controlled search index of registered functions.
You can look up your own functions, functions that have been shared with you,
or publicly accessible functions via the ``search_function()`` function.

.. code-block:: python

  search_results = fxc.search_function("my function", offset=0, limit=5)
  print(search_results)


.. _batching:

Batching
--------

The SDK includes a batch interface to reduce the overheads of launching a function many times.
To use this interface, you must first create a batch object and then pass that object
to the ``batch_run`` function. ``batch_run`` is non-blocking and returns a list of task ids
corresponding to the functions in the batch with the ordering preserved.

.. code-block:: python

  batch = fxc.create_batch()

  for x in range(0,5):
    batch.add(x, endpoint_id=tutorial_endpoint, function_id=func_id)

  # batch_run returns a list task ids
  batch_res = fxc.batch_run(batch)


The batch result interface is useful to to fetch the results of a collection of task_ids.
``get_batch_result`` is called with a list of task_ids. It is non-blocking and returns
a ``dict`` with task_ids as the keys and each value is a dict that contains status information
and a result if it is available.

.. code-block:: python

  >>> results = fxc.get_batch_result(batch_res)
  >>> print(results)

  {'10c9678c-b404-4e40-bfd4-81581f52f9db': {'pending': False,
                                            'status': 'success',
                                            'result': 0,
                                            'completion_t': '1632876695.6450012'},
   '587afd2e-59e0-4d2d-82ab-cee409784c4c': {'pending': False,
                                            'status': 'success',
                                            'result': 0,
                                            'completion_t': '1632876695.7048604'},
   '11f34d69-913a-4442-ae79-ede046585d8f': {'pending': True,
                                            'status': 'waiting-for-ep'},
   'a2d86014-28a8-486d-b86e-5f38c80d0333': {'pending': True,
                                            'status': 'waiting-for-ep'},
   'e453a993-73e6-4149-8078-86e7b8370c35': {'pending': True,
                                            'status': 'waiting-for-ep'}
  }


.. _client credentials with funcxclients:

Client Credentials with FuncXClients
------------------------------------

Client credentials can be useful if you need an endpoint to run in a service account or to be started automatically with a process manager.

The funcX SDK supports use of Globus Auth client credentials for login, if you have `registered a client. <https://docs.globus.org/api/auth/developer-guide/#register-app>`_

To use client credentials, you must set the envrionment variables **FUNCX_SDK_CLIENT_ID** to your client ID, and **FUNCX_SDK_CLIENT_SECRET** to your client secret.

When these envrionment variables are set they will take priority over any other credentials on the system and the FuncXClient will assume the identity of the client app.
This also applies when starting a funcX endpoint.

.. code:: bash

  $ export FUNCX_SDK_CLIENT_ID="b0500dab-ebd4-430f-b962-0c85bd43bdbb"
  $ export FUNCX_SDK_CLIENT_SECRET="ABCDEFGHIJKLMNOP0123456789="

.. note:: funcX clients and endpoints will use the client credentials if they are set, so it is important to ensure the client submitting requests has access to an endpoint.


.. _login manager:

Using a Custom LoginManager
---------------------------

To programmatically create a FuncXClient from tokens and remove the need to perform a Native App login flow you can use a custom *LoginManager*.
The LoginManager is responsible for serving tokens to the FuncXClient as needed. Typically, this would perform a Native App login flow, store tokens, and return them as needed.

A custom LoginManager can be used to simply return static tokens and enable programmatic use of the FuncXClient.

.. note::
    To access the funcX API the scope that needs to be requested from
    Globus auth is FuncXClient.FUNCXSCOPE, which is:

    .. code::

      https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all


More details on the funcX login manager prototcol are available `here. <https://github.com/funcx-faas/funcX/blob/main/funcx_sdk/funcx/sdk/login_manager/protocol.py>`_


.. code:: python

  import globus_sdk
  from globus_sdk.scopes import AuthScopes, SearchScopes
  from funcx.sdk.login_manager import LoginManager
  from funcx.sdk.web_client import FuncxWebClient
  from funcx import FuncXClient

  class FuncXLoginManager:
    """
    Implements the funcx.sdk.login_manager.protocol.LoginManagerProtocol class.
    """

    def __init__(self, authorizers: dict[str, globus_sdk.RefreshTokenAuthorizer]):
        self.authorizers = authorizers

    def get_auth_client(self) -> globus_sdk.AuthClient:
        return globus_sdk.AuthClient(
            authorizer=self.authorizers[AuthScopes.openid]
        )

    def get_search_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient(
            authorizer=self.authorizers[SearchScopes.all]
        )

    def get_funcx_web_client(self, *, base_url: str) -> FuncxWebClient:
        return FuncxWebClient(
            base_url=base_url,
            authorizer=self.authorizers[FuncXClient.FUNCX_SCOPE],
        )

    def ensure_logged_in(self):
        return True

    def logout(self):
        log.warning("logout cannot be invoked from here!")

  # Create authorizers from existing tokens
  funcx_auth = globus_sdk.AccessTokenAuthorizer(funcx_token)
  search_auth = globus_sdk.AccessTokenAuthorizer(search_token)
  openid_auth = globus_sdk.AccessTokenAuthorizer(openid_token)

  # Create a new login manager and use it to create a client
  funcx_login_manager = FuncXLoginManager(
      authorizers={FuncXClient.FUNCX_SCOPE: funcx_auth,
                   SearchScopes.all: search_auth,
                   AuthScopes.openid: openid_auth}
  )

  fx = FuncXClient(login_manager=funcx_login_manager)
