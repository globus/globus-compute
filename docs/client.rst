funcX Client
============

The **funcX Client** is the programmatic interface to FuncX from Python. The client provides a simple and intuitive
interface to:

1. Register functions
2. Register containers and execution environments
3. Launch registered function against endpoints
4. Check the status of launched functions and
5. Retrieve outputs from functions

The following shows an example of creating a client.::

  $ from funcx.sdk.client import FuncXClient
  $ fxc = FuncXClient()

Instantiating a client will start an authentication process where you will be asked to authenticate with Globus Auth.
We require every interaction with the funcX Web Service to be authenticated using a Bearer token, this allows funcX
to enforce access control on both functions and endpoints. As part of the authentication process we request access
to your identity information (to retrieve your email address) and Globus Groups management access. We require
Groups access in order to facilitate sharing. Users can share functions with others by associating a Globus Group
with the function. If a group is set, the funcX Web Service uses Groups Management access to determine whether a user
is a member of the specified group.

.. note:: The funcX Web Service internally caches function, endpoint, and access control lookups. Caches are based on user authentication tokens. To refresh the caches you can re-authenticate your client with `force_login=True`.

Registering Functions
---------------------

You can register a Python function with funcX via `register_function()`. Function registration will serialize the
function body and transmit it to the funcX Web Service. Once a function is registered with the service it will return
a UUID that can be used to invoke the function.

.. note:: You must import any dependencies required by the function inside the function body.

You can associate a Globus Group with a function to enable sharing. If set, the Web Service will check if the invoking
user is a member of the group. This is achieved by setting `group=<globus_group_id>` when registering a function.

You can also set a function to be publicly accssible by setting `public=True` when registering the function.


Running Functions
-----------------
You can invoke a function using the UUID returned when registering the function. The client's `run()` function
requires you to specify the `function_id` and `endpoint_id`. In addition, you can pass \*args and \**kwargs
to the run function and they will be used when invoking your function. We serialize all inputs and outputs when running a function.

The result of your function's invocation can be retrieved using the client's `get_result()` function. This will either
raise an exception if the task is still pending, or give you back the deserialized result of your invocation.

.. note:: If your function's execution raises an exception, get_result() will reraise it.

To minimize the overhead of communicating with the funcX Web Service we provide batch request and status capabilities.
Documentation on batch requests can be found below.


Client Throttling
-----------------

In order to avoid accidentally DoS'ing the funcX Web Service we place soft throttling restrictions on the funcX client.
There are two key throttling measures: firstly, we limit the number of requests a client can make to the Web Service
to 5 every 5 seconds, and secondly, we limit the size of inputs and outputs transmitted through the service to 2MB.

Batching requests and status can help reduce the number of requests made to the Web Service. In addition, the limit on
the number of requests made to the Web Service can be removed by setting `throttling_enabled` to False.::

  $ fxc = FuncXClient()
    fxc.throttling_enabled = False


FuncXClient Reference:
----------------------

.. autoclass:: funcx.FuncXClient
   :members:

