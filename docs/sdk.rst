SDK
============

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

  from funcx.sdk.client import FuncXClient
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
          entries, you can re-authenticate your client with `force_login=True`.

Registering Functions
---------------------

You can register a Python function with funcX via `register_function()`. Function registration serializes the
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
You can invoke a function using the UUID returned when registering the function. The `run()` function
requires that you specify the function (`function_id`) and endpoint (`endpoint_id`) on which to execute
the function. funcX will return a UUID for the executing function (called a task) via which you can
monitor status and retrieve results.

.. code-block:: python

  tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
  task_id = fxc.run(endpoint_id=tutorial_endpoint, function_id=func_uuid)


Retrieving Results
-------------------
The result of your function's invocation can be retrieved using the `get_result()` function. This will either
return the deserialized result of your invocation or raise an exception indicating that the
task is still pending.

.. note:: If your function raises an exception, get_result() will reraise it.

.. code-block:: python

  try:
    print(fxc.get_result(task_id))
  except Exception as e:
    print("Exception: {}".format(e))

.. note:: funcX caches results in the cloud until they have been retrieved. The SDK also caches results
          during a session. However, calling `get_result()` from a new session will not be able to access the results.


Arguments and data
------------------
funcX functions operate the same as any other Python function. You can pass arguments \*args and \**kwargs
and return values from functions. The only constraint is that data passed to/from a funcX function must be
serializable (e.g., via Pickle) and less than 2 MB in size.  Input arguments can be passed to the function
using the `run()` function. The following example shows how strings can be passed to and from a function.

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

To share with a group, set `group=<globus_group_id>` when registering a function.

.. code-block:: python

  fxc.register_function(funcx, description="My function", group=<globus_group_id>)


Upon execution, funcX will check group membership to ensure that the user is authorized to execute the function.

You can also set a function to be publicly accessible by setting `public=True` when registering the function.

.. code-block:: python

  fxc.register_function(funcx, description="My function", public=True)


Discovering Functions
----------------------

funcX maintains an access controlled search index of registered functions.
You can look up your own functions, functions that have been shared with you,
or publicly accessible functions via the `search_function()` function.

.. code-block:: python

  search_results = fxc.search_function("my function", offset=0, limit=5)
  print(search_results)


Batching
--------------

The SDK includes a batch interface to reduce the overheads of launching a function many times.
To use this interface, you must first create a batch object and then pass that object
to the `batch_run` function.

.. code-block:: python

  batch = fxc.create_batch()

  for x in range(0,5):
    batch.add(x, endpoint_id=tutorial_endpoint, function_id=func_id)

  batch_res = fxc.batch_run(batch)

There is also a batch result interface to retrieve the results of a batch.

.. code-block:: python

  fxc.get_batch_result(batch_res)


Client Throttling
-----------------

In order to avoid overloading funcX, we place soft throttling restrictions on the funcX client.
There are two key throttling measures: first, we limit the number of requests a client can make (20 requests every 10 seconds),
and second, we limit the size of input and output transmitted through the service (5 MB per request).

Batching requests and status can help reduce the number of requests made to the Web Service. In addition, the limit on
the number of requests made to the Web Service can be removed by setting `throttling_enabled` to False.

.. code-block:: python

  fxc = FuncXClient()
  fxc.throttling_enabled = False


FuncXClient Reference:
----------------------

.. autoclass:: funcx.FuncXClient
   :members:
