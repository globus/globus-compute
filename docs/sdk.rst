Globus Compute SDK User Guide
=============================

The **Globus Compute SDK** provides a programmatic interface to Globus Compute from Python.
The SDK provides a convenient Pythonic interface to:

1. Register functions
2. Register containers and execution environments
3. Launch registered functions on accessible endpoints
4. Check the status of launched functions
5. Retrieve outputs from functions

The SDK provides a client class for interacting with Globus Compute. The client
abstracts authentication and provides an interface to make Globus Compute
API calls without needing to know the Globus Compute REST endpoints for those operations.
You can instantiate a Globus Compute client as follows:

.. code-block:: python

  from globus_compute_sdk import Client
  gcc = Client()

Instantiating a client will start an authentication process where you will be asked to authenticate via Globus Auth.
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
   Please refer to the :doc:`limits` section for details.


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

The SDK includes a batch interface to reduce the overheads of launching a function many times.
To use this interface, you must first create a batch object and then pass that object
to the ``batch_run`` function. ``batch_run`` is non-blocking and returns a list of task ids
corresponding to the functions in the batch with the ordering preserved.

.. code-block:: python

  batch = gcc.create_batch()

  for x in range(0,5):
    batch.add(x, endpoint_id=tutorial_endpoint, function_id=func_id)

  # batch_run returns a list task ids
  batch_res = gcc.batch_run(batch)


The batch result interface is useful to to fetch the results of a collection of task_ids.
``get_batch_result`` is called with a list of task_ids. It is non-blocking and returns
a ``dict`` with task_ids as the keys and each value is a dict that contains status information
and a result if it is available.

.. code-block:: python

  >>> results = gcc.get_batch_result(batch_res)
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


.. _globus apps:

GlobusApps
-----------

The Compute |Client|_ uses |GlobusApp|_ objects to handle authentication and
authorization.  By default, the |Client|_ will instantiate a |UserApp|_ to facilitate a
native app login flow.  For headless setups that :ref:`export client credentials
<client credentials with globus compute clients>`, the |Client|_ will instantiate a
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

To create a |Client|_ from an existing access token and skip the interactive login flow, you can pass an |AccessTokenAuthorizer|_
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


.. _specifying-serde-strategy:

Specifying a Serialization Strategy
-----------------------------------

When sending functions and arguments for execution on a Compute endpoint, the SDK uses
the :class:`~globus_compute_sdk.serialize.ComputeSerializer` class to convert data to and from a format that can be easily
sent over the wire. Internally, :class:`~globus_compute_sdk.serialize.ComputeSerializer` uses instances of
:class:`~globus_compute_sdk.serialize.SerializationStrategy` to do the actual work of serializing (converting function code
and arguments to strings) and deserializing (converting well-formatted strings back into
function code and arguments).

The default strategies are :class:`~globus_compute_sdk.serialize.DillCode` for function code and :class:`~globus_compute_sdk.serialize.DillDataBase64` for
function ``*args`` and ``**kwargs``, which are both wrappers around |dill|_. To choose
another serializer, use the ``code_serialization_strategy`` and
``data_serialization_strategy`` members of the Compute :class:`~globus_compute_sdk.Client`:

.. code:: python

  from globus_compute_sdk import Client, Executor
  from globus_compute_sdk.serialize import CombinedCode, JSONData

  gcc = Client(
    code_serialization_strategy=CombinedCode(),
    data_serialization_strategy=JSONData()
  )
  gcx = Executor('4b116d3c-1703-4f8f-9f6f-39921e5864df', client=gcc)

  # do something with gcx

:doc:`See here for a up-to-date list of serialization strategies. </reference/serialization_strategies>`

To check whether a strategy works for a given use-case, use the :func:`~globus_compute_sdk.serialize.ComputeSerializer.check_strategies`
method:

.. code:: python

  from globus_compute_sdk.serialize import ComputeSerializer, DillCodeSource, JSONData

  def greet(name, greeting = "greetings"):
    """Greet someone."""

    return f"{greeting} {name}"

  serializer = ComputeSerializer(
    strategy_code=DillCodeSource(),
    strategy_data=JSONData()
  )

  serializer.check_strategies(greet, "world", greeting="hello")
  # serializes like the following:
  gcx.submit(greet, "world", greeting="hello")

  # use the return value of check_strategies:
  fn, args, kwargs = serializer.check_strategies(greet, "world", greeting="hello")
  assert fn(*args, **kwargs) == greet("world", greeting="hello")

.. _avoiding-serde-errors:

Avoiding Serialization Errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We strongly recommend that you use the same Python version as the target
Endpoint when using the SDK to submit new functions.

The serialization/deserialization mechanics in Python and the pickle/dill
libraries are implemented at the bytecode level and have evolved extensively
over time.  There is no backward/forward compatability guarantee between
versions.  Thus a function serialized in an older version of Python or dill
may not deserialize correctly in later versions, and the opposite is even more
problematic.

Even a single number difference in Python minor versions (e.g., from 3.12 |rarr| 3.13)
can generate issues.  Micro version differences (e.g., from 3.11.8 |rarr| 3.11.9)
are usually safe, though not universally.

Errors may surface as serialization/deserialization ``Exception``s, Globus
Compute task workers lost due to ``SEGFAULT``, or even incorrect results.

Note that the |Client|_ class's :func:`~globus_compute_sdk.Client.register_function` method can be used
to pre-serialize a function using the registering SDK's environment and
return a UUID identifier.   The resulting bytecode will then be deserialized
at run time by an Endpoint whenever a task that specifies this function UUID
is submitted (possibly from a different SDK environment) using the Client's
:func:`~globus_compute_sdk.Client.run` or the Executor's :func:`~globus_compute_sdk.Executor.submit_to_registered_function` methods.
On the other hand, the |Executor|_ 's :func:`~globus_compute_sdk.Executor.submit` takes a function argument
and serializes a fresh copy each time it is invoked.

.. |rarr| unicode:: 0x2192

.. |AccessTokenAuthorizer| replace:: ``AccessTokenAuthorizer``
.. _AccessTokenAuthorizer: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_authorizers.html#globus_sdk.AccessTokenAuthorizer
.. |Client| replace:: ``Client``
.. _Client: reference/client.html
.. |Executor| replace:: ``Executor``
.. _Executor: reference/executor.html
.. |dill| replace:: ``dill``
.. _dill: https://dill.readthedocs.io/en/latest/#basic-usage

.. |GlobusApp| replace:: ``GlobusApp``
.. _GlobusApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html
.. |UserApp| replace:: ``UserApp``
.. _UserApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html#globus_sdk.UserApp
.. |ClientApp| replace:: ``ClientApp``
.. _ClientApp: https://globus-sdk-python.readthedocs.io/en/stable/authorization/globus_app/apps.html#globus_sdk.ClientApp