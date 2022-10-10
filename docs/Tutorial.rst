funcX Tutorial
==============

funcX is a Function-as-a-Service (FaaS) platform for science that enables you to register functions in a cloud-hosted service and then reliably execute those functions on a remote funcX endpoint.
This tutorial is configured to use a tutorial endpoint hosted by the funcX team.

You can setup and use your own endpoint by following the `endpoint documentation <https://funcx.readthedocs.io/en/latest/endpoints.html>`_.

funcX Python SDK
----------------

The funcX Python SDK provides programming abstractions for interacting with the funcX service. Before running this tutorial you should first install the funcX SDK in its own `venv <https://docs.python.org/3/tutorial/venv.html>`_ environment:

.. code-block:: bash

    $ python3 -m venv path/to/funcx_venv
    $ source path/to/funcx_venv/bin/activate
    (funcx_venv) $ python3 -m pip install funcx

The funcX SDK exposes a ``FuncXClient`` object for all interactions with the funcX service.
In order to use the funcX service you must first authenticate using one of hundreds of supported identity provides (e.g., your institution, ORCID, Google).

As part of the authenticaiton process you must grant permission for funcX to access your identity information (to retrieve your email address), Globus Groups management access (to share functions and endpoints), and Globus Search (to discover functions and endpoints).

.. code-block:: python

    from funcx import FuncXClient

    fxc = FuncXClient()

Basic Usage
-----------

The following example demonstrates how you can register and execute a function.

Registering a Function
~~~~~~~~~~~~~~~~~~~~~~

funcX works like any other FaaS platform, you must first register a function with funcX before being able to execute it on a remote endpoint.
The registration process will serialize the function body and store it securely in the funcX service.
As we will see below, you may share functions with others and discover functions shared with you.

Upon registration funcX will return a UUID for the function. This UUID can then be used to manage and invoke the function.

.. code-block:: python

    def hello_world():
        return "Hello World!"

    func_uuid = fxc.register_function(hello_world)
    print(func_uuid)


Running a Function
~~~~~~~~~~~~~~~~~~

To invoke a function, you must provide a) the function's UUID; and b) the ``endpoint_id`` of the endpoint on which you wish to execute that function.
Note: here we use the public funcX tutorial endpoint, you may change the ``endpoint_id`` to the UUID of any endpoint for which you have permission to execute functions.

funcX functions are designed to be executed remotely and asynchrously.
To avoid synchronous invocation, the result of a function invocation (called a ``task``) is a UUID which may be introspected to monitor execution status and retrieve results.

The funcX service will manage the reliable execution of a task, for example by qeueing tasks when the endpoint is busy or offline and retrying tasks in case of node failures.

.. code-block:: python

    tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint
    res = fxc.run(endpoint_id=tutorial_endpoint, function_id=func_uuid)
    print(res)

Retrieving Results
~~~~~~~~~~~~~~~~~~

When the task has completed executing you can access the results via the funcX client as follows.

.. code-block:: python

    fxc.get_result(res)

Functions with Arguments
~~~~~~~~~~~~~~~~~~~~~~~~

funcX supports registration and invocation of functions with arbitrary arguments and returned parameters.
funcX will serialize any ``*args`` and ``**kwargs`` when invoking a function and it will serialize any return parameters or exceptions.

.. note::

    funcX uses standard Python serilaization libraries (e.g., Pickle, Dill) it also limits the size of input arguments and returned parameters to 5MB.

The following example shows a function that computes the sum of a list of input arguments.
First we register the function as above.

.. code-block:: python

    def funcx_sum(items):
        return sum(items)

    sum_function = fxc.register_function(funcx_sum)

When invoking the function you can pass in arguments like any other function, either by position or with keyword arguments.

.. code-block:: python

    items = [1, 2, 3, 4, 5]

    res = fxc.run(items, endpoint_id=tutorial_endpoint, function_id=sum_function)

    print (fxc.get_result(res))

Functions with Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

funcX requires that functions explictly state all dependencies within the function body.
It also assumes that the dependent libraries are available on the endpoint in which the function will execute.
For example, in the following function we import from ``datetime``:

.. code-block:: python

    def funcx_date():
        from datetime import date
        return date.today()

    date_function = fxc.register_function(funcx_date)

    res = fxc.run(endpoint_id=tutorial_endpoint, function_id=date_function)

    print (fxc.get_result(res))

Calling External Applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Depending on the configuration of the funcX endpoint you can often invoke external applications that are avaialble in the endpoint environment.

.. code-block:: python

    def funcx_echo(name):
        import os
        return os.popen("echo Hello %s" % name).read()

    echo_function = fxc.register_function(funcx_echo)

    res = fxc.run("World", endpoint_id=tutorial_endpoint, function_id=echo_function)

    print (fxc.get_result(res))

Catching Exceptions
~~~~~~~~~~~~~~~~~~~

When functions fail, the exception is captured and serialized by the funcX endpoint, and reraised when you try to get the result.
In the following example, the "deterministic failure" exception is raised when ``fxc.get_result`` is called on the failing function.

.. code-block:: python

    def failing():
        raise Exception("deterministic failure")

    failing_function = fxc.register_function(failing)

    res = fxc.run(endpoint_id=tutorial_endpoint, function_id=failing_function)

    fxc.get_result(res)

Running Functions Many Times
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After registering a function you can invoke it repeatedly.
The following example shows how the monte carlo method can be used to estimate pi.

Specifically, if a circle with radius r is inscribed inside a square with side length 2r, the area of the circle is πr\ :sup:`2` and the area of the square is (2r)\ :sup:`2`.
Thus, if N uniformly-distributed random points are dropped within the square, approximately Nπ/4 will be inside the circle.

.. code-block:: python

    import time

    # function that estimates pi by placing points in a box
    def pi(num_points):
        from random import random
        inside = 0
        for i in range(num_points):
            x, y = random(), random()  # Drop a random point in the box.
            if x**2 + y**2 < 1:        # Count points within the circle.
                inside += 1
        return (inside*4 / num_points)

    # register the function
    pi_function = fxc.register_function(pi)

    # execute the function 3 times
    estimates = []
    for i in range(3):
        estimates.append(fxc.run(10**5, endpoint_id=tutorial_endpoint, function_id=pi_function))

    # wait for tasks to complete
    time.sleep(5)

    # wait for all tasks to complete
    for e in estimates:
        while fxc.get_task(e)['pending'] == 'True':
            time.sleep(3)

    # get the results and calculate the total
    results = [fxc.get_result(i) for i in estimates]
    total = 0
    for r in results:
        total += r

    # print the results
    print("Estimates: %s" % results)
    print("Average: {:.5f}".format(total/len(results)))

Describing and Discovering Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

funcX manages a registry of functions that can be shared, discovered and reused.

When registering a function, you may choose to set a description to support discovery, as well as making it ``public`` (so that others can run it) and/or ``searchable`` (so that others can discover it).

.. code-block:: python

    def hello_world():
        return "Hello World!"

    func_uuid = fxc.register_function(hello_world, description="hello world function", public=True, searchable=True)
    print(func_uuid)

You can search previously registered functions to which you have access using ``search_function``.
The first parameter ``q`` is searched against all the fields, such as author, description, function name, and function source.
You can navigate through pages of results with the ``offset`` and ``limit`` keyword args.

The object returned is simple wrapper on a list, so you can index into it, but also can have a pretty-printed table.

.. code-block:: python

    search_results = fxc.search_function("hello", offset=0, limit=5)
    print(search_results)

Managing Endpoints
~~~~~~~~~~~~~~~~~~

funcX endpoints advertise whether or not they are online as well as information about their avaialble resources, queued tasks, and other information.
If you are permitted to execute functions on an endpoint you can also retrieve the status of the endpoint.
The following example shows how to look up the status (online or offline) and the number of number of waiting tasks and workers connected to the endpoint.

.. code-block:: python

    endpoint_status = fxc.get_endpoint_status(tutorial_endpoint)

    print("Status: %s" % endpoint_status['status'])
    print("Workers: %s" % endpoint_status['logs'][0]['total_workers'])
    print("Tasks: %s" % endpoint_status['logs'][0]['outstanding_tasks'])

Advanced Features
-----------------

funcX provides several features that address more advanced use cases.

Running Batches
~~~~~~~~~~~~~~~

After registering a function, you might want to invoke that function many times without making individual calls to the funcX service.
Such examples occur when running monte carlo simulations, ensembles, and parameter sweep applications.

funcX provides a batch interface which enables specification of a range of function invocations.
To use this interface you must create a funcX batch object and then add each invocation to that object.
You can then pass the constructed object to the ``batch_run`` interface.

.. code-block:: python

    def squared(x):
        return x**2

    squared_function = fxc.register_function(squared)

    inputs = list(range(10))
    batch = fxc.create_batch()

    for x in inputs:
        batch.add(x, endpoint_id=tutorial_endpoint, function_id=squared_function)

    batch_res = fxc.batch_run(batch)

Similary, funcX provides an interface to retrieve the status of the entire batch of invocations.

.. code-block:: python

    fxc.get_batch_result(batch_res)
