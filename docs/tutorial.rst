Globus Compute Tutorial
=======================

Globus Compute is a Function-as-a-Service (FaaS) platform that enables
fire-and-forget execution of Python functions on one or more remote
Globus Compute endpoints.

This tutorial is configured to use a tutorial endpoint hosted by the
Globus Compute team. You can setup your own endpoint on resources to
which you have access by following the `Globus Compute
documentation <https://globus-compute.readthedocs.io/en/latest/endpoints.html>`__.
Globus Compute endpoints can be deployed on many cloud platforms,
clusters with batch schedulers (e.g., Slurm, PBS), Kubernetes, or on a
local PC. After configuring an endpoint you can use it in this tutorial
by simply setting the ``endpoint_id`` below.

Note that although the tutorial endpoint has been made public by the Globus
Compute team, endpoints created by users can not be shared publicly.

Globus Compute Python SDK
~~~~~~~~~~~~~~~~~~~~~~~~~

The Globus Compute Python SDK provides programming abstractions for
interacting with the Globus Compute service. Before running this
tutorial you should first install the Globus Compute SDK as follows:

.. code:: shell

   $ pip install globus-compute-sdk

The Globus Compute SDK exposes a ``Client`` and ``Executor`` for
interacting with the Globus Compute service. In order to use Globus
Compute, you must first authenticate using one of hundreds of supported
identity provides (e.g., your institution, ORCID, or Google). As part of
the authentication process you must grant permission for Globus Compute
to access your identity information (to retrieve your email address) and
Globus Groups management access (for sharing functions).

.. code:: python

    from globus_compute_sdk import Executor


**Note**: Here we use the public Globus Compute tutorial endpoint.
You can use this endpoint to run the tutorial (the endpoint is shared
with all Globus Compute users). You can also change the
``endpoint_id`` to the UUID of any endpoint for which you have
permission to execute functions.

.. code:: python

    tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint
    gce = Executor(endpoint_id = tutorial_endpoint)
    print("Executor : ", gce)

Globus Compute 101
------------------

The following example demonstrates how you can execute a function with
the ``Executor`` interface.

Submitting a function
^^^^^^^^^^^^^^^^^^^^^

To execute a function, you simply call ``submit`` and pass a reference
to the function. Optionally, you may also specify any input arguments to
the function.

.. code:: python

    # Define the function for remote execution
    def hello_world():
        return "Hello World!"

    future = gce.submit(hello_world)

    print("Submit returned: ", future)

Getting results
^^^^^^^^^^^^^^^

When you ``submit`` a function for execution (called a ``task``), the
executor will return an instance of ``ComputeFuture`` in lieu of the
result from the function. Futures are a common way to reference
asynchronous tasks, enabling you to interrogate the future to find the
status, results, exceptions, etc. without blocking to wait for results.

``ComputeFuture``\ s returned from the ``Executor`` can be used in the
following ways: \* ``future.done()`` is a non-blocking call that returns
a boolean that indicates whether the task is finished. \*
``future.result()`` is a blocking call that returns the result from the
task execution or raises an exception if task execution failed.

.. code:: python

    # Returns a boolean that indicates task completion
    future.done()

    # Waits for the function to complete and returns the task result or exception on failure
    future.result()

Catching exceptions
~~~~~~~~~~~~~~~~~~~

When a task fails and you try to get its result, the ``future`` will
raise an exception. In the following example, the ``ZeroDivisionError``
exception is raised when ``future.result()`` is called.

.. code:: python

    def division_by_zero():
        return 42 / 0 # This will raise a ZeroDivisionError

    future = gce.submit(division_by_zero)

    try:
        future.result()
    except Exception as exc:
        print("Globus Compute returned an exception: ", exc)

Functions with arguments
~~~~~~~~~~~~~~~~~~~~~~~~

Globus Compute supports registration and execution of functions with
arbitrary arguments and returned parameters. Globus Compute will
serialize any ``*args`` and ``**kwargs`` when executing a function and
it will serialize any return parameters or exceptions.

Note: Globus Compute uses standard Python serilaization libraries (i.e.,
Dill). It also limits the size of input arguments and returned
parameters to 10 MB. For larger input or output data we suggest using
Globus.

The following example shows a function that computes the sum of a list
of input arguments.

.. code:: python

    def get_sum(a, b):
        return a + b

    future = gce.submit(get_sum, 40, 2)
    print(f"40 + 2 = {future.result()}")

Functions with dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to execute a function on a remote endpoint, Globus Compute
requires that functions explictly state all dependencies within the
function body. It also requires that any dependencies (e.g., libraries,
modules) are available on the endpoint on which the function will
execute. For example, in the following function, we explicitly import
the datetime module.

.. code:: python

    def get_date():
        from datetime import date
        return date.today()

    future = gce.submit(get_date)

    print("Date fetched from endpoint: ", future.result())

Calling external applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While Globus Compute is designed to execute Python functions, you can
easily invoke external applications that are accessible on the remote
endpoint. For example, the following function calls the Linux ``echo``
command.

.. code:: python

    def echo(name):
        import os
        return os.popen("echo Hello {} from $HOSTNAME".format(name)).read()

    future = gce.submit(echo, "World")

    print("Echo output: ", future.result())

Running functions many times
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One of the strengths of Globus Compute is the ease by which you can run
functions many times, perhaps with different input arguments. The
following example shows how you can use the Monte Carlo method to
estimate pi.

Specifically, if a circle with radius :math:`r` is inscribed inside a
square with side length :math:`2r`, the area of the circle is
:math:`\pi r^2` and the area of the square is :math:`(2r)^2`. Thus, if
:math:`N` uniformly-distributed points are dropped at random locations
within the square, approximately :math:`N\pi/4` will be inside the
circle and therfore we can estimate the value of :math:`\pi`.

.. code:: python

    import time

    # function that estimates pi by placing points in a box
    def pi(num_points):
        from random import random
        inside = 0

        for i in range(num_points):
            x, y = random(), random()  # Drop a point randomly within the box.
            if x**2 + y**2 < 1:        # Count points within the circle.
                inside += 1
        return (inside*4 / num_points)


    # execute the function 3 times
    estimates = []
    for i in range(3):
        estimates.append(gce.submit(pi,
                                   10**5))

    # get the results and calculate the total
    total = [future.result() for future in estimates]

    # print the results
    print("Estimates: {}".format(total))
    print("Average: {:.5f}".format(sum(total)/len(estimates)))

Endpoint operations
-------------------

You can retrieve information about endpoints including status and
information about how the endpoint is configured.

.. code:: python

    from globus_compute_sdk import Client
    gcc = Client()

    gcc.get_endpoint_status(tutorial_endpoint)
