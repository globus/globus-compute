Quickstart
==========

**funcX** client and endpoint software releases are available on `PyPI <https://pypi.org/project/funcx/>`_.

The latest version available on PyPI is ``v0.2.3``.

You can try funcX on `Binder <https://mybinder.org/v2/gh/funcx-faas/funcx/master?filepath=examples%2FTutorial.ipynb>`_


Installation
------------

**funcX** comes with two components: the **endpoint**, a user-managed software agent that must be deployed on a compute resource to make it accessible for function execution; and the **funcX client**, which provides a Python API for registration, execution, and management of functions across **endpoints**.

The pre-requisites for the `funcX endpoint` and the `funcX client` are

  1. Python3.6+
  2. The machine must have outbound network access

To check if you have the right Python version, run the following commands::

  >>> python3 --version

This should return the Python version, for example: ``Python 3.6.7``.

To check if your endpoint/client have network access and can connect to the funcX service, run ::

  >>> curl https://api2.funcx.org/v2/version

This should return a version string, for example: ``"0.2.2"``

.. note:: The funcx client is supported on MacOS, Linux, and Windows. The funcx-endpoint
   is only supported on Linux.

Installation using Pip
^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` and ``pip3`` can be used to install funcX we suggest the following approach
for reliable installation when many Python environments are available.

1. Install the funcX client::

     $ python3 -m pip install funcx

To update a previously installed funcX to a newer version, use: ``python3 -m pip install -U funcx``

2. Optionally install the funcX endpoint::

     $ python3 -m pip install funcx_endpoint

3. Install Jupyter for Tutorial notebooks::

     $ python3 -m pip install jupyter


.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_


Running a function
------------------------

After installing the funcX SDK, you can register functions and execute
them on available endpoints.  To use the SDK, you should first instantiate
a funcX client and authenticate with the funcX service. funcX uses
Globus to manage authentication and authorization, enabling you to
authenticate using one of several hundred supported identity providers
(e.g., institution, ORCID, Google). If you have not authenticated previously,
funcX will present a one-time URL that you can use to authenticate
with your chosen identity. You will then need to copy and paste the resulting
access code into the prompt.

.. code-block:: python

    from funcx.sdk.client import FuncXClient

    fxc = FuncXClient()


Like most FaaS platforms, you must first register a function before you can
execute or share it. To do so, you can simply write a Python function
and register it using the SDK.

.. code-block:: python

  def add_func(a, b):
    return a + b

  func_uuid = fxc.register_function(add_func)


When executing the function, you must specify the function ID and the
endpoint ID on which you wish to execute the function. You can pass
arbitrary input arguments like standard Python functions. The following
code shows how to run the add function on the tutorial endpoint.

Note: the tutorial endpoint is open for anyone to use; please limit
the number of functions you send to this endpoint.

.. code-block:: python

    tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint
    res = fxc.run(5, 10, function_id=func_uuid, endpoint_id=tutorial_endpoint)

Finally, you can retrieve the result (or check on the status of the execution)
via the SDK. The SDK will raise an exception if the result is not yet ready
or it will return the Python result from your function.

Note: the tutorial endpoint is hosted on a small Kubernetes cluster and
occasionally it becomes overwhelmed. If you are unable to retrieve the
result, please try again later (funcX will cache results until you return)
or deploy an endpoint on local resources.

.. code-block:: python

 print(fxc.get_result(res))


Deploying an endpoint
----------------------

You can deploy an endpoint on your laptop, cluster, or cloud
by downloading and installing the funcX endpoint software.
The funcX endpoint software is available on PyPI and a default
endpoint can be configured and started as follows. During the
configuration process you will be prompted to authenticate
following the same process as using the SDK.
For more advanced deployments (e.g., on clouds and clusters) please
refer to the `endpoints` documentation. ::

  $ python3 -m pip install funcx_endpoint

  $ funcx-endpoint configure

  $ funcx-endpoint start <ENDPOINT_NAME>
