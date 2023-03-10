Quickstart
==========

**Globus Compute** client and endpoint software releases are available on `PyPI <https://pypi.org/project/funcx/>`_.

You can try Globus Compute on a hosted Jupyter notebook with `Binder <https://mybinder.org/v2/gh/funcx-faas/examples/HEAD?filepath=notebooks%2FIntroduction.ipynb>`_


Installation
------------

**Globus Compute** comes with two components: the **endpoint**, a user-managed software agent that must be deployed on a compute resource to make it accessible for function execution; and the **Globus Compute client**, which provides a Python API for registration, execution, and management of functions across **endpoints**.

The pre-requisites for the `Globus Compute endpoint` and the `Globus Compute client` are

  1. Python3.7+
  2. The machine must have outbound network access

To check if you have the right Python version, run the following commands::

  >>> python3 --version

This should return the Python version, for example: ``Python 3.8.10``.

To check if your endpoint/client has network access and can connect to the Globus Compute service, run::

  >>> curl https://api2.funcx.org/v2/version

This should return a version string, for example: ``"1.0.5"``

.. note:: The Globus Compute client is supported on MacOS, Linux, and Windows. The globus-compute-endpoint
   is only supported on Linux.

Installing Globus Compute in a Virtual Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` and ``pip3`` can be used to install Globus Compute we suggest the following approach
for reliable installation to avoid python package dependency conflicts.

1. Install the Globus Compute client in its own `venv <https://docs.python.org/3/tutorial/venv.html>`_ environment::

    $ python3 -m venv path/to/globus_compute_venv
    $ source path/to/globus_compute_venv/bin/activate
    (globus_compute_venv) $ python3 -m pip install globus-compute-sdk

  To update a previously installed Globus Compute to a newer version in the virtual environment, use::

    (globus_compute_venv) $ python3 -m pip install -U globus-compute-sdk

2. (Optional) The Globus Compute endpoint can be installed using `Pipx <https://pypa.github.io/pipx/installation/>`_ or using pip in the venv::

     $ python3 -m pipx install globus-compute-endpoint

          or

     (globus_compute_venv) $ python3 -m pip install globus-compute-endpoint

3. (Optional) Install Jupyter for Tutorial notebooks in the venv::

     (globus_compute_venv) $ python3 -m pip install jupyter


.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_


First Run
---------

The Globus Compute SDK makes use of the Globus Compute web services, most of which restrict use
to Globus authenticated users.  Consequently, if you have not previously used
Globus Compute from your workstation, or have otherwise not authenticated with Globus,
then the Client will present a one-time URL.  The one-time URL workflow
will culminate in a token code to be pasted back into the terminal.  The
easiest approach is typically from the command line:

.. code-block:: python

    >>> from globus_compute_sdk import Client
    >>> Client()
    Please authenticate with Globus here:
    ------------------------------------
    https://auth.globus.org/v2/oauth2/authorize?[...very...long...url]&prompt=login
    ------------------------------------

    Enter the resulting Authorization Code here:

Globus Compute will then cache the credentials for future invocations, so this workflow
will only be initiated once.

Running a function
------------------

After installing the Globus Compute SDK, you can define a function and submit it for
execution to available endpoints.  For most use-cases that will use the
``Executor``:

.. code-block:: python

    from globus_compute_sdk import Executor

    # First, define the function ...
    def add_func(a, b):
        return a + b

    tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint
    # ... then create the executor, ...
    with Executor(endpoint_id=tutorial_endpoint_id) as gce:
        # ... then submit for execution, ...
        future = gce.submit(add_func, 5, 10)

        # ... and finally, wait for the result
        print(future.result())

.. note::
    Like most FaaS platforms, the function must be registered with the upstream
    web services before it can be executed on a remote endopint.  While one can
    manually register a function (see the Client or Executor API
    documentation), the above workflow will automatically handle registration.

A word on the above example: while the tutorial endpoint is open for anyone to
use, it is hosted on a small Kubernetes cluster -- somewhat intentionally
underpowered.  As it is a shared (and tiny) resource, please be conscientious
with the size and number of functions you send to this endpoint.

Deploying an endpoint
----------------------

You can deploy an endpoint on your laptop, cluster, or cloud
by downloading and installing the Globus Compute endpoint software.
The Globus Compute endpoint software is available on PyPI and a default
endpoint can be configured and started as follows. During the
configuration process you will be prompted to authenticate
following the same process as using the SDK.
For more advanced deployments (e.g., on clouds and clusters) please
refer to the `endpoints`_ documentation. ::

  $ python3 -m pip install globus-compute-endpoint

  $ globus-compute-endpoint configure

  $ globus-compute-endpoint start <ENDPOINT_NAME>


.. _endpoints: endpoints.html
