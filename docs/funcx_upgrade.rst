###########################################
Upgrading from funcX SDK and funcx-endpoint
###########################################

Overview
^^^^^^^^

The Globus team has renamed `funcX` to `Globus Compute` in order to centralize
our infrastructure under a single umbrella.

This migration involves changing the package names and most classes with
some variation of ``FuncX`` in the name.

TL;DR We recommend uninstalling the old packages and installing
``globus-compute-sdk`` / ``globus-compute-endpoint`` instead.  Then change
any import statements to reflect the name of the new packages.

For the SDK, ``FuncXClient`` and ``FuncXExecutor`` have been renamed to ``Client``
and ``Executor``.  For the Endpoint, merely using the command
``globus-compute-endpoint`` instead of ``funcx-endpoint`` should work.

If interacting with ``https://api2.funcx.org`` directly or the FuncX
Action Provider, please `see below for URL updates  <#URL-Changes>`_.

.. note::
  An alternate upgrade path is to install the latest ``funcx / funcx-endpoint``
  packages which are a wrapper around ``globus-compute-sdk`` and
  ``globus-compute-endpoint`` respectively. This should work as is for most
  users. However the funcx* packages are meant to be a temporary convenience
  and are soon to be deprecated.


funcX SDK
^^^^^^^^^

The ``funcx`` PyPI package was formerly the funcX SDK.  The SDK has been renamed
to the ``Globus Compute SDK`` and is `available on PyPI <https://pypi.org/project/globus-compute-sdk/>`_.

If you currently have ``funcx`` installed, we recommend these steps for upgrading:

  | $ pip uninstall funcx
  | $ mv ~/.funcx ~/.globus_compute   # Optional
  | $ `Install Globus Compute SDK in its own venv <quickstart.html#installing-the-globus-compute-endpoint-optional>`_

SDK Usage Changes
-----------------

Most users utilize only the ``FuncXClient`` and ``FuncXExecutor`` classes.
These have been renamed to ``Client`` and ``Executor`` respectively. The
required import changes are:

.. code-block:: python

  from funcx import FuncXClient
  from funcx import FuncXExecutor

to

.. code-block:: python

  from globus_compute_sdk import Client
  from globus_compute_sdk import Executor

All other module and classes that had ``"FuncX"`` in their names have been renamed
to ``Compute*``.  These changes `are detailed below <#SDK-Module-and-Class-Changes>`_.

The directory used for local cache storage is now ``~/.globus_compute`` instead of
``~/.funcx``.  We recommend renaming ``~/.funcx`` to ``~/.globus_compute`` after
installing the new package.  Otherwise, first usage of the new SDK will
automatically rename it and create a symlink from the old to the new.

For other, less relevant changes, `also see below <#SDK-Module-and-Class-Changes>`_.

funcX Endpoint
^^^^^^^^^^^^^^

``funcx-endpoint`` on PyPI was the former funcX endpoint package.  This is now
``Globus Compute Endpoint`` and is available on
`PyPI here <https://pypi.org/project/globus-compute-endpoint/>`_.

If you currently have ``funcx-endpoint`` installed, we recommend these steps for
upgrading:

  | $ pip uninstall funcx-endpoint
  | $ mv ~/.funcx ~/.globus_compute   # Optional
  | $ `Install Globus Compute Endpoint using pipx <quickstart.html#installing-the-globus-compute-endpoint-optional>`_

Endpoint Usage Changes
----------------------

For most users, the only change necessary is to use ``globus-compute-endpoint``
instead of ``funcx-endpoint`` when invoking endpoint commands.  i.e.

  | $ ``globus-compute-endpoint configure my_new_great_endpoint``
  | $ ``globus-compute-endpoint start my_new_great_endpoint``

The directory used to store endpoint info is now ``~/.globus_compute`` instead of
``~/.funcx``.  We recommend renaming your existing ``~/.funcx`` to
``~/.globus_compute`` after installing the new package.  On first start,
``globus-compute-endpoint`` will rename the directory and create a symlink from
the old to the new, if ``~.globus_compute`` doesn't already exist.

For other, less relevant changes, please `see below <#Endpoint-Module-and-Class-Changes>`_.

Detailed Change/Upgrade Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is not necessarily a comprehensive list of all changes. It is
only meant to detail items of relevance to most users.

funcx -> globus-compute-sdk
---------------------------

SDK Module and Class Changes
............................

These modules and classes have been renamed:

.. code-block:: python

  from funcx import FuncXClient
  from funcx import FuncXExecutor
  from funcx.errors import FuncxError
  from funcx.errors import FuncxTaskExecutionFailed
  from funcx.sdk.web_client import FuncXWebClient
  from funcx.sdk.asynchronous.funcx_task import FuncXTask
  from funcx.sdk.asynchronous.funcx_future import FuncXFuture  # Deprecated
  from funcx.serialize import FuncXSerializer
  from funcx.sdk.login_manager import FuncxScopes
  from funcx.sdk.login_manager import FuncxScopeBuilder

to

.. code-block:: python

  from globus_compute_sdk import Client
  from globus_compute_sdk import Executor
  from globus_compute_sdk.errors import ComputeError
  from globus_compute_sdk.errors import TaskExecutionFailed
  from globus_compute_sdk.sdk.web_client import WebClient
  from globus_compute_sdk.sdk.asynchronous.compute_future import ComputeFuture  # Deprecated
  from globus_compute_sdk.sdk.asynchronous.compute_task import ComputeTask
  from globus_compute_sdk.serialize import ComputeSerializer
  from globus_compute_sdk.sdk.login_manager import ComputeScopes
  from globus_compute_sdk.sdk.login_manager import ComputeScopeBuilder

Other SDK notes
...............

* ``LoginManager.get_funcx_web_client()`` has been renamed to ``.get_web_client()``

Most constants and variable names with ``FuncX`` in their names have **not**
changed in order to simplify the migration process:

* Client.FUNCX_SCOPE
* Client.FUNCX_SDK_CLIENT_ID
* Client.funcx_service_address,
* Client.funcx_home
* Client.fx_authorizer
* Client.fx_serializer
* Executor.funcx_client
* WebSocketPollingTask.funcx_client

The Scope value for the ``Globus Compute`` services has not changed with
respect to Globus Auth.


Using the new funcx wrapper package
...................................

* To ease the migration timeline for those who are not able to update all
  existing usage immediately, the ``funcx`` package will remain on PyPI
  for a limited time. Note, however, that as of v2.0.0, it is only a shim over
  ``globus-compute-sdk``.


The updated `funcx <https://pypi.org/project/funcx/>`_ package
begins with version 2.0.0, and is built on top of ``Globus Compute SDK`` 2.0.0.

These frequently used classes maintain their module hierarchy by linking to their
``Globus Compute SDK`` counterparts and do not require modification of scripts
that reference them:

.. code-block:: python

  from funcx import FuncXClient
  from funcx import FuncXExecutor
  import funcx.sdk.web_client
  from funcx.sdk.web_client import FuncXWebClient
  from funcx.sdk.login_manager import FuncxScopes
  from funcx.sdk.login_manager import LoginManager
  from funcx.sdk.login_manager import LoginManagerProtocol
  from funcx.sdk.login_manager import requires_login
  from funcx.sdk.serialize import FuncXSerializer

funcx-endpoint -> globus-compute-endpoint
-----------------------------------------

Endpoint Module and Class Changes
.................................

These modules and classes have been renamed:

.. code-block:: python

  from funcx_endpoint.logging_config import FXLogger
  from funcx_endpoint.logging_config import FuncxConsoleFormatter
  from funcx_endpoint.executors.high_throughput.funcx_worker import FuncXWorker


to

.. code-block:: python

  from globus_compute_endpoint.logging_config import ComputeLogger
  from globus_compute_endpoint.logging_config import ComputeConsoleFormatter
  from globus_compute_endpoint.executors.high_throughput.worker import Worker

Other endpoint notes
......................

* ``Config.funcx_service_address`` in ``globus_compute_endpoint.endpoint.utils.config`` has not been renamed.

Using the new funcx-endpoint wrapper package
............................................

* To ease the migration timeline for those who are not able to update all
  existing usage immediately, the ``funcx-endpoint`` package will remain on PyPI
  for a limited time. Note, however, that as of v2.0.0, it is only a shim over
  ``globus-compute-endpoint``.


The updated `funcx-endpoint <https://pypi.org/project/funcx-endpoint/>`_ package
begins with version 2.0.0, built on top of ``Globus Compute Endpoint`` 2.0.0.

These frequently used classes maintain their module hierarchy by linking to
Globus Compute Endpoint counterparts and do not require modification of scripts
that reference them:

.. code-block:: python

  from funcx_endpoint.endpoint.utils.config import Config
  from funcx_endpoint.executors import HighThroughputExecutor
  from funcx_endpoint.executors.high_throughput import Manager
  from funcx_endpoint.executors.high_throughput import FuncXWorker

URL Changes
-----------

* The Globus Compute API URL has changed from ``https://api2.funcx.org`` to
  ``https://compute.api.globus.org``
* The Action Provider URL has changed from ``https://automate.funcx.org``
  to ``https://compute.actions.globus.org``.  Please update any flows and
  remove the ActionScope in the definition, if present.

Note that while the old URLs are deprecated, they will continue to be available
while current users have a chance to migrate.

