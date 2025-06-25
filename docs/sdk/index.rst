Globus Compute SDK
==================

The **Globus Compute SDK** provides a convenient, Pythonic interface to Globus Compute.

The SDK is available on `PyPi <https://pypi.org/project/globus-compute-sdk/>`_, so installation
is as simple as:

.. code-block:: console

  $ python3 -m pip install globus-compute-sdk

We recommend the |Executor|_ class for most interactions. Under the hood, the |Executor|_
utilizes a |Client|_ object, which can be used directly for more granular control.


.. toctree::
   :maxdepth: 1

   client_user_guide
   executor_user_guide


.. |Client| replace:: ``Client``
.. _Client: client_user_guide.html
.. |Executor| replace:: ``Executor``
.. _Executor: executor_user_guide.html