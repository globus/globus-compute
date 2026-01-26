Globus Compute Executor
=======================

As a subclass of Python's |ConcurrentFuturesExecutor|_, Compute SDK's |Executor|
is simultaneously easier to use than the |Client|, more performant, and more efficient
on the network.

For usage examples and explanations, please see the :doc:`../sdk/executor_user_guide`.

----

.. autoclass:: globus_compute_sdk.Executor
    :members:
    :member-order: bysource

.. autoclass:: globus_compute_sdk.sdk.executor.ComputeFuture
    :members:
    :member-order: bysource

.. |ConcurrentFuturesExecutor| replace:: ``concurrent.futures.Executor``
.. _ConcurrentFuturesExecutor: https://docs.python.org/3/library/concurrent.futures.html#executor-objects

.. |Client| replace:: :class:`Client <globus_compute_sdk.sdk.client.Client>`

.. _Compute web services API: https://compute.api.globus.org/redoc