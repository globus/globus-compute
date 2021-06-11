FuncX Executor
==============

The `FuncXExecutor` provides a future based interface that simplifies both function submission
and result tracking. The key functionality in the `FuncXExecutor` is the asynchronous event based
result tracking that propagates new results to the user via the use of `Futures`.
This asynchronous behavior is widely used in Python and the `FuncXExecutor` extends the popular executor
interface from the `concurrent.futures.Executor` library.


Initializing the executor
-------------------------

.. code-block:: python

   from funcx import FuncXClient
   from funcx.sdk.executor import FuncXExecutor

   fx = FuncXExecutor(FuncXClient())


Running functions
-----------------

.. code-block:: python

   ...
   fx = FuncXExecutor(FuncXClient())

   def double(x):
       return x * 2

   # The executor.submit method deviates from the concurrent.futures.Executor in that
   # you can specify funcX specific attributes such as endpoint_id and container_id
   # as keyword args
   future = fx.submit(double, x, endpoint_id=endpoint_id)

   # the future.done() method can be used to check the status of the function, without blocking
   # this will return a Bool indicating whether the task is complete
   print("Status : ", future.done())

   # the future.result() is a blocking call that waits until the function's result is available
   # if the function failed, an exception would be raised
   print("Result : ", future.result())


More complex cases
------------------

.. code-block:: python

   ...
   fx = FuncXExecutor(FuncXClient())


   def double(x):
       return x * 2

   # Here's how you'd launch several functions:
   futures = []
   for i in range(10):
       futures.append(double(i))

   # Now wait and print each result:
   for f in futures:
       print("Result : ", f.result())
