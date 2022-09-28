funcX Executor
==============

The ``FuncXExecutor`` provides a future based interface that simplifies both function submission
and result tracking. The key functionality in the ``FuncXExecutor`` is the asynchronous event based
result tracking that propagates new results to the user via the use of ``Futures``.
This asynchronous behavior is widely used in Python and the ``FuncXExecutor`` extends the popular executor
interface from the ``concurrent.futures.Executor`` library.


Initializing the executor
-------------------------

.. code-block:: python

   from funcx import FuncXExecutor

   fx = FuncXExecutor(endpoint_id=endpoint_id)


Running functions
-----------------

.. code-block:: python

   def double(x):
       return x * 2

   future = fx.submit(double, x)  # will execute on endpoint_id

   # Use the .done() method to check the status of the function without blocking;
   # this will return a Bool indicating whether the result is ready
   print("Status : ", future.done())

   # Alternatively, the .result() method will block, not returning until the
   # function's result is available.  If the function instead fails, it will
   # raise an exception.
   print("Result : ", future.result())

   # When done with the executor, shut down the background threads via .shutdown():
   fx.shutdown()


More complex cases
------------------

.. code-block:: python

   import concurrent.futures

   # The FuncXExecutor also works as a `with` statement, avoiding the need
   # to remember to shut it down:
   with FuncXExecutor(endpoint_id=endpoint_id) as fx:

       def double(x):
           return f"{x} -> {x * 2}"

       # Launching several function invocations is the same as launching
       # one: .submit()

       futs = []
       for i in range(10):
           futs.append(fx.submit(double, i))

       # The futures were appended to the `futs` list in order, so one could
       # wait for each result in turn to get an ordered set:
       print("Results:", [f.result() for f in futs])

       # But often acting on the results *as they arrive* is more desirable
       # as results are NOT guaranteed to arrive in the order they were
       # submitted:
       def slow_double(x):
           import random, time
           time.sleep(x * random.random())
           return f"{x} -> {x * 2}"

       futs = [fx.submit(slow_double, i) for i in range(10, 20)]
       for f in concurrent.futures.as_completed(futs):
           print("Received:", f.result())
