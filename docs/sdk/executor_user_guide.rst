Executor User Guide
===================

The |Executor|_ class, a subclass of Python's |ConcurrentFuturesExecutor|_, is the
preferred approach to collecting results from the Globus Compute web services.  Over
polling (the historical approach) where the web service must be repeatedly
queried for the status of tasks and results eventually collected in bulk, the
|Executor|_ class instantiates an AMQPS connection that streams results
directly -- and immediately -- as they arrive at the server.  This is a far
more efficient paradigm, simultaneously in terms of bytes over the wire, time
spent waiting for results, and boilerplate code to check for results.

For most "simple" interactions with Globus Compute, this class will likely be the
quickest and easiest avenue to submit tasks and acquire results.  An
example interaction:

.. code-block:: python
    :caption: globus_compute_executor_basic_example.py

    from globus_compute_sdk import Executor

    def double(x):
        return x * 2

    tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
    with Executor(endpoint_id=tutorial_endpoint_id) as gce:
        fut = gce.submit(double, 7)

        print(fut.result())

This example is only a quick-reference, showing the basic mechanics of how to
use the |Executor|_ class and submitting a single task.  However, there
are a number of details to observe.  The first is that a |Executor|_
instance is associated with a specific endpoint.  We use the "well-known"
tutorial endpoint in this example, but that can point to any endpoint to which
you have access.

.. note::
    A friendly FYI: the tutorial endpoint is public -- available for any
    (authenticated) user.  You are welcome to use it, but please limit the size
    and number of functions you send to this endpoint.  It is hosted on a small
    VM with limited CPU and memory, intentionally underpowered.   Its primary
    intended purpose is for an introduction to the Globus Compute toolset.

    This endpoint has been made public by the Globus Compute team for tutorial
    use, but endpoints created by users can not be shared publicly.

Second, the waiting -- or "blocking" -- for a result is automatic.  The
|.submit()|_ call returns a |Future|_ immediately; the actual HTTP call to the
Globus Compute web-services will not have occurred yet, and neither will the task even
been executed (remotely), much less a result received.  The |.result()|_ call
blocks ("waits") until all of that has completed, and the result has been
received from the upstream services.

Third, |Executor|_ objects can be used as context managers (the ``with``
statement).  Underneath the hood, the |Executor|_ class uses threads to
implement the asynchronous interface -- a thread to coalesce and submit tasks,
and a thread to watch for incoming results.  The |Executor|_ logic cannot
determine when it will no longer receive tasks (i.e., no more |.submit()|_
calls) and so cannot prematurely shutdown.  Thus, it must be told, either
explicitly with a call to |.shutdown()|_, or implicitly when used as a context
manager.

Multiple Function Invocations
-----------------------------

Building on the asynchronous behavior of |.submit()|_, we can easily create
multiple tasks and wait for them all.  The simplest case is a for-loop over
submission and results.  As an example, consider the `Collatz conjecture`_,
alternatively known as the :math:`3n + 1` problem.  The conjecture is that
given a starting integer and two generation rules, the outcome sequence will
always end with 1.  The rules are:

- If :math:`N_i` is even, then :math:`N_{i+1} = N_i / 2`
- If :math:`N_i` is odd, then :math:`N_{i+1} = 3 N_i + 1`

To verify all of the sequences through 100, one brute-force approach is:

.. code-block:: python
    :caption: globus_compute_executor_collatz.py

    from globus_compute_sdk import Executor

    def generate_collatz_sequence(N: int, sequence_limit = 10_000):
        seq = [N]
        while N != 1 and len(seq) < sequence_limit:
            if N % 2:
                N = 3 * N + 1
            else:
                N //= 2  # okay because guaranteed integer result
            seq.append(N)
        return seq

    ep_id = "<your_endpoint_id>"

    generate_from = 1
    generate_through = 100
    futs, results, disproof_candidates = [], [], []
    with Executor(endpoint_id=ep_id) as gce:
        for n in range(generate_from, generate_through + 1):
            futs.append(gce.submit(generate_collatz_sequence, n))
        print("Tasks all submitted; waiting for results")

    # The futures were appended to the `futs` list in order, so one could wait
    # for each result in turn to get a submission-ordered set of results:
    for f in futs:
        r = f.result()
        results.append(r)
        if r[-1] != 1:
            # of course, given the conjecture, we don't expect this branch
            disproof_candidates.append(r[0])

    print(f"All sequences generated (from {generate_from} to {generate_through})")
    for res in results:
        print(res)

    if disproof_candidates:
        print("Possible conjecture disproving integers:", disproof_candidates)

Checking the Status of a Result
-------------------------------

Sometimes, it is desirable not to wait for a result, but just to check on the
status.  Futures make this simple with the |.done()|_ method:

.. code-block:: python

    ...
    future = gce.submit(generate_collatz_sequence, 1234567890)

    # Use the .done() method to check the status of the function without
    # blocking; this will return a Bool indicating whether the result is ready
    print("Status: ", future.done())


Handling Exceptions
-------------------

Assuming that a future will always have a result will lead to broken scripts.
Exceptions happen, whether from a condition the task function does not handle
or from an external execution error.  To robustly handle task exceptions, wrap
|.result()|_ calls in a ``try`` block.  The following code has updated the
sequence generator to throw an exception after ``sequence_limit`` steps rather
than summarily return, and the specific number chosen starts a sequence that
takes more than 100 steps to complete.

.. code-block:: python
    :caption: globus_compute_executor_handle_result_exceptions.py

    from globus_compute_sdk import Executor

    def generate_collatz_sequence(N: int, sequence_limit=100):
        seq = [N]
        while N != 1 and len(seq) < sequence_limit:
            if N % 2:
                N = 3 * N + 1
            else:
                N //= 2  # okay because guaranteed integer result
            seq.append(N)
        if N != 1:
            raise ValueError(f"Sequence not terminated in {sequence_limit} steps")
        return seq

    with Executor(endpoint_id=ep_id) as gce:
        future = gce.submit(generate_collatz_sequence, 1234567890)

    try:
        print(future.result())
    except Exception as exc:
        print(f"Oh no!  The task raised an exception: {exc})


Receiving Results Out of Order
------------------------------

So far, we've shown simple iteration through the list of Futures, but that's
not generally the most performant approach for overall workflow completion.
In the previous examples, a result may return early at the end of the list, but
the script will not recognize it until it "gets there," waiting in the meantime
for the other tasks to complete.  (Task functions are not guaranteed to be
scheduled in order, nor are they guaranteed to take the same amount of time to
finish.)  There are a number of ways to work with results as they arrive; this
example uses `concurrent.futures.as_completed`_:

.. code-block:: python
    :caption: globus_compute_executor_results_as_arrived.py

    import concurrent.futures

    def double(x):
        return f"{x} -> {x * 2}"

    def slow_double(x):
        import random, time
        time.sleep(x * random.random())
        return f"{x} -> {x * 2}"

    with Executor(endpoint_id=endpoint_id) as gce:
        futs = [gce.submit(double, i) for i in range(10)]

        # The futures were appended to the `futs` list in order, so one could
        # wait for each result in turn to get an ordered set:
        print("Results:", [f.result() for f in futs])

        # But often acting on the results *as they arrive* is more desirable
        # as results are NOT guaranteed to arrive in the order they were
        # submitted.
        #
        # NOTA BENE: handling results "as they arrive" must happen before the
        # executor is shutdown.  Since this executor was used in a `with`
        # statement, then to stream results, we must *stay* within the `with`
        # statement.  Otherwise, at the unindent, `.shutdown()` will be
        # implicitly invoked (with default arguments) and the script will not
        # continue until *all* of the futures complete.
        futs = [gce.submit(slow_double, i) for i in range(10, 20)]
        for f in concurrent.futures.as_completed(futs):
            print("Received:", f.result())

Reloading Tasks
---------------
Waiting for incoming results with the |Executor|_ requires an active
connection -- which is often at odds with closing a laptop clamshell (e.g.,
heading home for the weekend).  For longer running jobs like this, the
|Executor|_ offers the |.reload_tasks()|_ method.  This method will reach
out to the Globus Compute web-services to collect all of the tasks associated with the
|.task_group_id|_, create a list of associated futures, finish
(call |.set_result()|_) any previously finished tasks, and watch the unfinished
futures.  Consider the following (contrived) example:

.. code-block:: python
    :caption: globus_compute_executor_reload_tasks.py

    # execute initially as:
    # $ python globus_compute_executor_reload_tasks.py
    #  ... this Task Group ID: <TG_UUID_STR>
    #  ...
    # Then run with the Task Group ID as an argument:
    # $ python globus_compute_executor_reload_tasks.py <TG_UUID_STR>

    import os, signal, sys, time, typing as t
    from globus_compute_sdk import Executor
    from globus_compute_sdk.sdk.executor import ComputeFuture

    task_group_id = sys.argv[1] if len(sys.argv) > 1 else None

    def task_kernel(num):
        return f"your Globus Compute logic result, from task: {num}"

    ep_id = "<YOUR_ENDPOINT_UUID>"
    with Executor(endpoint_id=ep_id) as gce:
        futures: t.Iterable[ComputeFuture]
        if task_group_id:
            print(f"Reloading tasks from Task Group ID: {task_group_id}")
            gce.task_group_id = task_group_id
            futures = gce.reload_tasks()

        else:
            # Save the task_group_id somewhere.  Perhaps in a file, or less
            # robustly "as mere text" on your console:
            print(
                "New session; creating Globus Compute tasks; if this script dies, rehydrate"
                f" futures with this Task Group ID: {gce.task_group_id}"
            )
            num_tasks = 5
            futures = [gce.submit(task_kernel, i + 1) for i in range(num_tasks)]

            # Ensure all tasks have been sent upstream ...
            while gce.task_count_submitted < num_tasks:
                time.sleep(1)
                print(f"Tasks submitted upstream: {gce.task_count_submitted}")

            # ... before script death for [silly reason; did you lose power!?]
            bname = sys.argv[0]
            if sys.argv[0] != sys.orig_argv[0]:
                bname = f"{sys.orig_argv[0]} {bname}"

            print("Simulating unexpected process death!  Now reload the session")
            print("by rerunning this script with the task_group_id:\n")
            print(f"  {bname} {gce.task_group_id}\n")
            os.kill(os.getpid(), signal.SIGKILL)
            exit(1)  # In case KILL takes split-second to process

    # Get results:
    results, exceptions = [], []
    for f in futures:
        try:
            results.append(f.result(timeout=10))
        except Exception as exc:
            exceptions.append(exc)
    print("Results:\n ", "\n  ".join(results))

For a slightly more advanced usage, one could manually submit a batch of tasks
with the |Client|_, and wait for the results at a future time.  Submitting
the results might look like:

.. code-block:: python
    :caption: globus_compute_client_submit_batch.py

    from globus_compute_sdk import Client

    def expensive_task(task_arg):
        import time
        time.sleep(3600 * 24)  # 24 hours
        return "All done!"

    ep_id = "<endpoint_id>"
    gcc = Client()

    print(f"Task Group ID for later reloading: {gcc.session_task_group_id}")
    fn_id = gcc.register_function(expensive_task)
    batch = gcc.create_batch()
    for task_i in range(10):
        batch.add(fn_id, ep_id, args=(task_i,))
    gcc.batch_run(batch)

And ~24 hours later, could reload the tasks with the executor to continue
processing:

.. code-block:: python
    :caption: globus_compute_executor_reload_batch.py

    from globus_compute_sdk import Executor

    ep_id = "<endpoint_id>"
    tg_id = "Saved task group id from 'yesterday'"
    with Executor(endpoint_id=ep_id, task_group_id=tg_id) as gce:
        futures = gce.reload_tasks()
        for f in concurrent.futures.as_completed(futs):
            print("Received:", f.result())


Shell Functions
---------------

|ShellFunction| is the solution to executing commands remotely using Globus Compute.
The |ShellFunction| class allows for the specification of a command string, along
with runtime details such as a run directory, per-task sandboxing, walltime, etc and
returns a |ShellResult|. |ShellResult| encapsulates the outputs from executing the
command line string by wrapping the return code and snippets from the standard
streams (``stdout`` and ``stderr``).

Here's a basic example that demonstrates specifying a |ShellFunction| that is to be
formatted with a list of values at launch time.

.. code-block:: python
    :caption: globus_compute_shell_function.py

    from globus_compute_sdk import ShellFunction, Executor

    ep_id = "<SPECIFY_ENDPOINT_ID>"
    # The cmd will be formatted with kwargs at invocation time
    bf = ShellFunction("echo '{message}'")
    with Executor(endpoint_id=ep_id) as ex:
        for msg in ("hello", "hola", "bonjour"):
            future = ex.submit(bf, message=msg)
            shell_result = future.result()  # ShellFunctions return ShellResults
            print(shell_result.stdout)

Executing the above prints:

.. code-block:: text

    hello

    hola

    bonjour


The |ShellResult| object captures outputs relevant to simplify debugging when execution
failures. By default, |ShellFunction| captures 1,000 lines of stdout and stderr, but this
can be changed via the ``ShellFunction(snippet_lines)`` kwarg.


Shell Results
^^^^^^^^^^^^^

The output from a |ShellFunction| is encapsulated in a |ShellResult|. Here are the various fields made
available through the |ShellResult|:

* ``returncode``: The return code from the execution of the command supplied
* ``stdout``: A snippet of upto the last 1K lines captured from the stdout stream
* ``stderr``: A snippet of upto the last 1K lines captures form the stderr stream
* ``cmd``: The formatted command string executed on the endpoint

To return a JSON-compatible dict instead of a |ShellResult| object, set the ``return_dict`` argument to
``True`` when instantiating a |ShellFunction|:

.. code-block:: python
   :emphasize-lines: 3, 8

    from globus_compute_sdk import Executor, ShellFunction

    func = ShellFunction("echo '{message}'", return_dict=True)
    with Executor(endpoint_id="...") as ex:
        for msg in ("hello", "hola", "bonjour"):
            fut = ex.submit(func, message=msg)
            res = future.result()
            assert isinstance(res, dict)
            assert msg in res["stdout"]

.. note::
    Bear in mind that the snippet lines count toward the 10 MiB payload size limit.  The
    number of lines captured from ``stdout`` and ``stderr`` can be modified by setting
    the :doc:`snippet_lines <../reference/shell_function>` keyword argument.

Working Directory
^^^^^^^^^^^^^^^^^

Since ShellFunctions operate on files, overwriting files unintentionally is a
possibility.  To mitigate this, |ShellFunction| recommends enabling sandboxing through
the endpoint configuration.  By default the working directory is:
``~/.globus_compute/<ENDPOINT_NAME>/tasks_working_dir/``.  With sandboxing enabled, each
|ShellFunction| executes within a task specific directory named:
``~/.globus_compute/<ENDPOINT_NAME>/tasks_working_dir/<TASK_UUID>``.

Here's an example configuration:

.. code-block:: yaml

    display_name: SandboxExample
    engine:
      type: GlobusComputeEngine

      # Enable sandboxing
      run_in_sandbox: True


Walltime
^^^^^^^^

The ``walltime`` keyword argument to |ShellFunction| can be used to specify the maximum duration (in seconds)
after which execution should be interrupted. If the execution was prematurely terminated due to reaching
the walltime, the return code will be set to ``124``, which matches the behavior of the
`timeout <https://ss64.com/bash/timeout.html>`_ command.

Here's an example:

.. code-block:: python

    # Limit execution to 1s
    bf = ShellFunction("sleep 2", walltime=1)
    future = executor.submit(bf)
    print(future.returncode)

Executing the above prints:

.. code-block:: text

    124


.. _submitting-mpi:

Submitting MPI Tasks
--------------------

|MPIFunction| is the solution for executing MPI applications remotely using Globus Compute.
As an extension to |ShellFunction|, the |MPIFunction| supports the same interface
to specify the command to invoke on the endpoint as well as the capture of output
streams. However, |MPIFunction| diverges from |ShellFunction| in the following ways:

1. An |MPIFunction| requires a specification of the resources for its execution. This
   specification must be set on the executor on the client-side. The ``resource_specification: dict``
   takes the following options:

   .. code-block:: python

        executor.resource_specification = {
            'num_nodes': <int>,        # Number of nodes required for the application instance
            'ranks_per_node': <int>,   # Number of ranks / application elements to be launched per node
            'num_ranks': <int>,        # Number of ranks in total
        }

2. |MPIFunction| is designed to be used with |GlobusMPIEngine| on the endpoint.
   |GlobusMPIEngine| is required for the partitioning of a batch job (blocks) dynamically
   based on the ``resource_specification`` of the |MPIFunction|.


3. |MPIFunction| automatically prefixes the supplied command with ``$PARSL_MPI_PREFIX``
   which resolves to an appropriate mpi launcher prefix (for e.g, ``mpiexec -n 4 -host <NODE1,NODE2>``).


Multi-line commands
^^^^^^^^^^^^^^^^^^^

|MPIFunction| allows for multi-line commands, however the MPI launcher prefix is applied only to
the entire command. If multiple commands need to be launched, explicitly use the ``$PARSL_MPI_PREFIX``.
Here's an example:

.. code-block:: python

    MPIFunction("""true; # force the default prefix to launch a no-op
    $PARSL_MPI_PREFIX <command_1>
    $PARSL_MPI_PREFIX <command_2>
    """)

Results
^^^^^^^

|MPIFunction| encapsulates its output in a |ShellResult|, which captures relevant
outputs to simplify debugging. By default, |MPIFunction| captures 1,000 lines of
``stdout`` and ``stderr``, but this can be changed via the ``snippet_lines`` kwarg:

.. code-block:: python
    :emphasize-lines: 3

    MPIFunction(
        "my_mpi_application --arg1 val1",
        snippet_lines=2048
    )

`See the section on shell results <#shell-results>`_ for more information.

.. _specifying-serde-strategy:

Specifying a Submit-Side Serialization Strategy
-----------------------------------------------

When sending functions and arguments for execution on a Compute endpoint, the SDK uses
the :class:`~globus_compute_sdk.serialize.ComputeSerializer` class to convert data to and from a format that can be easily
sent over the wire. Internally, :class:`~globus_compute_sdk.serialize.ComputeSerializer` uses instances of
:class:`~globus_compute_sdk.serialize.SerializationStrategy` to do the actual work of serializing (converting function code
and arguments to strings) and deserializing (converting well-formatted strings back into
function code and arguments).

The default strategies are :class:`~globus_compute_sdk.serialize.DillCode` for function code and
:class:`~globus_compute_sdk.serialize.DillDataBase64` for function ``*args`` and ``**kwargs``, which are both wrappers around
|dill|_. To choose another serializer, use the ``serializer`` member of the Compute :class:`~globus_compute_sdk.Executor`:

.. code:: python

  from globus_compute_sdk import Executor
  from globus_compute_sdk.serialize import ComputeSerializer, PureSourceTextInspect, JSONData

  with Executor('<your-endpoint-id>') as gcx:
    gcx.serializer = ComputeSerializer(
      strategy_code=PureSourceTextInspect(), strategy_data=JSONData()
    )

:doc:`See here for a up-to-date list of serialization strategies. <../reference/serialization_strategies>`

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
over time.  There is no backward/forward compatibility guarantee between
versions.  Thus a function serialized in an older version of Python or dill
may not deserialize correctly in later versions, and the opposite is even more
problematic.

Even a single number difference in Python minor versions (e.g., from 3.12 |rarr| 3.13)
can generate issues.  Micro version differences (e.g., from 3.11.8 |rarr| 3.11.9)
are usually safe, though not universally.

Errors may surface as serialization/deserialization ``Exception``s, Globus
Compute task workers lost due to ``SEGFAULT``, or even incorrect results.

Note that the |Client| class's :func:`~globus_compute_sdk.Client.register_function`
method can be used to pre-serialize a function using the registering SDK's environment
and return a UUID identifier.  The resulting bytecode will then be deserialized at run
time by an Endpoint whenever a task that specifies this function UUID is submitted
(possibly from a different SDK environment) using the Client's
:func:`~globus_compute_sdk.Client.run` or the |Executor|'s
:func:`~globus_compute_sdk.Executor.submit_to_registered_function` methods.


Specifying Result Serialization Strategies
------------------------------------------

In addition to specifying what strategy to use when submitting tasks, it is also
possible to specify what strategy the endpoint should use when serializing the results
of any given task to be sent back to the SDK. This is achieved through the |Executor|'s
``result_serializers`` constructor argument and property:

.. code-block:: python

  from globus_compute_sdk import Executor
  from globus_compute_sdk.serialize import JSONData

  with Executor("your endpoint id", result_serializers=[JSONData()]) as gcx:
    # or set it directly:
    gcx.result_serializers = [JSONData()]

When a task submission specifies a list of result serializers, the endpoint will
attempt each serialization strategy in the list until one of them succeeds, returning
that first success. If all strategies in the list fail, the endpoint returns an error
with the details of each failure.


AMQP Port
---------

As of v2.11.0, newly configured endpoints use AMQP over port 443 by default,
since firewall rules usually allow outbound HTTPS.

The port that the Executor uses to fetch task results remains the default AMQP
5671, but can be overridden via the ``amqp_port`` parameter during
instantiation.  This may be necessary in cases where outbound 5671 is also
unavailable in the task submission environment, such as some cloud VMs
like BinderHub:

.. code-block:: python

  >>> from globus_compute_sdk import Executor
  >>> gce = Executor(amqp_port=443)


.. |rarr| unicode:: 0x2192

.. |Client| replace:: ``Client``
.. _Client: ../reference/client.html
.. |Executor| replace:: ``Executor``
.. _Executor: ../reference/executor.html
.. |Future| replace:: ``Future``
.. _Future: https://docs.python.org/3/library/concurrent.futures.html#future-objects
.. |ConcurrentFuturesExecutor| replace:: ``concurrent.futures.Executor``
.. _ConcurrentFuturesExecutor: https://docs.python.org/3/library/concurrent.futures.html#executor-objects
.. |.shutdown()| replace:: ``.shutdown()``
.. _.shutdown(): ../reference/executor.html#globus_compute_sdk.Executor.shutdown
.. |.submit()| replace:: ``.submit()``
.. _.submit(): ../reference/executor.html#globus_compute_sdk.Executor.submit
.. |.result()| replace:: ``.result()``
.. _.result(): https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.result
.. |.done()| replace:: ``.done()``
.. _.done(): https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.done
.. |.set_result()| replace:: ``.set_result()``
.. _.set_result(): https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.set_result
.. |.reload_tasks()| replace:: ``.reload_tasks()``
.. _.reload_tasks(): ../reference/executor.html#globus_compute_sdk.Executor.reload_tasks
.. |.task_group_id| replace:: ``.task_group_id``
.. _.task_group_id: ../reference/executor.html#globus_compute_sdk.Executor.task_group_id
.. _Collatz conjecture: https://en.wikipedia.org/wiki/Collatz_conjecture
.. _concurrent.futures.as_completed: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.as_completed
.. |dill| replace:: ``dill``
.. _dill: https://dill.readthedocs.io/en/latest/#basic-usage
.. |MPIFunction| replace:: :class:`~globus_compute_sdk.sdk.mpi_function.MPIFunction`
.. |ShellFunction| replace:: :class:`~globus_compute_sdk.sdk.shell_function.ShellFunction`
.. |ShellResult| replace:: :class:`~globus_compute_sdk.sdk.shell_function.ShellResult`
.. |GlobusMPIEngine| replace:: :class:`~globus_compute_endpoint.engines.GlobusMPIEngine`
