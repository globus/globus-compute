Bash Functions
--------------

|BashFunction|_ is the solution to executing commands remotely using Globus Compute.
The |BashFunction|_ class allows for the specification of a command string, along with
runtime details such as a run directory, per-task sandboxing, walltime, etc and returns a
|BashResult|_. |BashResult|_ encapsulates the outputs from executing the command line string
by wrapping the returnode and snippets from the standard streams (`stdout` and `stderr`).

Here's a basic example that demonstrates specifying a |BashFunction|_ that is to be
formatted with a list of values at launch time.

.. code-block:: python

   from globus_compute_sdk import BashFunction, Executor

   ep_id = <SPECIFY_ENDPOINT_ID>
   # The cmd will be formatted with kwargs at invocation time
   bf = BashFunction("echo '{message}'")
   with Executor(endpoint_id=ep_id) as ex:

       for msg in ["hello", "hola", "bonjour"]:
           future = ex.submit(bf, message=msg)
           bash_result = future.result()  # BashFunctions return BashResults
           print(bash_result.stdout)

   # Executing the above prints:
   hello

   hola

   bonjour


The |BashResult|_ object captures outputs relevant to simplify debugging when execution
failures. By default, |BashFunction|_ captures 1000 lines of stdout and stderr, but this
can be changed via the `BashFunction(snippet_lines)` kwarg.

Results
^^^^^^^

The output from a |BashFunction|_ is encapsulated in a |BashResult|_. Here are the various fields made
available through the |BashResult|_:

* `returncode`: The return code from the execution of the command supplied
* `stdout`: A snippet of upto the last 1K lines captured from the stdout stream
* `stderr`: A snippet of upto the last 1K lines captures form the stderr stream
* `cmd`: The formatted command string executed on the endpoint

.. note::
   The number of lines captured from stdout/err can be modified by setting `BashFunction(snippet_lines: int)`.
   Please keep in mind the result payload size limit of 10MB to which the snippet lines are counted.

Working Directory
^^^^^^^^^^^^^^^^^

Since BashFunctions operate on files, overwriting files unintentionally is a possibility. To mitigate this,
|BashFunction|_ enables sandboxing by default where each execution is set to a directory named after the task
UUID. The working directory is: `~/.globus_compute/<ENDPOINT_NAME>/tasks_working_dir/<TASK_UUID>`.
This functionality can be disabled by setting the `run_in_sandbox` keyword argument to `False`.
Here's an example:

.. code-block:: python

   bf = BashFunction("pwd")
   print(executor.submit(bf).result().stdout)

   # Executing the above prints:
   /Users/yadu/.globus_compute/test_endpoint/tasks_working_dir/70e53142-a391-4e27-a11d-582a786bf813

   bf = BashFunction("pwd", run_in_sandbox=False)
   print(executor.submit(bf).result().stdout)

   # Executing the above prints:
   /Users/yadu/.globus_compute/test_endpoint/tasks_working_dir

Walltime
^^^^^^^^

The `walltime` keyword argument to |BashFunction|_ can be used to specify the maximum duration (in seconds)
after which execution should be interrupted. If the execution was prematurely terminated due to reaching
the walltime, the returcode will be set to `124`, which matches the behavior of the
`timeout <https://ss64.com/bash/timeout.html>`_ command.

Here's an example:

.. code-block:: python

   # Limit execution to 1s
   bf = BashFunction("sleep 2", walltime=1)
   future = executor.submit(bf)
   print(future.returncode)

   # Executing the above prints:
   124


MPIFunctions
------------

|MPIFunction|_ extends |BashFunction|_ to support launching MPI applications. Similar
to the |BashFunction|_ interface, |MPIFunction|_ accepts a command string which it
launches using the appropriate MPI launcher as configured on a target endpoint configured
with |GlobusMPIEngine|_. This allows |MPIFunction|_ definitions to be agnostic to the
MPI environment on any of the endpoints it may be sent to. An |MPIFunction|_

Here's an example:

.. code-block:: python

   mpi_func = MPIFunction("lmp_mpi -in {input_file}")

   executor.resource_specification = {"num_nodes" : 2, "num_ranks": 4}

   future = executor.submit(mpi_func, input_file="/path/to/input_1")


Resource Specification
^^^^^^^^^^^^^^^^^^^^^^

Globus Compute allows for dynamically requesting MPI resources on a per-function basis
by updating the `resource_specification` attribute on the executor. `resource_specification`
takes these fields `num_nodes`, `num_ranks`, and `ranks_per_node`. Please refer to the
`resource specification docs from Parsl <https://parsl.readthedocs.io/en/stable/userguide/mpi_apps.html#writing-an-mpi-app>`_
for more details.

Here's an example of dynamically changing the resources requested:

.. code-block:: python

   mpi_func = MPIFunction("lmp_mpi -in {input_file}")

   all_futures = []
   for nodes in range(1,5):
      executor.resource_specification = {"num_nodes" : nodes,
                                         "num_ranks": nodes * 4}
      future = executor.submit(mpi_func, input_file="/path/to/input_file")
      all_futures.append(future)

   # Wait for all the MPI runs
   [future.result() for future in all_futures]





.. |BashFunction| replace:: ``BashFunction``
.. _BashFunction: reference/bash_function.html

.. |BashResult| replace:: ``BashResult``
.. _BashResult: reference/bash_function.html#globus_compute_sdk.sdk.bash_function.BashResult

.. |MPIFunction| replace:: ``MPIFunction``
.. _MPIFunction: reference/mpi_function.html

.. |GlobusMPIEngine| replace:: ``GlobusMPIEngine``
.. _GlobusMPIEngine: reference/mpi_engine.html
