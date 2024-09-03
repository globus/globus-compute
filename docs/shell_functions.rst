Shell Functions
---------------

|ShellFunction|_ is the solution to executing commands remotely using Globus Compute.
The |ShellFunction|_ class allows for the specification of a command string, along with
runtime details such as a run directory, per-task sandboxing, walltime, etc and returns a
|ShellResult|_. |ShellResult|_ encapsulates the outputs from executing the command line string
by wrapping the returnode and snippets from the standard streams (``stdout`` and ``stderr``).

Here's a basic example that demonstrates specifying a |ShellFunction|_ that is to be
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


The |ShellResult|_ object captures outputs relevant to simplify debugging when execution
failures. By default, |ShellFunction|_ captures 1,000 lines of stdout and stderr, but this
can be changed via the ``ShellFunction(snippet_lines)`` kwarg.


Shell Results
^^^^^^^^^^^^^

The output from a |ShellFunction|_ is encapsulated in a |ShellResult|_. Here are the various fields made
available through the |ShellResult|_:

* ``returncode``: The return code from the execution of the command supplied
* ``stdout``: A snippet of upto the last 1K lines captured from the stdout stream
* ``stderr``: A snippet of upto the last 1K lines captures form the stderr stream
* ``cmd``: The formatted command string executed on the endpoint

.. note::
    Bear in mind that the snippet lines count toward the 10 MiB payload size limit.  The
    number of lines captured from ``stdout`` and ``stderr`` can be modified by setting
    the :doc:`snippet_lines <reference/shell_function>` keyword argument.

Working Directory
^^^^^^^^^^^^^^^^^

Since ShellFunctions operate on files, overwriting files unintentionally is a
possibility.  To mitigate this, |ShellFunction|_ recommends enabling sandboxing through
the endpoint configuration.  By default the working directory is:
``~/.globus_compute/<ENDPOINT_NAME>/tasks_working_dir/``.  With sandboxing enabled, each
|ShellFunction|_ executes within a task specific directory named:
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

The ``walltime`` keyword argument to |ShellFunction|_ can be used to specify the maximum duration (in seconds)
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

.. include:: mpi_functions.rst
