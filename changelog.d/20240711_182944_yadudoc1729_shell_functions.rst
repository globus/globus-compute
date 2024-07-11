New Functionality
^^^^^^^^^^^^^^^^^

- Added a new ``ShellFunction`` class to support remote execution of commandline strings.


  .. code:: python

      bf = ShellFunction("echo '{message}'")
      future = executor.submit(bf, message="Hello World!")
      shell_result = future.result()  # ShellFunction returns a ShellResult
      print(shell_result.returncode)  # Exitcode
      print(shell_result.cmd)         # Reports the commandline string executed
      print(shell_result.stdout)      # Snippet of stdout captured
      print(shell_result.stderr)      # Snippet of stderr captured
