New Functionality
^^^^^^^^^^^^^^^^^

- Added a new `BashFunction` class to support remote execution of commandline strings.


  .. code:: python

      bf = BashFunction("echo '{message}'")
      future = executor.submit(bf)
      bash_result = future.result()  # BashFunction returns a BashResult
      print(bash_result.returncode)  # Exitcode
      print(bash_result.cmd)         # Reports the commandline string executed
      print(bash_result.stdout)      # Snippet of stdout captured
      print(bash_result.stderr)      # Snippet of stderr captured
