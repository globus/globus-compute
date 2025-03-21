Changed
^^^^^^^

- |ShellFunction| and |MPIFunction| erroneously used the full ``cmd`` string as a function name.
  These classes now default the function name to the class name. User may customize the function
  name using the ``name`` keyword argument:

  .. code-block:: python

    s_fn = ShellFunction("echo Hi", name="my_hi_function")
    mpi_fn = MPIFunction("lammps ..", name="my_lammps")
