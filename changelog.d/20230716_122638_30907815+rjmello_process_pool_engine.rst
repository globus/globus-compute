New Functionality
^^^^^^^^^^^^^^^^^

- Reimplemented ``ProcessPoolEngine``, which wraps ``concurrent.futures.ProcessPoolExecutor``,
  for concurrent local execution. We temporarily removed the former implementation because of a
  critical bug.