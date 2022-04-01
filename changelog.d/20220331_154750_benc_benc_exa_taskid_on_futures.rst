.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- FuncXExecutor.submit returns futures with a .task_id attribute
  that will contain the task ID of the corresponding FuncX task.
  If that task has not been submitted yet, then that attribute
  will contain None.
