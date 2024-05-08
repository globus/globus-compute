Bug Fixes
^^^^^^^^^

- Make Executor shutdown idempotent -- if a user manually shut down the
  Executor within a ``with`` block, the Executor shutdown could hang if there
  were outstanding task futures.  Now the Executor recognizes that it has
  already been shutdown once, and the function returns early.

Changed
^^^^^^^

- Improve Executor shutdown performance by no longer attempting to join the
  task submitting thread.  This thread is already set to ``daemon=True`` and
  will correctly stop at Executor shutdown, so observe that ``.join()`` is
  strictly a waiting operation.  It is not a clue to the Python interpreter to
  clean up any resources.
