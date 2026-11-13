Bug Fixes
^^^^^^^^^

- Fix the results publisher dropping results enqueued shortly before it stopped.
  An endpoint that shut down soon after producing results (e.g., one entering a
  bad state, which fails its outstanding tasks and exits) could lose those
  results, leaving the tasks "pending" at the web service instead of reporting
  them as failed.  The publisher now sends what is enqueued before closing its
  connection.  (#2217)
