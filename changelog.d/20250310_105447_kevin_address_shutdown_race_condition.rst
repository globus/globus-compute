Bug Fixes
^^^^^^^^^

- Address a shutdown time race condition where an endpoint could receive a task
  just as it was shutting down, effectively losing the task.  Implement a check
  after receiving tasks; if the endpoint is shutting down, do not
  ``ACK``nowledge the task so that the AMQP service will retain it for a later
  endpoint instance.
