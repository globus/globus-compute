New Functionality
^^^^^^^^^^^^^^^^^

- Enable use of the Executor with High Assurance (HA) endpoints.  The
  fundamental change is that rather than receiving the result directly via
  AMQP, it instead is only notified that a result is ready.  The Executor will
  then reach out to the web-services to collect the known-complete tasks,
  thereby initiating an HA check.
