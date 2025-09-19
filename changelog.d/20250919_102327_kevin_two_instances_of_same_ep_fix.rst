Bug Fixes
^^^^^^^^^

- 3.14.0 fixed an authorization-timeout bug for endpoint startups, but
  erroneously then allowed multiple endpoint instances to attempt to start from
  the same host.  The pre-3.14.0 behavior is returned, whereby the second
  instance will see the PID file (``daemon.pid``) and refuse to start.
