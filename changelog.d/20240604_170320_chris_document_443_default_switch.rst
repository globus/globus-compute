Changed
^^^^^^^

- Updated the Compute hosted services to use AMQP over port 443 by default, instead of
  the standard 5671. This can still be overridden in both the SDK and the Endpoint via
  ``amqp_port``.
