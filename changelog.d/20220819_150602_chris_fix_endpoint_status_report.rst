New Functionality
^^^^^^^^^^^^^^^^^

- Endpoints now report their online status immediately on startup (previously,
  endpoints waited ``heartbeat_period`` seconds before reporting their status).

- In order to support the new endpoint status format, endpoints now report their
  heartbeat period as part of their status report package.

Changed
^^^^^^^

- Changed the way that endpoint status is stored in the services - instead of storing a
  list of the most recent status reports, we now store the single most recent status
  report with a TTL set to the endpoint's heartbeat period. This affects the formatting
  of the return value of ``FuncXClient.get_endpoint_status``.
