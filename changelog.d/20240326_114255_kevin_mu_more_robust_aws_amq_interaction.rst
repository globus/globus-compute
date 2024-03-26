Changed
^^^^^^^

- Update AMQP reconnection handling; previously the reopen-connection logic was
  woefully optimistic of service or network downtime, assuming connectivity
  would be restored in ~a minute.  Reality is that a network can be down for
  hours and a service can take multiple minutes to update.  Consequently,
  update the number of retry attempts from 3 or 5 to 7,200.  (For context,
  reconnection attempts occur randomly between every 0.5s and 10s, so this
  means than an endpoint that has lost connectivity will attempt to reconnect
  to the web-services for somewhere between 1 and 20 hours.)  Hopefully, this
  is an adequate value to ensure that Compute endpoints weather most relevant
  connectivity outages.
