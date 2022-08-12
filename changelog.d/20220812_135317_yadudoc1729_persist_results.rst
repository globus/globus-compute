New Functionality
^^^^^^^^^^^^^^^^^

- New `ResultStore` class, that will store backlogged result messages to
  `<ENDPOINT_DIR>/unacked_results/`
- Upon disconnect from RabbitMQ, the endpoint will now retry connecting
  periodically while the executor continues to process tasks

Bug Fixes
^^^^^^^^^

- Fixed issue with `quiesce` event not getting set from the SIGINT handler,
  resulting in cleaner shutdowns

Removed
^^^^^^^

- `ResultsAckHandler` is removed, and `unacked_results.p` files are now
  obsolete.
