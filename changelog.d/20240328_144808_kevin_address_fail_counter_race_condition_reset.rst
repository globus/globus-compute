Bug Fixes
^^^^^^^^^

- Address a race-condition in detecting Endpoint stability.  Previously, the EP
  could keep resetting an internal fail counter, potentially allowing the EP to
  stay up indefinitely in a half-working state.  The EP logic now more
  faithfully detects an unrecoverable error and will shutdown rather than
  giving an appearance of being alive.
