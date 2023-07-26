.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
Bug Fixes
^^^^^^^^^

- Previously, starting an endpoint when it is already active or is currently locked will exit silently when ``globus-compute-endpoint start`` is run, with the only information available as a log line in endpoint.log.  Now, if start fails, a console message will display the reason on the command line.
