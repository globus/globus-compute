Bug Fixes
^^^^^^^^^

- Address 3.13-introduced bug that killed the endpoint if it failed to start up
  within 15s.  (This was most easily identified when the endpoint prompted for
  a Globus Auth authorization code.)
