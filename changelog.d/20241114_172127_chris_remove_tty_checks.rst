Changed
^^^^^^^

- Bumped dependency on ``globus-sdk-python`` to at least `version 3.47.0 <https://github.com/globus/globus-sdk-python/releases/tag/3.47.0>`_.
  This version includes changes to detect ``EOFErrors`` when logging in with the command
  line, obviating the need for existing ``globus-compute-sdk`` code that checks if a
  user is in an interactive terminal before logging in. The old Compute code raised a
  ``RuntimeError`` in that scenario; the new code raises a
  ``globus_sdk.login_flows.CommandLineLoginFlowEOFError`` if Python's ``input``
  function raises an ``EOFError`` - which, in Compute, can happen if a previously
  command-line-authenticated endpoint tries to re-authenticate but no longer has access
  to a STDIN.
