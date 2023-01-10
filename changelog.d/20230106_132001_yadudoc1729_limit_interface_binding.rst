.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
.. New Functionality
.. ^^^^^^^^^^^^^^^^^
..
.. - A bullet item for the New Functionality category.
..
.. Bug Fixes
.. ^^^^^^^^^
..
.. - A bullet item for the Bug Fixes category.
..
.. Removed
.. ^^^^^^^
..
.. - A bullet item for the Removed category.
..
.. Deprecated
.. ^^^^^^^^^^
..
.. - A bullet item for the Deprecated category.
..
Changed
^^^^^^^

- `HighThroughputExecutor.address` now accepts only IPv4 and IPv6. Example configs
  have been updated to use `parsl.address_by_interface` instead of `parsl.address_by_hostname`.
  Please note that following this change, endpoints that were previously configured
  with `HighThroughputExecutor(address=address_by_hostname())` will now raise a `ValueError`
  and will need updating.

- For better security, `HighThroughputExecutor` now listens only on a specific interface
  rather than all interfaces.
..
.. Security
.. ^^^^^^^^
..
.. - A bullet item for the Security category.
..
