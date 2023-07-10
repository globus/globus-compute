.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

* Auto-scaling support for ``GlobusComputeEngine``
  Here is an example configuration in python:

  ```
  engine = GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period_s=1,
        heartbeat_threshold=1,
        provider=LocalProvider(
            init_blocks=0,  # Start with 0 blocks
            min_blocks=0,   # 0 minimum blocks
            max_blocks=4,   # scale upto 4 blocks
        ),
        strategy=SimpleStrategy(
            # Shut down blocks idle for more that 30s
            max_idletime=30.0,
        ),
    )
  ```
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
.. Changed
.. ^^^^^^^
..
.. - A bullet item for the Changed category.
..
.. Security
.. ^^^^^^^^
..
.. - A bullet item for the Security category.
..
