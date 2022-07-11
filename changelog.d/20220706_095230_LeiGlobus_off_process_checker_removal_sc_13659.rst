.. A new scriv changelog fragment.
..
..
Removed
-------

- The off_process_checker, previously used to test function serialization methods, was removed

Changed
-------

- Pickle module references were replaced with dill

- The order of serialization method attempts has been changed to try dill.dumps first
