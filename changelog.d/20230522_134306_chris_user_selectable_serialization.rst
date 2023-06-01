.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- The strategies used to serialize functions and arguments are now selectable at the
  ``Client`` level via constructor arguments (``code_serialization_strategy`` and
  ``data_serialization_strategy``)
  - For example, to use ``DillCodeSource`` when serializing functions:
    ``client = Client(code_serialization_strategy=DillCodeSource())``
  - This functionality is available to ``Executor``s by passing a custom client. Using
    the client above: ``executor = Executor(funcx_client=client)``

Changed
^^^^^^^

- Simplified the logic used to select a serialization strategy when one isn't specified -
  rather than try every strategy in order, Globus Compute now simply defaults to
  ``DillCode`` and ``DillDataBase64`` for code and data respectively

