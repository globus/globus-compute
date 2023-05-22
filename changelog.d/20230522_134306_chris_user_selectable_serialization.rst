.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- The methods used to serialize functions and arguments are now selectable at the
  ``Client`` level via constructor arguments (``code_serializer_method`` and
  ``data_serializer_method``)
  - For example, to use ``DillCodeSource`` when serializing functions:
    ``client = Client(code_serializer_method=DillCodeSource())``
  - This functionality is available to ``Executor``s by passing a custom client. Using
    the client above: ``executor = Executor(funcx_client=client)``

Changed
^^^^^^^

- Simplified the logic used to select a serialization method when one isn't specified -
  rather than try every serializer in order, Globus Compute now simply defaults to
  ``DillCode`` and ``DillDataBase64`` for code and data respectively

