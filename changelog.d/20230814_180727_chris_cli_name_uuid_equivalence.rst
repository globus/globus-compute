.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- In the ``globus-compute-endpoint`` CLI, commands which operate on registered endpoints
  can now accept UUID values in addition to names.

  - The following sub-commands can now accept either a name or a UUID:

    - ``delete``

    - ``restart``

    - ``start``

    - ``stop``

    - ``update_funcx_config``

  - (The other sub-commands either do not accept endpoint name arguments, like ``list``,
    or cannot accept UUID arguments, like ``configure``.)
