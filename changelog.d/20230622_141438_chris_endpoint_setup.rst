.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- Added ``endpoint_setup`` option to endpoint config, which, if present, is run by the
  system shell during the endpoint startup process, before any other initialization.

  - For example:

    .. code-block:: yaml

      endpoint_setup: |
        python -m venv my_environment
        pip install -r requirements.txt
