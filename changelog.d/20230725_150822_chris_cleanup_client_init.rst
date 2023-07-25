.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..

Removed
^^^^^^^

- The following arguments to ``Client``, which were previously deprecated, have been
  removed:

  - ``asynchronous``

  - ``loop``

  - ``results_ws_uri``

  - ``warn_about_url_mismatch``

  - ``openid_authorizer``

  - ``search_authorizer``

  - ``fx_authorizer``

- Various internal classes relating to the former "asynchronous" mode of operating the
  ``Client``, such as ``WebSocketPollingTask`` and ``AtomicController``, have been
  removed alongside the removal of the ``asynchronous`` argument to the ``Client``.

Deprecated
^^^^^^^^^^

- The following arguments to ``Client``, which were previously unused, have been deprecated:

  - ``http_timeout``

  - ``funcx_home``

- The ``task_group_id`` argument to ``Client`` has been deprecated as a result of the
  new Task Group behavior.
