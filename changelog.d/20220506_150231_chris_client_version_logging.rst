New Functionality
^^^^^^^^^^^^^^^^^

- The `FuncXWebClient` now sends version information via `User-Agent` headers
  through the `app_name` property exposed by `globus-sdk`

  - Additionally, users can send custom metadata alongside this version
    information with `user_app_name`
