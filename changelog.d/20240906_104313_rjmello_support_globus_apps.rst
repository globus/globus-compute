New Functionality
^^^^^^^^^^^^^^^^^

- The SDK ``Client`` and ``WebClient`` now support using a ``GlobusApp`` for authentication.
  For standard interactive login flows, users can leave the ``app`` argument blank when
  initializing the ``Client``, or pass in a custom ``UserApp``. For client authentication,
  users can leave the ``app`` argument blank and set the ``GLOBUS_COMPUTE_CLIENT_ID`` and
  ``GLOBUS_COMPUTE_CLIENT_SECRET`` environment variables, or pass in a custom ``ClientApp``.

  For more information on how to use a ``GlobusApp``, see the `Globus SDK documentation
  <https://globus-sdk-python.readthedocs.io/en/stable/experimental/examples/oauth2/globus_app.html>`_.

  Users can still pass in a custom ``LoginManager`` to the ``login_manager`` argument, but
  this is mutually exclusive with the ``app`` argument.

  E.g.,

  .. code-block:: python

     from globus_compute_sdk import Client
     from globus_sdk.experimental.globus_app import UserApp

     gcc = Client()

     # or

     my_app = UserApp("my-app", client_id="...")
     gcc = Client(app=my_app)
