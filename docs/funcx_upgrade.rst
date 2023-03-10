###########################################
Upgrading from funcX SDK and funcx-endpoint
###########################################

Note
^^^^
This document will be expanded at a later date to include more details.

Background
^^^^^^^^^^

The Globus team is renaming funcX to Globus Compute in order centralize our
infrastructure under a single umbrealla.

funcX SDK
^^^^^^^^^

The `funcx` PyPI package was formerly the funcX SDK.  This is now named the `Globus
Compute SDK` and is `available on PyPI <https://pypi.org/project/globus-compute-sdk/>`_
under the same name.

If you currently have funcX installed, we recommend these steps to upgrade to
Globus Compute:

  | $ pip uninstall funcx
  | $ mv ~/.funcx ~/.compute   # Optional
  | $ Install Globus Compute SDK in its own venv `as detailed here <quickstart.rst#_install_gc_sdk>`__

The `funcx` package is still available on PyPI but will merely be a wrapper
around the Globus Compute SDK.  The wrapper will only be available for
a limited time to assist users as an easier migration path.

funcX Endpoint
^^^^^^^^^^^^^^

`funcx-endpoint` on PyPI was the former funcX endpoint package.  This is now called
the `Globus Compute Endpoint` and is
`available on PyPI <https://pypi.org/project/globus-compute-client/>`_.

  | $ pip uninstall funcx-endpoint
  | $ mv ~/.funcx ~/.compute   # Optional
  | $ Install Globus Compute Endpoint `using pipx <quickstart.rst#_install_gc_endpoint>`__

The `funcx-endpoint` package is still available on PyPI but will merely be a wrapper
around Globus Compute Endpoint.  The wrapper will only be available for
a limited time to assist users as an easier migration path.

