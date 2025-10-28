Dynamic Python Environments
***************************

To avoid serialization and other compatibility issues, :ref:`we recommend <avoiding-serde-errors>`
ensuring the Python version on endpoint workers matches the Python version used to submit tasks
via the SDK. This is particularly problematic for :doc:`multi-user <../endpoints/multi_user>`
endpoint setups because the endpoint administrators have little control over their user's SDK
environments. We recommend two different ways to address this:

#. Enable :doc:`dynamic worker container configuration <dynamic_containers>`
#. Enable dynamic worker Python environment activation based on the submitting user's
   Python version (covered in this tutorial)


Configure an Endpoint
=====================

If you are starting from scratch, you will need to initialize an endpoint with
the ``configure`` subcommand:


.. code-block:: console

   $ globus-compute-endpoint configure my-ep

.. tip::
   If running this command as a privileged user (i.e., root), please refer to
   :ref:`multi-user-configuration` for additional configuration steps.


Modify the Configuration Template
---------------------------------

Every configuration template can access :ref:`reserved variables <reserved-template-variables>`
that contain various useful information. The ``user_runtime`` object stores information about the
submitting client's runtime environment, including their Python and SDK versions. We can leverage
this to dynamically update the ``worker_init`` field -- a bash script executed when each worker
process starts -- to create and activate the appropriate Python virtual environments. The example
in this tutorial uses `uv`_ to create virtual environments, but you can modify it to use `conda`_
or any other package management system.


Verify ``uv`` installation
~~~~~~~~~~~~~~~~~~~~~~~~~~

We first add ``~/.local/bin`` (default ``uv`` installation directory) to the ``PATH`` environment
variable and verify that ``uv`` is installed:

.. important::
   Adjust the ``PATH`` configuration to match where ``uv`` is installed on your system.

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 6-10

   engine:
     type: GlobusComputeEngine
     provider:
        type: LocalProvider
        worker_init: |
          export PATH="$HOME/.local/bin:$PATH"
          if ! command -v uv &> /dev/null; then
            echo "ERROR: uv command not found"
            exit 1
          fi

Depending on your particular setup (e.g., :doc:`multi-user <../endpoints/multi_user>`) and
organization policies, you might choose to automatically install ``uv`` if it isn't already
available:

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 8, 9

   engine:
     type: GlobusComputeEngine
     provider:
        type: LocalProvider
        worker_init: |
          export PATH="$HOME/.local/bin:$PATH"
          if ! command -v uv &> /dev/null; then
            echo "uv command not found; installing..."
            curl -LsSf https://astral.sh/uv/install.sh | sh
          fi


Activate version-specific environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We then activate a virtual environment that matches the submitting user's Python version:

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 1, 14-16

   {% set major, minor, micro = user_runtime.python.version_tuple %}

   engine:
     type: GlobusComputeEngine
     provider:
        type: LocalProvider
        worker_init: |
          export PATH="$HOME/.local/bin:$PATH"
          if ! command -v uv &> /dev/null; then
            echo "ERROR: uv command not found"
            exit 1
          fi

          ENV_PATH="$HOME/.globus_compute/.venvs/py{{ major }}{{ minor }}"
          uv venv "$ENV_PATH" --python {{ major }}.{{ minor }}
          source "$ENV_PATH/bin/activate"

If the virtual environment doesn't already exist, ``uv`` will create it in the user's
``~/.globus_compute/.venvs/`` directory, automatically downloading the specified Python
version if it's not available on the system. See `uv documentation <https://docs.astral.sh/uv/pip/environments/>`_
for more details.


Install matching endpoint package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The last line installs the ``globus-compute-endpoint`` package, which is required in every
worker environment. Since the Compute SDK is a dependency of the endpoint package and both
are released together, we can match the endpoint version to the submitting client's SDK
version to ensure maximum compatibility.

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 18

   {% set major, minor, micro = user_runtime.python.version_tuple %}

   engine:
     type: GlobusComputeEngine
     provider:
        type: LocalProvider
        worker_init: |
          export PATH="$HOME/.local/bin:$PATH"
          if ! command -v uv &> /dev/null; then
            echo "ERROR: uv command not found"
            exit 1
          fi

          ENV_PATH="$HOME/.globus_compute/.venvs/py{{ major }}{{ minor }}"
          uv venv "$ENV_PATH" --python {{ major }}.{{ minor }}
          source "$ENV_PATH/bin/activate"

          uv pip install globus-compute-endpoint=={{ user_runtime.globus_compute_sdk_version }}


Start the Endpoint
==================

Once the endpoint is configured, we can start it up:

.. code-block:: console

   $ globus-compute-endpoint start my-ep
         >>> Endpoint ID: [endpoint_uuid] <<<
   ----> Mon Oct 27 10:15:08 2025


Submit Tasks from the SDK
=========================

The following example script runs the ``get_python_version()`` function on the endpoint that we
previously configured, returning the Python version running on the endpoint worker process:

.. code-block:: python
   :caption: ``get_worker_python.py``

   from globus_compute_sdk import Executor


   def get_python_version():
      import platform
      return platform.python_version()


   ep_id = "..."  # Endpoint ID from previous step

   with Executor(endpoint_id=ep_id) as ex:
      f = ex.submit(get_python_version)
      print(f.result())

Now let's run the script a few times with different Python versions and note the changing
environment:

.. code-block:: console

   $ uv run --python 3.12 --with globus-compute-sdk python get_worker_python.py
   3.12.7
   $ uv run --python 3.13 --with globus-compute-sdk python get_worker_python.py
   3.13.9


.. _uv: https://docs.astral.sh/uv/
.. _conda: https://docs.conda.io/en/latest/