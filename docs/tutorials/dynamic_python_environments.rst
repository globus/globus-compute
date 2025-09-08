Dynamic Python Environments
***************************

To avoid serialization issues, :ref:`we recommend <avoiding-serde-errors>` ensuring
that the Python version running on the endpoint workers matches the Python version
used to submit tasks via the SDK. This is particularly problematic for multi-user
endpoint setups because the endpoint administrators have little control over their
user's SDK environments. We recommend two different ways to address this:

1. Enable :doc:`dynamic worker container configuration <dynamic_containers>`
2. Enable dynamic worker Python environment activation based on the submitting user's
   Python versions

This tutorial covers the section option.


Create Multiple Python Virtual Environments
===========================================

We first need to create Python virtual environments on the endpoint host for every version
of Python we want to support. For this tutorial, we'll create 4 `conda`_ environments:

.. code-block:: console

   # conda create --name py310 python=3.10
   # conda create --name py311 python=3.11
   # conda create --name py312 python=3.12
   # conda create --name py313 python=3.13


Configure an Endpoint
=====================

If you are starting from scratch, you will need to initialize an endpoint with
the ``configure`` subcommand:


.. code-block:: console

   $ globus-compute-endpoint configure my-ep


Modify the Configuration Template
---------------------------------

Every configuration template can access :ref:`reserved variables <reserved-template-variables>`
that contain various useful information. The ``user_runtime`` variable stores information about
the submitting client's runtime environment, including their Python version. We can leverage this
to dynamically update the ``worker_init`` field -- a bash script that runs during worker process
startup -- to activate the appropriate `conda`_ environment:

.. code-block:: yaml+jinja
   :caption: Example ``user_config_template.yaml.j2``

   {% set supported_versions = ['3.13', '3.12', '3.11', '3.10'] %}
   {% set py_ver = supported_versions | select('in', user_runtime.python_version) | first | default('3.10') %}

   engine:
     type: GlobusComputeEngine
     provider:
        type: SlurmProvider
        worker_init: |
          ENV_NAME=py{{ py_ver }}
          if conda activate $ENV_NAME; then
            :
          else
            echo "Failed to activate $ENV_NAME; creating it first"
            conda create -n $ENV_NAME python={{ py_ver }} -y
            conda activate $ENV_NAME
          fi
          echo "Successfully activated environment: $ENV_NAME"
          conda install conda-forge::globus-compute-endpoint

.. note::

  Note that ``user_runtime.python_version`` contains the complete Python version string from
  ``sys.version``, so we need to use the ``in`` comparison operator, rather than ``==``.


.. _conda: https://docs.conda.io/en/latest/
.. |UserRuntime| replace:: :class:`UserRuntime <globus_compute_sdk.sdk.batch.UserRuntime>`