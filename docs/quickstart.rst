Quickstart
==========

**funcX** is currently in Alpha and early testing releases are available on `PyPI <https://pypi.org/project/funcx/>`_.

The latest version available on PyPI is ``v0.2.2``.

You can try funcX on `Binder <https://mybinder.org/v2/gh/funcx-faas/funcx/master?filepath=examples%2FTutorial.ipynb>`_


Installation
------------

**funcX** comes with two components: the **endpoint agent** which is a user-launched services that make
computation resources accessible for function executions, and the **funcX client** that enables
the registration, execution and tracking of functions across **endpoints**.

Here are some pre-requisites for both the `endpoints` and the `funcX client`

  1. Python3.6+
  2. The machine must have outbound network access

To check if you have the right Python version, run the following commands::

  >>> python3 --version

This should return the Python version, for example: ``Python 3.6.7``. Please note that that only the first two
version numbers need to match.


To check if you have network access, run ::

  >>> curl https://api2.funcx.org/v2/version

This should return a version string, for example: ``"0.2.2"``

.. note:: The funcx client is supported on MacOS, Linux, and Windows. The funcx-endpoint
   is only supported on Linux.

Installation using Pip
^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` and ``pip3`` can be used to install funcX we suggest the following approach
for reliable installation when many Python environments are avaialble.

1. Install the funcX client::

     $ python3 -m pip install funcx

To update a previously installed funcX to a newer version, use: ``python3 -m pip install -U funcx``

2. Optionally install the funcX endpoint agent::

     $ python3 -m pip install funcx_endpoint

3. Install Jupyter for Tutorial notebooks::

     $ python3 -m pip install jupyter


.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_
