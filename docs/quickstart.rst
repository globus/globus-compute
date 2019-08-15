Quickstart
==========

**funcX** is currently in Alpha and early testing releases are available on `PyPI <https://pypi.org/project/funcx/>`_.

The latest version available on PyPI is ``v0.0.1a0``.

.. todo:: Add pointer to binder to demo how you'd run tasks against funcX

Installation
------------

**funcX** comes with two components: the **endpoints** which are user launched services that make
computation resources accessible for function executions, and the **funcX client** that enables
the registration, execution and tracking of functions across **endpoints**.

Here are some pre-requisites for both the `endpoints` and the `funcX client`

  1. Python3.6
  2. The machine must have outbound network access

To check if you have the right Python version, run the following commands::

  >>> python3 --version

This should return the Python version, for eg: ``Python 3.6.7``. Please note that that only the first two version numbers need to match.


To check if you have network access, run ::

  >>> curl http://dev.funcx.org/api/v1/version

This should return a version string, for eg: ``"0.0.1"``

Installation using Pip
^^^^^^^^^^^^^^^^^^^^^^

While ``pip`` and ``pip3`` can be used to install funcX we suggest the following approach
for reliable installation when many Python environments are avaialble.

1. Install funcX::

     $ python3 -m pip install funcx

To update a previously installed funcX to a newer version, use: ``python3 -m pip install -U funcx``

2. Install Jupyter for Tutorial notebooks::

     $ python3 -m pip install jupyter


.. note:: For more detailed info on setting up Jupyter with Python3.5 go `here <https://jupyter.readthedocs.io/en/latest/install.html>`_


Installation using Conda
^^^^^^^^^^^^^^^^^^^^^^^^

1. Install Conda and set up python3.6 following the instructions `here <https://conda.io/docs/user-guide/install/macos.html>`_::

     $ conda create --name funcX_py36 python=3.6
     $ source activate funcx_py36

2. Install funcX::

     $ python3 -m pip install funcx


To update a previously installed funcX to a newer version, use: ``python3 -m pip install -U funcx``
