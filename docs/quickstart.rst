Quickstart
==========

.. todo:: Add pointer to binder to demo how you'd run tasks against funcX

Installation
------------

funcX is available on PyPI, but first make sure you have Python3.6+

>>> python3 --version

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
