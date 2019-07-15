funcX - Fast Function Serving
=============================
|licence| |build-status| |docs|

funcX is a high-performance function-as-a-service (FaaS) platform that enables
intuitive, flexible, efficient, scalable, and performant remote function execution
on existing infrastructure including clouds, clusters, and supercomputers.

.. |licence| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/funcx-faas/funcX/blob/master/LICENSE
   :alt: Apache Licence V2.0
.. |build-status| image:: https://travis-ci.com/funcx-faas/funcX.svg?branch=master
   :target: https://travis-ci.com/funcx-faas/funcX
   :alt: Build status
.. |docs| image:: https://readthedocs.org/projects/funcx/badge/?version=latest
   :target: http://funcx.readthedocs.io/en/stable/?badge=latest
   :alt: Documentation Status

Quickstart
==========

funcX is in alpha state and is not available on PyPI yet. To install funcX,
we recommend installing from source into a conda environment.


1. Install Conda and set up python3.6 following the instructions `here <https://conda.io/docs/user-guide/install/macos.html>`_::

   $ conda create --name funcx_py36 python=3.6
   $ source activate funcx_py36

2. Download funcX::

   $ git clone https://github.com/funcx-faas/funcX

3. Install from source::

   $ cd funcX
   $ pip install .

4. Use funcX!

.. note:: funcX currently only support Python3.6 functions and environment for execution.


Documentation
=============

Complete documentation for funcX is available `here <https://funcx.readthedocs.io>`_

