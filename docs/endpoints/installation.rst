Installing the Endpoint
***********************

The Globus Compute Endpoint is available as a PyPI package or as a native system
package (DEB and RPM).

Operating System Support
========================

Currently, the Globus Compute Endpoint is only supported on Linux.  While some
have reported success running Compute endpoints on macOS, we do not currently
support it.  If running on a non-Linux host OS is necessary, consider doing so
in a container running Linux.

.. note::

   Though the Compute Endpoint is only supported on Linux, the
   :doc:`Globus Compute SDK <../sdk/index>` *is* supported on other operating
   systems.

.. _pypi-based-installation:

Installing Directly via PyPI
============================

If the site administrator has not already installed the Globus Compute Endpoint
software, users typically install it from `PyPI
<https://pypi.org/project/globus-compute-endpoint/>`_.  We **strongly**
recommend installing the Globus Compute endpoint into an isolated virtual
environment (e.g., `venv
<https://docs.python.org/3/library/venv.html>`_, `virtualenv
<https://pypi.org/project/virtualenv/>`_).  We recommend use of |pipx for
library isolation|_:

.. code-block:: console

   $ python3 -m pipx install globus-compute-endpoint


.. _repo-based-installation:

Repository-Based Installation
=============================

The ``globus-compute-endpoint`` project is available on PyPI, and is also
available in Globus' repositories as native DEB and RPM packages.  The
repository package is ``globus-compute-agent``.

.. _compute-endpoint-pre-requisites:

Prerequisites
-------------

#. Supported Linux Distributions

   Where feasible, Globus Compute supports the `same Linux distributions as does
   Globus Connect Server`_.

#. Administrator Privileges

   Per usual semantics, installing the DEB or RPM packages will require
   administrative access on the target host.

#. TCP Ports

   * Port 443, outbound to ``compute.api.globus.org``
   * Port 443, outbound to ``compute.amqps.globus.org``

   .. note::

       We do not offer a range of specific IP addresses for firewall blocking
       rules.

What is Installed
-----------------

The Globus Compute Endpoint software will be installed in
``/opt/globus-compute-agent/`` and a shell-script wrapper will be installed to
``/usr/sbin/globus-compute-endpoint``.

The packages also rely on Globus' supplied Python.  As of this writing, that
is Python3.9, and is installed to ``/opt/globus-python/``.  While Globus Compute
is supported on any `Python version that is not EOL`_, the Compute agent
packages are built against Globus' Python.  As the Python version has
implications for :ref:`function and data serialization consistency
<avoiding-serde-errors>`, administrators may want to consider supporting
:ref:`multiple Python versions in the configuration template
<configure-multiple-python-versions>`.

.. _Python version that is not EOL: https://devguide.python.org/versions/


RPM Installation
----------------

.. code-block::

   # get the Globus installer
   dnf install https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-latest.noarch.rpm

   # install the Globus Compute Agent package
   dnf install globus-compute-agent

DEB Installation
----------------

.. code-block::

   # get the Globus installer
   curl -LOs https://downloads.globus.org/globus-connect-server/stable/installers/repo/deb/globus-repo_latest_all.deb
   dpkg -i globus-repo_latest_all.deb

   # install the Globus Compute Agent package
   apt update
   apt install globus-compute-agent

SUSE Installation
-----------------

.. code-block::

   # install Globus' public key
   rpm --import https://downloads.globus.org/globus-connect-server/stable/installers/keys/GPG-KEY-Globus
   zypper install https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-latest.noarch.rpm

   # install the Globus Compute Agent package
   zypper install globus-compute-agent


.. |pipx for library isolation| replace:: ``pipx`` for library isolation
.. _pipx for library isolation: https://pipx.pypa.io/stable/
.. _same Linux distributions as does Globus Connect Server: https://docs.globus.org/globus-connect-server/v5/#supported_linux_distributions

