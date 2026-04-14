Staging Data with Globus Transfer
*********************************

This article discusses the use of Globus Transfer and Globus Connect Server (GCS) for
orchestration of data in and out of functions on Globus Compute Endpoint (GCE) where the
GCE and GCS endpoints share a filesystem.

The assumption in this article is that GCS is installed on a separate host from GCE.
The GCS mapped collection is exported via the identifier <GC_UUID>, and the GCE host has
the GCS mapped collection storage mounted to its local filesystem at
``/mnt/collection``.  Consequently, actions local to the GCE host can interact with the
collection storage via normal filesystem semantics on ``/mnt/collection``, and mapped
users can move data to and from the collection storage with Globus Transfer.  For
instance, moving data to the file system using Globus CLI in preparation for a set of
Compute tasks, might look like:

.. code-block:: console

   $ globus transfer \
      <OTHER_COLLECTION_UUID>:/source_side_path/some_dir/ \
      <GC_UUID>:/my_user/dataset-track-c

A Compute task can then access the data directly:

.. code-block:: python

   def make_use_of_dataset(inp_dir: str, entity_name: str):
       import os
       collection_path = "/mnt/collection"
       data_set_path = os.path.join(collection_path, inp_dir)
       assert data_set_path == "/mnt/collection/my_user/dataset-track-c"
       ... work with file(s) in dataset-track-c ...
       ... return result ...

Notice that the function must define the full path of the data set, including the mount
point.  However, the mount point of the GCS storage on the GCE host is a host‑specific
|nbsp| --- |nbsp| and sometimes user‑specific |nbsp| --- |nbsp| detail that can (and
does) change.  The challenge then, as a site‑administrator, is how to communicate the
mount point to the running function in a maintainable fashion.


Approach: Environment Variables
===============================

We recommend environment variables as a best‑practice for enabling functions to discover
where the mapped collection storage is locally mounted by the GCE host.  Environment
variables can be generated on the fly for user endpoint processes and customized for
user‑specific mappings.  Updates to the environment variables (e.g., if the mount points
change) will require user processes to restart, however.


General environment (``user_environment.yaml``)
-----------------------------------------------

There are several approaches to injecting environment variables in the UEP process space
and they can be used in tandem with each other.  To inject a static environment variable
into all UEPs, use the ``user_environment.yaml`` file.  The content of this file is
parsed as YAML, and any top‑level keys are placed into the UEP process space.  Given the
setup discussed above, this file might look like:

.. code-block:: yaml
   :caption: A possible ``user_environment.yaml``

   GCS_LOCAL_MAPS: '{"<GC_UUID>": "/mnt/collection"}'

An embedded JSON document that a function can look for in a (well‑known) environment
variable (arbitrarily ``GCS_LOCAL_MAPS``).  The structure in this example is a JSON
document, but almost any scheme can work |nbsp| --- |nbsp| just make sure to communicate
that scheme to users so their codes can use it.  For example:

.. code-block:: python
   :emphasize-lines: 4, 8

   def make_use_of_dataset(collection_id: str, inp_dir: str, entity_name: str):
       import json, os

       gcs_local_maps = json.loads(os.environ["GCS_LOCAL_MAPS"])
       if collection_id not in gcs_local_maps:
           raise FileNotFoundError(f"{collection_id} not found in local GCS filesystem")

       collection_path = gcs_local_maps[collection_id]
       data_set_path = os.path.join(collection_path, inp_dir)
       assert data_set_path == "/mnt/collection/my_user/dataset-track-c"

       relative_result_path = os.path.join(inp_dir, f"results/{entity_name}.res"

       result_path = os.path.join(collection_path. relative_result_path)
       with open(result_path, "w") as res_file:
           print("results", file=res_file)

       return relative_result_path


Dynamic User‑Specific Variables: Shim the UEP Executable
--------------------------------------------------------

After the fork from the main Compute endpoint process, and after the child process
changes to the local POSIX user, the ``exec()`` call invokes the *PATH-found* executable
``globus-compute-endpoint``.  We describe the process in
:ref:`advanced_environment_customization`, but the synopsis is "use a temporary ``PATH``
environment variable (via ``user_environment.yaml``) to invoke an administrator‑written
executable named ``globus-compute-endpoint``."  This executable can be a compiled binary
or a simple shell script.  When the setup-logic is complete, it should ``exec()`` the
real ``globus-compute-endpoint`` executable.

As an example of this approach, the temporary (or "initial") ``PATH`` can be set in the
``user_environment.yaml`` file:

.. code-block:: yaml

    PATH: /usr/local/admin_scripts/

And then the shim script named ``globus-compute-endpoint`` might be:

.. code-block:: sh

   #!/bin/sh

   # this script is run as the user's UID

   # the full path to *this* script is:
   # /usr/local/admin_scripts/globus-compute-endpoint

   export USER_SPECIFIC_GCS_LOCAL_MAPS='{"<GC_UUID>": ...}'
   export PATH=/site/specific:/paths
   module load ...
   # ...

   exec /desired/venv/bin/globus-compute-endpoint "$@"

This provides an avenue to run *any* logic the site would like, as the user's UID, prior
to the user's Compute endpoint process starting up.


Dynamic User‑Specific Variables: ``pam_env.so``
-----------------------------------------------

For Compute sites implementing PAM, the `pam_env`_ module is a non-Compute-specific
approach to dynamic environment variables.  PAM setup is described in § :ref:`pam`.


Approach: Mapping File at Well‑Known Path
=========================================

Another option is a mapping file at a well‑known and globally accessible path on the
filesystem.  This is analogous to the environment variable approach discussed above, and
the logic is almost the same.  Using <GC_UUID>, this file could be the same content as
above:

.. code-block:: json

   {
     "<GC_UUID>": "/mnt/collection"
   }

The only difference within user code is loading the mapping from a filesystem
(``os.read()``) instead of from an environment variable.

.. code-block:: python
   :caption: Hypothetical user function that needs data in GC_UUID
   :emphasize-lines: 4-5, 9-10, 13, 15

   def make_use_of_dataset(collection_id: str, inp_dir: str, entity_name: str):
       import json, os

       well_known_gcs_maps_path = "/opt/well/known/path/gcs_local_maps.json"
       gcs_local_map = json.loads(open(well_known_path).read())
       if collection_id not in gcs_local_maps:
           raise FileNotFoundError(f"{collection_id} not found in local GCS filesystem")

       collection_path = gcs_local_maps[collection_id]
       data_set_path = os.path.join(collection_path, inp_dir)
       assert data_set_path == "/mnt/collection/my_user/dataset-track-c"

       relative_result_path = os.path.join(inp_dir, f"results/{entity_name}.res"

       result_path = os.path.join(collection_path. relative_result_path)
       with open(result_path, "w") as res_file:
           print("results", file=res_file)

       return relative_result_path

One nice aspect of this approach is that it's a single-solve for all users, and can
potentially be generated by a script.  Changes are also immediate, so if an update is
made while a long running function was in progress, and if the function were to reread
the file at the end of processing, it would transparently use the new location.
However, there are two down-sides of this approach:

- As a globally‑accessible file, it will not be appropriate for user‑specific logic and
  mappings.

- As a file, it will incur access overhead, which, even if only tiny, may not be
  warranted on a heavily accessed cluster filesystem.


Approach: Symbolic Links
========================

As symbolic links may point directly to a mounted collection, user code would only need
to know the specific symlink path, and the administrator can update the link at any
point, even in the middle of a task.

.. code-block:: console

   # ln -s /mnt/collection /data

The user code need merely make use of ``/data``, and need not know the
collection‑specific path.

.. code-block:: python
   :caption: Function does not need a ``collection_id`` argument

   def make_use_of_dataset(inp_dir: str, entity_name: str):
       data_set_path = os.path.join("/data", inp_dir)
       assert data_set_path == "/data/my_user/dataset-track-c"

       relative_result_path = os.path.join(inp_dir, f"results/{entity_name}.res"

       result_path = os.path.join(data_set_path, relative_result_path)
       with open(result_path, "w") as res_file:
           print("results", file=res_file)

       return relative_result_path

For user‑specific links, this approach can also be taken dynamically, for example in the
``endpoint_setup`` stanza of the GCE template configuration.  While there's not much
value‑add over the simpler ``/data`` link in this scenario, the ``endpoint_setup``
setup, as it is run by the user UID at the user endpoint process startup, can do
user‑specific symlinks (e.g., collections).

.. code-block:: yaml+jinja

   endpoint_setup: |
     mkdir -p ~/.globus_collections/
     ln -s /mnt/collection/ ~/.globus_collections/<GC_UUID>

   engine:
     type: GlobusComputeEngine
     ...

One drawback to symbolic links, especially if created within the user's home directory
by administrative logic, is upkeep.  (For example, if GCE moves its mount point of the
GCS storage.)


Best Practice
=============

While we generally recommend using environment variables for sharing information with
user endpoint processes, they are not a one-size-fits-all solution.  They fit well with
the proscribed scenario in this article (GCS and GCE hosts sharing access to a
filesystem) and they have a negligible maintenance burden for the administrator.
However, other approaches may well be warranted, and choosing the approach that
best-fits a site's needs requires the administrator's site-specific understanding.


.. |nbsp| unicode:: 0xA0
   :trim:

.. _Transfer Activity tab: https://app.globus.org/activity
.. _pam_env: https://www.man7.org/linux/man-pages/man8/pam_env.8.html
