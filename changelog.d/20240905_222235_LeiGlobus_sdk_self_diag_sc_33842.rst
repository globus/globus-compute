Changed
^^^^^^^

- The Globus Compute self-diagnostic is now available as a stand-alone console
  script installed as part of the globus-compute-sdk package, instead of only
  as the ``self-diagnostic`` sub-command of the globus-compute-endpoint CLI.

  For more information, see ``globus-compute-diagnostic --help``.

  Note that the new diagnostic command creates a gzipped file by default,
  whereas previously it printed the output to console by default and
  only created the compressed file if the -z argument is provided.


Deprecated
^^^^^^^^^^

- ``globus-compute-endpoint``'s ``self-diagnostic`` sub-command has been
  deprecated and now just redirects to ``globus-compute-diagnostic``.
