New Functionality
^^^^^^^^^^^^^^^^^

- Endpoint administrators now have access to information about the user's mapped
  identity in the user configuration template (``user_config_template.yaml.j2``)
  via the ``mapped_identity`` variable. The following fields are available:

  - ``mapped_identity.local.uname``: Local user's username
  - ``mapped_identity.local.uid``: Local user's ID
  - ``mapped_identity.local.gid``: Local user's primary group ID
  - ``mapped_identity.local.groups``: List of group IDs the local user is a member of
  - ``mapped_identity.local.gecos``: Local user's GECOS field
  - ``mapped_identity.local.shell``: Local user's login shell
  - ``mapped_identity.local.dir``: Local user's home directory
  - ``mapped_identity.globus.id``: Matched Globus identity ID
