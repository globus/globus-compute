Working with Templates
**********************

Globus Compute endpoints often need different configurations for different environments, workflows,
or users (see :doc:`multi_user`). Rather than maintaining multiple endpoints or configuration
files, endpoint administrators can create a single :ref:`configuration template <user-config-template-yaml-j2>`
to generate customized configurations based on variables provided during task submission.

Familiarity with `Jinja template`_ concepts is a prerequisite to this guide, which
focuses on the unique features and peculiarities of Globus Compute configuration
templates.


User Input Security
===================

To prevent injection attacks, Globus Compute JSON-serializes all user-defined strings
before rendering the template. This adds double quotes to the string, which will require
special handling in some scenarios.

For example, let's imagine we want our configuration to change depending on the value of
a user-defined ``environment`` variable:

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2``
   :emphasize-lines: 5

   engine:
     type: GlobusComputeEngine
     provider:
       type: SlurmProvider
   {% if environment == '"test"' %}
       partition: test
       worker_init: "source test_setup.sh"
       walltime: 00:01:00
    {% else %}
       partition: default
       worker_init: "source normal_setup.sh"
       walltime: 00:30:00
    {% endif %}

Note that we are using the full JSON-serialized string for the if-condition. The template
would fail to render if we used ``'test'`` or ``"test"`` instead of ``'"test"'``.


.. _reserved-template-variables:

Reserved Variables
==================

Every template has access to reserved variables that cannot be overridden by the user or admin:

- ``parent_config``: Contains the configuration values of the parent manager endpoint.
  This can be helpful in situations involving Python-based configuration files.

- ``user_runtime``: Contains information about the runtime environment of the user submitting tasks,
  such as their Python version. The following fields are available:

  - ``globus_compute_sdk_version``: Version string of the Globus Compute SDK
  - ``globus_sdk_version``: Version string of the base Globus SDK dependency
  - ``python_version``: Complete Python version string from ``sys.version``, including implementation
    details

- ``mapped_identity``: Contains information about the user's mapped identity. The following
  fields are available:

  - ``mapped_identity.local.uname``: Local user's username
  - ``mapped_identity.local.uid``: Local user's ID
  - ``mapped_identity.local.gid``: Local user's primary group ID
  - ``mapped_identity.local.groups``: List of group IDs the local user is a member of
  - ``mapped_identity.local.gecos``: Local user's GECOS field
  - ``mapped_identity.local.shell``: Local user's login shell
  - ``mapped_identity.local.dir``: Local user's home directory
  - ``mapped_identity.globus.id``: Matched Globus identity ID

  .. code-block:: yaml+jinja
     :caption: Example usage of ``mapped_identity`` in a template

      engine:
         type: GlobusComputeEngine
         provider:
            type: SlurmProvider
      {% if 1001 in mapped_identity.local.groups %}
            partition: {{ partition }}
      {% else %}
            partition: default
      {% endif %}


Combining Templates
===================

Endpoint administrators can combine multiple templates with the ``extends``, ``include``,
and ``import`` Jinja tags.  However, since these templates are rendered in user space, the
administrator must:

1. Move the template files to a directory that every mapped local user account has
   read access to.
2. Specify the main template file path with the ``user_config_template_path``
   variable in the :ref:`endpoint manager configuration <endpoint-manager-config>`.

.. code-block:: yaml+jinja
   :caption: Example usage of ``extends`` and ``include`` in a template

   {% extends "base_config.yaml" %}

   provider:
     type: SlurmProvider

   {% if environment == '"test"' %}
   {% include "test_config.yaml" %}
   {% else %}
   {% include "default_config.yaml" %}
   {% endif %}


.. _Jinja template: https://jinja.palletsprojects.com/en/3.1.x/
