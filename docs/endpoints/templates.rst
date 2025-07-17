Configuration Templates
***********************

Globus Compute endpoints often need different configurations for different users,
environments, or workflows. Rather than maintaining multiple endpoints or configuration
files, administrators can create a single `Jinja template`_ to generate customized
configurations based on user-provided variables. Both :ref:`single-user
<endpoints_templating_configuration>` (template-able) and :ref:`multi-user
<user-config-template-yaml-j2>` endpoints support this feature.

Familiarity with `Jinja template`_ concepts is a prerequisite to this guide, which
focuses on the unique features, capabilities and peculiarities of Globus Compute
configuration templates.


Default Template
================

The default template file is named ``user_config_template.yaml.j2`` and lives in the
main endpoint directory. Admins can modify the file path via the ``user_config_template_path``
variable in the :ref:`endpoint manager configuration <endpoint-manager-config>`.

The initial template implements two user-defined variables: ``endpoint_setup`` and
``worker_init``.  Both of these default to the empty string if not specified by the
user (i.e., ``...|default()``).

.. code-block:: yaml+jinja

   endpoint_setup: {{ endpoint_setup|default() }}
   engine:
     ...
     provider:
       ...
       worker_init: {{ worker_init|default() }}

   idle_heartbeats_soft: 10
   idle_heartbeats_hard: 5760

Given the above template, users submitting to this endpoint would be able to specify the
``endpoint_setup`` and ``worker_init`` values.  All other values will remain unchanged
when the user endpoint process starts up.

As linked on the left, :doc:`there are a number of example configurations
<endpoint_examples>` to showcase the available options, but ``idle_heartbeats_soft`` and
``idle_heartbeats_hard`` bear describing.

- ``idle_heartbeats_soft``: if there are no outstanding tasks still processing, and the
  endpoint has been idle for this many heartbeats, shutdown the endpoint

- ``idle_heartbeats_hard``: if endpoint is *apparently* idle (e.g., there are
  outstanding tasks, but they have not moved) for this many heartbeats, then shutdown
  anyway.

A heartbeat occurs every 30s; if ``idle_heartbeats_hard`` is set to 7, and no tasks
or results move (i.e., tasks received from the web service or results received from
workers), then the endpoint will shutdown after 3m30s (7 × 30s).


Basic User Workflow
===================

After starting the endpoint, users can specify values for the template variables
when submitting a task:

.. code-block:: python
   :emphasize-lines: 4,8,11

   from globus_compute_sdk import Executor

   ep_id = "..."
   config = {"worker_init": "source /path/to/venv1/bin/activate"}

   with Executor(
       endpoint_id=ep_id,
       user_endpoint_config=config
   ) as ex:
       print(ex.submit(some_task, 1).result())
       ex.user_endpoint_config["worker_init"] = "source /path/to/venv2/bin/activate"
       print(ex.submit(some_task, 2).result())

The manager endpoint process will render the configuration template with the user-defined
variables, then launch the user endpoint process. Any changes to these values will result
in a new user endpoint process.


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


.. _template-variable-validation:

User Input Validation
=====================

Admins can define a `JSON schema <https://json-schema.org/>`_ to validate user-defined
variables. The default schema file is named ``user_config_schema.json`` and lives in the
main endpoint directory. Admins can modify the file path via the ``user_config_schema_path``
variable in the :ref:`endpoint manager configuration <endpoint-manager-config>`.

The default schema is quite permissive, enforcing that the two default template variables
are strings, then allowing any other user-defined properties:

.. code-block:: json
   :caption: Default ``user_config_schema.json``

   {
     "$schema": "https://json-schema.org/draft/2020-12/schema",
     "type": "object",
     "properties": {
       "endpoint_setup": { "type": "string" },
       "worker_init": { "type": "string" }
     },
     "additionalProperties": true
   }

.. important::

   The default schema sets ``additionalProperties`` to ``true``, allowing properties
   not explicitly defined in the schema. This enables the default template to work
   without customization.

   Endpoint administrators who require stricter input validation should consider
   setting ``additionalProperties`` to ``false`` to reject unexpected properties.

Reserved Variables
==================

Every template has access to reserved variables that cannot be overridden by the user or admin:

- ``parent_config``: Contains the configuration values of the parent manager endpoint.
  This can be helpful in situations involving Python-based configuration files.

- ``user_runtime``: Contains information about the runtime that the user used when
  submitting the task request, such as Python version. See |UserRuntime| for a complete
  list of available information.

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

Administrators can combine multiple templates with the ``extends``, ``include``, and
``import`` Jinja tags.  However, since these templates are rendered in user space,
the administrator must:

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
.. |UserRuntime| replace:: :class:`UserRuntime <globus_compute_sdk.sdk.batch.UserRuntime>`
