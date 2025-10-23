Working with Templates
**********************

Globus Compute endpoints often need different configurations for different
environments, workflows, or users (see :doc:`multi_user`).  Rather than
maintaining multiple endpoints or configuration files, endpoint administrators
can create a single :ref:`configuration template <user-config-template-yaml-j2>`
to generate customized configurations based on variables provided during task
submission.

Familiarity with `Jinja template`_ concepts is a prerequisite to this guide,
which focuses on the unique features and peculiarities of Globus Compute
configuration templates.


.. _testing-templates:

Testing Templates
=================

The Compute Endpoint CLI includes a tool, ``globus-compute-endpoint render-user-config``,
which can be used to test the template rendering process without having to submit a
task or even configure an endpoint.

.. note::

   For the most up-to-date documentation on available options, run
   ``globus-compute-endpoint render-user-config --help``.

In its simplest mode, calling the tool might look like this:

.. code-block:: console
   :caption: Example usage with local endpoint

   $ globus-compute-endpoint render-user-config -e my_endpoint -o user_options.json

   > Rendered user config:
   engine:
      type: GlobusComputeEngine
      provider:
         type: LocalProvider
         worker_init: "echo 'hello world'"

.. code-block:: yaml+jinja
   :caption: ``~/.globus_compute/my_endpoint/user_config_template.yaml.j2``

   engine:
      type: GlobusComputeEngine
      provider:
         type: LocalProvider
         worker_init: {{ worker_init }}

.. code-block:: json
   :caption: ``user_options.json``

   { "worker_init": "echo 'hello world'" }

The ``-e`` option, short for ``--endpoint``, takes a local endpoint name/UUID, and the
``-o`` option, short for ``--user-options``, takes a filename pointing to JSON data.
Here, the tool reads the endpoint's config, renders the associated template with any
values in ``user_options.json`` included, and outputs the rendered template to ``stdout``.

Alternatively, the render tool can be pointed directly to the template file, along with
any other files it might need:

.. code-block:: console
   :caption: Example "offline" usage without an endpoint

   $ cat user_options.json | globus-compute-endpoint render-user-config \
      --template user_config_template.yaml.j2 \
      --user-options-file - \
      <other options...>

This applies the same rendering logic and also outputs the final value to ``stdout`` as
before - the main difference is that this approach does not require a pre-configured
endpoint. Note that the ``-`` argument used here for ``--user-options-file`` indicates
that the file should be read from ``stdin``, which works with any other file-based
options as well.

These approaches can also be mixed. Whenever the ``--endpoint`` option is present, file
paths are pulled from the endpoint's config by default, but any file paths supplied as
options take precedence. This makes it easy to A/B test changes without modifying files
in production.

Testing Reserved Variables
--------------------------

When rendering :ref:`reserved variables <reserved-template-variables>`, the render tool
makes reasonable guesses and supplies dummy data. In cases where admins want more
control, they can specify the value(s) to render to any reserved variable:

.. code-block:: console
   :caption: Custom ``user_runtime``

   $ globus-compute-endpoint render-user-config -e template-snippet.yaml.j2 \
      --user-runtime my-custom-user-runtime.json

   > Rendered user config:
   worker_init: |
      echo 'Globus Compute SDK version: "arbitrary value"'
      echo 'foo: "bar"'

.. code-block:: yaml+jinja
   :caption: ``template-snippet.yaml.j2``

   worker_init: |
      echo 'Globus Compute SDK version: {{ user_runtime.globus_compute_sdk_version }}'
      echo 'foo: {{ user_runtime.foo }}'

.. code-block:: json
   :caption: ``my-custom-user-runtime.json``

   {
      "globus_compute_sdk_version": "arbitrary value",
      "foo": "bar"
   }

Validating YAML Syntax
----------------------

When working with Jinja (or any templating engine for that matter), it's easy to
accidentally produce syntactically invalid output. Since the render tool outputs the
rendered config to ``stdout`` and all other information to ``stderr``, admins can pipe
to another tool for handling validation:

.. code-block:: console
   :caption: Using ``yq`` for validation
   :emphasize-lines: 3

   $ globus-compute-endpoint render-user-config \
      -t bad-template.yaml.j2 \
      | yq - >/dev/null

   > Rendered user config:
   Error: bad file '-': yaml: line 2: mapping values are not allowed in this context

.. code-block:: yaml+jinja
   :caption: ``bad-template.yaml.j2``

   engine: there shouldn't be any text here!
      type: GlobusComputeEngine

This example leverages `yq`__ to validate the rendered YAML, which prints syntax errors
to ``stderr``. Specifically, this invocation takes its input from ``stdin`` with the
``-`` argument, and silences its output (usually just the input YAML repeated) by
redirecting to ``/dev/null``.

.. __: https://mikefarah.gitbook.io/yq

User Input Security
===================

To prevent injection attacks, Globus Compute JSON-serializes all user-defined
strings before rendering the template.  This adds double quotes to the string,
which will require special handling in some scenarios.

For example, let's imagine we want our configuration to change depending on the
value of a user-defined ``environment`` variable:

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

Note that we are using the full JSON-serialized string for the if-condition.
The template would fail to render if we used ``'test'`` or ``"test"`` instead of
``'"test"'``.


.. _reserved-template-variables:

Reserved Variables
==================

Every template has access to reserved variables that cannot be overridden by the
user or admin:

- ``parent_config``: Contains the configuration values of the parent manager
  endpoint.  This can be helpful in situations involving Python-based
  configuration files.

- ``user_runtime``: Contains information about the runtime environment of the
  user submitting tasks, such as their Python version.  The following fields are
  available:

  - ``globus_compute_sdk_version``: Version string of the Globus Compute SDK
  - ``globus_sdk_version``: Version string of the base Globus SDK dependency
  - ``python_version``: Complete Python version string from ``sys.version``,
    including implementation details

- ``mapped_identity``: Contains information about the user's mapped identity.
  The following fields are available:

  - ``mapped_identity.local.uname``: Local user's username
  - ``mapped_identity.local.uid``: Local user's ID
  - ``mapped_identity.local.gid``: Local user's primary group ID
  - ``mapped_identity.local.groups``: List of group IDs the local user is a
    member of
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

Endpoint administrators can combine multiple templates with the ``extends``,
``include``, and ``import`` Jinja tags.  However, since these templates are
rendered in user space, the administrator must:

1. Move the template files to a directory that every mapped local user account
   has read access to.
2. Specify the main template file path with the ``user_config_template_path``
   variable in the :ref:`endpoint manager configuration
   <endpoint-manager-config>`.

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
