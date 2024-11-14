Globus Flows Action Provider
============================

Globus Compute exposes asynchronous `Action Provider <https://globus-automate-client.readthedocs.io/en/latest/globus_action_providers.html>`_
interfaces to allow functions to be used in a `Globus Flow <https://www.globus.org/platform/services/flows>`_.

There are currently two supported action providers for Globus Compute: [1]_

* V3 Action Provider: https://compute.actions.globus.org/v3
* V2 Action Provider: https://compute.actions.globus.org

Both Action Providers return their outputs in the same format. The V3 Action Provider
generally supports more functionality than the V2 Action Provider, with some caveats;
see below for details and migration.

V3 Action Input Schema
----------------------

The V3 Action Provider supports all of the same input options as the |V3SubmitRoute|_
in the Compute hosted web service, with two major differences. The V3 Action Provider
accepts task input as JSON data, while the web service route only accepts task input as
Compute-serialized strings. The V3 Action Provider also takes the endpoint ID as a JSON
parameter, rather than in the URL.

A complete task submission might look like this:

.. code-block:: json

  {
    "endpoint_id": "0c48c27a-0c2d-4028-a71b-d78697abbc4a",
    "tasks": [
      {
        "function_id": "ff960aba-fa23-43d5-9cbe-3f4f91a066e1",
        "args": [ "..." ],
        "kwargs": { "...": "..." }
      },
      {
        "function_id": "0a98fd06-edbd-11ed-abcd-0d705ebb4c49",
        "args": [ "..." ],
        "kwargs": { "...": "..." }
      }
    ],
    "task_group_id": "97241626-8ff4-4550-9938-5909bd221869",
    "user_endpoint_config": {
      "min_blocks": 0,
      "max_blocks": 1,
      "scheduler_options": "#SBATCH --constraint=knl,quad,cache"
    },
    "resource_specification": {
      "num_nodes": 2,
      "ranks_per_node": 2,
      "num_ranks": 4,
      "launcher_options": "--cpu-bind quiet --mem 3072"
    },
    "create_queue": false
  }

The two required arguments are ``endpoint_id`` and ``tasks``. The former must be a valid
UUID, and should point to an endpoint you have access to; the latter is a list of
objects that represent the tasks to execute on that endpoint. Submissions must contain at
least one task.

Each task object must have a valid ``function_id``, while ``args`` and ``kwargs`` are
optional (although if a function expects either, the flow will fail when the endpoint
attempts to execute it). The ``args`` option is a list of arbitrary JSON data, and the
``kwargs`` option is an object that maps argument keywords to argument values as
arbitrary JSON data.

For each task, the given endpoint will call the given function with any given arguments
and keyword arguments, and the V3 Action Provider will return with the results of those
calls.

The other top-level arguments, such as ``resource_specification`` and
``user_endpoint_config``, are optional - see the |V3SubmitRoute|_ for more information
on what they each mean.

Migrating from V2 Actions
.........................

The major differences between the V2 and V3 Action Providers are as follows:

* The V2 Action Provider accepts its ``args`` and ``kwargs`` as *strings* containing
  JSON data, while the V3 Action Provider accepts those as raw JSON data.

* With the V3 Action Provider, ``endpoint`` and ``function`` are now ``endpoint_id`` and
  ``function_id``, to better match the web service submit route.

* Unlike the V2 Action Provider, the V3 Action Provider only allows for submission to a
  single endpoint at a time. If multiple endpoint submissions are needed, separate those
  into multiple action calls, one per endpoint.

* The V3 Action Provider has no top-level ``function``, ``args`` or ``kwargs`` arguments.
  Instead, each task, including the first, goes into the ``tasks`` array.

* The V3 Action Provider no longer supports the ``payload`` argument aliasing ``kwargs``,
  which was included in V2 for backward compatibility. Use ``kwargs`` directly instead.

Taking these points in mind, this V2 input:

.. code-block:: json

  {
    "endpoint": "0c48c27a-0c2d-4028-a71b-d78697abbc4a",
    "function": "ff960aba-fa23-43d5-9cbe-3f4f91a066e1",
    "args": "['arg1', 'arg2']",
    "payload": "{ 'foo': 'bar', 'fuzz': 'buzz' }",
    "tasks": [
      {
        "endpoint": "0c48c27a-0c2d-4028-a71b-d78697abbc4a",
        "function": "0a98fd06-edbd-11ed-abcd-0d705ebb4c49",
        "args": "['arg1', 'arg2']",
        "payload": "{ 'foo': 'bar', 'fuzz': 'buzz' }",
      }
    ]
  }

maps to this V3 input:

.. code-block:: json

  {
    "endpoint_id": "0c48c27a-0c2d-4028-a71b-d78697abbc4a",
    "tasks": [
      {
        "function_id": "ff960aba-fa23-43d5-9cbe-3f4f91a066e1",
        "args": ["arg1", "arg2"],
        "kwargs": { "foo": "bar", "fuzz": "buzz" }
      },
      {
        "function_id": "0a98fd06-edbd-11ed-abcd-0d705ebb4c49",
        "args": ["arg1", "arg2"],
        "kwargs": { "foo": "bar", "fuzz": "buzz" }
      }
    ]


.. |V3SubmitRoute| replace:: V3 submit batch route
.. _V3SubmitRoute: https://compute.api.globus.org/redoc#tag/Endpoints/operation/submit_batch_v3_endpoints__endpoint_uuid__submit_post

V2 Action Input Schema
----------------------

The V2 Action Provider input schema accepts input for a single task, plus,
optionally, a list of task objects each with a similar schema.

Each run input consists of the required ``'endpoint'`` and ``'function'``
fields which are uuid string identifiers of the endpoint and function to
be executed, and two optional arguments ``'args'`` and ``'kwargs'`` which
are a list and an object respectively representing inputs to the function.

Notes
.....

* The ``'tasks'`` field can be empty, if only a single invocation is
  desired.  The single invocation uses ``'endpoint'`` and ``'function'`` in
  the main input body, in addition to ``'args'`` and ``'kwargs'``.
* ``'args'`` can be a single item or a list of items.
* ``'payload'`` can be used in place of ``'kwargs'`` to specify
  kwargs to the function.  This is for backwards compatibility with a prior
  implementation of the Action Provider which only recognized ``'payload'``.


.. code-block::

  'endpoint': '<COMPUTE_ENDPOINT_UUID>',
  'function': '<COMPUTE_FUNCTION_UUID>',
  'args':  [ 'Argument 1', 2, 'Third Argument' ],
  'kwargs': { "kwarg_one": "abc", "kwarg_two": 234 },
  'tasks': [
             {
               'endpoint': '<COMPUTE_ENDPOINT_UUID>',
               'function': '<COMPUTE_FUNCTION_UUID>',
               'args':  [LIST OF ARGS],
               'payload': {DICT OF INPUT ARGS}
             },
             { ...},
             { ...},
           ]


When defining a Globus Compute function to use within a flow it is recommended
to define the specific args and kwargs that will be passed in as payload. If
the arguments are not known, a function can be defined to accept arbitrary
args and kwargs using the ``*`` and ``**`` operators, e.g.:

.. code-block::

  'Parameters': {'tasks': [{'endpoint': '$.input.fx_ep',
                            'function': '$.input.fx_fn',
                            'args': '$.input.fx_args',
                            'kwargs': '$.input.fx_kwargs'}]},

  def my_function(*args, **kwargs):
      ...

Action Output
-------------

The output of the Action from Globus Compute will reside in the 'details'
field of the output.  The ``details`` section has two lists, ``'result'``
and ``'results'``.

The ``'result'`` field contains a list of task results from the submitted
functions.  This field is meant to be backwards compatible with an earlier
implementation of the Globus Compute Action Provider.

The ``'results'`` field contains a list of objects, each containing a
``'task_id'`` field and an ``'output'`` field.  The ``'task_id'`` field
identifies the task submitted, and the ``'output'`` field contains the result
from that run.  In the future, more information about the task execution
will be added to this object as additional fields.  (A list of all the 'output'
fields from the ``'results'`` dict is the same as what is in the ``'result'`` field)

Example output:

.. code-block::

    ...
    "RunResult": {
      "action_id": "tg_44f09e3d-c920-abcd-969a-301b019af1b1",
      "completion_time": "2023-04-14T19:26:33.981517+00:00",
      "creator_id": "urn:globus:auth:identity:12345678-9323-4fe6-93ef-abc6f9ff05d2",
      "details": {
        "result": [
          "Result of task 1",
          "Result of task 2",
        ],
        "results": [
          {
            "output": "Result of task 1",
            "task_id": "c971987e-643b-48ab-af6f-47411234abcd"
          },
          {
            "output": "Result of task 2",
            "task_id": "abc12345-643b-48ab-af6f-12345beabcde"
          }
        ]
      },
    ...


Gladier
-------

The `Gladier <https://gladier.readthedocs.io/en/latest/>`_ toolkit provides useful tools to simplify and accelerate
the development of flows that use Globus Compute. For example, Gladier validates inputs prior to starting a flow and will re-register
functions when they are modified. Additionally, it includes capabilities to automatically
generate flow definitions.

.. [1] Early users might be aware of another Action Provider under the URL
   https://dev.funcx.org/automate; this was deprecated in April of 2023.
