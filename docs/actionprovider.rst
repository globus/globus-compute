Globus Flows Action Provider
============================

Globus Compute exposes an asynchronous `Action Provider <https://globus-automate-client.readthedocs.io/en/latest/globus_action_providers.html>`_
interface to allow functions to be used in a `Globus Flow <https://www.globus.org/platform/services/flows>`_.

The Globus Compute Action Provider interface uses:

* ``ActionUrl`` -- 'https://compute.actions.globus.org'


Action Input Schema
-------------------

The Action Provider input schema accepts input for a single task, plus,
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

