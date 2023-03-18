Globus Flows Action Provider
============================

Globus Compute exposes an asynchronous `Action Provider <https://globus-automate-client.readthedocs.io/en/latest/globus_action_providers.html>`_
interface to allow functions to be used in a `Globus Flow <https://www.globus.org/platform/services/flows>`_.

The Globus Compute Action Provider interface uses:

* ``ActionUrl`` -- 'https://automate.funcx.org'
* ``ActionScope`` -- 'https://auth.globus.org/scopes/b3db7e59-a6f1-4947-95c2-59d6b7a70f8c/action_all'


Action Input Schema
-------------------

The Action Provider input schema accepts a list of tasks, with each task requiring an ``endpoint``, ``function``, and ``payload`` field.
The endpoint and function arguments are UUIDs and the payload is a dictionary of kwargs to be passed to the function.

.. code-block::

  'tasks': [{'endpoint.$': '<COMPUTE ENDPOINT UUID>',
             'function': '<COMPUTE FUNCTION UUID>',
             'payload.$': '<DICT OF INPUT ARGS>'}],


When defining a Globus Compute function to use within a flow it is recommended to define the specific kwargs that will be passed in as payload.
If the kwargs are not known, a function can be defined to accept arbitrary kwargs using the ``**`` operator, e.g.:

.. code-block::

  'Parameters': {'tasks': [{'endpoint.$': '$.input.fx_ep',
                            'function': '$.input.fx_fn',
                            'payload.$': '$.input'}]},

  def my_function(**data):
      ...


Gladier
-------

The `Gladier <https://gladier.readthedocs.io/en/latest/>`_ toolkit provides useful tools to simplify and accelerate
the development of flows that use Globus Compute. For example, Gladier validates inputs prior to starting a flow and will re-register
functions when they are modified. Additionally, it includes capabilities to automatically
generate flow definitions.

