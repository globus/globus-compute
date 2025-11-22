Globus Compute for AWS Lambda Developers
****************************************

This guide is intended for prospective Globus Compute endpoint administrators and users
who are familiar with AWS Lambda but not with Compute, and aims to explain core
concepts in Compute in terms of similar concepts in Lambda.


Major Differences
=================

.. list-table::
    :header-rows: 1
    :stub-columns: 1

    * -
      - AWS Lambda
      - Globus Compute
    * - Pricing Model
      - Usage-based billing
      - Free; :doc:`optional subscription <../subscriptions>`
    * - Deployment
      - Everything in the cloud
      - Hybrid cloud and on-premises
    * - Execution Environment
      - Managed by AWS
      - Managed by endpoint administrators
    * - Function Definition
      - Packaged code with handler function
      - Plain Python functions
    * - Dependencies
      - Deployment package, layers
      - Managed by endpoint administrators


Functions & Endpoints
=====================

In Compute, **functions** are simply Python or Bash source code that users register
with the service. Unlike Lambda, they are not tied to any particular execution
context. Instead, when calling a function, users choose an **endpoint** they can
access to execute their code. Like any Lambda runtime, a Compute endpoint is what is
ultimately responsible for executing a function's code.

To manage Compute endpoints, an endpoint administrator :doc:`installs the package
</endpoints/installation>` on a Linux-based machine they want to share with others,
then uses the ``globus-compute-endpoint`` executable to create endpoints, configure
them, and register them with the hosted Compute services. During the registration
process, the Compute web service assigns the endpoint a UUID identifier that users can
use to submit to that endpoint. Generally any function can be run on any endpoint that
a user has access to. :doc:`For security reasons </endpoints/security_posture>`
endpoints are only accessible to their owners by default, but they can easily be opened
up as needed.


Defining and Registering Functions
==================================

Because Compute functions are standard Python functions, they are frequently defined,
registered, and invoked all in the same Python context. They can also be defined and
registered separately, similar to Lambda.

Consider a simple AWS Lambda function that computes statistics on an array:

.. code-block:: python
  :caption: ``lambda.py``: simple Lambda function definition

  import numpy as np

  def lambda_handler(event, context):
    array = np.array(event.get("array", []))
    mean = np.mean(array)
    std = np.std(array)
    return {
      "statusCode": 200,
      "body": {
        "mean": mean,
        "std": std
      }
    }

.. code-block:: shell
  :caption: Registering ``lambda.py`` to Lambda

  zip function.zip lambda.py

  aws lambda create-function \
    --function-name arrayStats \
    --runtime python3.11 \
    --handler lambda.lambda_handler \
    --zip-file fileb://function.zip \
    --role <some IAM role with lambda execution permissions> \
    --layers <some layer with numpy>

.. note::
  This example assumes there is a pre-configured AWS Lambda layer that includes ``numpy``.

Compare with its Compute analog:

.. code-block:: python
  :caption: Defining and registering in Compute with the :doc:`Executor <../sdk/executor_user_guide>`
  :emphasize-lines: 5, 13

  from globus_compute_sdk import Executor

  # define
  def array_stats(array):
    import numpy as np

    array = np.array(array)
    mean = np.mean(array)
    std = np.std(array)
    return { "mean": mean, "std": std }

  # register
  function_id = Executor().register_function(array_stats)
  # function_id is a UUID, eg eca16448-bfe2-445a-900c-cd46b1426522

.. important::
  In order for this to run without errors, ``numpy`` must be present on the executing
  side. See :ref:`compute_for_lambda_dependency_management` for details.

Unlike Lambda handler functions, which must follow a specific calling convention,
Compute functions can have arbitrary signatures (ie ``def function(what, *ever,
**args)``) and return arbitrary objects (ie ``return anything``). And while Lambda
functions are registered by shipping entire modules to AWS, Compute functions are
registered by shipping standalone functions to the Compute web service. Generally this
is achieved by serializing the in-memory representation of the function, but there are
:doc:`various alternative strategies </reference/serialization_strategies>` such as
packaging the function's source code.

One side-effect of Compute's approach, and a notable difference from Lambda, is that
any code written outside of the function body is *not* executed as a matter of course.
This is why, in the Compute example, ``import numpy as np`` is inside the function body
instead of at the top of the file.

While this style covers the vast majority of use cases, Compute also supports a "module
with a handler function" approach like Lambda:

.. code-block:: python
  :caption: ``module.py``

  import numpy as np

  def array_stats(array):
    array = np.array(array)
    mean = np.mean(array)
    std = np.std(array)
    return { "mean": mean, "std": std }

.. code-block:: python
  :caption: Using ``module.py``

  from pathlib import Path
  from globus_compute_sdk import Executor

  other_module_code = Path("module.py").read_text()

  function_id = Executor().register_source_code(
    other_module_code, function_name="array_stats"
  )
  # function_id is a UUID, eg a8cfa586-d8ff-489f-bf77-e62f3d0ffa1a


Calling Functions
=================

While Compute provides a `public REST API <https://compute.api.globus.org/redoc>`_ that
can be used to call functions, Compute endpoints generally expect arguments to be
serialized a certain Pythonic way that the Compute SDK handles automatically.

Continuing the example from the previous sections, here's what a function call might
look like in Lambda:

.. code-block:: python
  :caption: Invoking Lambda function
  :emphasize-lines: 9-12

  import boto3
  import json

  client = boto3.client('lambda')
  payload = {
    "array": [1, 2, 3, 4, 5]
  }

  response = client.invoke(
    FunctionName="arrayStats",
    Payload=json.dumps(payload),
  )
  result = json.loads(response['Payload'].read())
  print(result)

And this is what a function call might look like in Compute:

.. code-block:: python
  :caption: Submitting to Compute
  :emphasize-lines: 9-11

  from globus_compute_sdk import Executor

  endpoint_id = "<some endpoint UUID>"
  array_stats_function_id = "<the function ID from the previous example>"

  array = [1, 2, 3, 4, 5]

  with Executor(endpoint_id) as gcx:
    future = gcx.submit_to_registered_function(
      array_stats_function_id, array
    )
    print(future.result())

This example may look synchronous, but the Compute SDK has constructed a ``future``
that resolves asynchronously with the result of the computation. Notice also the use of
the context manager (``with``) |nbsp| --- |nbsp| an :class:`~globus_compute_sdk.Executor`
does a lot of work in the background to track tasks and futures, so it's good practice
to encapsulate its usage in a ``with``-statement to clean it up even if other
components break unexpectedly.

.. note::
  See the :doc:`Executor User Guide </sdk/executor_user_guide>` for more details on
  how Compute futures and the :class:`~globus_compute_sdk.Executor` work together.

So far these examples have shown how to register and call functions as separate steps,
like in Lambda. Compute also supports a mode where function registration and submission
happen transparently in the same call:

.. code-block:: python
  :caption: ``Executor.submit``
  :emphasize-lines: 13

  from globus_compute_sdk import Executor

  def string_stats(string):
    return {
      "length": len(string),
      "uppercase": string.upper(),
      "lowercase": string.lower()
    }

  string = "Hello, Compute!"

  with Executor(endpoint_id) as gcx:
    future = gcx.submit(string_stats, string)
    print(future.result())

While there is no direct analog to Lambda's event-source mappings, Compute functions
can be called in response to events using `Globus Flows
<https://docs.globus.org/api/flows/getting-started/>`_. See also the documentation on
the :doc:`Compute Action Provider for Flows </actionprovider>`.


.. _compute_for_lambda_dependency_management:

Dependency Management
=====================

Since Compute functions contain no dependency information beside ``import`` statements,
endpoint administrators must take care to ensure any needed dependencies are present on
the endpoint side. For static workflows a manual approach is sufficient |nbsp| --- |nbsp|
ie, administrators install packages as needed to the Python environment that the
endpoint is running in |nbsp| --- |nbsp| but there are mechanisms that allow for more
dynamic situations.

These lean on Compute's :ref:`configuration template <user-config-template-yaml-j2>`
capabilities, which don't have an analog in Lambda. In short, Compute endpoints spawn
child processes that actually handle the execution of the task, and the configuration
of these child processes is determined by a combination of the endpoint's configuration
template and the details of the task at hand.

One approach allows users to :doc:`specify a container
<../tutorials/dynamic_containers>` with their task. With the following configuration
options included:

.. code-block:: yaml+jinja
   :caption: ``user_config_template.yaml.j2`` with dynamic containers
   :emphasize-lines: 4-6

   display_name: My Containerized Endpoint
   engine:
     type: GlobusComputeEngine
     container_type: {{ container_type }}
     container_uri: {{ container_uri }}
     container_cmd_options: {{ container_cmd_options|default() }}
     provider:
       init_blocks: 1
       max_blocks: 1
       min_blocks: 0
       type: LocalProvider

Users can then leverage the ``user_endpoint_config`` feature of the executor to choose
a container:

.. code-block:: python
  :caption: Submitting with a container

   from globus_compute_sdk import Executor

   ep_id = "..."
   config = {
      "container_type": "docker", # or podman, apptainer...
      "container_uri": "<my docker container>",
   }

   with Executor(
      endpoint_id=ep_id,
      user_endpoint_config=config,
   ) as gcx:
      gcx.submit(...)

    # or:

    with Executor(endpoint_id=ep_id) as gcx:
      gcx.user_endpoint_config = config
      gcx.submit(...)

.. important::
  The container being used must have the ``globus-compute-endpoint`` package installed.

For static workflows where users always invoke the same function, administrators can
simply install dependencies directly into the endpoint's Python environment. In these
cases administrators might want to ensure that *only* this known function is called,
which can be achieved with a :ref:`function allow list <function-allowlist>`:

.. code-block:: yaml
  :caption: :ref:`config.yaml <endpoint-manager-config>` with a function allow list

  allowed_functions:
    - eca16448-bfe2-445a-900c-cd46b1426522

.. |nbsp| unicode:: 0xA0
   :trim:
