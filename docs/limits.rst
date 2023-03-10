Limits
------

This section describes the limits placed by Globus Compute to ensure high availability and reliability to
all users sharing this hosted service. Routing high volumes of tasks and results between users
and endpoints is resource-intensive from a computation, memory, and network transport perspective.
To guarantee truly fire and forget tasks and fairness for all users Globus Compute applies the limitations
described below uniformly to all users.

There are three mechanisms by which Globus Compute controls the flow of tasks through the system:

* Task-Rate limiting: Controls the rate at which you can submit new tasks to Globus Compute
* Data limits: Restricts the data volume for task inputs and outputs
* Task TTL: Sets the maximum task lifetime after which stale tasks are abandoned


Task-Rate Limiting
^^^^^^^^^^^^^^^^^^

Globus Compute uses Task-Rate limiting to ensure the quality of service to all users on the Globus Compute platform.
These limits ensure that users might not accidentally overload the service with requests and overload
the system for everyone else. Currently, Globus Compute limits the number of requests submitted from a client to
**20 requests per 10 seconds**.

If this submission rate is exceeded, the SDK will raise a ``MaxRequestsExceeded`` exception.

The recommended mechanism to submit a large number of tasks quickly while avoiding the rate-limiting is
to use :ref:`Batching`. Please note that while many tasks could be submitted together as a single batch,
each batch request is still subject to the limits on the data payload allowed per request described below.


Data Limits
^^^^^^^^^^^

Data limits are used to ensure that the inputs and results associated with functions can be handled
by Globus Compute both at the user level as well as in the aggregate. Without these limits functions that either
consume or produce large volumes of data could overload the system.

The current data limit is set to **5MB** on task submissions, which applies to both individual functions
as well as batch submissions. The SDK will raise a ``MaxRequestSizeExceeded`` exception when this limit
is exceeded. This limit currently is much larger than the limit on result size since this limit applies
to batch job requests.

Similarly, there is a data limit of **512KB** on result size on a per-function basis. If the serialized
result from a task exceeds this limit, a ``MaxResultSizeExceeded`` exception is reported by the SDK when
the task request is queried. In this case, the result is unrecoverable.

If your function results are larger than supported limits, the recommended approach is to write the
results to disk and have a data transfer solution like `Globus <https://www.globus.org/data-transfer>`_
move the data independently.


Task TTL
^^^^^^^^

Task time to live (TTL) is a mechanism to identify tasks that are possibly abandoned. In a computing
environment where tasks may take several days to be allocated compute resources (eg, by a cluster) or
simply take days to run to completion, distinguishing between tasks that are blocked vs abandoned is
difficult. On the other hand, tasks once launched can be lost on the client-side due to a crash or a
programmatic error. To avoid such cases, Globus Compute considers a task to be abandoned when there has been
no activity on a task for more than **2 weeks**. However, when a result is available but has not been
fetched by the client within **30 minutes** of completion, the result is marked as abandoned.





