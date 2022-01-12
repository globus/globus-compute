Reference
#########


funcX SDK
=========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    funcx
    funcx.sdk.client.FuncXClient
    funcx.sdk.error_handling_client.FuncXErrorHandlingClient
    funcx.sdk.utils.batch.Batch
    funcx.serialize.base
    funcx.serialize.concretes

funcx-endpoint
==============

.. autosummary::
    :toctree: stubs
    :nosignatures:

    funcx_endpoint.endpoint.endpoint
    funcx_endpoint.endpoint.endpoint_manager.EndpointManager
    funcx_endpoint.endpoint.interchange.EndpointInterchange
    funcx_endpoint.endpoint.taskqueue.TaskQueue
    funcx_endpoint.strategies.base.BaseStrategy
    funcx_endpoint.strategies.simple.SimpleStrategy
    funcx_endpoint.strategies.kube_simple.KubeSimpleStrategy


Exceptions
==========

.. autosummary::
    :toctree: stubs
    :nosignatures:

    funcx.utils.errors.TaskPending
    funcx.utils.errors.RegistrationError
    funcx.utils.errors.FuncXUnreachable
    funcx.utils.errors.MalformedResponse
    funcx.utils.errors.FailureResponse
    funcx.utils.errors.VersionMismatch
    funcx.utils.errors.SerializationError
    funcx.utils.errors.UserCancelledException
    funcx.utils.errors.InvalidScopeException
    funcx.utils.errors.MaxResultSizeExceeded
    funcx.utils.errors.HTTPError
    funcx.sdk.utils.throttling.MaxRequestsExceeded
    funcx.sdk.utils.throttling.MaxRequestSizeExceeded
