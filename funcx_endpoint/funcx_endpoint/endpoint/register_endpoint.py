import json
import logging
import os

import pika

import funcx_endpoint

log = logging.getLogger(__name__)


def mock_register_endpoint(
    endpoint_name: str, endpoint_uuid: str, endpoint_version: str = None
) -> str:
    """This is only a mock function that currently returns a URL to a
    default local RabbitMQ service

    Parameters
    ----------
    endpoint_name: str
        User facing name of the endpoint
    endpoint_uuid: str
        Endpoint UUID
    endpoint_version: str
        funcx-endpoint version number. Optional

    Returns
    -------
    pika.URLParameters to the RabbitMQ pipes
    """
    log.info(
        f"Registering endpoint:{endpoint_name}:{endpoint_uuid}"
        " of version:{endpoint_version}"
    )
    return "amqp://guest:guest@localhost:5672/"


def register_endpoint(funcx_client, endpoint_uuid, endpoint_dir, endpoint_name):
    """Register the endpoint and return the registration info. This function needs
    to be isolated so that the function can both be called from the endpoint start
    process as well as the daemon process that it spawns.

    Parameters
    ----------
    funcx_client : FuncXClient
        The auth'd client to communicate with the funcX service

    endpoint_uuid : str
        The uuid to register the endpoint with

    endpoint_dir : str
        The endpoint directory path to store data in

    endpoint_name : str
        The name of the endpoint

    """
    log.debug("Attempting registration")
    log.debug(f"Trying with eid : {endpoint_uuid}")

    pika_url = mock_register_endpoint(
        endpoint_name, endpoint_uuid, endpoint_version=funcx_endpoint.__version__
    )

    # NOTE: While all registration info is saved to endpoint.json, only the
    # endpoint UUID is reused from this file. The latest forwarder URI is used
    # every time we fetch registration info and register
    with open(os.path.join(endpoint_dir, "endpoint.json"), "w+") as fp:
        endpoint_info = {
            "endpoint_name": endpoint_name,
            # This is named endpoint_id for backward compatibility when
            # funcx-endpoint list is called
            "pika_conn_info": pika_url,
            "endpoint_id": endpoint_uuid,
        }
        json.dump(endpoint_info, fp)
        log.debug(
            "Registration info written to {}".format(
                os.path.join(endpoint_dir, "endpoint.json")
            )
        )

    reg_info = pika.URLParameters(pika_url)
    return reg_info
