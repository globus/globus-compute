import json
import logging
import os
import re
import typing as t

import funcx_endpoint
from funcx import FuncXClient

log = logging.getLogger(__name__)


def register_endpoint(
    funcx_client: FuncXClient,
    endpoint_uuid: str,
    endpoint_dir: str,
    endpoint_name: str,
    multi_tenant: bool = False,
) -> t.Tuple[t.Dict, t.Dict]:
    """Register the endpoint and return the registration info. This function needs
    to be isolated so that the function can both be called from the endpoint start
    process and the daemon process that it spawns.

    :param funcx_client: FuncXClient The auth'd client to communicate with
     the funcX service
    :param endpoint_uuid: str The uuid to register the endpoint with
    :param endpoint_dir: str The endpoint directory path to store data in
    :param endpoint_name: str The name of the endpoint
    :param multi_tenant: bool Whether the endpoint should be multi-tenant

    :return: Registration params
    :rtype: tuple[dict, dict]

        Eg return value:
        ({'pika_conn_params': <PIKA_URLParameters>,
          'exchange_name': 'tasks',
          'exchange_type': 'direct',
          'task_url': 'amqp://funcx:rabbitmq@192.168.49.2:5672/'},
         {'pika_conn_params': <PIKA_URLParameters>,
          'exchange_name': 'results',
          'exchange_type': 'topic',
          'task_url': 'amqp://funcx:rabbitmq@192.168.49.2:5672/'}
         )
    """
    log.debug("Attempting registration")
    log.debug(f"Trying with eid : {endpoint_uuid}")

    reg_info = funcx_client.register_endpoint(
        endpoint_name,
        endpoint_uuid,
        endpoint_version=funcx_endpoint.__version__,
        multi_tenant=multi_tenant,
    )
    if reg_info.get("endpoint_id") != endpoint_uuid:
        raise ValueError("Unexpected response from server: mismatched endpoint id.")

    # sanitize passwords in logs
    log_reg_info = re.subn(r"://.*?@", r"://***:***@", repr(reg_info))
    log.info(f"Registration returned: {log_reg_info}")

    # NOTE: While all registration info is saved to endpoint.json, only the
    # endpoint UUID is reused from this file.
    with open(os.path.join(endpoint_dir, "endpoint.json"), "w+") as fp:
        endpoint_info = {
            "endpoint_name": endpoint_name,
            # This is named endpoint_id for backward compatibility when
            # funcx-endpoint list is called
            "endpoint_id": endpoint_uuid,
        }
        json.dump(endpoint_info, fp)
        log.debug(
            "Registration info written to {}".format(
                os.path.join(endpoint_dir, "endpoint.json")
            )
        )

    return reg_info["task_queue_info"], reg_info["result_queue_info"]
