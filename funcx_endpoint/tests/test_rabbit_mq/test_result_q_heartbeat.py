import time
from urllib import parse

import pika
import pytest


@pytest.mark.parametrize("use_heartbeat", [None, 1])
def test_no_heartbeat(
    start_result_q_publisher, rabbitmq_conn_url, create_result_queue_info, use_heartbeat
):
    """Confirm that result_q_publisher does not disconnect when delay
    between messages exceed heartbeat period
    """
    url_parts = list(parse.urlparse(rabbitmq_conn_url))
    query_parts = parse.parse_qs(url_parts[4])
    query_parts.pop("heartbeat", None)
    if use_heartbeat:
        query_parts["heartbeat"] = use_heartbeat
    url_parts[4] = parse.urlencode(query_parts)
    conn_url = parse.urlunparse(url_parts)

    q_info = create_result_queue_info(connection_url=conn_url)

    result_pub = start_result_q_publisher(override_params=q_info)

    # simply ensure no crash between two message sends
    result_pub.publish(b"Hello test_no_heartbeat: 1")
    time.sleep(5)
    result_pub.publish(b"Hello test_no_heartbeat: 2")


def test_fail_after_manual_close(start_result_q_publisher):
    """Confirm that result_q_publisher raises an error following a manual conn close"""
    result_pub = start_result_q_publisher()

    result_pub.publish(b"Hello")
    result_pub.close()
    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        result_pub.publish(b"Hello test_fail_after_manual_close")


def test_reconnect_after_disconnect(start_result_q_publisher):
    result_pub = start_result_q_publisher()
    result_pub.publish(b"Hello test_reconnect_after_disconnect: 1")
    result_pub.close()
    result_pub.connect()
    result_pub.publish(b"Hello test_reconnect_after_disconnect: 2")
