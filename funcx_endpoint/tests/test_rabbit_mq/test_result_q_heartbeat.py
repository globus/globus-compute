import copy
import time

import pika
import pytest


@pytest.mark.parametrize("use_heartbeat", [None, 1])
def test_no_heartbeat(start_result_q_publisher, pika_conn_params, use_heartbeat):
    """Confirm that result_q_publisher does not disconnect when delay
    between messages exceed heartbeat period
    """
    conn_params = copy.deepcopy(pika_conn_params)
    conn_params.heartbeat = use_heartbeat
    conn_params.blocked_connection_timeout = 2

    result_pub = start_result_q_publisher(override_params=conn_params)

    # simply ensure no crash between two message sends
    result_pub.publish(b"Hello")
    time.sleep(5)
    result_pub.publish(b"Hello")


def test_fail_after_manual_close(start_result_q_publisher):
    """Confirm that result_q_publisher raises an error following a manual conn close"""
    result_pub = start_result_q_publisher()

    result_pub.publish(b"Hello")
    result_pub.close()
    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        result_pub.publish(b"Hello")


def test_reconnect_after_disconnect(start_result_q_publisher):
    result_pub = start_result_q_publisher()
    result_pub.publish(b"Hello")
    result_pub.close()
    result_pub.connect()
    result_pub.publish(b"Hello")
